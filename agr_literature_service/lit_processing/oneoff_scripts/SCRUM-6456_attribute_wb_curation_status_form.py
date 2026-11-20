"""SCRUM-6456: attribute the WB curation statuses loaded from the Caltech curation status form to that form.

The SCRUM-6518 migration gave every existing curation_status row one source association, with its MOD's ABC
curator source (abc_literature_system). For WB that is wrong for the rows Caltech loaded from its curation status
form: they were never set in the ABC. Those rows are the ones created by a WBPerson agent (the form loader wrote
the curator's WBPerson id as created_by) with

    curation_status ATP:0000299 (won't curate), any topic, or
    curation_status ATP:0000239 (curation status positive, not in OA) on ATP:0000061 / ATP:0000062.

For each of them the ABC association is moved to WB's curation_status_form source: the association keeps its id
and its reported values, only tag_source_id changes. The curation_status rows themselves are not touched.

Skipped and reported instead of changed:
  - a row that already has a curation_status_form association (nothing to move);
  - a row edited after the load: the row or its ABC association was last updated by someone other than the
    WBPerson who created it. An edit in the ABC updates both together, so their values still agree; the
    updater is what shows it (on prod, 2026-10-08, a curator added tags and notes to three form rows);
  - a row whose ABC association no longer matches the row (status, tag or note): the row was changed without
    the ABC source, so it is left for a curator to check.

Run before the WBPerson agents are remapped to AGRKB persons (SCRUM-6458): the WBPerson label is what identifies
the form rows. The associations are updated through the ORM, so Continuum records the change in
curation_status_source_association_version with this script as the author.

Usage (idempotent; run on each database, dev then prod):
    python3 SCRUM-6456_attribute_wb_curation_status_form.py            # dry run: counts only
    python3 SCRUM-6456_attribute_wb_curation_status_form.py --apply    # move the associations
"""
import argparse
import logging
from collections import Counter
from os import environ, path
from typing import Dict, List, Tuple

from sqlalchemy import and_, or_
from sqlalchemy.orm import Load, Session

from agr_literature_service.api.models import CurationStatusModel, CurationStatusSourceAssociationModel, \
    ModModel, TagSourceModel, UserModel
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ABC_SOURCE_METHOD = "abc_literature_system"
FORM_SOURCE_METHOD = "curation_status_form"
WONT_CURATE = "ATP:0000299"
POSITIVE_NOT_IN_OA = "ATP:0000239"
POSITIVE_NOT_IN_OA_TOPICS = ("ATP:0000061", "ATP:0000062")
FORM_CREATOR_PREFIX = "WBPerson"


def find_source(db: Session, mod_id: int, source_method: str) -> TagSourceModel:
    """The MOD's one source with this method; ids differ per database, so look it up."""
    return db.query(TagSourceModel).filter(
        TagSourceModel.source_method == source_method,
        TagSourceModel.secondary_data_provider_id == mod_id).one()


def find_form_rows(db: Session, mod_id: int, abc_source_id: int
                   ) -> List[Tuple[CurationStatusSourceAssociationModel, CurationStatusModel]]:
    """ABC associations of the curation_status rows the curation status form loaded.

    The deferred audit columns are loaded here: the edit check and the update listener read them, and loading
    them one row at a time costs a query per row."""
    return db.query(CurationStatusSourceAssociationModel, CurationStatusModel).options(
        Load(CurationStatusSourceAssociationModel).undefer("*"),
        Load(CurationStatusModel).undefer("*")
    ).join(
        CurationStatusModel,
        CurationStatusModel.curation_status_id == CurationStatusSourceAssociationModel.curation_status_id
    ).join(
        UserModel, UserModel.user_id == CurationStatusModel.created_by_id
    ).filter(
        CurationStatusSourceAssociationModel.tag_source_id == abc_source_id,
        CurationStatusModel.mod_id == mod_id,
        UserModel.automation_username.like(f"{FORM_CREATOR_PREFIX}%"),
        or_(CurationStatusModel.curation_status == WONT_CURATE,
            and_(CurationStatusModel.curation_status == POSITIVE_NOT_IN_OA,
                 CurationStatusModel.topic.in_(POSITIVE_NOT_IN_OA_TOPICS)))
    ).order_by(CurationStatusModel.curation_status_id).all()


def _edited_after_load(association: CurationStatusSourceAssociationModel, row: CurationStatusModel) -> bool:
    """Someone other than the loader updated the row or its ABC association since the load."""
    return row.updated_by_id != row.created_by_id or association.updated_by_id != row.created_by_id


def _matches_row(association: CurationStatusSourceAssociationModel, row: CurationStatusModel) -> bool:
    return (association.curation_status == row.curation_status
            and association.curation_tag == row.curation_tag
            and association.note == row.note)


def attribute_to_form(db: Session, mod_abbreviation: str = "WB", apply: bool = False,
                      batch_size: int = 1000) -> Dict[str, int]:
    """Move the form rows' ABC associations to the form source. Returns counts per outcome."""
    mod = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).one()
    abc_source = find_source(db, mod.mod_id, ABC_SOURCE_METHOD)
    form_source = find_source(db, mod.mod_id, FORM_SOURCE_METHOD)
    logger.info(f"{mod_abbreviation}: ABC source {abc_source.tag_source_id} -> "
                f"form source {form_source.tag_source_id} ({form_source.description})")

    rows = find_form_rows(db, mod.mod_id, abc_source.tag_source_id)
    already_form = {cs_id for (cs_id,) in db.query(CurationStatusSourceAssociationModel.curation_status_id).filter(
        CurationStatusSourceAssociationModel.tag_source_id == form_source.tag_source_id,
        CurationStatusSourceAssociationModel.curation_status_id.in_(
            [row.curation_status_id for _, row in rows]))} if rows else set()

    counts: Counter = Counter()
    to_move = []
    for association, row in rows:
        if row.curation_status_id in already_form:
            counts["skipped: already has the form source"] += 1
            logger.info(f"  skip curation_status {row.curation_status_id}: already has the form source")
        elif _edited_after_load(association, row):
            counts["skipped: edited after the load"] += 1
            logger.info(f"  skip curation_status {row.curation_status_id}: edited after the load "
                        f"(row updated by {row.updated_by}, ABC association updated by {association.updated_by})")
        elif not _matches_row(association, row):
            counts["skipped: ABC association differs from the row"] += 1
            logger.info(f"  skip curation_status {row.curation_status_id}: ABC association "
                        f"({association.curation_status}, {association.curation_tag}, {association.note!r}) "
                        f"differs from the row ({row.curation_status}, {row.curation_tag}, {row.note!r})")
        else:
            to_move.append(association)
            counts[f"move: {row.curation_status} {row.topic if row.curation_status == POSITIVE_NOT_IN_OA else ''}"
                   .strip()] += 1
    counts["move: total"] = len(to_move)

    for outcome, count in sorted(counts.items()):
        logger.info(f"  {outcome}: {count}")
    if not apply:
        logger.info("dry run: nothing written; pass --apply")
        return dict(counts)

    # Committing a batch would otherwise expire every loaded row, and the next batch would reload them one by one.
    db.expire_on_commit = False
    for start in range(0, len(to_move), batch_size):
        for association in to_move[start:start + batch_size]:
            association.tag_source_id = form_source.tag_source_id
        db.commit()
        logger.info(f"  moved {min(start + batch_size, len(to_move))}/{len(to_move)}")
    return dict(counts)


def main():
    parser = argparse.ArgumentParser(description="SCRUM-6456: attribute WB curation status form rows to the form")
    parser.add_argument("--apply", action="store_true", help="commit the change; without it nothing is written")
    args = parser.parse_args()

    db = create_postgres_session(False)
    if args.apply:
        # creates the script's automation agent, so only when writing
        set_global_user_id(db, path.basename(__file__).replace(".py", ""))
    logger.info(f"database {environ.get('PSQL_HOST', 'localhost')}/{environ.get('PSQL_DATABASE', '')}")
    attribute_to_form(db, apply=args.apply)


if __name__ == "__main__":
    main()
