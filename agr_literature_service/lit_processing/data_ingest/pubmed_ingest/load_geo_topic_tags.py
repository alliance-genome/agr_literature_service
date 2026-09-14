"""Tag references carrying a GEO dataset link with the "high throughput assay"
topic (SCRUM-6338).

The GEO Series cross-references themselves are put on references by
backfill_geo_links.py; this script only adds the topic entity tag, so it makes
no NCBI calls and can be re-run at will.

Scope is one MOD (FB by default): FlyBase asked for the topic, while MGI and
SGD explicitly declined it (see SCRUM-3950), and a tag is visible to the MOD
named by its source's secondary_data_provider. Only references in that MOD's
corpus are tagged.

Two ways to run it:
    load_geo_topic_tags.py                 # rollout backfill: every FB corpus
                                           # reference that has a GEO xref
    load_geo_topic_tags.py --since-days 7  # weekly cron: only GEO xrefs added
                                           # in the last week, so a tag a
                                           # curator deleted is not recreated

There is deliberately no cleanup of tags whose GEO xref has gone away:
backfill_geo_links only ever inserts GEO cross-references, so -- unlike the PDB
pipeline, which diffs against a complete current view from RCSB -- there is no
authoritative "no longer in GEO" set to delete against.
"""
import argparse
import logging
from os import path
from typing import Dict, List, Optional, Tuple

from fastapi import HTTPException
from sqlalchemy import and_
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.topic_entity_tag_crud import create_tag
from agr_literature_service.api.models import (
    CrossReferenceModel,
    ModCorpusAssociationModel,
    ModModel,
    ReferenceModel,
    TopicEntityTagSourceModel,
)
from agr_literature_service.api.schemas.topic_entity_tag_schemas import (
    TopicEntityTagSchemaPost,
)
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.backfill_geo_links import (
    _since_days_threshold,
)
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import (
    create_postgres_session,
)

GEO_CURIE_PREFIX = "GEO"
DEFAULT_MOD_ABBREVIATION = "FB"

HIGH_THROUGHPUT_ASSAY_ATP = "ATP:0000150"
ECO_AUTOMATIC_ASSERTION = "ECO_0006156"
# ATP:0000335 "data novelty": the column is non-null and a GEO link says
# nothing about whether the data is new, so the unspecific parent term is used
# (the PDB pipeline does the same).
DATA_NOVELTY_NOT_NEW = "ATP:0000335"
# ATP:0000325 "experimentally studied data" (SCRUM-5697): a GEO series is data
# the paper generated and studied, not a dataset it merely mentions.
DATA_CONTEXT_EXPERIMENTALLY_STUDIED = "ATP:0000325"
SOURCE_METHOD = "GEO dataset association pipeline"
SOURCE_DATA_PROVIDER = "GEO"
SOURCE_DESCRIPTION = (
    "High throughput data from the GEO database associated with references via "
    "load_geo_topic_tags.py. The GEO accessions themselves live in cross_reference."
)

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_or_create_source(db: Session, mod_abbreviation: str = DEFAULT_MOD_ABBREVIATION,
                         create: bool = True) -> Optional[int]:
    """Return the topic_entity_tag_source.id for the GEO pipeline, creating it if absent.

    ``create=False`` (what --dry-run passes) reports a missing source instead of
    inserting one, so a dry run leaves the database exactly as it found it.
    """
    mod = db.query(ModModel).filter_by(abbreviation=mod_abbreviation).one()
    existing = db.query(TopicEntityTagSourceModel).filter_by(
        source_evidence_assertion=ECO_AUTOMATIC_ASSERTION,
        source_method=SOURCE_METHOD,
        data_provider=SOURCE_DATA_PROVIDER,
        secondary_data_provider_id=mod.mod_id,
    ).one_or_none()
    if existing:
        return existing.topic_entity_tag_source_id
    if not create:
        logger.info("No GEO pipeline TET source for %s yet; a live run would create it",
                    mod_abbreviation)
        return None
    source = TopicEntityTagSourceModel(
        source_evidence_assertion=ECO_AUTOMATIC_ASSERTION,
        source_method=SOURCE_METHOD,
        data_provider=SOURCE_DATA_PROVIDER,
        secondary_data_provider_id=mod.mod_id,
        validation_type=None,
        description=SOURCE_DESCRIPTION,
    )
    db.add(source)
    db.commit()
    db.refresh(source)
    logger.info("Created GEO pipeline TET source id=%s for %s",
                source.topic_entity_tag_source_id, mod_abbreviation)
    return source.topic_entity_tag_source_id


def _references_with_geo_xref(db: Session,
                              mod_abbreviation: str = DEFAULT_MOD_ABBREVIATION,
                              limit: int = 0,
                              since_days: int = 0) -> List[Tuple[int, str]]:
    """Return [(reference_id, reference_curie), ...] for the MOD's corpus
    references that carry at least one GEO cross-reference.

    `since_days`, when > 0, restricts the result to references whose GEO
    cross-reference was created within the last N days -- what the weekly cron
    uses so that already-processed references are left alone.
    """
    q = (db.query(ReferenceModel.reference_id, ReferenceModel.curie)
         .join(CrossReferenceModel,
               CrossReferenceModel.reference_id == ReferenceModel.reference_id)
         .join(ModCorpusAssociationModel,
               ModCorpusAssociationModel.reference_id == ReferenceModel.reference_id)
         .join(ModModel, ModModel.mod_id == ModCorpusAssociationModel.mod_id)
         .filter(and_(CrossReferenceModel.curie_prefix == GEO_CURIE_PREFIX,
                      CrossReferenceModel.is_obsolete.is_(False),
                      ModCorpusAssociationModel.corpus.is_(True),
                      ModModel.abbreviation == mod_abbreviation)))
    threshold = _since_days_threshold(since_days)
    if threshold is not None:
        q = q.filter(CrossReferenceModel.date_created >= threshold)
    # A reference can carry several GEO series; it still gets one topic tag.
    q = q.distinct().order_by(ReferenceModel.reference_id)
    if limit > 0:
        q = q.limit(limit)
    return [(reference_id, curie) for reference_id, curie in q.all()]


def _build_topic_tet_payload(reference_curie: str, source_id: int) -> TopicEntityTagSchemaPost:
    return TopicEntityTagSchemaPost(
        reference_curie=reference_curie,
        topic=HIGH_THROUGHPUT_ASSAY_ATP,
        topic_entity_tag_source_id=source_id,
        data_novelty=DATA_NOVELTY_NOT_NEW,
        data_context=DATA_CONTEXT_EXPERIMENTALLY_STUDIED,
        negated=False,
    )


def _create_topic_tet(db: Session, source_id: Optional[int], reference_curie: str,
                      dry_run: bool, counts: Dict[str, int]) -> None:
    """Add the topic-only tag for one reference.

    ``create_tag`` is idempotent: it raises HTTPException(409) for a true
    duplicate and returns ``(tag_id, was_upsert=True)`` when an existing tag
    absorbed the request -- neither writes a new row.
    """
    if dry_run:
        logger.info("DRY-RUN would create %s tag for %s",
                    HIGH_THROUGHPUT_ASSAY_ATP, reference_curie)
        counts["tet_created"] += 1
        return
    assert source_id is not None  # only a dry run runs without a source
    try:
        _tag_id, was_upsert = create_tag(db, _build_topic_tet_payload(reference_curie, source_id))
    except HTTPException as e:
        if e.status_code == 409:
            counts["tet_skipped_duplicate"] += 1
            return
        db.rollback()
        counts["errors"] += 1
        logger.warning("TET create failed for %s: %s", reference_curie, e.detail)
        return
    except Exception as e:
        # create_tag only rolls back on IntegrityError; any other failure
        # (OperationalError, deadlock, connection blip) would leave the
        # transaction aborted and break every subsequent tag in the run.
        db.rollback()
        counts["errors"] += 1
        logger.warning("TET create failed for %s: %s", reference_curie, e)
        return
    if was_upsert:
        counts["tet_skipped_duplicate"] += 1
    else:
        counts["tet_created"] += 1


def load(mod_abbreviation: str = DEFAULT_MOD_ABBREVIATION,
         limit: int = 0,
         since_days: int = 0,
         dry_run: bool = False,
         db: Optional[Session] = None,
         references: Optional[List[Tuple[int, str]]] = None) -> Dict[str, int]:
    own_session = db is None
    if own_session:
        db = create_postgres_session(False)
    assert db is not None
    if not dry_run:
        # Registers the automation user that stamps created_by -- an INSERT, so
        # a dry run skips it along with every other write.
        set_global_user_id(db, path.basename(__file__).replace(".py", ""))
    counts = {"refs_scanned": 0, "tet_created": 0, "tet_skipped_duplicate": 0, "errors": 0}
    try:
        source_id = get_or_create_source(db, mod_abbreviation, create=not dry_run)
        if references is None:
            references = _references_with_geo_xref(db, mod_abbreviation=mod_abbreviation,
                                                   limit=limit, since_days=since_days)
        counts["refs_scanned"] = len(references)
        scope_msg = f" in the {mod_abbreviation} corpus"
        if since_days > 0:
            scope_msg += f" whose GEO xref was added in the last {since_days} day(s)"
        logger.info("Found %d references with a GEO xref%s", len(references), scope_msg)

        for _reference_id, reference_curie in references:
            _create_topic_tet(db, source_id, reference_curie, dry_run, counts)

        logger.info("GEO topic tag pipeline done: refs_scanned=%d tet_created=%d "
                    "tet_skipped_duplicate=%d errors=%d",
                    counts["refs_scanned"], counts["tet_created"],
                    counts["tet_skipped_duplicate"], counts["errors"])
        return counts
    finally:
        if own_session:
            db.close()


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description='Add the "high throughput assay" topic tag to references with a GEO xref.')
    parser.add_argument("--mod", default=DEFAULT_MOD_ABBREVIATION,
                        help=f"MOD whose corpus is tagged (default {DEFAULT_MOD_ABBREVIATION}); "
                             "also the tag's secondary_data_provider")
    parser.add_argument("--limit", type=int, default=0,
                        help="Cap the number of references processed (0 = no cap)")
    parser.add_argument("--since-days", type=int, default=0,
                        help="Only process references whose GEO xref was added in the last N "
                             "days (0 = no time filter). Weekly cron uses --since-days 7.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Log intended tags but write nothing")
    args = parser.parse_args()
    load(mod_abbreviation=args.mod, limit=args.limit,
         since_days=args.since_days, dry_run=args.dry_run)


if __name__ == "__main__":  # pragma: no cover
    main()
