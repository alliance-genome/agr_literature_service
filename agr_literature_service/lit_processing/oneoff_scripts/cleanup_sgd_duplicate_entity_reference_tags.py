"""
cleanup_sgd_duplicate_entity_reference_tags.py
==============================================

Remove SGD entity topic entity tags (TETs) from the shared SGD
reference-curation source (SCRUM-6404) that duplicate a tag a curator created
directly in the ABC curation interface (source_method abc_literature_system).

The SGD entity-tag scripts (``load_sgd_entity_reference_tags.py`` and
``update_sgd_entity_reference_tags.py``) now skip an association already
tagged in the ABC (see sgd_reference_tag_utils.load_abc_entity_tags), but
runs before that fix created duplicates: the same entity shows twice on a
reference, once per source (e.g. gene AAD3 tagged by a curator in the ABC and
again by the loader). This one-off deletes the loader's copy; the curator's
ABC tag always wins and is never touched.

A duplicate is either a pure entity tag (topic == entity_type, one of the
four SGD entity ATPs) from the SGD reference-curation source whose reference /
entity_type / species / entity all match an abc_literature_system tag of the
SGD mod -- the same matching the loaders now apply, topic excluded, so a
curator's richer annotation of the same entity (a real topic) also counts --
or a topic-only tag (topic == the root topic ATP, no entity, identified by
its display_tag) whose reference / species / display_tag match an entity-less
abc_literature_system tag, again regardless of that tag's topic (a curator's
entity-less omics tag carries a specific HTP topic but the same display_tag).
This mirrors load_abc_entity_tags, which prevents these duplicates going
forward; the cleanup removes the ones created when a curator tags the same
thing in the ABC after a load.

Deletion goes through the ORM, one tag at a time, so sqlalchemy-continuum
writes a version row for each (the expected volume -- associations curated
twice by hand -- is small, unlike the bulk ZFIN over-cap cleanup). Deletes
are committed per reference and each affected reference is revalidated
immediately after its commit, so an interrupted run leaves already-processed
references consistent and a rerun resumes the rest.

Caveat: dataset_entry.supporting_topic_entity_tag_id is ON DELETE SET NULL, so
deleting a tag cited by an ML dataset entry nulls that support. The dry run
reports how many dataset_entry rows would be affected so the impact is visible
before anyone passes ``--delete``.

Safe by default: without ``--delete`` the script only reports what it would
remove (dry run). Pass ``--delete`` to actually delete. The full duplicate
list is written to LOG_PATH/sgd_duplicate_entity_reference_tags.log either way.
"""
import argparse
import logging
from collections import defaultdict
from os import path
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from sqlalchemy import text

# sgd_reference_tag_utils (which initializes the api models via the crud
# imports) must be imported before api.user, or user.py's import of user_crud
# hits a circular import through the versioning plugins.
from agr_literature_service.lit_processing.data_ingest.for_migration.sgd_reference_tag_utils import (
    ABC_SOURCE_METHOD,
    ENTITY_TYPE_TO_ATP,
    ROOT_TOPIC_ATP,
    SECONDARY_DATA_PROVIDER_ABBR,
    SOURCE_METHOD,
    deliver_report,
    write_id_log,
)
from agr_literature_service.api.crud.topic_entity_tag_crud import revalidate_all_tags
from agr_literature_service.api.models import TopicEntityTagModel
from agr_literature_service.api.models.dataset_model import DatasetEntryModel
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import (
    create_postgres_session,
)

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()

DUPLICATES_LOG = "sgd_duplicate_entity_reference_tags.log"

# Cap the number of duplicates listed inline in the emailed report; the full
# list is always written to the log file.
REPORT_LIST_CAP = 100

ATP_TO_ENTITY_TYPE = {atp: label for label, atp in ENTITY_TYPE_TO_ATP.items()}

# A duplicate: a tag from the SGD reference-curation source that matches an
# abc_literature_system tag of the SGD mod (any topic) on the same key
# load_abc_entity_tags skips on -- for a pure entity tag, on
# reference/entity_type/species/entity; for a topic-only tag (topic == root,
# no entity), on reference/species/display_tag against an entity-less ABC tag.
_FIND_DUPLICATES_SQL = text("""
    SELECT sgd.topic_entity_tag_id, sgd.reference_id, r.curie,
           sgd.entity_type, sgd.entity, sgd.display_tag
    FROM   topic_entity_tag sgd
    JOIN   reference r ON sgd.reference_id = r.reference_id
    WHERE  sgd.topic_entity_tag_source_id = ANY(:sids)
    AND    ((sgd.topic = sgd.entity_type
             AND sgd.entity_type = ANY(:atps)
             AND EXISTS (
                 SELECT 1
                 FROM   topic_entity_tag abc
                 JOIN   topic_entity_tag_source tets
                        ON abc.topic_entity_tag_source_id = tets.topic_entity_tag_source_id
                 JOIN   mod m ON tets.secondary_data_provider_id = m.mod_id
                 WHERE  tets.source_method = :abc_method
                 AND    m.abbreviation = :abbr
                 AND    abc.reference_id = sgd.reference_id
                 AND    abc.entity_type = sgd.entity_type
                 AND    abc.species = sgd.species
                 AND    abc.entity = sgd.entity
             ))
            OR (sgd.topic = :root_topic
                AND sgd.entity IS NULL
                AND sgd.display_tag IS NOT NULL
                AND EXISTS (
                    SELECT 1
                    FROM   topic_entity_tag abc
                    JOIN   topic_entity_tag_source tets
                           ON abc.topic_entity_tag_source_id = tets.topic_entity_tag_source_id
                    JOIN   mod m ON tets.secondary_data_provider_id = m.mod_id
                    WHERE  tets.source_method = :abc_method
                    AND    m.abbreviation = :abbr
                    AND    abc.reference_id = sgd.reference_id
                    AND    abc.entity IS NULL
                    AND    abc.species = sgd.species
                    AND    abc.display_tag = sgd.display_tag
                )))
    ORDER  BY sgd.reference_id, sgd.entity_type, sgd.entity, sgd.display_tag
""")

# What find_duplicate_tags returns per duplicate: (topic_entity_tag_id,
# reference_id, reference_curie, entity_type_atp, entity, display_tag) --
# entity_type_atp and entity are None for a topic-only duplicate, which is
# identified by its display_tag instead.
DuplicateRow = Tuple[int, int, str, Optional[str], Optional[str], Optional[str]]


def find_sgd_source_ids(db) -> List[int]:
    """Return the topic_entity_tag_source ids of the SGD reference-curation
    source (source_method = sgd_reference_curation for the SGD mod). Expected
    to be a single row (see sgd_reference_tag_utils.get_or_create_source), but
    read as a list so a duplicate source row cannot hide tags from the
    cleanup. Empty if the loaders have never run."""
    rows = db.execute(text(
        "SELECT tets.topic_entity_tag_source_id "
        "FROM   topic_entity_tag_source tets "
        "JOIN   mod m ON tets.secondary_data_provider_id = m.mod_id "
        "WHERE  tets.source_method = :method "
        "AND    m.abbreviation = :abbr"
    ), {"method": SOURCE_METHOD, "abbr": SECONDARY_DATA_PROVIDER_ABBR}).fetchall()
    return [row[0] for row in rows]


def find_duplicate_tags(db, source_ids: List[int]) -> List[DuplicateRow]:
    """Return the SGD reference-curation tags duplicating an ABC-curated tag,
    as (topic_entity_tag_id, reference_id, reference_curie, entity_type_atp,
    entity, display_tag), ordered by reference. entity_type_atp/entity are
    None for topic-only duplicates (matched on display_tag)."""
    rows = db.execute(_FIND_DUPLICATES_SQL, {
        "sids": source_ids,
        "atps": list(ENTITY_TYPE_TO_ATP.values()),
        "root_topic": ROOT_TOPIC_ATP,
        "abc_method": ABC_SOURCE_METHOD,
        "abbr": SECONDARY_DATA_PROVIDER_ABBR,
    }).fetchall()
    return [(row[0], row[1], row[2], row[3], row[4], row[5]) for row in rows]


def _duplicate_label(entity_type_atp: Optional[str], entity: Optional[str],
                     display_tag: Optional[str]) -> Tuple[str, str]:
    """(type label, entity label) for one duplicate, for the by-type counts and
    the log/report lines: a pure entity duplicate labels as its entity type and
    entity curie, a topic-only one as "topic-only" and its display_tag."""
    if entity is None:
        return "topic-only", f"display:{display_tag}"
    return ATP_TO_ENTITY_TYPE.get(entity_type_atp or "", entity_type_atp or ""), entity


def count_affected_dataset_entries(db, tag_ids: List[int]) -> int:
    """Count ML dataset_entry rows that cite a to-be-deleted tag as their support.

    dataset_entry.supporting_topic_entity_tag_id is ON DELETE SET NULL, so deleting
    such a tag nulls the entry's support (a state the application otherwise rejects).
    Surfaced in the dry run so the impact is visible before ``--delete``."""
    if not tag_ids:
        return 0
    return db.query(DatasetEntryModel).filter(
        DatasetEntryModel.supporting_topic_entity_tag_id.in_(tag_ids)
    ).count()


def cleanup_sgd_duplicate_entity_reference_tags(delete: bool = False,
                                                revalidate: bool = True) -> Dict:
    """Find (and, with ``delete``, remove) SGD reference-curation entity tags
    that duplicate an ABC-curated tag.

    Returns a dict of counts describing the run (also used to build the report).
    """
    db = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, script_name)

    counts: Dict = {"deleted": delete}

    try:
        source_ids = find_sgd_source_ids(db)
        if not source_ids:
            logger.info("SGD reference-curation TET source not found; nothing to clean up.")
            counts["source_missing"] = True
            return counts

        duplicates = find_duplicate_tags(db, source_ids)
        counts["duplicates"] = len(duplicates)
        by_type: Dict[str, int] = defaultdict(int)
        by_reference: Dict[int, List[DuplicateRow]] = defaultdict(list)
        for dup in duplicates:
            by_type[_duplicate_label(dup[3], dup[4], dup[5])[0]] += 1
            by_reference[dup[1]].append(dup)
        counts["by_type"] = dict(by_type)
        counts["affected_references"] = len(by_reference)
        counts["duplicate_rows"] = duplicates
        logger.info(
            "%d SGD reference-curation tags on %d references duplicate an "
            "ABC-curated tag%s", len(duplicates), len(by_reference),
            "" if delete else " [dry-run]",
        )

        counts["dataset_entries_affected"] = count_affected_dataset_entries(
            db, [dup[0] for dup in duplicates])
        if counts["dataset_entries_affected"]:
            logger.warning(
                "%d ML dataset_entry rows cite tags slated for deletion; their "
                "supporting_topic_entity_tag_id would be set NULL",
                counts["dataset_entries_affected"],
            )

        write_id_log(DUPLICATES_LOG,
                     f"SGD reference-curation tags duplicating an ABC-curated tag "
                     f"({len(duplicates)}){'' if delete else ' [dry-run]'}",
                     ["{}\t{}\t{}\t{}".format(
                         tag_id, ref_curie,
                         *_duplicate_label(entity_type_atp, entity, display_tag))
                      for tag_id, _ref_id, ref_curie, entity_type_atp, entity, display_tag
                      in duplicates])

        if not delete:
            return counts

        # Delete a reference's duplicates in one transaction, then revalidate
        # it immediately so an interrupted run stays self-consistent.
        for reference_id, dups in by_reference.items():
            for tag_id, _ref_id, ref_curie, entity_type_atp, entity, display_tag in dups:
                tag = db.query(TopicEntityTagModel).filter_by(
                    topic_entity_tag_id=tag_id).one_or_none()
                if tag is None:  # already gone (e.g. deleted by a curator mid-run)
                    continue
                db.delete(tag)
                type_label, entity_label = _duplicate_label(entity_type_atp, entity, display_tag)
                logger.info("  deleted tag %d (%s / %s / %s)",
                            tag_id, ref_curie, type_label, entity_label)
            db.commit()
            if revalidate:
                try:
                    revalidate_all_tags(curie_or_reference_id=str(reference_id))
                except Exception as e:  # best-effort: one failure must not abort the rest
                    logger.warning("Revalidation failed for reference_id=%d: %s",
                                   reference_id, e)
        return counts
    finally:
        db.close()


def compose_report_message(counts: Dict) -> str:
    """Compose the HTML email report message from the run counts."""
    message = "<b>SGD Duplicate Entity-Reference Tag Cleanup Report</b><p>"
    if counts.get("source_missing"):
        message += "<ul><li>SGD reference-curation TET source not found; nothing to clean up.</ul>"
        return message
    mode = "DELETED" if counts.get("deleted") else "DRY-RUN (no changes made)"
    message += "<ul>"
    message += f"<li>Mode: {mode}"
    message += (f"<li>SGD reference-curation tags duplicating an ABC-curated tag: "
                f"{counts.get('duplicates', 0)}")
    for label, n in sorted(counts.get("by_type", {}).items()):
        message += f"<li>{label.capitalize()}: {n}"
    message += f"<li>References affected: {counts.get('affected_references', 0)}"
    dataset_affected = counts.get("dataset_entries_affected", 0)
    if dataset_affected:
        message += (f"<li><b>WARNING: {dataset_affected} ML dataset_entry rows cite tags slated "
                    f"for deletion; their supporting_topic_entity_tag_id would be set NULL</b>")
    duplicates = counts.get("duplicate_rows", [])
    if duplicates:
        message += f"<li>Duplicates ({len(duplicates)}):<br>"
        for (_tag_id, _ref_id, ref_curie, entity_type_atp,
             entity, display_tag) in duplicates[:REPORT_LIST_CAP]:
            type_label, entity_label = _duplicate_label(entity_type_atp, entity, display_tag)
            message += f"{ref_curie}\t{type_label}\t{entity_label}<br>"
        if len(duplicates) > REPORT_LIST_CAP:
            message += (f"...and {len(duplicates) - REPORT_LIST_CAP} more; "
                        f"full list in the log file<br>")
    message += "</ul>"
    return message


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Remove SGD reference-curation entity tags that duplicate a tag "
                    "curated directly in the ABC (source_method abc_literature_system)"
    )
    parser.add_argument(
        "-d", "--delete",
        action="store_true",
        help="Actually delete the duplicate tags (default: dry-run report only)",
    )
    parser.add_argument(
        "--no-revalidate",
        action="store_true",
        help="Skip revalidation of affected references after deletion",
    )
    parser.add_argument(
        "-n", "--no-email",
        action="store_true",
        help="Do not email the report (for testing)",
    )
    args = parser.parse_args()

    run_counts = cleanup_sgd_duplicate_entity_reference_tags(
        delete=args.delete, revalidate=not args.no_revalidate)
    report = compose_report_message(run_counts)
    deliver_report("SGD Duplicate Entity-Reference Tag Cleanup Report", report, args.no_email)
