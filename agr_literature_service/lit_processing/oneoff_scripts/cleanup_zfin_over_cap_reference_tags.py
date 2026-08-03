"""
cleanup_zfin_over_cap_reference_tags.py
=======================================

Remove ZFIN gene/allele entity topic entity tags (TETs) for papers that carry
more than MAX_ASSOCIATIONS_PER_PAPER associations from the shared ZFIN
reference-curation source (SCRUM-6363).

The weekly loaders (``load_zfin_gene_reference_tags.py`` and
``load_zfin_allele_reference_tags.py``) now skip such papers at load time, but
they are add-only and never retract what a prior run created. This one-off
cleanup deletes the already-loaded over-cap tags so the reference documents stop
overflowing the Elasticsearch nested_objects.limit and the search reindex can
complete.

The cap is applied per entity type, exactly as the loaders apply it: a paper is
over cap for genes when it has more than MAX_ASSOCIATIONS_PER_PAPER pure gene
tags (topic == entity_type == gene) from this source, and independently for
alleles. When a paper is over cap for a type, ALL of that type's tags for the
paper are removed. Deleting a tag cascades to its validation rows at the database
level; each affected reference is revalidated immediately after its delete
commits, so an interrupted run leaves already-processed references consistent and
a rerun (which recomputes over-cap membership from the live table) resumes the
rest.

Caveat: dataset_entry.supporting_topic_entity_tag_id is ON DELETE SET NULL, so
deleting a tag cited by an ML dataset entry nulls that support. The dry run
reports how many dataset_entry rows would be affected so the impact is visible
before anyone passes ``--delete``.

Safe by default: without ``--delete`` the script only reports what it would
remove (dry run). Pass ``--delete`` to actually delete.
"""
import argparse
import logging
from collections import defaultdict
from os import path
from typing import Dict, List, Tuple

from dotenv import load_dotenv
from sqlalchemy import func, select

from agr_literature_service.api.crud.topic_entity_tag_crud import revalidate_all_tags
from agr_literature_service.api.models import TopicEntityTagModel
from agr_literature_service.api.models.dataset_model import DatasetEntryModel
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.data_ingest.for_migration.load_zfin_allele_reference_tags import (
    ALLELE_ATP,
)
from agr_literature_service.lit_processing.data_ingest.for_migration.load_zfin_gene_reference_tags import (
    GENE_ATP,
)
from agr_literature_service.lit_processing.data_ingest.for_migration.zfin_reference_tag_utils import (
    MAX_ASSOCIATIONS_PER_PAPER,
    deliver_report,
    find_zfin_source_id,
)
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import (
    create_postgres_session,
)

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()

# (report label, entity ATP) pairs; the loaders cap each type independently.
ENTITY_TYPES: List[Tuple[str, str]] = [("gene", GENE_ATP), ("allele", ALLELE_ATP)]


def find_over_cap_references(db, source_id: int, entity_atp: str) -> List[Tuple[int, int]]:
    """Return [(reference_id, tag_count)] for references whose pure entity tag
    count (topic == entity_type == entity_atp) for this source exceeds
    MAX_ASSOCIATIONS_PER_PAPER, largest first."""
    rows = (
        db.query(TopicEntityTagModel.reference_id, func.count().label("n"))
        .filter(
            TopicEntityTagModel.topic_entity_tag_source_id == source_id,
            TopicEntityTagModel.topic == entity_atp,
            TopicEntityTagModel.entity_type == entity_atp,
        )
        .group_by(TopicEntityTagModel.reference_id)
        .having(func.count() > MAX_ASSOCIATIONS_PER_PAPER)
        .order_by(func.count().desc())
        .all()
    )
    return [(row[0], row[1]) for row in rows]


def delete_reference_tags(db, source_id: int, entity_atp: str, reference_id: int) -> int:
    """Delete every pure entity tag of this type for this reference/source via the
    ORM (so versioning/audit fire and validation rows cascade). Returns the number
    of tags deleted."""
    tags = (
        db.query(TopicEntityTagModel)
        .filter(
            TopicEntityTagModel.topic_entity_tag_source_id == source_id,
            TopicEntityTagModel.topic == entity_atp,
            TopicEntityTagModel.entity_type == entity_atp,
            TopicEntityTagModel.reference_id == reference_id,
        )
        .all()
    )
    for tag in tags:
        db.delete(tag)
    return len(tags)


def count_affected_dataset_entries(db, source_id: int,
                                   work_by_reference: Dict[int, List[Tuple[str, str]]]) -> int:
    """Count ML dataset_entry rows that cite a to-be-deleted tag as their support.

    dataset_entry.supporting_topic_entity_tag_id is ON DELETE SET NULL, so deleting
    such a tag nulls the entry's support (a state the application otherwise rejects).
    Surfaced in the dry run so the impact is visible before ``--delete``."""
    if not work_by_reference:
        return 0
    refs_by_atp: Dict[str, List[int]] = defaultdict(list)
    for reference_id, type_items in work_by_reference.items():
        for _label, entity_atp in type_items:
            refs_by_atp[entity_atp].append(reference_id)
    total = 0
    for entity_atp, reference_ids in refs_by_atp.items():
        tag_ids = select(TopicEntityTagModel.topic_entity_tag_id).where(
            TopicEntityTagModel.topic_entity_tag_source_id == source_id,
            TopicEntityTagModel.topic == entity_atp,
            TopicEntityTagModel.entity_type == entity_atp,
            TopicEntityTagModel.reference_id.in_(reference_ids),
        )
        total += db.query(DatasetEntryModel).filter(
            DatasetEntryModel.supporting_topic_entity_tag_id.in_(tag_ids)
        ).count()
    return total


def cleanup_zfin_over_cap_reference_tags(delete: bool = False,
                                         revalidate: bool = True) -> Dict:
    """Find (and, with ``delete``, remove) ZFIN over-cap gene/allele entity tags.

    Returns a dict of counts describing the run (also used to build the report).
    """
    db = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, script_name)

    counts: Dict = {"deleted": delete}

    try:
        source_id = find_zfin_source_id(db)
        if source_id is None:
            logger.info("ZFIN reference-curation TET source not found; nothing to clean up.")
            counts["source_missing"] = True
            return counts

        # Gather over-cap work grouped by reference so a paper over cap for both
        # entity types is deleted and revalidated as a single unit.
        work_by_reference: Dict[int, List[Tuple[str, str]]] = {}
        for label, entity_atp in ENTITY_TYPES:
            over_cap = find_over_cap_references(db, source_id, entity_atp)
            counts[f"{label}_papers"] = len(over_cap)
            counts[f"{label}_tags"] = sum(n for _ref, n in over_cap)
            for reference_id, _n in over_cap:
                work_by_reference.setdefault(reference_id, []).append((label, entity_atp))
            logger.info(
                "%s: %d papers over the %d cap (%d tags)%s",
                label, len(over_cap), MAX_ASSOCIATIONS_PER_PAPER,
                counts[f"{label}_tags"], "" if delete else " [dry-run]",
            )

        counts["affected_references"] = len(work_by_reference)
        counts["dataset_entries_affected"] = count_affected_dataset_entries(
            db, source_id, work_by_reference)
        if counts["dataset_entries_affected"]:
            logger.warning(
                "%d ML dataset_entry rows cite tags slated for deletion; their "
                "supporting_topic_entity_tag_id would be set NULL",
                counts["dataset_entries_affected"],
            )

        if not delete:
            return counts

        # Delete both entity types for a reference in one transaction, then
        # revalidate it immediately so an interrupted run stays self-consistent.
        for reference_id, type_items in work_by_reference.items():
            for label, entity_atp in type_items:
                deleted = delete_reference_tags(db, source_id, entity_atp, reference_id)
                logger.info("  deleted %d %s tags for reference_id=%d",
                            deleted, label, reference_id)
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
    message = "<b>ZFIN Over-Cap Reference-Tag Cleanup Report</b><p>"
    if counts.get("source_missing"):
        message += "<ul><li>ZFIN reference-curation TET source not found; nothing to clean up.</ul>"
        return message
    mode = "DELETED" if counts.get("deleted") else "DRY-RUN (no changes made)"
    message += "<ul>"
    message += f"<li>Mode: {mode}"
    message += f"<li>Cap: {MAX_ASSOCIATIONS_PER_PAPER} associations per paper"
    for label in ("gene", "allele"):
        message += (f"<li>{label.capitalize()}: {counts.get(f'{label}_papers', 0)} papers over cap, "
                    f"{counts.get(f'{label}_tags', 0)} tags")
    message += f"<li>References affected: {counts.get('affected_references', 0)}"
    dataset_affected = counts.get("dataset_entries_affected", 0)
    if dataset_affected:
        message += (f"<li><b>WARNING: {dataset_affected} ML dataset_entry rows cite tags slated "
                    f"for deletion; their supporting_topic_entity_tag_id would be set NULL</b>")
    message += "</ul>"
    return message


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Remove ZFIN gene/allele entity tags for papers exceeding the "
                    "per-paper association cap"
    )
    parser.add_argument(
        "-d", "--delete",
        action="store_true",
        help="Actually delete the over-cap tags (default: dry-run report only)",
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

    run_counts = cleanup_zfin_over_cap_reference_tags(
        delete=args.delete, revalidate=not args.no_revalidate)
    report = compose_report_message(run_counts)
    deliver_report("ZFIN Over-Cap Reference-Tag Cleanup Report", report, args.no_email)
