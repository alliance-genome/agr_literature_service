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
paper are removed. Deleting a tag cascades to its prop and validation rows at the
database level; the affected references are revalidated once at the end so the
validation of any surviving tags stays consistent.

Safe by default: without ``--delete`` the script only reports what it would
remove (dry run). Pass ``--delete`` to actually delete.
"""
import argparse
import logging
from os import path
from typing import Dict, List, Set, Tuple

from dotenv import load_dotenv
from sqlalchemy import func

from agr_literature_service.api.crud.topic_entity_tag_crud import revalidate_all_tags
from agr_literature_service.api.models import TopicEntityTagModel
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
    ORM (so versioning/audit fire and prop/validation rows cascade). Returns the
    number of tags deleted."""
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


def cleanup_zfin_over_cap_reference_tags(delete: bool = False,
                                         revalidate: bool = True) -> Dict:
    """Find (and, with ``delete``, remove) ZFIN over-cap gene/allele entity tags.

    Returns a dict of counts describing the run (also used to build the report).
    """
    db = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, script_name)

    counts: Dict = {"deleted": delete}
    affected_reference_ids: Set[int] = set()

    try:
        source_id = find_zfin_source_id(db)
        if source_id is None:
            logger.info("ZFIN reference-curation TET source not found; nothing to clean up.")
            counts["source_missing"] = True
            return counts

        for label, entity_atp in ENTITY_TYPES:
            over_cap = find_over_cap_references(db, source_id, entity_atp)
            counts[f"{label}_papers"] = len(over_cap)
            counts[f"{label}_tags"] = sum(n for _ref, n in over_cap)
            logger.info(
                "%s: %d papers over the %d cap (%d tags)%s",
                label, len(over_cap), MAX_ASSOCIATIONS_PER_PAPER,
                counts[f"{label}_tags"], "" if delete else " [dry-run]",
            )
            if not delete:
                continue
            for reference_id, _n in over_cap:
                deleted = delete_reference_tags(db, source_id, entity_atp, reference_id)
                db.commit()
                affected_reference_ids.add(reference_id)
                logger.info("  deleted %d %s tags for reference_id=%d",
                            deleted, label, reference_id)

        counts["affected_references"] = len(affected_reference_ids)
        if delete and revalidate and affected_reference_ids:
            logger.info("Revalidating %d affected references", len(affected_reference_ids))
            for reference_id in sorted(affected_reference_ids):
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
    if counts.get("deleted"):
        message += f"<li>References revalidated: {counts.get('affected_references', 0)}"
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
