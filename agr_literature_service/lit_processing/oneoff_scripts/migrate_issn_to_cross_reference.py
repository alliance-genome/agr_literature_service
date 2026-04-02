"""
One-off script to migrate print_issn and online_issn from the resource table
into the cross_reference table with the new issn_type column.

For each resource with a non-null print_issn or online_issn:
- If a cross_reference row already exists with that ISSN curie and resource_id,
  update its issn_type.
- Otherwise, insert a new cross_reference row.

Usage:
    python -m agr_literature_service.lit_processing.oneoff_scripts.migrate_issn_to_cross_reference
    python -m agr_literature_service.lit_processing.oneoff_scripts.migrate_issn_to_cross_reference --execute
"""
import argparse
import logging
from os import path
from sqlalchemy import text
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import CrossReferenceModel
from agr_literature_service.api.user import set_global_user_id

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BATCH_SIZE = 250


def migrate_issn_to_cross_reference(dry_run: bool = True):
    db = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, script_name)

    query = text("""
        SELECT resource_id, print_issn, online_issn
        FROM resource
        WHERE print_issn IS NOT NULL OR online_issn IS NOT NULL
    """)
    rows = db.execute(query).fetchall()
    logger.info(f"Found {len(rows)} resources with ISSN values")

    inserted = 0
    updated = 0
    skipped = 0
    count = 0

    for row in rows:
        resource_id, print_issn, online_issn = row

        for issn_value, issn_type in [(print_issn, 'print'), (online_issn, 'online')]:
            if not issn_value:
                continue

            curie = f"ISSN:{issn_value}"

            existing = db.execute(
                text("""
                    SELECT cross_reference_id, issn_type
                    FROM cross_reference
                    WHERE curie = :curie AND resource_id = :resource_id
                """),
                {"curie": curie, "resource_id": resource_id}
            ).fetchone()

            if existing:
                xref_id, existing_issn_type = existing
                if existing_issn_type == issn_type:
                    skipped += 1
                    logger.debug(f"SKIP: {curie} resource_id={resource_id} "
                                 f"already has issn_type='{issn_type}'")
                else:
                    if not dry_run:
                        xref_obj = db.query(CrossReferenceModel).get(
                            xref_id)
                        xref_obj.issn_type = issn_type
                    updated += 1
                    logger.info(f"UPDATE: {curie} resource_id={resource_id} "
                                f"issn_type '{existing_issn_type}' -> '{issn_type}'")
            else:
                if not dry_run:
                    xref_obj = CrossReferenceModel(
                        curie=curie,
                        curie_prefix='ISSN',
                        resource_id=resource_id,
                        issn_type=issn_type,
                        is_obsolete=False
                    )
                    db.add(xref_obj)
                inserted += 1
                logger.info(f"INSERT: {curie} resource_id={resource_id} "
                            f"issn_type='{issn_type}'")

            count += 1
            if count % BATCH_SIZE == 0 and not dry_run:
                db.commit()
                logger.info(f"Committed batch ({count} processed so far)")

    if not dry_run:
        db.commit()
        logger.info("Final commit done")
    else:
        logger.info("DRY RUN - no changes made")

    logger.info(f"Summary: {inserted} inserted, {updated} updated, "
                f"{skipped} skipped (already correct)")

    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Migrate ISSN values from resource table to cross_reference"
    )
    parser.add_argument(
        '--execute',
        action='store_true',
        help='Actually execute the changes (default is dry run)'
    )
    args = parser.parse_args()

    dry_run = not args.execute

    if dry_run:
        logger.info("Running in DRY RUN mode - no changes will be made")
    else:
        logger.info("Running in EXECUTE mode - changes will be committed")

    migrate_issn_to_cross_reference(dry_run=dry_run)
