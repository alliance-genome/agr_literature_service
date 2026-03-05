"""
One-off script to fix cross_reference rows where curie is missing the prefix.

For resource-linked cross_references where curie doesn't contain ':':
1. If a correct version exists (curie_prefix:curie), delete the malformed row
2. If no correct version exists, update the curie to include the prefix

Usage:
    python fix_resource_curie_prefix.py          # Dry run (default)
    python fix_resource_curie_prefix.py --execute  # Actually make changes
"""
import argparse
import logging
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_db_session():
    """Create database session from environment variables."""
    host = os.environ.get('PSQL_HOST', 'localhost')
    port = os.environ.get('PSQL_PORT', '5432')
    database = os.environ.get('PSQL_DATABASE', 'literature')
    username = os.environ.get('PSQL_USERNAME', 'postgres')
    password = os.environ.get('PSQL_PASSWORD', 'postgres')

    connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    return Session()


def fix_resource_curie_prefixes(dry_run: bool = True):
    """
    Fix cross_reference rows where curie is missing the prefix.

    :param dry_run: If True, only report what would be done without making changes
    """
    db_session = get_db_session()

    # Find all cross_reference rows where curie doesn't contain ':' and has resource_id
    query = text("""
        SELECT cross_reference_id, curie_prefix, curie, resource_id
        FROM cross_reference
        WHERE resource_id IS NOT NULL
          AND curie NOT LIKE '%:%'
    """)

    result = db_session.execute(query)
    malformed_rows = result.fetchall()

    logger.info(f"Found {len(malformed_rows)} cross_reference rows with malformed curie (missing prefix)")

    updated_count = 0
    deleted_count = 0
    skipped_count = 0

    for row in malformed_rows:
        cross_reference_id, curie_prefix, current_curie, resource_id = row
        correct_curie = f"{curie_prefix}:{current_curie}"

        # Check if a row with the correct curie already exists for this resource
        check_existing = text("""
            SELECT cross_reference_id FROM cross_reference
            WHERE resource_id = :resource_id AND curie = :correct_curie
        """)
        existing = db_session.execute(
            check_existing,
            {"resource_id": resource_id, "correct_curie": correct_curie}
        ).fetchone()

        if existing:
            # Correct version exists - delete the malformed row
            logger.info(f"DELETE: id={cross_reference_id} curie={current_curie} "
                        f"(resource_id={resource_id}) - correct version {correct_curie} already exists")
            if not dry_run:
                delete_query = text("DELETE FROM cross_reference WHERE cross_reference_id = :id")
                db_session.execute(delete_query, {"id": cross_reference_id})
            deleted_count += 1
        else:
            # Check if another resource already has this correct curie
            check_conflict = text("""
                SELECT cross_reference_id, resource_id FROM cross_reference
                WHERE curie = :correct_curie AND resource_id != :resource_id
            """)
            conflict = db_session.execute(
                check_conflict,
                {"correct_curie": correct_curie, "resource_id": resource_id}
            ).fetchone()

            if conflict:
                # Another resource owns this curie - skip to avoid conflict
                logger.warning(f"SKIP: id={cross_reference_id} curie={current_curie} "
                               f"(resource_id={resource_id}) - correct curie {correct_curie} "
                               f"belongs to resource_id={conflict[1]}")
                skipped_count += 1
            else:
                # Safe to update
                logger.info(f"UPDATE: id={cross_reference_id} {current_curie} -> {correct_curie} "
                            f"(resource_id={resource_id})")
                if not dry_run:
                    update_query = text("""
                        UPDATE cross_reference SET curie = :correct_curie
                        WHERE cross_reference_id = :id
                    """)
                    db_session.execute(update_query, {"correct_curie": correct_curie, "id": cross_reference_id})
                updated_count += 1

    if not dry_run:
        db_session.commit()
        logger.info("Changes committed to database")
    else:
        logger.info("DRY RUN - no changes made")

    logger.info(f"Summary: {updated_count} would be updated, {deleted_count} would be deleted, "
                f"{skipped_count} skipped")

    db_session.close()
    return updated_count, deleted_count, skipped_count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fix cross_reference rows where curie is missing the prefix"
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

    fix_resource_curie_prefixes(dry_run=dry_run)
