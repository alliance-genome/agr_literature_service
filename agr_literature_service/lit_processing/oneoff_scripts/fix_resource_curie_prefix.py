"""
One-off script to fix cross_reference rows where curie is missing the prefix.

For resource-linked cross_references where curie doesn't contain ':':
1. Simple fix: If no conflict, update curie to include prefix
2. DELETE_CORRECT: If malformed resource has refs and correct resource has 0 refs,
   fix the malformed curie and delete the unused correct resource
3. DELETE_MALFORMED: If malformed resource has 0 refs and correct resource has refs,
   delete the malformed cross_reference and resource
4. MERGE_NEEDED: If both have refs, transfer references from malformed to correct
   resource and delete the malformed resource

Usage:
    python fix_resource_curie_prefix.py              # Dry run (default)
    python fix_resource_curie_prefix.py --execute    # Actually make changes
"""
import argparse
import logging
from sqlalchemy import text
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_reference_count(db, resource_id):
    """Get count of references for a resource."""
    result = db.execute(
        text("SELECT COUNT(*) FROM reference WHERE resource_id = :rid"),
        {"rid": resource_id}
    ).fetchone()
    return result[0] if result else 0


def transfer_references(db, from_resource_id, to_resource_id, dry_run=True):
    """Transfer all references from one resource to another."""
    if not dry_run:
        db.execute(
            text("UPDATE reference SET resource_id = :to_rid WHERE resource_id = :from_rid"),
            {"to_rid": to_resource_id, "from_rid": from_resource_id}
        )


def delete_resource_and_xrefs(db, resource_id, dry_run=True):
    """Delete a resource and all its cross_references."""
    if not dry_run:
        # Delete cross_references first
        db.execute(
            text("DELETE FROM cross_reference WHERE resource_id = :rid"),
            {"rid": resource_id}
        )
        # Delete editors
        db.execute(
            text("DELETE FROM editor WHERE resource_id = :rid"),
            {"rid": resource_id}
        )
        # Delete the resource
        db.execute(
            text("DELETE FROM resource WHERE resource_id = :rid"),
            {"rid": resource_id}
        )


def fix_resource_curie_prefixes(dry_run: bool = True):
    """
    Fix cross_reference rows where curie is missing the prefix.

    :param dry_run: If True, only report what would be done without making changes
    """
    db = create_postgres_session(False)

    # Find all cross_reference rows where curie doesn't contain ':' and has resource_id
    query = text("""
        SELECT cross_reference_id, curie_prefix, curie, resource_id
        FROM cross_reference
        WHERE resource_id IS NOT NULL
          AND curie NOT LIKE '%:%'
    """)

    result = db.execute(query)
    malformed_rows = result.fetchall()

    logger.info(f"Found {len(malformed_rows)} cross_reference rows with malformed curie (missing prefix)")

    updated_count = 0
    deleted_xref_count = 0
    deleted_resource_count = 0
    merged_count = 0
    skipped_count = 0

    # Track resources we've already processed to avoid double-processing
    processed_resources = set()

    for row in malformed_rows:
        cross_reference_id, curie_prefix, current_curie, resource_id = row
        correct_curie = f"{curie_prefix}:{current_curie}"

        # Skip if we've already processed this resource
        if resource_id in processed_resources:
            continue

        # Check if a row with the correct curie already exists for this resource
        existing = db.execute(
            text("""
                SELECT cross_reference_id FROM cross_reference
                WHERE resource_id = :resource_id AND curie = :correct_curie
            """),
            {"resource_id": resource_id, "correct_curie": correct_curie}
        ).fetchone()

        if existing:
            # Correct version exists for same resource - just delete the malformed duplicate
            logger.info(f"DELETE DUPLICATE: id={cross_reference_id} curie={current_curie} "
                        f"(resource_id={resource_id}) - correct version already exists")
            if not dry_run:
                db.execute(
                    text("DELETE FROM cross_reference WHERE cross_reference_id = :id"),
                    {"id": cross_reference_id}
                )
            deleted_xref_count += 1
            continue

        # Check if another resource has this correct curie
        conflict = db.execute(
            text("""
                SELECT cross_reference_id, resource_id FROM cross_reference
                WHERE curie = :correct_curie AND resource_id != :resource_id
            """),
            {"correct_curie": correct_curie, "resource_id": resource_id}
        ).fetchone()

        if not conflict:
            # No conflict - simple update
            logger.info(f"UPDATE: id={cross_reference_id} {current_curie} -> {correct_curie} "
                        f"(resource_id={resource_id})")
            if not dry_run:
                db.execute(
                    text("UPDATE cross_reference SET curie = :correct_curie WHERE cross_reference_id = :id"),
                    {"correct_curie": correct_curie, "id": cross_reference_id}
                )
            updated_count += 1
        else:
            # Conflict exists - check reference counts to decide action
            conflict_xref_id, correct_resource_id = conflict
            malformed_refs = get_reference_count(db, resource_id)
            correct_refs = get_reference_count(db, correct_resource_id)

            if malformed_refs > 0 and correct_refs == 0:
                # DELETE_CORRECT: Keep malformed resource (has refs), delete unused correct resource
                logger.info(f"DELETE_CORRECT: Fix curie on resource_id={resource_id} ({malformed_refs} refs), "
                            f"delete unused resource_id={correct_resource_id} (0 refs) - {correct_curie}")
                if not dry_run:
                    # First delete the unused correct resource and its xrefs
                    delete_resource_and_xrefs(db, correct_resource_id, dry_run=False)
                    # Now update the malformed curie
                    db.execute(
                        text("UPDATE cross_reference SET curie = :correct_curie WHERE cross_reference_id = :id"),
                        {"correct_curie": correct_curie, "id": cross_reference_id}
                    )
                updated_count += 1
                deleted_resource_count += 1
                processed_resources.add(correct_resource_id)

            elif malformed_refs == 0 and correct_refs > 0:
                # DELETE_MALFORMED: Delete the unused malformed resource
                logger.info(f"DELETE_MALFORMED: Delete unused resource_id={resource_id} (0 refs), "
                            f"keep resource_id={correct_resource_id} ({correct_refs} refs) - {correct_curie}")
                if not dry_run:
                    delete_resource_and_xrefs(db, resource_id, dry_run=False)
                deleted_resource_count += 1
                processed_resources.add(resource_id)

            elif malformed_refs == 0 and correct_refs == 0:
                # BOTH_UNUSED: Delete the malformed one, keep the correct one
                logger.info(f"BOTH_UNUSED: Delete unused resource_id={resource_id}, "
                            f"keep resource_id={correct_resource_id} - {correct_curie}")
                if not dry_run:
                    delete_resource_and_xrefs(db, resource_id, dry_run=False)
                deleted_resource_count += 1
                processed_resources.add(resource_id)

            else:
                # MERGE_NEEDED: Both have refs - transfer refs from malformed to correct,
                # then delete malformed resource
                logger.info(f"MERGE: Transfer {malformed_refs} refs from resource_id={resource_id} "
                            f"to resource_id={correct_resource_id} ({correct_refs} refs), "
                            f"then delete resource_id={resource_id} - {correct_curie}")
                if not dry_run:
                    # Transfer references
                    transfer_references(db, resource_id, correct_resource_id, dry_run=False)
                    # Delete the malformed resource
                    delete_resource_and_xrefs(db, resource_id, dry_run=False)
                merged_count += 1
                deleted_resource_count += 1
                processed_resources.add(resource_id)

    if not dry_run:
        db.commit()
        logger.info("Changes committed to database")
    else:
        logger.info("DRY RUN - no changes made")

    logger.info(f"Summary: {updated_count} curies updated, {deleted_xref_count} duplicate xrefs deleted, "
                f"{merged_count} resources merged, {deleted_resource_count} resources deleted, "
                f"{skipped_count} skipped")

    db.close()
    return updated_count, deleted_xref_count, merged_count, deleted_resource_count, skipped_count


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
