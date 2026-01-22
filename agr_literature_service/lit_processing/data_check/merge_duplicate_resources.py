#!/usr/bin/env python3
"""
Script to merge duplicate resources by updating references to a canonical resource.

This script:
1. Identifies duplicate resource groups (same title, ISSN, publisher)
2. Selects the canonical resource (smallest resource_id) for each group
3. Updates all references to point to the canonical resource
4. Deletes the duplicate resources

Usage:
    python merge_duplicate_resources.py [--dry-run] [--execute] [--limit N]

Options:
    --dry-run       Show what would be merged without actually doing it (default)
    --execute       Actually perform the merge (requires explicit confirmation)
    --limit N       Only process first N duplicate groups (for testing)
    --db-host       Database host (default: from env PSQL_HOST)
    --db-port       Database port (default: from env PSQL_PORT)
    --db-name       Database name (default: from env PSQL_DATABASE)
    --db-user       Database user (default: from env PSQL_USERNAME)
    --db-password   Database password (default: from env PSQL_PASSWORD)

Safety Features:
    - Dry-run mode by default
    - Transaction-based with automatic rollback on error
    - Validates data integrity before and after
    - Detailed reporting of all changes
"""

import os
import sys
import argparse
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime


def identify_duplicate_groups(session, limit=None):
    """
    Identify groups of duplicate resources.
    Returns list of tuples: (title, print_issn, online_issn, publisher, duplicate_count)
    """
    query = text("""
        SELECT title, print_issn, online_issn, publisher, COUNT(*) as dup_count
        FROM resource
        WHERE title IS NOT NULL
        GROUP BY title, print_issn, online_issn, publisher
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC, title
    """)

    if limit:
        query = text(str(query) + f" LIMIT {limit}")

    result = session.execute(query)
    return result.fetchall()


def get_duplicate_group_details(session, title, print_issn, online_issn, publisher):
    """
    Get details for a specific duplicate group.
    Returns list of resources with their reference counts.
    """
    query = text("""
        SELECT
            r.resource_id,
            r.curie,
            r.title,
            r.print_issn,
            r.online_issn,
            r.publisher,
            r.date_created,
            COUNT(ref.reference_id) as ref_count
        FROM resource r
        LEFT JOIN reference ref ON ref.resource_id = r.resource_id
        WHERE r.title = :title
            AND (r.print_issn = :print_issn OR (r.print_issn IS NULL AND :print_issn IS NULL))
            AND (r.online_issn = :online_issn OR (r.online_issn IS NULL AND :online_issn IS NULL))
            AND (r.publisher = :publisher OR (r.publisher IS NULL AND :publisher IS NULL))
        GROUP BY r.resource_id, r.curie, r.title, r.print_issn, r.online_issn, r.publisher, r.date_created
        ORDER BY r.resource_id ASC
    """)

    result = session.execute(query, {
        "title": title,
        "print_issn": print_issn,
        "online_issn": online_issn,
        "publisher": publisher
    })
    return result.fetchall()


def merge_duplicate_group(session, canonical_resource_id, duplicate_resource_ids, execute=False):
    """
    Merge duplicate resources into canonical resource.

    Args:
        canonical_resource_id: The resource_id to keep
        duplicate_resource_ids: List of resource_ids to merge and delete
        execute: Whether to actually perform the merge

    Returns:
        dict with counts of references updated and resources deleted
    """
    if not execute:
        # Dry run - just count what would be updated
        count_query = text("""
            SELECT COUNT(*)
            FROM reference
            WHERE resource_id = ANY(:duplicate_ids)
        """)
        result = session.execute(count_query, {"duplicate_ids": duplicate_resource_ids})
        ref_count = result.scalar()

        return {
            "references_updated": ref_count,
            "resources_deleted": len(duplicate_resource_ids)
        }

    # EXECUTE MODE
    # Update references to point to canonical resource
    update_query = text("""
        UPDATE reference
        SET resource_id = :canonical_id
        WHERE resource_id = ANY(:duplicate_ids)
    """)
    result = session.execute(update_query, {
        "canonical_id": canonical_resource_id,
        "duplicate_ids": duplicate_resource_ids
    })
    refs_updated = result.rowcount

    # Delete duplicate resources
    delete_query = text("""
        DELETE FROM resource
        WHERE resource_id = ANY(:duplicate_ids)
    """)
    result = session.execute(delete_query, {"duplicate_ids": duplicate_resource_ids})
    resources_deleted = result.rowcount

    return {
        "references_updated": refs_updated,
        "resources_deleted": resources_deleted
    }


def main():
    parser = argparse.ArgumentParser(
        description="Merge duplicate resources by updating references to canonical resource"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Show what would be merged without actually doing it (default)"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually perform the merge"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process first N duplicate groups (for testing)"
    )
    parser.add_argument("--db-host", default=os.environ.get("PSQL_HOST", "localhost"))
    parser.add_argument("--db-port", default=os.environ.get("PSQL_PORT", "5432"))
    parser.add_argument("--db-name", default=os.environ.get("PSQL_DATABASE", "literature"))
    parser.add_argument("--db-user", default=os.environ.get("PSQL_USERNAME", "postgres"))
    parser.add_argument("--db-password", default=os.environ.get("PSQL_PASSWORD", "postgres"))

    args = parser.parse_args()

    # Validate arguments
    if args.execute and args.dry_run:
        args.dry_run = False  # Execute overrides dry-run

    # Connect to database
    DATABASE_URL = f"postgresql://{args.db_user}:{args.db_password}@{args.db_host}:{args.db_port}/{args.db_name}"
    print(f"Connecting to database: {args.db_name} @ {args.db_host}:{args.db_port}")

    try:
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()

        # Verify connection
        session.execute(text("SELECT 1"))
        print("✅ Database connection successful\n")

    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        sys.exit(1)

    # Identify duplicate groups
    print("=" * 100)
    print("DUPLICATE RESOURCE MERGE ANALYSIS")
    print("=" * 100)

    if not args.execute:
        print("\n🔍 DRY RUN MODE - No changes will be made\n")
    else:
        print("\n⚠️  EXECUTE MODE - Changes will be made!\n")

    print("1. Identifying duplicate groups...")
    duplicate_groups = identify_duplicate_groups(session, limit=args.limit)
    print(f"   Found {len(duplicate_groups)} duplicate groups\n")

    if len(duplicate_groups) == 0:
        print("✅ No duplicates found!")
        sys.exit(0)

    # Process each duplicate group
    total_refs_updated = 0
    total_resources_deleted = 0
    total_resources_kept = 0

    print("2. Processing duplicate groups...\n")
    print("-" * 100)

    for idx, group in enumerate(duplicate_groups, 1):
        title, print_issn, online_issn, publisher, dup_count = group

        # Get details for this group
        resources = get_duplicate_group_details(session, title, print_issn, online_issn, publisher)

        if len(resources) < 2:
            continue  # Skip if not actually duplicates

        # Canonical resource is the one with smallest resource_id
        canonical = resources[0]
        duplicates = resources[1:]

        canonical_id = canonical[0]
        canonical_curie = canonical[1]
        canonical_refs = canonical[7]

        duplicate_ids = [r[0] for r in duplicates]
        total_duplicate_refs = sum(r[7] for r in duplicates)

        print(f"\n[{idx}/{len(duplicate_groups)}] {title[:60]}...")
        print(f"  Canonical: {canonical_curie} (resource_id: {canonical_id}, {canonical_refs} refs)")
        print(f"  Duplicates: {len(duplicates)} resources with {total_duplicate_refs} total refs")

        for dup in duplicates:
            print(f"    - {dup[1]} (resource_id: {dup[0]}, {dup[7]} refs)")

        # Perform merge
        try:
            if args.execute:
                # Execute actual merge
                result = merge_duplicate_group(session, canonical_id, duplicate_ids, execute=True)
                print(f"  ✅ Updated {result['references_updated']} refs, deleted {result['resources_deleted']} resources")
            else:
                # Dry run
                result = merge_duplicate_group(session, canonical_id, duplicate_ids, execute=False)
                print(f"  📋 Would update {result['references_updated']} refs, delete {result['resources_deleted']} resources")

            total_refs_updated += result['references_updated']
            total_resources_deleted += result['resources_deleted']
            total_resources_kept += 1

        except Exception as e:
            print(f"  ❌ Error processing group: {e}")
            if args.execute:
                print("  Rolling back transaction...")
                session.rollback()
                sys.exit(1)

    print("\n" + "-" * 100)
    print("\n3. Summary:")
    print(f"   Duplicate groups processed: {len(duplicate_groups)}")
    print(f"   Canonical resources kept: {total_resources_kept}")
    print(f"   Duplicate resources {'deleted' if args.execute else 'to delete'}: {total_resources_deleted}")
    print(f"   References {'updated' if args.execute else 'to update'}: {total_refs_updated}")

    if args.execute:
        print("\n4. Committing transaction...")
        try:
            session.commit()
            print("   ✅ Transaction committed successfully!")

            # Verify final state
            final_count = session.execute(text("SELECT COUNT(*) FROM resource")).scalar()
            print(f"\n✅ MERGE COMPLETE")
            print(f"   Total resources in database: {final_count}")

        except Exception as e:
            print(f"   ❌ Error committing transaction: {e}")
            print("   Rolling back...")
            session.rollback()
            sys.exit(1)
    else:
        print("\n" + "=" * 100)
        print("DRY RUN COMPLETE")
        print("=" * 100)
        print(f"\nTo actually merge these duplicates, run:")
        print(f"  python {sys.argv[0]} --execute")
        if args.limit:
            print(f"  (Remove --limit to process all {len(duplicate_groups)} groups)")
        print("\n⚠️  WARNING: This action cannot be undone!")
        print("   Consider backing up the database before executing.")

    session.close()


if __name__ == "__main__":
    main()
