#!/usr/bin/env python3
"""
Script to delete duplicate resources with zero references.

This script safely removes duplicate resource entries that have:
1. No references pointing to them (reference_count = 0)
2. Duplicate title/ISSN/publisher with other resources

Usage:
    python delete_duplicate_resources.py --csv /path/to/deletable_duplicates.csv [--dry-run] [--execute]

Options:
    --csv FILE      Path to CSV file containing deletable duplicates
    --dry-run       Show what would be deleted without actually deleting (default)
    --execute       Actually perform the deletion (requires explicit confirmation)
    --db-host       Database host (default: from env PSQL_HOST)
    --db-port       Database port (default: from env PSQL_PORT)
    --db-name       Database name (default: from env PSQL_DATABASE)
    --db-user       Database user (default: from env PSQL_USERNAME)
    --db-password   Database password (default: from env PSQL_PASSWORD)
"""

import os
import sys
import csv
import argparse
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


def load_deletable_curies(csv_file):
    """Load CURIEs from CSV file."""
    curies = []
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            curies.append(row['curie'])
    return curies


def verify_no_references(session, curies):
    """Verify that all resources have zero references."""
    query = text("""
        SELECT r.curie, COUNT(ref.reference_id) as ref_count
        FROM resource r
        LEFT JOIN reference ref ON ref.resource_id = r.resource_id
        WHERE r.curie = ANY(:curies)
        GROUP BY r.curie
        HAVING COUNT(ref.reference_id) > 0
    """)
    result = session.execute(query, {"curies": curies})
    resources_with_refs = result.fetchall()
    return resources_with_refs


def verify_resources_exist(session, curies):
    """Verify that all CURIEs exist in the database."""
    query = text("""
        SELECT COUNT(*) as count
        FROM resource
        WHERE curie = ANY(:curies)
    """)
    result = session.execute(query, {"curies": curies})
    count = result.scalar()
    return count


def delete_resources(session, curies, execute=False):
    """Delete resources with the given CURIEs."""
    if not execute:
        print("\n🔍 DRY RUN MODE - No changes will be made\n")
        query = text("""
            SELECT curie, title, print_issn, online_issn
            FROM resource
            WHERE curie = ANY(:curies)
            ORDER BY title, curie
            LIMIT 20
        """)
        result = session.execute(query, {"curies": curies})
        resources = result.fetchall()

        print(f"Would delete {len(curies)} resources. First 20:")
        print("-" * 100)
        for r in resources:
            print(f"  {r[0]} | {r[1][:60]}")

        if len(curies) > 20:
            print(f"  ... and {len(curies) - 20} more")
        print("-" * 100)
        return 0

    # EXECUTE MODE
    print("\n⚠️  EXECUTE MODE - Changes will be made!\n")
    print("Starting transaction...")

    try:
        # Begin explicit transaction
        session.execute(text("BEGIN"))

        # Get count before deletion
        count_before = session.execute(text("SELECT COUNT(*) FROM resource")).scalar()
        print(f"Total resources before deletion: {count_before}")

        # Perform deletion
        delete_query = text("""
            DELETE FROM resource
            WHERE curie = ANY(:curies)
        """)
        result = session.execute(delete_query, {"curies": curies})
        deleted_count = result.rowcount
        print(f"Deleted {deleted_count} resources")

        # Get count after deletion
        count_after = session.execute(text("SELECT COUNT(*) FROM resource")).scalar()
        print(f"Total resources after deletion: {count_after}")
        print(f"Expected: {count_before - len(curies)}, Actual: {count_after}")

        # Verify the deletion
        if count_after != count_before - deleted_count:
            raise Exception("Count mismatch! Rolling back.")

        if deleted_count != len(curies):
            print(f"⚠️  WARNING: Expected to delete {len(curies)} but deleted {deleted_count}")
            print("Some CURIEs may not exist in the database.")

        # Commit the transaction
        session.execute(text("COMMIT"))
        print("\n✅ Transaction committed successfully!")
        return deleted_count

    except Exception as e:
        print(f"\n❌ Error occurred: {e}")
        print("Rolling back transaction...")
        session.execute(text("ROLLBACK"))
        print("Transaction rolled back. No changes made.")
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Delete duplicate resources with zero references"
    )
    parser.add_argument(
        "--csv",
        required=True,
        help="Path to CSV file containing deletable duplicates"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Show what would be deleted without actually deleting (default)"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually perform the deletion"
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

    # Load CURIEs from CSV
    print(f"Loading CURIEs from {args.csv}...")
    try:
        curies = load_deletable_curies(args.csv)
        print(f"Loaded {len(curies)} CURIEs")
    except Exception as e:
        print(f"Error loading CSV: {e}")
        sys.exit(1)

    # Connect to database
    DATABASE_URL = f"postgresql://{args.db_user}:{args.db_password}@{args.db_host}:{args.db_port}/{args.db_name}"
    print(f"\nConnecting to database: {args.db_name} @ {args.db_host}:{args.db_port}")

    try:
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()

        # Verify connection
        session.execute(text("SELECT 1"))
        print("✅ Database connection successful")

    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        sys.exit(1)

    # Verify resources exist
    print("\n1. Verifying resources exist in database...")
    exists_count = verify_resources_exist(session, curies)
    print(f"   Found {exists_count} / {len(curies)} resources in database")

    if exists_count == 0:
        print("❌ No resources found! Check your CSV file and database connection.")
        sys.exit(1)

    if exists_count < len(curies):
        print(f"⚠️  WARNING: {len(curies) - exists_count} CURIEs not found in database")

    # Verify no references
    print("\n2. Verifying resources have zero references...")
    resources_with_refs = verify_no_references(session, curies)

    if resources_with_refs:
        print(f"❌ ERROR: {len(resources_with_refs)} resources have references!")
        print("\nResources with references (CANNOT DELETE):")
        for r in resources_with_refs[:10]:
            print(f"   {r[0]} - {r[1]} references")
        print("\n⚠️  Aborting deletion. Please review the CSV file.")
        sys.exit(1)

    print("   ✅ All resources have zero references")

    # Perform deletion (dry-run or execute)
    print("\n3. Performing deletion...")
    try:
        deleted = delete_resources(session, curies, execute=args.execute)

        if not args.execute:
            print("\n" + "=" * 100)
            print("DRY RUN COMPLETE")
            print("=" * 100)
            print(f"\nTo actually delete these {len(curies)} resources, run:")
            print(f"  python {sys.argv[0]} --csv {args.csv} --execute")
            print("\n⚠️  WARNING: This action cannot be undone!")
            print("   Consider backing up the database before executing.")
        else:
            print("\n" + "=" * 100)
            print("DELETION COMPLETE")
            print("=" * 100)
            print(f"\n✅ Successfully deleted {deleted} duplicate resources")
            print(f"   Resources remaining in database: {session.execute(text('SELECT COUNT(*) FROM resource')).scalar()}")

    except Exception as e:
        print(f"\n❌ Deletion failed: {e}")
        sys.exit(1)

    finally:
        session.close()


if __name__ == "__main__":
    main()
