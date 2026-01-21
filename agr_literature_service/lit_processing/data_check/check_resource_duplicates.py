#!/usr/bin/env python3
"""
Script to check for duplicate rows in the resource table.
"""
import os
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Database connection parameters from .env.test
DB_USER = os.environ.get('PSQL_USERNAME', 'postgres')
DB_PASSWORD = os.environ.get('PSQL_PASSWORD', 'postgres')
DB_HOST = os.environ.get('PSQL_HOST', 'localhost')
DB_PORT = os.environ.get('PSQL_PORT', '5433')
DB_NAME = os.environ.get('PSQL_DATABASE', 'literature-test')

# Create database connection
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

try:
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    print(f"Connected to database: {DB_NAME} at {DB_HOST}:{DB_PORT}")
    print("=" * 80)

    # Check 1: Total resource count
    result = session.execute(text("SELECT COUNT(*) FROM resource"))
    total_count = result.scalar()
    print(f"\n1. Total resource records: {total_count}")

    # Check 2: Duplicate CURIEs (should be none due to unique constraint)
    print("\n2. Checking for duplicate CURIEs...")
    query = text("""
        SELECT curie, COUNT(*) as count
        FROM resource
        WHERE curie IS NOT NULL
        GROUP BY curie
        HAVING COUNT(*) > 1
        ORDER BY count DESC
    """)
    result = session.execute(query)
    duplicates = result.fetchall()
    if duplicates:
        print(f"   Found {len(duplicates)} CURIEs with duplicates:")
        for row in duplicates[:10]:  # Show first 10
            print(f"   - CURIE: {row[0]}, Count: {row[1]}")
    else:
        print("   No duplicate CURIEs found (as expected)")

    # Check 3: Duplicate titles
    print("\n3. Checking for duplicate titles...")
    query = text("""
        SELECT title, COUNT(*) as count,
               STRING_AGG(curie, ', ' ORDER BY curie) as curies
        FROM resource
        WHERE title IS NOT NULL AND title != ''
        GROUP BY title
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 20
    """)
    result = session.execute(query)
    duplicates = result.fetchall()
    if duplicates:
        print(f"   Found {len(duplicates)} titles with multiple resources:")
        for row in duplicates[:10]:  # Show first 10
            print(f"   - Title: {row[0][:60]}... Count: {row[1]}")
            print(f"     CURIEs: {row[2]}")
    else:
        print("   No duplicate titles found")

    # Check 4: Duplicate print ISSNs
    print("\n4. Checking for duplicate print ISSNs...")
    query = text("""
        SELECT print_issn, COUNT(*) as count,
               STRING_AGG(curie, ', ' ORDER BY curie) as curies,
               STRING_AGG(title, ' | ' ORDER BY curie) as titles
        FROM resource
        WHERE print_issn IS NOT NULL AND print_issn != ''
        GROUP BY print_issn
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 20
    """)
    result = session.execute(query)
    duplicates = result.fetchall()
    if duplicates:
        print(f"   Found {len(duplicates)} print ISSNs with multiple resources:")
        for row in duplicates[:10]:
            print(f"   - ISSN: {row[0]}, Count: {row[1]}")
            print(f"     CURIEs: {row[2]}")
            print(f"     Titles: {row[3][:100]}...")
    else:
        print("   No duplicate print ISSNs found")

    # Check 5: Duplicate online ISSNs
    print("\n5. Checking for duplicate online ISSNs...")
    query = text("""
        SELECT online_issn, COUNT(*) as count,
               STRING_AGG(curie, ', ' ORDER BY curie) as curies,
               STRING_AGG(title, ' | ' ORDER BY curie) as titles
        FROM resource
        WHERE online_issn IS NOT NULL AND online_issn != ''
        GROUP BY online_issn
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 20
    """)
    result = session.execute(query)
    duplicates = result.fetchall()
    if duplicates:
        print(f"   Found {len(duplicates)} online ISSNs with multiple resources:")
        for row in duplicates[:10]:
            print(f"   - ISSN: {row[0]}, Count: {row[1]}")
            print(f"     CURIEs: {row[2]}")
            print(f"     Titles: {row[3][:100]}...")
    else:
        print("   No duplicate online ISSNs found")

    # Check 6: Potential semantic duplicates (same title and issn)
    print("\n6. Checking for semantic duplicates (same title + ISSN)...")
    query = text("""
        SELECT title, print_issn, online_issn, COUNT(*) as count,
               STRING_AGG(curie, ', ' ORDER BY curie) as curies
        FROM resource
        WHERE title IS NOT NULL
        GROUP BY title, print_issn, online_issn
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 20
    """)
    result = session.execute(query)
    duplicates = result.fetchall()
    if duplicates:
        print(f"   Found {len(duplicates)} potential semantic duplicates:")
        for row in duplicates[:10]:
            print(f"   - Title: {row[0][:60]}...")
            print(f"     Print ISSN: {row[1]}, Online ISSN: {row[2]}")
            print(f"     Count: {row[3]}, CURIEs: {row[4]}")
    else:
        print("   No semantic duplicates found")

    # Check 7: Resources with NULL or empty CURIEs
    print("\n7. Checking for resources with NULL or empty CURIEs...")
    query = text("""
        SELECT resource_id, title, print_issn, online_issn
        FROM resource
        WHERE curie IS NULL OR curie = ''
        LIMIT 10
    """)
    result = session.execute(query)
    null_curies = result.fetchall()
    if null_curies:
        print(f"   Found {len(null_curies)} resources with NULL/empty CURIEs:")
        for row in null_curies:
            print(f"   - ID: {row[0]}, Title: {row[1][:60]}...")
    else:
        print("   No NULL/empty CURIEs found")

    print("\n" + "=" * 80)
    print("Duplicate check complete!")

    session.close()

except Exception as e:
    print(f"Error connecting to database: {e}")
    print(f"Connection string: postgresql://{DB_USER}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    sys.exit(1)
