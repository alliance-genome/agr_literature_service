"""migrate_users_id_to_person_curie

Revision ID: c8d9e0f1a2b3
Revises: a7b4c9d2e5f8
Create Date: 2026-04-24

This migration updates users.id from Okta IDs to person.curie values for
human users (those with person_id not null). It also updates all referencing
created_by/updated_by columns in 26 tables.

Prerequisites:
- person.curie must be populated for all persons linked to users
- Run this query to verify: SELECT COUNT(*) FROM users u JOIN person p
  ON u.person_id = p.person_id WHERE p.curie IS NULL OR p.curie = '';
  Result should be 0.
"""
from alembic import op


# revision identifiers, used by Alembic.
revision = "c8d9e0f1a2b3"
down_revision = "a7b4c9d2e5f8"
branch_labels = None
depends_on = None

# All tables with created_by/updated_by foreign keys to users.id
TABLES_WITH_AUDIT_COLUMNS = [
    "author",
    "cross_reference",
    "curation_status",
    "dataset",
    "editor",
    "email",
    "indexing_priority",
    "manual_indexing_tag",
    "mod",
    "mod_corpus_association",
    "person",
    "person_cross_reference",
    "person_name",
    "person_note",
    "person_setting",
    "reference",
    "reference_email",
    "reference_mod_referencetype",
    "referencefile",
    "referencefile_mod",
    "resource",
    "topic_entity_tag",
    "topic_entity_tag_source",
    "workflow_tag",
    "workflow_tag_topic",
    "workflow_transition",
]

# Tables that have duplicate FK constraints (4 instead of 2)
# These will have constraints like tablename_created_by_fkey and tablename_created_by_fkey1
TABLES_WITH_DUPLICATE_FKS = [
    "author",
    "cross_reference",
    "editor",
    "mod",
    "mod_corpus_association",
    "reference",
    "reference_mod_referencetype",
    "referencefile",
    "referencefile_mod",
    "resource",
    "topic_entity_tag",
    "topic_entity_tag_source",
    "workflow_tag",
    "workflow_transition",
]


def upgrade():
    # -------------------------------------------------------------------------
    # Step 0: Verify prerequisites - person.curie must be populated
    # -------------------------------------------------------------------------
    conn = op.get_bind()
    result = conn.execute("""
        SELECT COUNT(*) FROM users u
        JOIN person p ON u.person_id = p.person_id
        WHERE p.curie IS NULL OR p.curie = ''
    """)
    count = result.scalar()
    if count > 0:
        raise RuntimeError(
            f"Cannot run migration: {count} users have person records without curie values. "
            "Please populate person.curie for all persons linked to users first."
        )

    # -------------------------------------------------------------------------
    # Step 1: Create temporary mapping table (old_id -> new_id)
    # -------------------------------------------------------------------------
    op.execute("""
        CREATE TEMP TABLE user_id_mapping AS
        SELECT u.id AS old_id, p.curie AS new_id
        FROM users u
        JOIN person p ON u.person_id = p.person_id
        WHERE u.person_id IS NOT NULL
          AND p.curie IS NOT NULL
          AND p.curie != ''
    """)

    # Also create a permanent backup table for potential rollback
    op.execute("""
        CREATE TABLE IF NOT EXISTS user_id_migration_backup AS
        SELECT u.id AS old_id, p.curie AS new_id, u.person_id
        FROM users u
        JOIN person p ON u.person_id = p.person_id
        WHERE u.person_id IS NOT NULL
          AND p.curie IS NOT NULL
          AND p.curie != ''
    """)

    # -------------------------------------------------------------------------
    # Step 2: Drop all foreign key constraints referencing users.id
    # -------------------------------------------------------------------------
    for table in TABLES_WITH_AUDIT_COLUMNS:
        # Drop created_by FK
        op.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {table}_created_by_fkey")
        # Drop updated_by FK
        op.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {table}_updated_by_fkey")

        # Drop duplicate FKs if they exist (for tables that have them)
        if table in TABLES_WITH_DUPLICATE_FKS:
            op.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {table}_created_by_fkey1")
            op.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {table}_updated_by_fkey1")

    # -------------------------------------------------------------------------
    # Step 3: Update all referencing columns (created_by/updated_by)
    # -------------------------------------------------------------------------
    for table in TABLES_WITH_AUDIT_COLUMNS:
        op.execute(f"""
            UPDATE {table}
            SET created_by = m.new_id
            FROM user_id_mapping m
            WHERE {table}.created_by = m.old_id
        """)
        op.execute(f"""
            UPDATE {table}
            SET updated_by = m.new_id
            FROM user_id_mapping m
            WHERE {table}.updated_by = m.old_id
        """)

    # -------------------------------------------------------------------------
    # Step 4: Update users.id (the primary key)
    # -------------------------------------------------------------------------
    op.execute("""
        UPDATE users u
        SET id = m.new_id
        FROM user_id_mapping m
        WHERE u.id = m.old_id
    """)

    # -------------------------------------------------------------------------
    # Step 5: Recreate foreign key constraints (only one per column)
    # -------------------------------------------------------------------------
    for table in TABLES_WITH_AUDIT_COLUMNS:
        op.execute(f"""
            ALTER TABLE {table}
            ADD CONSTRAINT {table}_created_by_fkey
            FOREIGN KEY (created_by) REFERENCES users(id)
        """)
        op.execute(f"""
            ALTER TABLE {table}
            ADD CONSTRAINT {table}_updated_by_fkey
            FOREIGN KEY (updated_by) REFERENCES users(id)
        """)

    # Drop temp table
    op.execute("DROP TABLE IF EXISTS user_id_mapping")


def downgrade():
    """
    Rollback the migration by reversing all user ID changes.
    Uses the backup table created during upgrade.
    """
    conn = op.get_bind()

    # Check if backup table exists
    result = conn.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'user_id_migration_backup'
        )
    """)
    if not result.scalar():
        raise RuntimeError(
            "Cannot rollback: user_id_migration_backup table not found. "
            "Manual intervention required."
        )

    # -------------------------------------------------------------------------
    # Step 1: Create reverse mapping (new_id -> old_id)
    # -------------------------------------------------------------------------
    op.execute("""
        CREATE TEMP TABLE user_id_reverse_mapping AS
        SELECT new_id AS current_id, old_id AS target_id
        FROM user_id_migration_backup
    """)

    # -------------------------------------------------------------------------
    # Step 2: Drop all foreign key constraints
    # -------------------------------------------------------------------------
    for table in TABLES_WITH_AUDIT_COLUMNS:
        op.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {table}_created_by_fkey")
        op.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {table}_updated_by_fkey")

    # -------------------------------------------------------------------------
    # Step 3: Revert all referencing columns
    # -------------------------------------------------------------------------
    for table in TABLES_WITH_AUDIT_COLUMNS:
        op.execute(f"""
            UPDATE {table}
            SET created_by = m.target_id
            FROM user_id_reverse_mapping m
            WHERE {table}.created_by = m.current_id
        """)
        op.execute(f"""
            UPDATE {table}
            SET updated_by = m.target_id
            FROM user_id_reverse_mapping m
            WHERE {table}.updated_by = m.current_id
        """)

    # -------------------------------------------------------------------------
    # Step 4: Revert users.id
    # -------------------------------------------------------------------------
    op.execute("""
        UPDATE users u
        SET id = m.target_id
        FROM user_id_reverse_mapping m
        WHERE u.id = m.current_id
    """)

    # -------------------------------------------------------------------------
    # Step 5: Recreate foreign key constraints
    # Note: This recreates without duplicates. If you need the original
    # duplicate constraints, manual intervention is required.
    # -------------------------------------------------------------------------
    for table in TABLES_WITH_AUDIT_COLUMNS:
        op.execute(f"""
            ALTER TABLE {table}
            ADD CONSTRAINT {table}_created_by_fkey
            FOREIGN KEY (created_by) REFERENCES users(id)
        """)
        op.execute(f"""
            ALTER TABLE {table}
            ADD CONSTRAINT {table}_updated_by_fkey
            FOREIGN KEY (updated_by) REFERENCES users(id)
        """)

    # Clean up
    op.execute("DROP TABLE IF EXISTS user_id_reverse_mapping")
    # Keep the backup table for safety - can be manually dropped later
    # op.execute("DROP TABLE IF EXISTS user_id_migration_backup")
