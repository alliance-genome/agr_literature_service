"""cleanup_orphan_okta_ids

Revision ID: d9e0f1a2b3c4
Revises: c8d9e0f1a2b3
Create Date: 2026-04-24

This migration cleans up any Okta IDs (starting with '00u' or '0oa') in
created_by/updated_by columns that weren't mapped to person curies.
These are replaced with 'default_user'.

Script names like 'load_pmc_metadata' are left unchanged.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "d9e0f1a2b3c4"
down_revision = "c8d9e0f1a2b3"
branch_labels = None
depends_on = None

# All tables with created_by/updated_by columns
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


def upgrade():
    conn = op.get_bind()

    # Ensure 'default_user' exists in users table
    result = conn.execute(sa.text(
        "SELECT id FROM users WHERE id = 'default_user'"
    ))
    if result.fetchone() is None:
        op.execute(sa.text("""
            INSERT INTO users (id, automation_username)
            VALUES ('default_user', 'default_user')
        """))

    # Update all Okta IDs (starting with '00u' or '0oa') to 'default_user'
    # in created_by and updated_by columns
    for table in TABLES_WITH_AUDIT_COLUMNS:
        # Update created_by where it looks like an Okta ID
        op.execute(sa.text(f"""
            UPDATE {table}
            SET created_by = 'default_user'
            WHERE created_by ~ '^0(0u|oa)[a-zA-Z0-9]+$'
        """))

        # Update updated_by where it looks like an Okta ID
        op.execute(sa.text(f"""
            UPDATE {table}
            SET updated_by = 'default_user'
            WHERE updated_by ~ '^0(0u|oa)[a-zA-Z0-9]+$'
        """))

    # Also clean up the users table - remove orphan Okta ID users
    # that are no longer referenced anywhere
    # First, find Okta IDs in users table that have no person_id
    # and are not referenced by any table
    op.execute(sa.text("""
        DELETE FROM users
        WHERE id ~ '^0(0u|oa)[a-zA-Z0-9]+$'
          AND person_id IS NULL
    """))


def downgrade():
    # This cleanup is not easily reversible - the original Okta IDs
    # would need to be restored from a backup.
    # Downgrade is a no-op; manual intervention required if needed.
    pass
