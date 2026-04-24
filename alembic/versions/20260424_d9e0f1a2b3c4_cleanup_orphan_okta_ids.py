"""cleanup_orphan_okta_ids

Revision ID: d9e0f1a2b3c4
Revises: c8d9e0f1a2b3
Create Date: 2026-04-24

This migration cleans up the admin Okta ID (0oa1cs2ineBqEFiD85d7) in
created_by/updated_by columns, replacing it with 'default_user'.
Also removes the admin Okta ID user from the users table.
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

    # Update the admin Okta ID to 'default_user' in all audit columns
    admin_okta_id = '0oa1cs2ineBqEFiD85d7'

    for table in TABLES_WITH_AUDIT_COLUMNS:
        op.execute(sa.text(f"""
            UPDATE {table}
            SET created_by = 'default_user'
            WHERE created_by = '{admin_okta_id}'
        """))

        op.execute(sa.text(f"""
            UPDATE {table}
            SET updated_by = 'default_user'
            WHERE updated_by = '{admin_okta_id}'
        """))

    # Remove the admin Okta ID user from users table
    op.execute(sa.text(f"""
        DELETE FROM users WHERE id = '{admin_okta_id}'
    """))


def downgrade():
    # This cleanup is not easily reversible - the original Okta IDs
    # would need to be restored from a backup.
    # Downgrade is a no-op; manual intervention required if needed.
    pass
