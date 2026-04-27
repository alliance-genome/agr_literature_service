"""cleanup_orphan_okta_ids

Revision ID: d9e0f1a2b3c4
Revises: c8d9e0f1a2b3
Create Date: 2026-04-24

This migration cleans up orphan IDs in created_by/updated_by columns,
replacing them with 'default_user':
1. Admin Okta ID (0oa1cs2ineBqEFiD85d7)
2. UUID-format automation user IDs (AWS Cognito IDs)
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

    # All orphan IDs to clean up:
    # - Admin Okta ID
    # - UUID-format automation user IDs (AWS Cognito IDs)
    orphan_ids = [
        '0oa1cs2ineBqEFiD85d7',  # admin Okta ID
        'b4a874c8-9051-7001-b629-9f86dbabffda',
        '74e854e8-70a1-7001-07e9-7c8d755cd538',
        '14881418-3031-7079-b093-25a52efb4a39',
        '1498f4a8-1041-7032-2de7-85fa5ab77658',
    ]
    placeholders = ", ".join(f"'{uid}'" for uid in orphan_ids)

    for table in TABLES_WITH_AUDIT_COLUMNS:
        op.execute(sa.text(f"""
            UPDATE {table}
            SET created_by = 'default_user'
            WHERE created_by IN ({placeholders})
        """))
        op.execute(sa.text(f"""
            UPDATE {table}
            SET updated_by = 'default_user'
            WHERE updated_by IN ({placeholders})
        """))

    # Note: We do NOT delete these users from the users table
    # because the transaction table (for versioning) references users.user_id
    # and we don't want to orphan historical transaction records.
    # These users are now effectively orphaned from created_by/updated_by columns.


def downgrade():
    # This cleanup is not easily reversible - the original Okta IDs
    # would need to be restored from a backup.
    # Downgrade is a no-op; manual intervention required if needed.
    pass
