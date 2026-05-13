"""change email.person_id FK from CASCADE to SET NULL

Revision ID: 0a43280cf638
Revises: 5f3a8e2d1c4b
Create Date: 2026-05-11

This migration changes the foreign key constraint on email.person_id
from ON DELETE CASCADE to ON DELETE SET NULL.

This ensures that when a person is deleted, their emails that have
reference relations are preserved (detached) instead of being deleted
along with their reference_email links.
"""
from alembic import op


# revision identifiers, used by Alembic.
revision = "0a43280cf638"
down_revision = "5f3a8e2d1c4b"
branch_labels = None
depends_on = None


def upgrade():
    # Drop the existing FK constraint with CASCADE
    op.drop_constraint("email_person_id_fkey", "email", type_="foreignkey")

    # Create new FK constraint with SET NULL
    op.create_foreign_key(
        "email_person_id_fkey",
        "email",
        "person",
        ["person_id"],
        ["person_id"],
        ondelete="SET NULL",
    )


def downgrade():
    # Drop the SET NULL FK constraint
    op.drop_constraint("email_person_id_fkey", "email", type_="foreignkey")

    # Restore the original CASCADE FK constraint
    op.create_foreign_key(
        "email_person_id_fkey",
        "email",
        "person",
        ["person_id"],
        ["person_id"],
        ondelete="CASCADE",
    )
