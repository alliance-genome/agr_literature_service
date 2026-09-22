"""drop person active_status/privacy CHECK constraints

The allowed values are validated by the Pydantic Literal types in
person_schemas.py (ActiveStatus, Privacy), which are the single source of truth
and are also served to the UI via /vocabulary. The enumerated-value CHECK
constraints duplicated that list and forced a migration on every value change,
so they are removed.

Revision ID: 87f14b1e221d
Revises: 5cc9b5fee91f
Create Date: 2026-07-23

"""
from alembic import op


revision = "87f14b1e221d"
down_revision = "5cc9b5fee91f"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TABLE person DROP CONSTRAINT IF EXISTS ck_person_active_status")
    op.execute("ALTER TABLE person DROP CONSTRAINT IF EXISTS ck_person_privacy")


def downgrade():
    op.create_check_constraint(
        "ck_person_active_status", "person",
        "active_status IN ('active', 'retired', 'deceased')",
    )
    op.create_check_constraint(
        "ck_person_privacy", "person",
        "privacy IN ('show_all', 'logged_in_only', 'fully_hidden', 'hide_email')",
    )
