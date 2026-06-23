"""add_privacy_to_person

Revision ID: c4d5e6f7a8b9
Revises: b3c4d5e6f7a8
Create Date: 2026-06-22

Adds privacy column to person and person_version tables (controlled
vocabulary: show_all / logged_in_only / fully_hidden / hide_email,
default hide_email).
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision = "c4d5e6f7a8b9"
down_revision = "b3c4d5e6f7a8"
branch_labels = None
depends_on = None


def _column_exists(table, column):
    """Check if a column exists on the given table."""
    bind = op.get_bind()
    insp = sa_inspect(bind)
    columns = [col["name"] for col in insp.get_columns(table)]
    return column in columns


def _constraint_exists(table, name):
    """Check if a check constraint exists on the given table."""
    bind = op.get_bind()
    insp = sa_inspect(bind)
    for con in insp.get_check_constraints(table):
        if con["name"] == name:
            return True
    return False


def upgrade():
    # 1. Add privacy column to person
    if not _column_exists("person", "privacy"):
        op.add_column(
            "person",
            sa.Column("privacy", sa.String(), nullable=False,
                      server_default="hide_email"))

    # 2. Add privacy and privacy_mod columns to person_version
    if not _column_exists("person_version", "privacy"):
        op.add_column(
            "person_version",
            sa.Column("privacy", sa.String(),
                      autoincrement=False, nullable=True))
    if not _column_exists("person_version", "privacy_mod"):
        op.add_column(
            "person_version",
            sa.Column("privacy_mod", sa.Boolean(),
                      server_default=sa.text("false"),
                      nullable=False))

    # 3. Add check constraint for the controlled vocabulary
    if not _constraint_exists("person", "ck_person_privacy"):
        op.create_check_constraint(
            "ck_person_privacy",
            "person",
            "privacy IN ('show_all', 'logged_in_only', 'fully_hidden', 'hide_email')")


def downgrade():
    # 1. Drop check constraint
    if _constraint_exists("person", "ck_person_privacy"):
        op.drop_constraint("ck_person_privacy", "person", type_="check")

    # 2. Drop version columns
    if _column_exists("person_version", "privacy_mod"):
        op.drop_column("person_version", "privacy_mod")
    if _column_exists("person_version", "privacy"):
        op.drop_column("person_version", "privacy")

    # 3. Drop privacy column
    if _column_exists("person", "privacy"):
        op.drop_column("person", "privacy")
