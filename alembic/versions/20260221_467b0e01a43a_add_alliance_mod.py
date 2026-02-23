"""add_alliance_mod

Revision ID: 467b0e01a43a
Revises: 283e37c0f96d
Create Date: 2026-02-21 10:00:00.000000

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "467b0e01a43a"
down_revision = "283e37c0f96d"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()

    # Prefer a valid existing created_by from mod table (avoids FK / NOT NULL surprises)
    user = conn.execute(
        sa.text("SELECT created_by FROM mod WHERE created_by IS NOT NULL LIMIT 1")
    ).scalar()

    if user is None:
        user = "default_user"

    # Insert into mod if not exists. Use NOW() in SQL (not as a bound parameter).
    conn.execute(
        sa.text(
            """
            INSERT INTO mod (abbreviation, short_name, full_name, taxon_ids,
                             date_created, created_by, date_updated, updated_by)
            SELECT :abbr, :short_name, :full_name, NULL,
                   NOW(), :user, NOW(), :user
            WHERE NOT EXISTS (
                SELECT 1 FROM mod WHERE abbreviation = :abbr
            )
            """
        ),
        {
            "abbr": "alliance",
            "short_name": "Alliance",
            "full_name": "Alliance of Genome Resources",
            "user": user,
        },
    )


def downgrade():
    conn = op.get_bind()
    conn.execute(
        sa.text("DELETE FROM mod WHERE abbreviation = :abbr"),
        {"abbr": "alliance"},
    )
