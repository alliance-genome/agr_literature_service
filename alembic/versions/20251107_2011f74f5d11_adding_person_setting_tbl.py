"""adding_person_setting_tbl

Revision ID: 2011f74f5d11
Revises: 5122e153906e
Create Date: 2025-11-07 18:27:40.260508
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "2011f74f5d11"
down_revision = "5122e153906e"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "person_setting",
        sa.Column("person_setting_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("person_id", sa.Integer(), sa.ForeignKey("person.person_id", ondelete="CASCADE"), nullable=False),
        sa.Column("component_name", sa.String(), nullable=False),
        sa.Column("setting_name", sa.String(), nullable=False),
        sa.Column("default_setting", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column(
            "json_settings",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )

    # Composite lookup index (person_id, component_name)
    op.create_index(
        "ix_person_setting_person_component",
        "person_setting",
        ["person_id", "component_name"],
        unique=False,
    )

    # Partial unique index: only one row with default_setting = true per (person_id, component_name)
    op.execute(
        """
        CREATE UNIQUE INDEX uq_person_setting_one_default
        ON person_setting (person_id, component_name)
        WHERE default_setting = true;
        """
    )


def downgrade():
    # Drop partial unique index first (depends on the table)
    op.execute("DROP INDEX IF EXISTS uq_person_setting_one_default;")

    # Drop the composite index
    op.drop_index("ix_person_setting_person_component", table_name="person_setting")

    # Drop the table
    op.drop_table("person_setting")
