"""add_new_resource_license_cols

Revision ID: 0b9f10606114
Revises: ba9eada77d41
Create Date: 2026-03-04 00:52:38.793618
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0b9f10606114"
down_revision = "ba9eada77d41"
branch_labels = None
depends_on = None


def upgrade():
    # --- resource ---
    op.add_column("resource", sa.Column("copyright_license_id", sa.Integer(), nullable=True))
    op.add_column("resource", sa.Column("license_list", sa.ARRAY(sa.String()), nullable=True))
    op.add_column("resource", sa.Column("license_start_year", sa.Integer(), nullable=True))

    op.create_index(
        op.f("ix_resource_copyright_license_id"),
        "resource",
        ["copyright_license_id"],
        unique=False,
    )

    op.create_foreign_key(
        "fk_resource_copyright_license_id",
        "resource",
        "copyright_license",
        ["copyright_license_id"],
        ["copyright_license_id"],
        ondelete="SET NULL",
    )

    # --- resource_version (because ResourceModel is versioned) ---
    op.add_column(
        "resource_version",
        sa.Column("copyright_license_id", sa.Integer(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "resource_version",
        sa.Column("license_list", sa.ARRAY(sa.String()), autoincrement=False, nullable=True),
    )
    op.add_column(
        "resource_version",
        sa.Column("license_start_year", sa.Integer(), autoincrement=False, nullable=True),
    )

    op.add_column(
        "resource_version",
        sa.Column("copyright_license_id_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
    )
    op.add_column(
        "resource_version",
        sa.Column("license_list_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
    )
    op.add_column(
        "resource_version",
        sa.Column("license_start_year_mod", sa.Boolean(), server_default=sa.text("false"), nullable=False),
    )

    op.create_index(
        op.f("ix_resource_version_copyright_license_id"),
        "resource_version",
        ["copyright_license_id"],
        unique=False,
    )


def downgrade():
    # --- resource_version ---
    op.drop_index(op.f("ix_resource_version_copyright_license_id"), table_name="resource_version")

    op.drop_column("resource_version", "license_start_year_mod")
    op.drop_column("resource_version", "license_list_mod")
    op.drop_column("resource_version", "copyright_license_id_mod")

    op.drop_column("resource_version", "license_start_year")
    op.drop_column("resource_version", "license_list")
    op.drop_column("resource_version", "copyright_license_id")

    # --- resource ---
    op.drop_constraint("fk_resource_copyright_license_id", "resource", type_="foreignkey")
    op.drop_index(op.f("ix_resource_copyright_license_id"), table_name="resource")

    op.drop_column("resource", "license_start_year")
    op.drop_column("resource", "license_list")
    op.drop_column("resource", "copyright_license_id")
