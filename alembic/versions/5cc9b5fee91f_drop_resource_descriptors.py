"""drop resource_descriptors tables (moved to in-memory cache)

Revision ID: 5cc9b5fee91f
Revises: f2a9c4d16b83
Create Date: 2026-07-20

"""
from alembic import op
import sqlalchemy as sa


revision = "5cc9b5fee91f"
down_revision = "f2a9c4d16b83"
branch_labels = None
depends_on = None


def upgrade():
    # version tables first (if SQLAlchemy-Continuum created them), then base tables.
    op.execute("DROP TABLE IF EXISTS resource_descriptor_pages_version CASCADE")
    op.execute("DROP TABLE IF EXISTS resource_descriptors_version CASCADE")
    op.execute("DROP TABLE IF EXISTS resource_descriptor_pages CASCADE")
    op.execute("DROP TABLE IF EXISTS resource_descriptors CASCADE")


def downgrade():
    op.create_table(
        "resource_descriptors",
        sa.Column("resource_descriptor_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("db_prefix", sa.String(), nullable=False, unique=True),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("aliases", sa.ARRAY(sa.String()), nullable=True),
        sa.Column("example_gid", sa.String(), nullable=True),
        sa.Column("gid_pattern", sa.String(), nullable=True),
        sa.Column("default_url", sa.String(), nullable=True),
    )
    op.create_table(
        "resource_descriptor_pages",
        sa.Column("resource_descriptor_pages_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("url", sa.String(), nullable=False),
        sa.Column("resource_descriptor_id", sa.Integer(),
                  sa.ForeignKey("resource_descriptors.resource_descriptor_id", ondelete="CASCADE"),
                  index=True),
    )
