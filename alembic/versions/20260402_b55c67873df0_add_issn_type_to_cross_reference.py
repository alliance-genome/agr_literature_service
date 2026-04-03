"""add_issn_type_to_cross_reference

Revision ID: b55c67873df0
Revises: 203f59588f5c
Create Date: 2026-04-02

Adds issn_type column to cross_reference and cross_reference_version tables.
Relaxes idx_curie and idx_curie_res unique constraints to exclude ISSN prefix,
allowing the same ISSN curie to appear in multiple cross_reference rows.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision = "b55c67873df0"
down_revision = "203f59588f5c"
branch_labels = None
depends_on = None


def _index_exists(table, name):
    """Check if an index exists on the given table."""
    bind = op.get_bind()
    insp = sa_inspect(bind)
    for idx in insp.get_indexes(table):
        if idx["name"] == name:
            return True
    return False


def _column_exists(table, column):
    """Check if a column exists on the given table."""
    bind = op.get_bind()
    insp = sa_inspect(bind)
    columns = [col["name"] for col in insp.get_columns(table)]
    return column in columns


def upgrade():
    # 1. Add issn_type column to cross_reference
    if not _column_exists("cross_reference", "issn_type"):
        op.add_column(
            "cross_reference",
            sa.Column("issn_type", sa.String(), nullable=True))

    # 2. Add issn_type and issn_type_mod columns to cross_reference_version
    if not _column_exists("cross_reference_version", "issn_type"):
        op.add_column(
            "cross_reference_version",
            sa.Column("issn_type", sa.String(),
                      autoincrement=False, nullable=True))
    if not _column_exists("cross_reference_version", "issn_type_mod"):
        op.add_column(
            "cross_reference_version",
            sa.Column("issn_type_mod", sa.Boolean(),
                      server_default=sa.text("false"),
                      nullable=False))

    # 3. Drop and recreate idx_curie to exclude ISSN prefix
    if _index_exists("cross_reference", "idx_curie"):
        op.drop_index("idx_curie", table_name="cross_reference")
    op.create_index(
        "idx_curie", "cross_reference", ["curie"],
        unique=True,
        postgresql_where=sa.text(
            "is_obsolete IS FALSE AND curie_prefix != 'ISSN'"
        )
    )

    # 4. Drop and recreate idx_curie_res to exclude ISSN prefix
    if _index_exists("cross_reference", "idx_curie_res"):
        op.drop_index("idx_curie_res", table_name="cross_reference")
    op.create_index(
        "idx_curie_res", "cross_reference", ["curie", "resource_id"],
        unique=True,
        postgresql_where=sa.text(
            "resource_id IS NOT NULL AND curie_prefix != 'ISSN'"
        )
    )


def downgrade():
    # 1. Restore original idx_curie_res
    if _index_exists("cross_reference", "idx_curie_res"):
        op.drop_index("idx_curie_res", table_name="cross_reference")
    op.create_index(
        "idx_curie_res", "cross_reference", ["curie", "resource_id"],
        unique=True,
        postgresql_where=sa.text("resource_id IS NOT NULL")
    )

    # 2. Restore original idx_curie
    if _index_exists("cross_reference", "idx_curie"):
        op.drop_index("idx_curie", table_name="cross_reference")
    op.create_index(
        "idx_curie", "cross_reference", ["curie"],
        unique=True,
        postgresql_where=sa.text("is_obsolete IS FALSE")
    )

    # 3. Drop version columns
    if _column_exists("cross_reference_version", "issn_type_mod"):
        op.drop_column("cross_reference_version", "issn_type_mod")
    if _column_exists("cross_reference_version", "issn_type"):
        op.drop_column("cross_reference_version", "issn_type")

    # 4. Drop issn_type column
    if _column_exists("cross_reference", "issn_type"):
        op.drop_column("cross_reference", "issn_type")
