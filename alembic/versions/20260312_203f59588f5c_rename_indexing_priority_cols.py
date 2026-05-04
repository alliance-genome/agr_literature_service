"""rename_indexing_priority_cols

Revision ID: 203f59588f5c
Revises: 0b9f10606114
Create Date: 2026-03-12

Renames:
  indexing_priority        -> predicted_indexing_priority  (now nullable)
  validation_by_biocurator -> curator_indexing_priority    (+ ATP: check)
Drops:
  source_id column (FK + index)
Changes unique constraint to (mod_id, reference_id) only.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision = "203f59588f5c"
down_revision = "0b9f10606114"
branch_labels = None
depends_on = None


def _constraint_exists(table, name):
    """Check if a constraint exists on the given table."""
    bind = op.get_bind()
    insp = sa_inspect(bind)
    for c in insp.get_unique_constraints(table):
        if c["name"] == name:
            return True
    for c in insp.get_check_constraints(table):
        if c["name"] == name:
            return True
    for c in insp.get_foreign_keys(table):
        if c["name"] == name:
            return True
    return False


def _column_exists(table, column):
    """Check if a column exists on the given table."""
    bind = op.get_bind()
    insp = sa_inspect(bind)
    columns = [col["name"] for col in insp.get_columns(table)]
    return column in columns


def _index_exists(table, name):
    """Check if an index exists on the given table."""
    bind = op.get_bind()
    insp = sa_inspect(bind)
    for idx in insp.get_indexes(table):
        if idx["name"] == name:
            return True
    return False


def upgrade():
    tbl = "indexing_priority"

    # 1. Drop old constraints (may already be gone from partial prior run)
    if _constraint_exists(tbl, "uq_indexing_priority_mod_ref_priority"):
        op.drop_constraint("uq_indexing_priority_mod_ref_priority", tbl, type_="unique")
    if _constraint_exists(tbl, "ck_indexing_priority_prefix"):
        op.drop_constraint("ck_indexing_priority_prefix", tbl, type_="check")

    # 2. Drop source_id FK, index, then column
    if _constraint_exists(tbl, "indexing_priority_source_id_fkey"):
        op.drop_constraint("indexing_priority_source_id_fkey", tbl, type_="foreignkey")
    if _index_exists(tbl, "ix_indexing_priority_source_id"):
        op.drop_index("ix_indexing_priority_source_id", table_name=tbl)
    if _column_exists(tbl, "source_id"):
        op.drop_column(tbl, "source_id")

    # 3. Drop old index on indexing_priority column before rename
    if _index_exists(tbl, "ix_indexing_priority_indexing_priority"):
        op.drop_index("ix_indexing_priority_indexing_priority", table_name=tbl)

    # 4. Rename columns (preserves data) — skip if already renamed
    if _column_exists(tbl, "indexing_priority"):
        op.alter_column(tbl, "indexing_priority", new_column_name="predicted_indexing_priority")
    if _column_exists(tbl, "validation_by_biocurator"):
        op.alter_column(tbl, "validation_by_biocurator", new_column_name="curator_indexing_priority")

    # 5. Make predicted_indexing_priority nullable
    op.alter_column(tbl, "predicted_indexing_priority", existing_type=sa.String(), nullable=True)

    # 6. Replace old 3-col unique constraint with 2-col (mod_id, reference_id)
    if _constraint_exists(tbl, "uq_indexing_priority_mod_ref_predicted"):
        op.drop_constraint("uq_indexing_priority_mod_ref_predicted", tbl, type_="unique")
    if not _constraint_exists(tbl, "uq_indexing_priority_mod_ref"):
        op.create_unique_constraint("uq_indexing_priority_mod_ref", tbl, ["mod_id", "reference_id"])

    # 7. Recreate check constraints
    if not _constraint_exists(tbl, "ck_predicted_indexing_priority_prefix"):
        op.create_check_constraint(
            "ck_predicted_indexing_priority_prefix", tbl,
            "predicted_indexing_priority IS NULL OR predicted_indexing_priority LIKE 'ATP:%'",
        )
    if not _constraint_exists(tbl, "ck_curator_indexing_priority_prefix"):
        op.create_check_constraint(
            "ck_curator_indexing_priority_prefix", tbl,
            "curator_indexing_priority IS NULL OR curator_indexing_priority LIKE 'ATP:%'",
        )

    # 8. At least one priority field must be non-null
    if not _constraint_exists(tbl, "ck_at_least_one_priority"):
        op.create_check_constraint(
            "ck_at_least_one_priority", tbl,
            "predicted_indexing_priority IS NOT NULL OR curator_indexing_priority IS NOT NULL",
        )

    # 9. Recreate index on predicted_indexing_priority
    if not _index_exists(tbl, "ix_indexing_priority_predicted_indexing_priority"):
        op.create_index(
            "ix_indexing_priority_predicted_indexing_priority", tbl,
            ["predicted_indexing_priority"], unique=False,
        )


def downgrade():
    tbl = "indexing_priority"

    # Reverse: drop new constraints/indexes
    if _index_exists(tbl, "ix_indexing_priority_predicted_indexing_priority"):
        op.drop_index("ix_indexing_priority_predicted_indexing_priority", table_name=tbl)
    if _constraint_exists(tbl, "ck_at_least_one_priority"):
        op.drop_constraint("ck_at_least_one_priority", tbl, type_="check")
    if _constraint_exists(tbl, "ck_curator_indexing_priority_prefix"):
        op.drop_constraint("ck_curator_indexing_priority_prefix", tbl, type_="check")
    if _constraint_exists(tbl, "ck_predicted_indexing_priority_prefix"):
        op.drop_constraint("ck_predicted_indexing_priority_prefix", tbl, type_="check")
    if _constraint_exists(tbl, "uq_indexing_priority_mod_ref"):
        op.drop_constraint("uq_indexing_priority_mod_ref", tbl, type_="unique")

    # Revert nullable
    op.alter_column(tbl, "predicted_indexing_priority", existing_type=sa.String(), nullable=False)

    # Rename columns back
    op.alter_column(tbl, "curator_indexing_priority", new_column_name="validation_by_biocurator")
    op.alter_column(tbl, "predicted_indexing_priority", new_column_name="indexing_priority")

    # Re-add source_id column (nullable since original data is lost)
    op.add_column(tbl, sa.Column("source_id", sa.Integer(), nullable=True))
    op.create_index("ix_indexing_priority_source_id", tbl, ["source_id"], unique=False)
    op.create_foreign_key(
        "indexing_priority_source_id_fkey", tbl, "topic_entity_tag_source",
        ["source_id"], ["topic_entity_tag_source_id"], ondelete="CASCADE",
    )

    # Recreate old index and constraints
    op.create_index("ix_indexing_priority_indexing_priority", tbl, ["indexing_priority"], unique=False)
    op.create_check_constraint(
        "ck_indexing_priority_prefix", tbl, "indexing_priority LIKE 'ATP:%'",
    )
    op.create_unique_constraint(
        "uq_indexing_priority_mod_ref_priority", tbl,
        ["mod_id", "reference_id", "indexing_priority"],
    )
