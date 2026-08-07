"""author: person_id link + person/author constraints

Revision ID: 9a1c7f2e4d10
Revises: 8603439f2008
Create Date: 2026-08-07
"""
from alembic import op
import sqlalchemy as sa

revision = "9a1c7f2e4d10"
down_revision = "8603439f2008"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("author", sa.Column("person_id", sa.Integer(), nullable=True))
    op.create_foreign_key("author_person_id_fkey", "author", "person",
                          ["person_id"], ["person_id"], ondelete="RESTRICT")
    op.create_index("ix_author_person_id", "author", ["person_id"])

    op.alter_column("author", "reference_id", existing_type=sa.Integer(), nullable=False)

    op.create_check_constraint("ck_author_person_or_order", "author",
                               "person_id IS NOT NULL OR author_order IS NOT NULL")
    op.create_check_constraint(
        "ck_person_only_link_only", "author",
        "author_order IS NOT NULL OR ("
        "name IS NULL AND first_name IS NULL AND last_name IS NULL "
        "AND first_initial IS NULL AND orcid IS NULL AND affiliations IS NULL "
        "AND COALESCE(first_author, false) = false "
        "AND COALESCE(corresponding_author, false) = false)")

    op.create_index("uq_author_ref_person", "author", ["reference_id", "person_id"], unique=True)
    op.create_index("uq_author_ref_order", "author", ["reference_id", "author_order"], unique=True)


def downgrade():
    op.drop_index("uq_author_ref_order", table_name="author")
    op.drop_index("uq_author_ref_person", table_name="author")
    op.drop_constraint("ck_person_only_link_only", "author", type_="check")
    op.drop_constraint("ck_author_person_or_order", "author", type_="check")
    op.alter_column("author", "reference_id", existing_type=sa.Integer(), nullable=True)
    op.drop_index("ix_author_person_id", table_name="author")
    op.drop_constraint("author_person_id_fkey", "author", type_="foreignkey")
    op.drop_column("author", "person_id")
