"""add_uniq_constraint_to_ref_relation

Revision ID: a286f0a59a44
Revises: 08b0e96a8c49
Create Date: 2025-03-04 04:02:25.411587

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a286f0a59a44'
down_revision = '08b0e96a8c49'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(
        "idx_unique_reference_relation_pair",
        "reference_relation",
        [
            text("LEAST(reference_id_from, reference_id_to)"),
            text("GREATEST(reference_id_from, reference_id_to)")
        ],
        unique=True,
    )


def downgrade():
    op.drop_index("idx_unique_reference_relation_pair", table_name="reference_relation")
