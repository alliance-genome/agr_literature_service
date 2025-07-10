"""add_uniq_constraint_to_wft

Revision ID: 1b775cad9334
Revises: 3cc8bc63529e
Create Date: 2025-07-07 01:39:53.651284
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '1b775cad9334'
down_revision = '3cc8bc63529e'
branch_labels = None
depends_on = None


def upgrade():
    # add unique constraint on mod_id + reference_id + workflow_tag_id
    op.create_unique_constraint(
        'uq_workflow_tag_mod_ref_tag',
        'workflow_tag',
        ['mod_id', 'reference_id', 'workflow_tag_id']
    )
    # ensure workflow_tag_id starts with 'ATP:'
    op.create_check_constraint(
        'ck_workflow_tag_id_prefix',
        'workflow_tag',
        "workflow_tag_id LIKE 'ATP:%'"
    )


def downgrade():
    # drop check constraint first (some DBs require this order)
    op.drop_constraint('ck_workflow_tag_id_prefix', 'workflow_tag', type_='check')
    # then drop the unique constraint
    op.drop_constraint('uq_workflow_tag_mod_ref_tag', 'workflow_tag', type_='unique')
