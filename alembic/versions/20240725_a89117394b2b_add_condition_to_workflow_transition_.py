"""Add condition to workflow_transition_table

Revision ID: a89117394b2b
Revises: 92dfe508cc9c
Create Date: 2024-07-25 09:58:42.295394

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a89117394b2b'
down_revision = '92dfe508cc9c'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('workflow_transition', sa.Column('condition', sa.String(), nullable=True))
    op.add_column('workflow_transition_version', sa.Column('condition', sa.String(), autoincrement=False, nullable=True))
    op.add_column('workflow_transition_version', sa.Column('condition_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('workflow_transition_version', 'condition_mod')
    op.drop_column('workflow_transition_version', 'condition')
    op.drop_column('workflow_transition', 'condition')
    # ### end Alembic commands ###
