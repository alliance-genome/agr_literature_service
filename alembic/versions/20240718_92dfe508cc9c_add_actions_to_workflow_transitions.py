"""Add actions to workflow_transitions

Revision ID: 92dfe508cc9c
Revises: 70945a51c994
Create Date: 2024-07-18 10:49:38.669804

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '92dfe508cc9c'
down_revision = '70945a51c994'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('workflow_transition', sa.Column('actions', sa.ARRAY(sa.String()), nullable=True))
    op.add_column('workflow_transition_version', sa.Column('actions', sa.ARRAY(sa.String()), autoincrement=False, nullable=True))
    op.add_column('workflow_transition_version', sa.Column('actions_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('workflow_transition_version', 'actions_mod')
    op.drop_column('workflow_transition_version', 'actions')
    op.drop_column('workflow_transition', 'actions')
    # ### end Alembic commands ###