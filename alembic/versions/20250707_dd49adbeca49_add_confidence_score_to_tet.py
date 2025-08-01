"""add_confidence_score_to_tet

Revision ID: dd49adbeca49
Revises: 1b775cad9334
Create Date: 2025-07-07 19:53:10.951756

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'dd49adbeca49'
down_revision = '1b775cad9334'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('topic_entity_tag', sa.Column('confidence_score', sa.Float(), nullable=True))
    op.add_column('topic_entity_tag_version', sa.Column('confidence_score', sa.Float(), autoincrement=False, nullable=True))
    op.add_column('topic_entity_tag_version', sa.Column('confidence_score_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('topic_entity_tag_version', 'confidence_score_mod')
    op.drop_column('topic_entity_tag_version', 'confidence_score')
    op.drop_column('topic_entity_tag', 'confidence_score')
    # ### end Alembic commands ###
