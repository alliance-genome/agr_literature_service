"""add novel_topic_data to topic_entity_tag

Revision ID: 2ceacbde8ac2
Revises: ad9d2e2447f1
Create Date: 2023-10-26 11:03:16.500039

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2ceacbde8ac2'
down_revision = 'ad9d2e2447f1'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('topic_entity_tag', sa.Column('novel_topic_data', sa.Boolean(), server_default='false', nullable=False))
    op.add_column('topic_entity_tag_version', sa.Column('novel_topic_data', sa.Boolean(), server_default='false', autoincrement=False, nullable=True))
    op.add_column('topic_entity_tag_version', sa.Column('novel_topic_data_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('topic_entity_tag_version', 'novel_topic_data_mod')
    op.drop_column('topic_entity_tag_version', 'novel_topic_data')
    op.drop_column('topic_entity_tag', 'novel_topic_data')
    # ### end Alembic commands ###