"""nullable negated field

Revision ID: e5923f158550
Revises: a93d99d26cb0
Create Date: 2023-12-19 18:42:13.356732

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e5923f158550'
down_revision = 'a93d99d26cb0'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('topic_entity_tag', 'negated',
               existing_type=sa.BOOLEAN(),
               nullable=True)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('topic_entity_tag', 'negated',
               existing_type=sa.BOOLEAN(),
               nullable=False)
    # ### end Alembic commands ###