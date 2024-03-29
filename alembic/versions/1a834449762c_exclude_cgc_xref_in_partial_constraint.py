"""exclude CGC XREF in partial constraint

Revision ID: 1a834449762c
Revises: dbb7862ac37d
Create Date: 2023-02-23 01:54:19.639155

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1a834449762c'
down_revision = 'dbb7862ac37d'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_index('idx_curie_prefix_ref_no_cgc', 'cross_reference', ['curie_prefix', 'reference_id'], unique=True, postgresql_where=sa.text("is_obsolete IS false AND reference_id IS NOT NULL AND curie_prefix != 'CGC'"))
    op.drop_index('idx_curie_prefix_reference', table_name='cross_reference')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_index('idx_curie_prefix_reference', 'cross_reference', ['curie_prefix', 'reference_id'], unique=True)
    op.drop_index('idx_curie_prefix_ref_no_cgc', table_name='cross_reference')
    # ### end Alembic commands ###
