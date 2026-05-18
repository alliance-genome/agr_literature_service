"""exclude PDB XREF in partial constraint

Revision ID: e7a4d3b8c1f5
Revises: 9f8e7d6c5b4a
Create Date: 2026-05-18

Allows multiple PDB cross_reference rows per reference (one paper can describe
many structures). The per-(curie, reference_id) and global-curie uniqueness
indexes still prevent true duplicates.
"""
from alembic import op
import sqlalchemy as sa


revision = 'e7a4d3b8c1f5'
down_revision = '9f8e7d6c5b4a'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_index('idx_curie_prefix_ref_no_cgc', table_name='cross_reference')
    op.create_index(
        'idx_curie_prefix_ref_no_cgc',
        'cross_reference',
        ['curie_prefix', 'reference_id'],
        unique=True,
        postgresql_where=sa.text(
            "is_obsolete IS false AND reference_id IS NOT NULL "
            "AND curie_prefix NOT IN ('CGC', 'PDB')"
        ),
    )


def downgrade():
    op.drop_index('idx_curie_prefix_ref_no_cgc', table_name='cross_reference')
    op.create_index(
        'idx_curie_prefix_ref_no_cgc',
        'cross_reference',
        ['curie_prefix', 'reference_id'],
        unique=True,
        postgresql_where=sa.text(
            "is_obsolete IS false AND reference_id IS NOT NULL "
            "AND curie_prefix != 'CGC'"
        ),
    )
