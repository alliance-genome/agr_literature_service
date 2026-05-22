"""allow multiple GEO xrefs per reference

Revision ID: a3f7c1d8b4e2
Revises: e7a4d3b8c1f5
Create Date: 2026-05-19

Allows multiple GEO cross_reference rows per reference (one paper can be
associated with many GEO Series accessions). The per-(curie, reference_id)
and global-curie uniqueness indexes still prevent true duplicates.
"""
from alembic import op
import sqlalchemy as sa


revision = 'a3f7c1d8b4e2'
down_revision = 'e7a4d3b8c1f5'
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
            "AND curie_prefix NOT IN ('CGC', 'PDB', 'GEO')"
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
            "AND curie_prefix NOT IN ('CGC', 'PDB')"
        ),
    )
