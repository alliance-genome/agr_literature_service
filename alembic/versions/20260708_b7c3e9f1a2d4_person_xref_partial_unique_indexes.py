"""person_cross_reference: non-obsolete-only uniqueness

Revision ID: b7c3e9f1a2d4
Revises: 1a1274ba1f52
Create Date: 2026-07-08

Mirrors the Biblio cross_reference table: uniqueness on person_cross_reference
should apply only among non-obsolete rows. The prior plain UniqueConstraints
counted obsolete (soft-deleted) rows, so an old obsolete xref blocked re-adding
the same curie/prefix. Replace them with partial unique indexes gated on
is_obsolete IS FALSE.
"""
from alembic import op
import sqlalchemy as sa


revision = 'b7c3e9f1a2d4'
down_revision = '1a1274ba1f52'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint('uq_person_xref_curie', 'person_cross_reference', type_='unique')
    op.drop_constraint('uq_person_xref_person_prefix', 'person_cross_reference', type_='unique')
    op.create_index(
        'uq_person_xref_curie',
        'person_cross_reference',
        ['curie'],
        unique=True,
        postgresql_where=sa.text('is_obsolete IS false'),
    )
    op.create_index(
        'uq_person_xref_person_prefix',
        'person_cross_reference',
        ['person_id', 'curie_prefix'],
        unique=True,
        postgresql_where=sa.text('is_obsolete IS false AND person_id IS NOT NULL'),
    )


def downgrade():
    op.drop_index('uq_person_xref_person_prefix', table_name='person_cross_reference')
    op.drop_index('uq_person_xref_curie', table_name='person_cross_reference')
    op.create_unique_constraint('uq_person_xref_curie', 'person_cross_reference', ['curie'])
    op.create_unique_constraint(
        'uq_person_xref_person_prefix',
        'person_cross_reference',
        ['person_id', 'curie_prefix'],
    )
