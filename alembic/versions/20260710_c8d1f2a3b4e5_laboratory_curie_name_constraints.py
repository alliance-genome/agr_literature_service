"""laboratory: require unique curie and a name or strain_designation

Revision ID: c8d1f2a3b4e5
Revises: b7c3e9f1a2d4
Create Date: 2026-07-10

The laboratory table was created without DB-level constraints on its identifying
fields. Bring it in line with reference/resource/person: curie becomes NOT NULL
and UNIQUE (curie is server-allocated from MATI, so this only makes the existing
guarantee explicit). Also add a check constraint requiring at least one of
strain_designation or name, mirroring ck_at_least_one_priority on indexing_priority
and the API-level validator.
"""
from alembic import op
import sqlalchemy as sa


revision = 'c8d1f2a3b4e5'
down_revision = 'b7c3e9f1a2d4'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('laboratory', 'curie', existing_type=sa.String(), nullable=False)
    op.create_unique_constraint('laboratory_curie_key', 'laboratory', ['curie'])
    op.create_check_constraint(
        'ck_laboratory_name_or_strain',
        'laboratory',
        'strain_designation IS NOT NULL OR name IS NOT NULL',
    )
    # The unique constraint's index serves all curie lookups; drop the now-redundant
    # non-unique index created with the table.
    op.drop_index('ix_laboratory_curie', table_name='laboratory')


def downgrade():
    op.create_index('ix_laboratory_curie', 'laboratory', ['curie'])
    op.drop_constraint('ck_laboratory_name_or_strain', 'laboratory', type_='check')
    op.drop_constraint('laboratory_curie_key', 'laboratory', type_='unique')
    op.alter_column('laboratory', 'curie', existing_type=sa.String(), nullable=True)
