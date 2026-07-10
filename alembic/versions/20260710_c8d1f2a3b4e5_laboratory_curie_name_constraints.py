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


def _guard_existing_data(bind):
    """Fail fast with an actionable message if any existing laboratory row would
    violate the constraints added below, instead of aborting mid-DDL on a raw
    Postgres error. Non-mutating: this only reads."""
    null_curie = bind.execute(
        sa.text("SELECT count(*) FROM laboratory WHERE curie IS NULL")
    ).scalar()
    dup_curie = bind.execute(
        sa.text(
            "SELECT count(*) FROM ("
            "SELECT curie FROM laboratory GROUP BY curie HAVING count(*) > 1"
            ") d"
        )
    ).scalar()
    no_name_or_strain = bind.execute(
        sa.text(
            "SELECT count(*) FROM laboratory "
            "WHERE name IS NULL AND strain_designation IS NULL"
        )
    ).scalar()

    problems = []
    if null_curie:
        problems.append(f"{null_curie} row(s) with NULL curie (blocks NOT NULL)")
    if dup_curie:
        problems.append(f"{dup_curie} duplicate curie value(s) (blocks UNIQUE)")
    if no_name_or_strain:
        problems.append(
            f"{no_name_or_strain} row(s) with both name and strain_designation NULL "
            "(blocks ck_laboratory_name_or_strain)"
        )
    if problems:
        raise RuntimeError(
            "laboratory table has rows that violate the new constraints: "
            + "; ".join(problems)
            + ". Backfill/fix these rows (assign a curie, set a name or "
            "strain_designation, dedupe curie) before running this migration."
        )


def upgrade():
    _guard_existing_data(op.get_bind())
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
