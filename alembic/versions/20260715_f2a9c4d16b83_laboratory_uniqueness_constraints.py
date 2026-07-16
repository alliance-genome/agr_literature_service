"""laboratory: unique name/strain, non-obsolete xref + allele uniqueness

Revision ID: f2a9c4d16b83
Revises: c8d1f2a3b4e5
Create Date: 2026-07-15

Three related tightenings on the laboratory family:

  1. laboratory.name and laboratory.strain_designation each become unique
     (case-sensitive; NULLs still allowed multiple times).
  2. laboratory_cross_reference uniqueness (curie, and (laboratory_id,
     curie_prefix)) is enforced only among non-obsolete rows, mirroring
     person_cross_reference / the biblio cross_reference table. A soft-deleted
     xref no longer blocks re-adding the same curie/prefix.
  3. laboratory_allele_designation gains an is_obsolete column and its
     (laboratory_id, mod_id) uniqueness becomes non-obsolete-only, same pattern.
"""
from alembic import op
import sqlalchemy as sa


revision = 'f2a9c4d16b83'
down_revision = 'c8d1f2a3b4e5'
branch_labels = None
depends_on = None


def _guard_existing_data(bind):
    """Fail fast with an actionable message if existing laboratory rows would
    violate the new name/strain unique constraints, instead of aborting mid-DDL
    on a raw Postgres error. Non-mutating: this only reads."""
    dup_name = bind.execute(
        sa.text(
            "SELECT count(*) FROM ("
            "SELECT name FROM laboratory WHERE name IS NOT NULL "
            "GROUP BY name HAVING count(*) > 1) d"
        )
    ).scalar()
    dup_strain = bind.execute(
        sa.text(
            "SELECT count(*) FROM ("
            "SELECT strain_designation FROM laboratory "
            "WHERE strain_designation IS NOT NULL "
            "GROUP BY strain_designation HAVING count(*) > 1) d"
        )
    ).scalar()

    problems = []
    if dup_name:
        problems.append(f"{dup_name} duplicate name value(s) (blocks uq_laboratory_name)")
    if dup_strain:
        problems.append(
            f"{dup_strain} duplicate strain_designation value(s) "
            "(blocks uq_laboratory_strain_designation)"
        )
    if problems:
        raise RuntimeError(
            "laboratory table has rows that violate the new unique constraints: "
            + "; ".join(problems)
            + ". Dedupe these rows (make each name/strain_designation unique) "
            "before running this migration."
        )


def upgrade():
    _guard_existing_data(op.get_bind())

    # 1. laboratory: unique name and strain_designation (nullable -> multiple
    #    NULLs remain allowed, matching laboratory_curie_key and siblings).
    op.create_unique_constraint('uq_laboratory_name', 'laboratory', ['name'])
    op.create_unique_constraint(
        'uq_laboratory_strain_designation', 'laboratory', ['strain_designation']
    )

    # 2. laboratory_cross_reference: replace the full unique constraints with
    #    partial unique indexes scoped to non-obsolete rows (mirrors
    #    person_cross_reference).
    op.drop_constraint('uq_laboratory_xref_curie', 'laboratory_cross_reference', type_='unique')
    op.drop_constraint(
        'uq_laboratory_xref_laboratory_prefix', 'laboratory_cross_reference', type_='unique'
    )
    op.create_index(
        'uq_laboratory_xref_curie', 'laboratory_cross_reference', ['curie'],
        unique=True, postgresql_where=sa.text('is_obsolete IS FALSE'),
    )
    op.create_index(
        'uq_laboratory_xref_laboratory_prefix', 'laboratory_cross_reference',
        ['laboratory_id', 'curie_prefix'], unique=True,
        postgresql_where=sa.text('is_obsolete IS FALSE AND laboratory_id IS NOT NULL'),
    )

    # 3. laboratory_allele_designation: add is_obsolete (+ version columns) and
    #    make (laboratory_id, mod_id) uniqueness non-obsolete-only.
    op.add_column(
        'laboratory_allele_designation',
        sa.Column('is_obsolete', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    )
    op.add_column(
        'laboratory_allele_designation_version',
        sa.Column('is_obsolete', sa.Boolean(), autoincrement=False, nullable=True),
    )
    op.add_column(
        'laboratory_allele_designation_version',
        sa.Column('is_obsolete_mod', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    )
    op.drop_constraint(
        'uq_laboratory_allele_designation_lab_mod',
        'laboratory_allele_designation', type_='unique',
    )
    op.create_index(
        'uq_laboratory_allele_designation_lab_mod', 'laboratory_allele_designation',
        ['laboratory_id', 'mod_id'], unique=True,
        postgresql_where=sa.text('is_obsolete IS FALSE'),
    )


def downgrade():
    # 3. laboratory_allele_designation
    op.drop_index('uq_laboratory_allele_designation_lab_mod', table_name='laboratory_allele_designation')
    op.create_unique_constraint(
        'uq_laboratory_allele_designation_lab_mod',
        'laboratory_allele_designation', ['laboratory_id', 'mod_id'],
    )
    op.drop_column('laboratory_allele_designation_version', 'is_obsolete_mod')
    op.drop_column('laboratory_allele_designation_version', 'is_obsolete')
    op.drop_column('laboratory_allele_designation', 'is_obsolete')

    # 2. laboratory_cross_reference
    op.drop_index('uq_laboratory_xref_laboratory_prefix', table_name='laboratory_cross_reference')
    op.drop_index('uq_laboratory_xref_curie', table_name='laboratory_cross_reference')
    op.create_unique_constraint(
        'uq_laboratory_xref_curie', 'laboratory_cross_reference', ['curie']
    )
    op.create_unique_constraint(
        'uq_laboratory_xref_laboratory_prefix',
        'laboratory_cross_reference', ['laboratory_id', 'curie_prefix'],
    )

    # 1. laboratory
    op.drop_constraint('uq_laboratory_strain_designation', 'laboratory', type_='unique')
    op.drop_constraint('uq_laboratory_name', 'laboratory', type_='unique')
