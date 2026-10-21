"""ateam_merge: refuse to run on a database without the A-team tables

Revision ID: c4a1e7b93d20
Revises: 80595e1e0d4d
Create Date: 2026-10-21 12:00:00.000000

SCRUM-6720: step 2 of the shared database with the A-team persistent store. Its revisions live
on the ateam_merge git branch, chained after the main head, and are applied only to the merged
database (ABC + A-team, with A-team's person and reference renamed ateam_person and
ateam_reference):

  alembic -x db_url=postgresql://.../literature_ateam_merge_test upgrade head

This first revision changes nothing. It stops the chain on a database that lacks the A-team
tables the later revisions need, so the ateam_merge revisions cannot be applied to a plain ABC
database by mistake. Plan: docs/plans/2026-10-21-scrum-6720-ateam-merge-plan.md.
"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'c4a1e7b93d20'
down_revision = '80595e1e0d4d'
branch_labels = None
depends_on = None

REQUIRED_ATEAM_TABLES = ('ontologyterm', 'ontologytermclosure', 'ontologyterm_subsets', 'biologicalentity',
                         'ateam_reference', 'ateam_person')


def missing_ateam_tables(bind) -> list:
    return [table for table in REQUIRED_ATEAM_TABLES
            if bind.execute(text("SELECT to_regclass(:t)"), {"t": f"public.{table}"}).scalar() is None]


def upgrade():
    missing = missing_ateam_tables(op.get_bind())
    if missing:
        raise RuntimeError(
            f"not a merged ABC + A-team database (missing: {', '.join(missing)}); the ateam_merge "
            "revisions only run on the merged database, see docs/plans/2026-10-21-scrum-6720-ateam-merge-plan.md")


def downgrade():
    pass
