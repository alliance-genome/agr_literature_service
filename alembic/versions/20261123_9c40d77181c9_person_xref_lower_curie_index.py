"""person_xref_lower_curie_index

Revision ID: 9c40d77181c9
Revises: 2b4c3070f84a
Create Date: 2026-11-23

Adds an index on lower(curie) of the non-obsolete person cross references
(SCRUM-6729). created_by / updated_by values shaped like a person cross reference
(WB:WBPerson1843, ORCID:...) are matched case-insensitively against them, and
without this index each such value scanned the whole table.
"""
from alembic import op


revision = "9c40d77181c9"
down_revision = "2b4c3070f84a"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE INDEX IF NOT EXISTS ix_person_xref_lower_curie "
               "ON person_cross_reference (lower(curie)) WHERE obsolete IS FALSE")


def downgrade():
    op.execute("DROP INDEX IF EXISTS ix_person_xref_lower_curie")
