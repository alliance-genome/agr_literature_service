"""person_note_existing_notes_internal

Revision ID: c4e1a7d90b23
Revises: 9923dad57b42
Create Date: 2026-11-17

Every person_note written before the API could set ``internal`` is a private
curator note (SGD's colleague notes, WormBase's curator comments), but
migration 47d74bfbbe87 gave them the column default, false (public). Mark them
internal (SCRUM-6889).

A database that already holds an internal note has had its flags set on
purpose (4005 was set by hand, and WormBase's import writes public and private
notes), so it is left alone: running there would make public notes private.

A plain UPDATE: it writes no person_note_version rows.
"""
from alembic import op


revision = "c4e1a7d90b23"
down_revision = "9923dad57b42"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        UPDATE person_note SET internal = true
         WHERE NOT internal
           AND NOT EXISTS (SELECT 1 FROM person_note WHERE internal)
    """)


def downgrade():
    # which notes were public before can't be told apart afterwards
    pass
