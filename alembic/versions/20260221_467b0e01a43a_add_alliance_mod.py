"""add_alliance_mod

Revision ID: 467b0e01a43a
Revises: abc123def456
Create Date: 2026-02-21 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import table, column


# revision identifiers, used by Alembic.
revision = '467b0e01a43a'
down_revision = 'abc123def456'
branch_labels = None
depends_on = None


def upgrade():
    # Insert the 'alliance' MOD into the mod table
    mod_table = table(
        'mod',
        column('abbreviation', sa.String),
        column('short_name', sa.String),
        column('full_name', sa.String),
        column('taxon_ids', sa.ARRAY(sa.String))
    )

    op.bulk_insert(mod_table, [
        {
            'abbreviation': 'alliance',
            'short_name': 'Alliance',
            'full_name': 'Alliance of Genome Resources',
            'taxon_ids': None
        }
    ])


def downgrade():
    # Remove the 'alliance' MOD from the mod table
    op.execute("DELETE FROM mod WHERE abbreviation = 'alliance'")
