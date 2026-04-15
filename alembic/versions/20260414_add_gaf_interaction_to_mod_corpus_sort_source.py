"""add_gaf_and_interaction_to_mod_corpus_sort_source

Revision ID: 8a3f5c7d9e2b
Revises: 78e9d4f87ed5
Create Date: 2026-04-14

Adds 'Gaf' and 'Interaction' values to the modcorpussortsourcetype enum.
"""
from alembic import op


revision = "8a3f5c7d9e2b"
down_revision = "78e9d4f87ed5"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TYPE modcorpussortsourcetype ADD VALUE IF NOT EXISTS 'Gaf'")
    op.execute("ALTER TYPE modcorpussortsourcetype ADD VALUE IF NOT EXISTS 'Interaction'")


def downgrade():
    # PostgreSQL doesn't support removing enum values directly.
    # To remove these values, you would need to:
    # 1. Create a new enum type without these values
    # 2. Update all columns to use the new type
    # 3. Drop the old type and rename the new one
    # This is typically not done in production.
    pass
