"""add_complex_portal_to_mod_corpus_sort_source

Revision ID: 9923dad57b42
Revises: 039dbc7de1ef
Create Date: 2026-11-08

Adds 'Complex_portal' to the modcorpussortsourcetype enum, for papers that
load_complex_papers.py adds to the SGD corpus from the Complex Portal
(SCRUM-6833).
"""
from alembic import op


revision = "9923dad57b42"
down_revision = "039dbc7de1ef"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TYPE modcorpussortsourcetype ADD VALUE IF NOT EXISTS 'Complex_portal'")


def downgrade():
    # PostgreSQL can't drop an enum value; see
    # 20260414_add_gaf_interaction_to_mod_corpus_sort_source.py.
    pass
