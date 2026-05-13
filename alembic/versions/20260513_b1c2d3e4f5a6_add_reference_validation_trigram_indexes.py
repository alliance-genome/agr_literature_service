"""add reference validation trigram indexes

Revision ID: b1c2d3e4f5a6
Revises: 0a43280cf638
Create Date: 2026-05-13

Add expression GIN trigram indexes for AI curation validation lookups that
match literature references by partial titles and paper-derived identifiers.
"""
from alembic import op


# revision identifiers, used by Alembic.
revision = "b1c2d3e4f5a6"
down_revision = "0a43280cf638"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_reference_upper_curie_trgm
        ON public.reference USING gin (UPPER(curie) gin_trgm_ops)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_reference_upper_title_trgm
        ON public.reference USING gin (UPPER(title) gin_trgm_ops)
        WHERE title IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_cross_reference_upper_curie_trgm
        ON public.cross_reference USING gin (UPPER(curie) gin_trgm_ops)
        WHERE is_obsolete IS FALSE
        """
    )


def downgrade():
    op.execute("DROP INDEX IF EXISTS ix_cross_reference_upper_curie_trgm")
    op.execute("DROP INDEX IF EXISTS ix_reference_upper_title_trgm")
    op.execute("DROP INDEX IF EXISTS ix_reference_upper_curie_trgm")
    # pg_trgm is intentionally left installed because other migrations and
    # indexes may depend on it.
