"""cross_reference: partial functional index on lower(curie) for DOI lookups

The monthly DOI backfill scripts (SCRUM-4525) compare DOIs
case-insensitively (WHERE curie_prefix = 'DOI' AND lower(curie) IN ...),
which no existing index covers — idx_curie and friends are on the raw
curie column. Without this, every flush window (up to ~100 per CrossRef
run) sequential-scans cross_reference (~4M rows).

Operational note: if the CONCURRENTLY build is interrupted (deadlock,
cancelled statement, deploy timeout), Postgres leaves the index behind
marked invalid — and on re-run IF NOT EXISTS will see it and "succeed"
without rebuilding, leaving an index that writes maintain but the
planner never uses. After applying, verify with
    SELECT indisvalid FROM pg_index
    WHERE indexrelid = 'idx_cross_reference_doi_lower'::regclass;
and if it is false, DROP INDEX CONCURRENTLY idx_cross_reference_doi_lower
before running the migration again.

Revision ID: b7c1d0e2a4f6
Revises: 9a1c7f2e4d10
Create Date: 2026-08-28
"""
from alembic import op

revision = "b7c1d0e2a4f6"
down_revision = "9a1c7f2e4d10"
branch_labels = None
depends_on = None

INDEX_NAME = "idx_cross_reference_doi_lower"


def upgrade():
    # CREATE INDEX CONCURRENTLY cannot run inside a transaction block, and
    # non-concurrent creation would lock a hot 4M-row table.
    with op.get_context().autocommit_block():
        op.execute(
            f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {INDEX_NAME} "
            f"ON cross_reference (lower(curie)) WHERE curie_prefix = 'DOI'"
        )


def downgrade():
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {INDEX_NAME}")
