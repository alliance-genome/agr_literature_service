"""ateam_merge: reference.retraction_status -> FK ontologyterm, with history

Revision ID: a5c7e9f1b328
Revises: f4b6d8e0a217
Create Date: 2026-10-24 10:00:00.000000

SCRUM-6720 Phase 1, the history pattern of a9c1e3f5b072 on the reference table (ATP terms):

  reference.retraction_status -> retraction_status_id
      (ATP:0000346 retracted, ATP:0000347 partially, ATP:0000348 fully retracted; NULL if not)

reference_version is 8.17M rows (15GB) on the merged test DB (2026-10-24), of which 178,005 have a
value, so its rewrite is avoided: the id column is added without a default, the UPDATEs touch only
rows with a value, and retraction_status_id_mod is added with a constant DEFAULT false (no rewrite on
PostgreSQL 11+) and set true only where retraction_status_mod is. No constraint, index, function or
view names the column. The upgrade stops, listing the values, if any value is not an ATP term (none:
1,549 live rows).
"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'a5c7e9f1b328'
down_revision = 'f4b6d8e0a217'
branch_labels = None
depends_on = None

TABLE = 'reference'
HISTORY = 'reference_version'
COLUMN = 'retraction_status'
ID_COLUMN = 'retraction_status_id'


def _resolve(table: str) -> None:
    """Add the id column to table, fill it from the curie, stop if a non-NULL curie is not an ATP term."""
    op.execute(f"ALTER TABLE {table} ADD COLUMN {ID_COLUMN} bigint")
    op.execute(f"UPDATE {table} t SET {ID_COLUMN} = ot.id FROM ontologyterm ot "
               f"WHERE ot.curie = t.{COLUMN} AND ot.ontologytermtype = 'ATPTerm'")
    unresolved = op.get_bind().execute(text(
        f"SELECT {COLUMN}, count(*) FROM {table} WHERE {COLUMN} IS NOT NULL AND {ID_COLUMN} IS NULL "
        "GROUP BY 1 ORDER BY 2 DESC")).fetchall()
    if unresolved:
        raise RuntimeError(f"{table}.{COLUMN} values that are not ATP terms in ontologyterm: "
                           + ", ".join(f"{value} ({count})" for value, count in unresolved))


def upgrade():
    _resolve(TABLE)
    op.execute(f"ALTER TABLE {TABLE} ADD CONSTRAINT {TABLE}_{ID_COLUMN}_fkey "
               f"FOREIGN KEY ({ID_COLUMN}) REFERENCES ontologyterm (id)")
    op.execute(f"ALTER TABLE {TABLE} DROP COLUMN {COLUMN}")

    _resolve(HISTORY)
    op.execute(f"ALTER TABLE {HISTORY} ADD COLUMN {ID_COLUMN}_mod BOOLEAN NOT NULL DEFAULT false")
    op.execute(f"UPDATE {HISTORY} SET {ID_COLUMN}_mod = true WHERE {COLUMN}_mod")
    op.execute(f"ALTER TABLE {HISTORY} DROP COLUMN {COLUMN}, DROP COLUMN {COLUMN}_mod")


def downgrade():
    op.execute(f"ALTER TABLE {HISTORY} ADD COLUMN {COLUMN} varchar, "
               f"ADD COLUMN {COLUMN}_mod BOOLEAN NOT NULL DEFAULT false")
    op.execute(f"UPDATE {HISTORY} h SET {COLUMN} = ot.curie FROM ontologyterm ot WHERE ot.id = h.{ID_COLUMN}")
    op.execute(f"UPDATE {HISTORY} SET {COLUMN}_mod = true WHERE {ID_COLUMN}_mod")
    op.execute(f"ALTER TABLE {HISTORY} DROP COLUMN {ID_COLUMN}, DROP COLUMN {ID_COLUMN}_mod")

    op.execute(f"ALTER TABLE {TABLE} ADD COLUMN {COLUMN} varchar")
    op.execute(f"UPDATE {TABLE} t SET {COLUMN} = ot.curie FROM ontologyterm ot WHERE ot.id = t.{ID_COLUMN}")
    op.execute(f"ALTER TABLE {TABLE} DROP COLUMN {ID_COLUMN}")
