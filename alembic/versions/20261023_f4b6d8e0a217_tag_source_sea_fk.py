"""ateam_merge: tag_source.source_evidence_assertion -> FK ontologyterm, with history

Revision ID: f4b6d8e0a217
Revises: e1a3c5d7f906
Create Date: 2026-10-23 14:00:00.000000

SCRUM-6720 Phase 1, the history pattern of a9c1e3f5b072 on a column of two term types:

  tag_source.source_evidence_assertion -> source_evidence_assertion_id
      (an ECO term, or for manual assertions ATP:0000035 / ATP:0000036 an ATP term)

First the one value that is not a curie is corrected: the PDB association pipeline wrote
"ECO_0006156" (load_pdb_associations.ECO_AUTOMATIC_ASSERTION, fixed in the same change) for
ECO:0006156, which the GEO pipeline already uses. On the merged test DB (2026-10-23) that is 1 live
row (tag_source 227) and 2 history rows; the corrected value collides with no other row of
tag_source_unique. The downgrade does not restore the typo.

tag_source_version gets the id column and source_evidence_assertion_id_mod; tag_source_unique and
the indexes move to the id. The upgrade stops, listing the values, if any value is still not an ECO
or ATP term (75 live and 61 history rows).
"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'f4b6d8e0a217'
down_revision = 'e1a3c5d7f906'
branch_labels = None
depends_on = None

TABLE = 'tag_source'
HISTORY = 'tag_source_version'
COLUMN = 'source_evidence_assertion'
ID_COLUMN = 'source_evidence_assertion_id'
TERM_TYPES = ('ECOTerm', 'ATPTerm')
TYPO, CURIE = 'ECO_0006156', 'ECO:0006156'


def _resolve(table: str) -> None:
    """Add the id column to table, fill it from the curie, stop if a non-NULL curie is not a term."""
    op.execute(f"ALTER TABLE {table} ADD COLUMN {ID_COLUMN} bigint")
    op.get_bind().execute(text(
        f"UPDATE {table} t SET {ID_COLUMN} = ot.id FROM ontologyterm ot "
        f"WHERE ot.curie = t.{COLUMN} AND ot.ontologytermtype = ANY(:types)"), {"types": list(TERM_TYPES)})
    unresolved = op.get_bind().execute(text(
        f"SELECT {COLUMN}, count(*) FROM {table} WHERE {COLUMN} IS NOT NULL AND {ID_COLUMN} IS NULL "
        "GROUP BY 1 ORDER BY 2 DESC")).fetchall()
    if unresolved:
        raise RuntimeError(f"{table}.{COLUMN} values that are not ECO or ATP terms in ontologyterm: "
                           + ", ".join(f"{value} ({count})" for value, count in unresolved))


def upgrade():
    for table in (TABLE, HISTORY):
        op.get_bind().execute(text(f"UPDATE {table} SET {COLUMN} = :curie WHERE {COLUMN} = :typo"),
                              {"curie": CURIE, "typo": TYPO})

    _resolve(TABLE)
    op.execute(f"ALTER TABLE {TABLE} ALTER COLUMN {ID_COLUMN} SET NOT NULL")
    op.execute(f"ALTER TABLE {TABLE} ADD CONSTRAINT {TABLE}_{ID_COLUMN}_fkey "
               f"FOREIGN KEY ({ID_COLUMN}) REFERENCES ontologyterm (id)")
    op.execute(f"CREATE INDEX ix_{TABLE}_{ID_COLUMN} ON {TABLE} ({ID_COLUMN})")
    op.execute(f"ALTER TABLE {TABLE} DROP CONSTRAINT tag_source_unique")
    op.execute(f"ALTER TABLE {TABLE} ADD CONSTRAINT tag_source_unique "
               f"UNIQUE ({ID_COLUMN}, source_method, data_provider, secondary_data_provider_id)")
    op.execute(f"DROP INDEX ix_{TABLE}_{COLUMN}")
    op.execute(f"ALTER TABLE {TABLE} DROP COLUMN {COLUMN}")

    _resolve(HISTORY)
    op.execute(f"ALTER TABLE {HISTORY} ADD COLUMN {ID_COLUMN}_mod BOOLEAN NOT NULL DEFAULT false")
    op.execute(f"UPDATE {HISTORY} SET {ID_COLUMN}_mod = {COLUMN}_mod")
    op.execute(f"CREATE INDEX ix_{HISTORY}_{ID_COLUMN} ON {HISTORY} ({ID_COLUMN})")
    op.execute(f"DROP INDEX ix_{HISTORY}_{COLUMN}")
    op.execute(f"ALTER TABLE {HISTORY} DROP COLUMN {COLUMN}, DROP COLUMN {COLUMN}_mod")


def downgrade():
    op.execute(f"ALTER TABLE {HISTORY} ADD COLUMN {COLUMN} varchar, ADD COLUMN {COLUMN}_mod BOOLEAN NOT NULL DEFAULT false")
    op.execute(f"UPDATE {HISTORY} h SET {COLUMN} = ot.curie FROM ontologyterm ot WHERE ot.id = h.{ID_COLUMN}")
    op.execute(f"UPDATE {HISTORY} SET {COLUMN}_mod = {ID_COLUMN}_mod")
    op.execute(f"CREATE INDEX ix_{HISTORY}_{COLUMN} ON {HISTORY} ({COLUMN})")
    op.execute(f"ALTER TABLE {HISTORY} DROP COLUMN {ID_COLUMN}, DROP COLUMN {ID_COLUMN}_mod")

    op.execute(f"ALTER TABLE {TABLE} ADD COLUMN {COLUMN} varchar")
    op.execute(f"UPDATE {TABLE} t SET {COLUMN} = ot.curie FROM ontologyterm ot WHERE ot.id = t.{ID_COLUMN}")
    op.execute(f"ALTER TABLE {TABLE} ALTER COLUMN {COLUMN} SET NOT NULL")
    op.execute(f"CREATE INDEX ix_{TABLE}_{COLUMN} ON {TABLE} ({COLUMN})")
    op.execute(f"ALTER TABLE {TABLE} DROP CONSTRAINT tag_source_unique")
    op.execute(f"ALTER TABLE {TABLE} ADD CONSTRAINT tag_source_unique "
               f"UNIQUE ({COLUMN}, source_method, data_provider, secondary_data_provider_id)")
    op.execute(f"ALTER TABLE {TABLE} DROP COLUMN {ID_COLUMN}")
