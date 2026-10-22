"""ateam_merge: dataset.data_type -> FK ontologyterm, with its history table

Revision ID: a9c1e3f5b072
Revises: f2b4d6e8a170
Create Date: 2026-10-22 20:00:00.000000

SCRUM-6720 Phase 1, first versioned table (the pattern for the other history tables):

  dataset.data_type (ATP curie)  ->  data_type_id (bigint, FK ontologyterm.id, ATPTerm)
  dataset_version.data_type, data_type_mod  ->  data_type_id, data_type_id_mod

The history table gets the same id column (no FK, as Continuum makes history columns) and a
data_type_id_mod flag copied from data_type_mod (BOOLEAN NOT NULL DEFAULT false, the convention of
SCRUM-6719's linkml_audited_object). The upgrade stops, listing the values, if a live value is not
an ATP term; a history value that is not (none on the merged test DB, 2026-10-22: 29 live and 72
history rows) would also stop it, so no history is lost silently. The unique key
(mod_id, data_type, dataset_type, version) moves to data_type_id.
"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'a9c1e3f5b072'
down_revision = 'f2b4d6e8a170'
branch_labels = None
depends_on = None

TABLE = 'dataset'
COLUMN = 'data_type'
TERM_TYPES = ('ATPTerm',)


def _resolve(table: str, column: str, term_types) -> None:
    """Add <column>_id to table, fill it from the curie, stop if any non-NULL curie is not a term."""
    op.execute(f"ALTER TABLE {table} ADD COLUMN {column}_id bigint")
    op.get_bind().execute(text(
        f"UPDATE {table} t SET {column}_id = ot.id FROM ontologyterm ot "
        f"WHERE ot.curie = t.{column} AND ot.ontologytermtype = ANY(:types)"), {"types": list(term_types)})
    unresolved = op.get_bind().execute(text(
        f"SELECT {column}, count(*) FROM {table} WHERE {column} IS NOT NULL AND {column}_id IS NULL "
        "GROUP BY 1 ORDER BY 2 DESC")).fetchall()
    if unresolved:
        raise RuntimeError(f"{table}.{column} values that are not {'/'.join(term_types)} in ontologyterm: "
                           + ", ".join(f"{value} ({count})" for value, count in unresolved))


def upgrade():
    _resolve(TABLE, COLUMN, TERM_TYPES)
    op.execute(f"ALTER TABLE {TABLE} ALTER COLUMN {COLUMN}_id SET NOT NULL")
    op.execute(f"ALTER TABLE {TABLE} ADD CONSTRAINT {TABLE}_{COLUMN}_id_fkey "
               f"FOREIGN KEY ({COLUMN}_id) REFERENCES ontologyterm (id)")
    op.execute(f"ALTER TABLE {TABLE} DROP CONSTRAINT unique_dataset")
    op.execute(f"ALTER TABLE {TABLE} ADD CONSTRAINT unique_dataset UNIQUE (mod_id, {COLUMN}_id, dataset_type, version)")
    op.execute(f"ALTER TABLE {TABLE} DROP COLUMN {COLUMN}")

    history = f"{TABLE}_version"
    _resolve(history, COLUMN, TERM_TYPES)
    op.execute(f"ALTER TABLE {history} ADD COLUMN {COLUMN}_id_mod BOOLEAN NOT NULL DEFAULT false")
    op.execute(f"UPDATE {history} SET {COLUMN}_id_mod = {COLUMN}_mod")
    op.execute(f"ALTER TABLE {history} DROP COLUMN {COLUMN}, DROP COLUMN {COLUMN}_mod")


def downgrade():
    history = f"{TABLE}_version"
    op.execute(f"ALTER TABLE {history} ADD COLUMN {COLUMN} varchar, "
               f"ADD COLUMN {COLUMN}_mod BOOLEAN NOT NULL DEFAULT false")
    op.execute(f"UPDATE {history} h SET {COLUMN} = ot.curie, {COLUMN}_mod = h.{COLUMN}_id_mod "
               f"FROM ontologyterm ot WHERE ot.id = h.{COLUMN}_id")
    op.execute(f"UPDATE {history} SET {COLUMN}_mod = {COLUMN}_id_mod WHERE {COLUMN}_id IS NULL")
    op.execute(f"ALTER TABLE {history} DROP COLUMN {COLUMN}_id, DROP COLUMN {COLUMN}_id_mod")

    op.execute(f"ALTER TABLE {TABLE} ADD COLUMN {COLUMN} varchar")
    op.execute(f"UPDATE {TABLE} t SET {COLUMN} = ot.curie FROM ontologyterm ot WHERE ot.id = t.{COLUMN}_id")
    op.execute(f"ALTER TABLE {TABLE} ALTER COLUMN {COLUMN} SET NOT NULL")
    op.execute(f"ALTER TABLE {TABLE} DROP CONSTRAINT unique_dataset")
    op.execute(f"ALTER TABLE {TABLE} ADD CONSTRAINT unique_dataset UNIQUE (mod_id, {COLUMN}, dataset_type, version)")
    op.execute(f"ALTER TABLE {TABLE} DROP COLUMN {COLUMN}_id")
