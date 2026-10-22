"""ateam_merge: workflow_tag workflow_tag_id / curation_tag -> FK ontologyterm, with history

Revision ID: d8f0b2c4e695
Revises: c7e9a1b3d584
Create Date: 2026-10-23 10:00:00.000000

SCRUM-6720 Phase 1, the history pattern of a9c1e3f5b072 on the largest curie table:

  workflow_tag.workflow_tag_id -> workflow_tag_term_id (ATPTerm, NOT NULL, indexed)
  workflow_tag.curation_tag    -> curation_tag_id      (ATPTerm)

The id column of workflow_tag_id has a new name (workflow_tag_term_id, not workflow_tag_id_id or a
reused workflow_tag_id), so raw SQL still naming workflow_tag_id fails loudly. The model keeps
workflow_tag_id as the curie. workflow_tag_version gets the same id columns and *_id_mod flags copied
from the old flags; its index on workflow_tag_id moves to workflow_tag_term_id.

The upgrade stops, listing the values, if any live or history value is not an ATP term (none on the
merged test DB, 2026-10-23: 2,155,301 live and 3,007,055 history rows). uq_workflow_tag_mod_ref_tag
moves to workflow_tag_term_id; the LIKE 'ATP:%' CHECK goes. No triggers, functions or views name these
columns.
"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'd8f0b2c4e695'
down_revision = 'c7e9a1b3d584'
branch_labels = None
depends_on = None

TABLE = 'workflow_tag'
HISTORY = 'workflow_tag_version'
# old curie column -> new id column
COLUMNS = (('workflow_tag_id', 'workflow_tag_term_id'), ('curation_tag', 'curation_tag_id'))


def _resolve(table: str, column: str, id_column: str) -> None:
    """Add id_column to table, fill it from the curie column, stop if a non-NULL curie is not an ATP term."""
    op.execute(f"ALTER TABLE {table} ADD COLUMN {id_column} bigint")
    op.execute(f"UPDATE {table} t SET {id_column} = ot.id FROM ontologyterm ot "
               f"WHERE ot.curie = t.{column} AND ot.ontologytermtype = 'ATPTerm'")
    unresolved = op.get_bind().execute(text(
        f"SELECT {column}, count(*) FROM {table} WHERE {column} IS NOT NULL AND {id_column} IS NULL "
        "GROUP BY 1 ORDER BY 2 DESC")).fetchall()
    if unresolved:
        raise RuntimeError(f"{table}.{column} values that are not ATP terms in ontologyterm: "
                           + ", ".join(f"{value} ({count})" for value, count in unresolved))


def upgrade():
    for column, id_column in COLUMNS:
        _resolve(TABLE, column, id_column)
        op.execute(f"ALTER TABLE {TABLE} ADD CONSTRAINT {TABLE}_{id_column}_fkey "
                   f"FOREIGN KEY ({id_column}) REFERENCES ontologyterm (id)")
    op.execute(f"ALTER TABLE {TABLE} ALTER COLUMN workflow_tag_term_id SET NOT NULL")
    op.execute(f"CREATE INDEX ix_{TABLE}_workflow_tag_term_id ON {TABLE} (workflow_tag_term_id)")
    op.execute(f"ALTER TABLE {TABLE} DROP CONSTRAINT uq_workflow_tag_mod_ref_tag")
    op.execute(f"ALTER TABLE {TABLE} ADD CONSTRAINT uq_workflow_tag_mod_ref_tag "
               "UNIQUE (mod_id, reference_id, workflow_tag_term_id)")
    op.execute(f"ALTER TABLE {TABLE} DROP CONSTRAINT ck_workflow_tag_id_prefix")
    op.execute(f"DROP INDEX ix_{TABLE}_workflow_tag_id")
    op.execute(f"ALTER TABLE {TABLE} DROP COLUMN workflow_tag_id, DROP COLUMN curation_tag")

    for column, id_column in COLUMNS:
        _resolve(HISTORY, column, id_column)
        op.execute(f"ALTER TABLE {HISTORY} ADD COLUMN {id_column}_mod BOOLEAN NOT NULL DEFAULT false")
        op.execute(f"UPDATE {HISTORY} SET {id_column}_mod = {column}_mod")
    op.execute(f"CREATE INDEX ix_{HISTORY}_workflow_tag_term_id ON {HISTORY} (workflow_tag_term_id)")
    op.execute(f"DROP INDEX ix_{HISTORY}_workflow_tag_id")
    op.execute(f"ALTER TABLE {HISTORY} DROP COLUMN workflow_tag_id, DROP COLUMN workflow_tag_id_mod, "
               "DROP COLUMN curation_tag, DROP COLUMN curation_tag_mod")


def downgrade():
    for column, id_column in COLUMNS:
        op.execute(f"ALTER TABLE {HISTORY} ADD COLUMN {column} varchar, "
                   f"ADD COLUMN {column}_mod BOOLEAN NOT NULL DEFAULT false")
        op.execute(f"UPDATE {HISTORY} h SET {column} = ot.curie FROM ontologyterm ot WHERE ot.id = h.{id_column}")
        op.execute(f"UPDATE {HISTORY} SET {column}_mod = {id_column}_mod")
    op.execute(f"CREATE INDEX ix_{HISTORY}_workflow_tag_id ON {HISTORY} (workflow_tag_id)")
    op.execute(f"ALTER TABLE {HISTORY} DROP COLUMN workflow_tag_term_id, DROP COLUMN workflow_tag_term_id_mod, "
               "DROP COLUMN curation_tag_id, DROP COLUMN curation_tag_id_mod")

    for column, id_column in COLUMNS:
        op.execute(f"ALTER TABLE {TABLE} ADD COLUMN {column} varchar")
        op.execute(f"UPDATE {TABLE} t SET {column} = ot.curie FROM ontologyterm ot WHERE ot.id = t.{id_column}")
    op.execute(f"ALTER TABLE {TABLE} ALTER COLUMN workflow_tag_id SET NOT NULL")
    op.execute(f"CREATE INDEX ix_{TABLE}_workflow_tag_id ON {TABLE} (workflow_tag_id)")
    op.execute(f"ALTER TABLE {TABLE} ADD CONSTRAINT ck_workflow_tag_id_prefix CHECK (workflow_tag_id LIKE 'ATP:%')")
    op.execute(f"ALTER TABLE {TABLE} DROP CONSTRAINT uq_workflow_tag_mod_ref_tag")
    op.execute(f"ALTER TABLE {TABLE} ADD CONSTRAINT uq_workflow_tag_mod_ref_tag "
               "UNIQUE (mod_id, reference_id, workflow_tag_id)")
    op.execute(f"ALTER TABLE {TABLE} DROP COLUMN workflow_tag_term_id, DROP COLUMN curation_tag_id")
