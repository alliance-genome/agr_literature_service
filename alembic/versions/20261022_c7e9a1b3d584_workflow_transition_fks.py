"""ateam_merge: workflow_transition transition_from / transition_to -> FK ontologyterm, with history

Revision ID: c7e9a1b3d584
Revises: a9c1e3f5b072
Create Date: 2026-10-22 21:00:00.000000

SCRUM-6720 Phase 1, the history pattern of a9c1e3f5b072 on two NOT NULL columns:

  workflow_transition.transition_from -> transition_from_id (ATPTerm)
  workflow_transition.transition_to   -> transition_to_id   (ATPTerm)

and in workflow_transition_version the same id columns plus <column>_id_mod flags copied from the
<column>_mod flags. The upgrade stops, listing the values, if any live or history value is not an
ATP term (none on the merged test DB, 2026-10-22: 543 live and 482 history rows). No constraints or
indexes name these columns.

START_STATE: SCRUM-6754 (lit_processing/oneoff_scripts/SCRUM-6754_replace_start_state.py, run first)
replaced the made-up transition_from value START_STATE with ATP:0000177 (workflow process) in the live rows
(ZFIN rows 812 and 824), and left it in their history rows as the record of what was there. A
history column cannot hold a value that is not a term, so the history rows are mapped to
ATP:0000177 too (decided 2026-10-22); the downgrade does not restore START_STATE. A live
START_STATE still stops the upgrade: run the script first.
"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'c7e9a1b3d584'
down_revision = 'a9c1e3f5b072'
branch_labels = None
depends_on = None

TABLE = 'workflow_transition'
COLUMNS = ('transition_from', 'transition_to')
TERM_TYPES = ('ATPTerm',)
START_STATE, WORKFLOW_PROCESS = 'START_STATE', 'ATP:0000177'


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
    history = f"{TABLE}_version"
    for column in COLUMNS:
        _resolve(TABLE, column, TERM_TYPES)
        op.execute(f"ALTER TABLE {TABLE} ALTER COLUMN {column}_id SET NOT NULL")
        op.execute(f"ALTER TABLE {TABLE} ADD CONSTRAINT {TABLE}_{column}_id_fkey "
                   f"FOREIGN KEY ({column}_id) REFERENCES ontologyterm (id)")
        op.execute(f"ALTER TABLE {TABLE} DROP COLUMN {column}")

        op.get_bind().execute(text(f"UPDATE {history} SET {column} = :process WHERE {column} = :start"),
                              {"process": WORKFLOW_PROCESS, "start": START_STATE})
        _resolve(history, column, TERM_TYPES)
        op.execute(f"ALTER TABLE {history} ADD COLUMN {column}_id_mod BOOLEAN NOT NULL DEFAULT false")
        op.execute(f"UPDATE {history} SET {column}_id_mod = {column}_mod")
        op.execute(f"ALTER TABLE {history} DROP COLUMN {column}, DROP COLUMN {column}_mod")


def downgrade():
    history = f"{TABLE}_version"
    for column in COLUMNS:
        op.execute(f"ALTER TABLE {history} ADD COLUMN {column} varchar, "
                   f"ADD COLUMN {column}_mod BOOLEAN NOT NULL DEFAULT false")
        op.execute(f"UPDATE {history} h SET {column} = ot.curie FROM ontologyterm ot WHERE ot.id = h.{column}_id")
        op.execute(f"UPDATE {history} SET {column}_mod = {column}_id_mod")
        op.execute(f"ALTER TABLE {history} DROP COLUMN {column}_id, DROP COLUMN {column}_id_mod")

        op.execute(f"ALTER TABLE {TABLE} ADD COLUMN {column} varchar")
        op.execute(f"UPDATE {TABLE} t SET {column} = ot.curie FROM ontologyterm ot WHERE ot.id = t.{column}_id")
        op.execute(f"ALTER TABLE {TABLE} ALTER COLUMN {column} SET NOT NULL")
        op.execute(f"ALTER TABLE {TABLE} DROP COLUMN {column}_id")
