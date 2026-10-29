"""ateam_merge: topic_entity_tag ontology term columns -> FK ontologyterm, with history

Revision ID: b7d9f1a3c540
Revises: a5c7e9f1b328
Create Date: 2026-10-24 12:00:00.000000

SCRUM-6720 Phase 1, the history pattern of a9c1e3f5b072 on the last and largest curie table:

  topic_entity_tag.topic        -> topic_id        (ATPTerm, NOT NULL)
  topic_entity_tag.entity_type  -> entity_type_id  (ATPTerm)
  topic_entity_tag.species      -> species_id      (NCBITaxonTerm)
  topic_entity_tag.display_tag  -> display_tag_id  (ATPTerm)
  topic_entity_tag.data_novelty -> data_novelty_id (ATPTerm, NOT NULL)
  topic_entity_tag.data_context -> data_context_id (ATPTerm, NOT NULL)

entity stays a string (Phase 2). On the merged test DB (2026-10-24) the table is 4,158,490 live and
8,647,129 history rows and every value resolves. So each table is rewritten once: the indexes on the
old columns are dropped first, then a single UPDATE sets all six ids (and, on the history table, the
*_id_mod flags copied from the old ones). valid_entity_type_dependencies moves to entity_type_id. The
upgrade stops, listing the values, if any value is not a term of its type. No trigger, function or
view names these columns.
"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'b7d9f1a3c540'
down_revision = 'a5c7e9f1b328'
branch_labels = None
depends_on = None

TABLE = 'topic_entity_tag'
HISTORY = 'topic_entity_tag_version'
# old curie column -> ontologytermtype
COLUMNS = {
    'topic': 'ATPTerm',
    'entity_type': 'ATPTerm',
    'species': 'NCBITaxonTerm',
    'display_tag': 'ATPTerm',
    'data_novelty': 'ATPTerm',
    'data_context': 'ATPTerm',
}
INDEXED = ('topic', 'entity_type', 'species', 'data_novelty', 'data_context')
NOT_NULL = ('topic', 'data_novelty', 'data_context')
CHECK = ("((entity_type{s} IS NOT NULL AND entity IS NOT NULL AND entity_id_validation IS NOT NULL) "
         "OR (entity_type{s} IS NULL AND entity IS NULL AND entity_id_validation IS NULL))")


def _resolve(table: str, history: bool) -> None:
    """Add the id columns to table, fill them from the curies in one UPDATE, stop if a non-NULL
    curie is not a term of its type."""
    for column in INDEXED:
        op.execute(f"DROP INDEX ix_{table}_{column}")
    op.execute(f"ALTER TABLE {table} " + ", ".join(f"ADD COLUMN {column}_id bigint" for column in COLUMNS))
    assignments = [f"{column}_id = (SELECT ot.id FROM ontologyterm ot "
                   f"WHERE ot.curie = t.{column} AND ot.ontologytermtype = '{termtype}')"
                   for column, termtype in COLUMNS.items()]
    if history:
        op.execute(f"ALTER TABLE {table} "
                   + ", ".join(f"ADD COLUMN {column}_id_mod BOOLEAN NOT NULL DEFAULT false" for column in COLUMNS))
        assignments += [f"{column}_id_mod = {column}_mod" for column in COLUMNS]
    op.execute(f"UPDATE {table} t SET " + ", ".join(assignments))
    values = ", ".join(f"('{column}', t.{column}, t.{column}_id)" for column in COLUMNS)
    unresolved = op.get_bind().execute(text(
        f"SELECT x.column_name, x.curie, count(*) FROM {table} t, LATERAL (VALUES {values}) "
        "AS x(column_name, curie, term_id) WHERE x.curie IS NOT NULL AND x.term_id IS NULL "
        "GROUP BY 1, 2 ORDER BY 1, 3 DESC")).fetchall()
    if unresolved:
        raise RuntimeError(f"{table} values that are not terms of their type in ontologyterm: "
                           + ", ".join(f"{column}={value} ({count})" for column, value, count in unresolved))
    for column in INDEXED:
        op.execute(f"CREATE INDEX ix_{table}_{column}_id ON {table} ({column}_id)")


def upgrade():
    _resolve(TABLE, history=False)
    for column in COLUMNS:
        op.execute(f"ALTER TABLE {TABLE} ADD CONSTRAINT {TABLE}_{column}_id_fkey "
                   f"FOREIGN KEY ({column}_id) REFERENCES ontologyterm (id)")
    for column in NOT_NULL:
        op.execute(f"ALTER TABLE {TABLE} ALTER COLUMN {column}_id SET NOT NULL")
    op.execute(f"ALTER TABLE {TABLE} DROP CONSTRAINT valid_entity_type_dependencies")
    op.execute(f"ALTER TABLE {TABLE} ADD CONSTRAINT valid_entity_type_dependencies CHECK {CHECK.format(s='_id')}")
    op.execute(f"ALTER TABLE {TABLE} " + ", ".join(f"DROP COLUMN {column}" for column in COLUMNS))

    _resolve(HISTORY, history=True)
    op.execute(f"ALTER TABLE {HISTORY} "
               + ", ".join(f"DROP COLUMN {column}, DROP COLUMN {column}_mod" for column in COLUMNS))


def _restore(table: str, history: bool) -> None:
    """Add the curie columns back to table and fill them from the ids in one UPDATE."""
    for column in INDEXED:
        op.execute(f"DROP INDEX ix_{table}_{column}_id")
    op.execute(f"ALTER TABLE {table} " + ", ".join(f"ADD COLUMN {column} varchar" for column in COLUMNS))
    assignments = [f"{column} = (SELECT ot.curie FROM ontologyterm ot WHERE ot.id = t.{column}_id)"
                   for column in COLUMNS]
    if history:
        op.execute(f"ALTER TABLE {table} "
                   + ", ".join(f"ADD COLUMN {column}_mod BOOLEAN NOT NULL DEFAULT false" for column in COLUMNS))
        assignments += [f"{column}_mod = {column}_id_mod" for column in COLUMNS]
    op.execute(f"UPDATE {table} t SET " + ", ".join(assignments))
    for column in INDEXED:
        op.execute(f"CREATE INDEX ix_{table}_{column} ON {table} ({column})")


def downgrade():
    _restore(HISTORY, history=True)
    op.execute(f"ALTER TABLE {HISTORY} "
               + ", ".join(f"DROP COLUMN {column}_id, DROP COLUMN {column}_id_mod" for column in COLUMNS))

    _restore(TABLE, history=False)
    for column in NOT_NULL:
        op.execute(f"ALTER TABLE {TABLE} ALTER COLUMN {column} SET NOT NULL")
    op.execute(f"ALTER TABLE {TABLE} DROP CONSTRAINT valid_entity_type_dependencies")
    op.execute(f"ALTER TABLE {TABLE} ADD CONSTRAINT valid_entity_type_dependencies CHECK {CHECK.format(s='')}")
    op.execute(f"ALTER TABLE {TABLE} " + ", ".join(f"DROP COLUMN {column}_id" for column in COLUMNS))
