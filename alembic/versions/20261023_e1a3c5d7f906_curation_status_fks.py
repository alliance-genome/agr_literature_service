"""ateam_merge: curation_status and its source associations -> FK ontologyterm, with history

Revision ID: e1a3c5d7f906
Revises: d8f0b2c4e695
Create Date: 2026-10-23 12:00:00.000000

SCRUM-6720 Phase 1, the history pattern of a9c1e3f5b072 on two related tables (ATP terms):

  curation_status.topic                              -> topic_id (NOT NULL, indexed)
  curation_status.curation_status                    -> curation_status_term_id
  curation_status.curation_tag                       -> curation_tag_id
  curation_status_source_association.curation_status -> curation_status_term_id
  curation_status_source_association.curation_tag    -> curation_tag_id

curation_status is stored as curation_status_term_id: curation_status_id is the key attribute of
curation_status and, on the source associations, the FK to it (the same reason as
workflow_tag_term_id). The _version tables get the id columns and *_mod flags copied from the old
ones; curation_status_unique and the topic indexes move to topic_id. The upgrade stops, listing the
values, if any live or history value is not an ATP term (none on the merged test DB, 2026-10-23:
23,653 / 23,632 live and 35 / 14 history rows).
"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'e1a3c5d7f906'
down_revision = 'd8f0b2c4e695'
branch_labels = None
depends_on = None

# table -> ((old curie column, new id column), ...)
TABLES = {
    'curation_status': (('topic', 'topic_id'), ('curation_status', 'curation_status_term_id'),
                        ('curation_tag', 'curation_tag_id')),
    'curation_status_source_association': (('curation_status', 'curation_status_term_id'),
                                           ('curation_tag', 'curation_tag_id')),
}


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
    for table, columns in TABLES.items():
        for column, id_column in columns:
            _resolve(table, column, id_column)
            op.execute(f"ALTER TABLE {table} ADD CONSTRAINT {table}_{id_column}_fkey "
                       f"FOREIGN KEY ({id_column}) REFERENCES ontologyterm (id)")
            history = f"{table}_version"
            _resolve(history, column, id_column)
            op.execute(f"ALTER TABLE {history} ADD COLUMN {id_column}_mod BOOLEAN NOT NULL DEFAULT false")
            op.execute(f"UPDATE {history} SET {id_column}_mod = {column}_mod")

    op.execute("ALTER TABLE curation_status ALTER COLUMN topic_id SET NOT NULL")
    op.execute("CREATE INDEX ix_curation_status_topic_id ON curation_status (topic_id)")
    op.execute("CREATE INDEX ix_curation_status_version_topic_id ON curation_status_version (topic_id)")
    op.execute("ALTER TABLE curation_status DROP CONSTRAINT curation_status_unique")
    op.execute("ALTER TABLE curation_status ADD CONSTRAINT curation_status_unique UNIQUE (topic_id, reference_id, mod_id)")
    op.execute("DROP INDEX ix_curation_status_topic")
    op.execute("DROP INDEX ix_curation_status_version_topic")

    for table, columns in TABLES.items():
        for column, _id_column in columns:
            op.execute(f"ALTER TABLE {table} DROP COLUMN {column}")
            op.execute(f"ALTER TABLE {table}_version DROP COLUMN {column}, DROP COLUMN {column}_mod")


def downgrade():
    for table, columns in TABLES.items():
        for column, id_column in columns:
            history = f"{table}_version"
            op.execute(f"ALTER TABLE {history} ADD COLUMN {column} varchar, "
                       f"ADD COLUMN {column}_mod BOOLEAN NOT NULL DEFAULT false")
            op.execute(f"UPDATE {history} h SET {column} = ot.curie FROM ontologyterm ot WHERE ot.id = h.{id_column}")
            op.execute(f"UPDATE {history} SET {column}_mod = {id_column}_mod")
            op.execute(f"ALTER TABLE {table} ADD COLUMN {column} varchar")
            op.execute(f"UPDATE {table} t SET {column} = ot.curie FROM ontologyterm ot WHERE ot.id = t.{id_column}")

    op.execute("ALTER TABLE curation_status ALTER COLUMN topic SET NOT NULL")
    op.execute("CREATE INDEX ix_curation_status_topic ON curation_status (topic)")
    op.execute("CREATE INDEX ix_curation_status_version_topic ON curation_status_version (topic)")
    op.execute("ALTER TABLE curation_status DROP CONSTRAINT curation_status_unique")
    op.execute("ALTER TABLE curation_status ADD CONSTRAINT curation_status_unique UNIQUE (topic, reference_id, mod_id)")

    for table, columns in TABLES.items():
        for _column, id_column in columns:
            op.execute(f"ALTER TABLE {table} DROP COLUMN {id_column}")
            op.execute(f"ALTER TABLE {table}_version DROP COLUMN {id_column}, DROP COLUMN {id_column}_mod")
