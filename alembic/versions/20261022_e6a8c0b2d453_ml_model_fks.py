"""ateam_merge: ml_model topic / species / data_novelty / data_context -> FK ontologyterm

Revision ID: e6a8c0b2d453
Revises: d3f5a7c9e142
Create Date: 2026-10-22 16:00:00.000000

SCRUM-6720 Phase 1, the pattern of b8d2f4e6a031 on four nullable columns:

  ml_model.topic         -> topic_id         (ATPTerm, indexed)
  ml_model.species       -> species_id       (NCBITaxonTerm)
  ml_model.data_novelty  -> data_novelty_id  (ATPTerm)
  ml_model.data_context  -> data_context_id  (ATPTerm)

The upgrade stops, listing the values, if any non-NULL value is not a term of the column's type
(none on the merged test DB, 2026-10-22: 53 / 17 / 47 / 25 values). The unique key
(task_type, mod_id, topic, version_num) moves to topic_id. No history table.
"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'e6a8c0b2d453'
down_revision = 'd3f5a7c9e142'
branch_labels = None
depends_on = None

COLUMNS = (('topic', 'ATPTerm'), ('species', 'NCBITaxonTerm'), ('data_novelty', 'ATPTerm'),
           ('data_context', 'ATPTerm'))


def upgrade():
    for column, term_type in COLUMNS:
        op.execute(f"ALTER TABLE ml_model ADD COLUMN {column}_id bigint")
        op.execute(f"""
            UPDATE ml_model m SET {column}_id = ot.id
            FROM ontologyterm ot
            WHERE ot.curie = m.{column} AND ot.ontologytermtype = '{term_type}'
        """)
        unresolved = op.get_bind().execute(text(
            f"SELECT {column}, count(*) FROM ml_model WHERE {column} IS NOT NULL AND {column}_id IS NULL "
            "GROUP BY 1 ORDER BY 2 DESC")).fetchall()
        if unresolved:
            raise RuntimeError(f"ml_model.{column} values that are not {term_type}s in ontologyterm: "
                               + ", ".join(f"{value} ({count})" for value, count in unresolved))
        op.execute(f"ALTER TABLE ml_model ADD CONSTRAINT ml_model_{column}_id_fkey "
                   f"FOREIGN KEY ({column}_id) REFERENCES ontologyterm (id)")
    op.execute("CREATE INDEX ix_ml_model_topic_id ON ml_model (topic_id)")
    op.execute("ALTER TABLE ml_model DROP CONSTRAINT uq_ml_model_task_mod_topic_version")
    op.execute("ALTER TABLE ml_model ADD CONSTRAINT uq_ml_model_task_mod_topic_version "
               "UNIQUE (task_type, mod_id, topic_id, version_num)")
    op.execute("DROP INDEX ix_ml_model_topic")
    for column, _term_type in COLUMNS:
        op.execute(f"ALTER TABLE ml_model DROP COLUMN {column}")


def downgrade():
    for column, _term_type in COLUMNS:
        op.execute(f"ALTER TABLE ml_model ADD COLUMN {column} varchar")
        op.execute(f"UPDATE ml_model m SET {column} = ot.curie FROM ontologyterm ot WHERE ot.id = m.{column}_id")
    op.execute("CREATE INDEX ix_ml_model_topic ON ml_model (topic)")
    op.execute("ALTER TABLE ml_model DROP CONSTRAINT uq_ml_model_task_mod_topic_version")
    op.execute("ALTER TABLE ml_model ADD CONSTRAINT uq_ml_model_task_mod_topic_version "
               "UNIQUE (task_type, mod_id, topic, version_num)")
    for column, _term_type in COLUMNS:
        op.execute(f"ALTER TABLE ml_model DROP COLUMN {column}_id")
