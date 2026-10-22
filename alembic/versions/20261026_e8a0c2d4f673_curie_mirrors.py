"""ateam_merge: trigger-maintained curie mirrors for the search pipeline

Revision ID: e8a0c2d4f673
Revises: d5f7b9c1e262
Create Date: 2026-10-26 10:00:00.000000

SCRUM-6720 Phase 4 (decided 2026-10-22). Debezium/ksqlDB/Elasticsearch need the curies of the
ontology term columns that Phase 1 turned into ontologyterm ids. Each streamed id column gets a
read-only <name>_curie mirror, set by a BEFORE INSERT/UPDATE trigger from ontologyterm
(api/triggers/curie_mirror_sql_func_triggers.py, which also installs them in new databases). ksql
reads the mirrors and emits the old field names, so the ES documents do not change. Streaming
ontologyterm and joining it in ksqlDB instead would put A-team's 492,000 terms through Kafka and
re-emit every referencing row (1.9M tags for ATP:0000005) whenever A-team touched a term.

The mirrors are backfilled here: topic_entity_tag (4.2M rows) and workflow_tag (2.2M) are rewritten
once each; reference only where retraction_status_id is set. The spec is copied, not imported, so
later changes to the app module do not change what this revision did.
"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'e8a0c2d4f673'
down_revision = 'd5f7b9c1e262'
branch_labels = None
depends_on = None

CURIE_MIRRORS = {
    'reference': [('retraction_status_id', 'retraction_status_curie')],
    'tag_source': [('source_evidence_assertion_id', 'source_evidence_assertion_curie')],
    'topic_entity_tag': [('topic_id', 'topic_curie'), ('entity_type_id', 'entity_type_curie'),
                         ('species_id', 'species_curie'), ('display_tag_id', 'display_tag_curie'),
                         ('data_novelty_id', 'data_novelty_curie'), ('data_context_id', 'data_context_curie')],
    'curation_status': [('topic_id', 'topic_curie'), ('curation_status_term_id', 'curation_status_curie')],
    'workflow_tag': [('workflow_tag_term_id', 'workflow_tag_curie')],
    'indexing_priority': [('predicted_indexing_priority_id', 'predicted_indexing_priority_curie'),
                          ('curator_indexing_priority_id', 'curator_indexing_priority_curie')],
    'manual_indexing_tag': [('curation_tag_id', 'curation_tag_curie')],
}


def upgrade():
    for table, pairs in CURIE_MIRRORS.items():
        # IF NOT EXISTS / DROP TRIGGER IF EXISTS: an app start before this revision installs them too
        op.execute(f"ALTER TABLE {table} "
                   + ", ".join(f"ADD COLUMN IF NOT EXISTS {curie} varchar" for _id, curie in pairs))
        assignments = [f"{curie} = (SELECT curie FROM ontologyterm WHERE id = t.{id_column})"
                       for id_column, curie in pairs]
        any_set = " OR ".join(f"t.{id_column} IS NOT NULL" for id_column, _curie in pairs)
        op.execute(f"UPDATE {table} t SET " + ", ".join(assignments) + f" WHERE {any_set}")

        function_body = "\n".join(f"    NEW.{curie} := (SELECT curie FROM ontologyterm WHERE id = NEW.{id_column});"
                                  for id_column, curie in pairs)
        op.execute(f"""CREATE OR REPLACE FUNCTION {table}_curie_mirror()
  RETURNS trigger
  LANGUAGE plpgsql
AS $$
BEGIN
{function_body}
    RETURN NEW;
END;
$$""")
        columns = ", ".join(column for pair in pairs for column in pair)
        op.execute(f"DROP TRIGGER IF EXISTS {table}_curie_mirror ON {table}")
        op.execute(f"CREATE TRIGGER {table}_curie_mirror BEFORE INSERT OR UPDATE OF {columns} ON {table} "
                   f"FOR EACH ROW EXECUTE FUNCTION {table}_curie_mirror()")


def downgrade():
    for table, pairs in CURIE_MIRRORS.items():
        op.execute(f"DROP TRIGGER IF EXISTS {table}_curie_mirror ON {table}")
        op.execute(f"DROP FUNCTION IF EXISTS {table}_curie_mirror()")
        op.execute(f"ALTER TABLE {table} " + ", ".join(f"DROP COLUMN {curie}" for _id, curie in pairs))
