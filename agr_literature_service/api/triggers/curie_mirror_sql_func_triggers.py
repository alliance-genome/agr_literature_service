"""Curie mirrors for the search pipeline (SCRUM-6720 Phase 4).

ABC's ontology term columns hold A-team ontologyterm ids (Phase 1). Debezium streams ABC's rows to
ksqlDB and Elasticsearch, which need the curies. Rather than stream A-team's whole ontologyterm table
(about 492,000 rows, of which ABC uses about 4,500) and join it in ksqlDB, which would re-emit every
referencing row whenever A-team touches a term, each streamed id column has a read-only <name>_curie
column next to it, filled by a BEFORE INSERT/UPDATE trigger from ontologyterm. A term's curie never
changes for its id, so the mirror cannot go stale.

The mirror columns are not mapped in the models: the application never reads or writes them, and
they are not versioned. alembic/env.py leaves them out of autogenerate. Revision e8a0c2d4f673 adds
them to existing databases; add_curie_mirror_triggers (run by create_all_triggers on every start)
adds them to new ones and, as it only creates what is missing, takes no lock once they exist.
"""
from typing import Dict, List, Tuple

from sqlalchemy import text

# table -> [(ontologyterm id column, curie mirror column)]: what debezium/ksql_queries.ksql reads
CURIE_MIRRORS: Dict[str, List[Tuple[str, str]]] = {
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


def mirror_columns(table: str) -> List[str]:
    return [curie for _id_column, curie in CURIE_MIRRORS.get(table, [])]


def mirror_function_sql(table: str) -> str:
    """CREATE OR REPLACE FUNCTION <table>_curie_mirror(): sets each mirror from its id column."""
    assignments = "\n".join(f"    NEW.{curie} := (SELECT curie FROM ontologyterm WHERE id = NEW.{id_column});"
                            for id_column, curie in CURIE_MIRRORS[table])
    return f"""CREATE OR REPLACE FUNCTION {table}_curie_mirror()
  RETURNS trigger
  LANGUAGE plpgsql
AS $$
BEGIN
{assignments}
    RETURN NEW;
END;
$$;"""


def mirror_trigger_sql(table: str) -> str:
    """The trigger: on insert, and on any update of an id column or of a mirror (which it overwrites)."""
    columns = ", ".join(column for pair in CURIE_MIRRORS[table] for column in pair)
    return (f"CREATE TRIGGER {table}_curie_mirror BEFORE INSERT OR UPDATE OF {columns} ON {table} "
            f"FOR EACH ROW EXECUTE FUNCTION {table}_curie_mirror()")


def _install_if_missing_sql(table: str) -> str:
    """Add the missing mirror columns and the trigger if it is missing; nothing (and no lock) otherwise."""
    statements = [f"""
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_schema = 'public' AND table_name = '{table}' AND column_name = '{curie}') THEN
        EXECUTE 'ALTER TABLE {table} ADD COLUMN {curie} varchar';
    END IF;""" for curie in mirror_columns(table)]
    statements.append(f"""
    IF NOT EXISTS (SELECT 1 FROM pg_trigger
                   WHERE tgname = '{table}_curie_mirror' AND tgrelid = 'public.{table}'::regclass) THEN
        EXECUTE '{mirror_trigger_sql(table)}';
    END IF;""")
    return "DO $do$\nBEGIN" + "".join(statements) + "\nEND\n$do$;"


def add_curie_mirror_triggers(db_session):
    for table in CURIE_MIRRORS:
        db_session.execute(text(mirror_function_sql(table)))
        db_session.execute(text(_install_if_missing_sql(table)))
    db_session.commit()
