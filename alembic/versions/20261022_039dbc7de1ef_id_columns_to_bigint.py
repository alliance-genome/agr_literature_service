"""ABC id columns -> bigint

Revision ID: 039dbc7de1ef
Revises: 80595e1e0d4d
Create Date: 2026-10-22 12:00:00.000000

SCRUM-6761: A-team conventions before the merge, after SCRUM-6719.

A-team ids are bigint; ABC ids are integer. This turns every ABC id column into bigint: each
primary key, each foreign key and each other *_id column listed in BIGINT_COLUMNS, the same-named
columns of its Continuum history twin (<table>_version), and the sequence behind each primary
key. Other integer columns (author_order, version, image_count, years) stay integer. Continuum's
transaction.id, transaction_id and end_transaction_id are bigint already.

An int4 -> int8 change rewrites the table and rebuilds its indexes, so all of a table's columns
change in one ALTER TABLE (one rewrite per table). Before the rewrites the migration drops what
would make Postgres check or rebuild something twice, or refuse:
  - every foreign key on or to a listed column (re-added, and so re-validated, once at the end);
  - every trigger whose WHEN clause names a listed column (ALTER TYPE refuses to run under it).
A view or rule on a listed column stops the migration instead: none exists today.

Any other integer id column stops the migration too, except in NOT_ABC_TABLES: in the database
merged with the A-team persistent store (SCRUM-6720) A-team's own tables sit next to ABC's, and
Flyway's bookkeeping table has an integer primary key (flyway_schema_history.installed_rank) that
is not an ABC id and is left alone. A-team's own ids are bigint already.

Function signatures do not follow a column type, and CREATE OR REPLACE with other argument
types adds an overload instead of replacing the function (and a new return type is an error),
so the functions in FUNCTION_TYPE_EDITS are dropped and re-created here from exact edits of
their current definition. App startup then re-creates them from api/triggers/*.py with the same
signatures.

Everything runs in one transaction: old table files are kept until commit, so the database
needs about its own size free (46 GB on literature-dev on 2026-10-22, 320 GB free). The
rewritten columns lose their planner statistics, so the tables are analyzed at the end.
"""
import logging
import time
from typing import Dict, List, Tuple

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '039dbc7de1ef'
down_revision = '80595e1e0d4d'
branch_labels = None
depends_on = None

logger = logging.getLogger('alembic.runtime.migration')

# Every integer id column of literature-dev on 2026-10-22: primary keys, foreign keys and the
# remaining *_id columns. History tables follow their table.
BIGINT_COLUMNS: Dict[str, Tuple[str, ...]] = {
    'agent': ('id', 'person_id'),
    'author': ('created_by_id', 'id', 'person_id', 'reference_id', 'updated_by_id'),
    'citation': ('id',),
    'copyright_license': ('id',),
    'cross_reference': ('created_by_id', 'id', 'reference_id', 'resource_id', 'updated_by_id'),
    'curation_status': ('created_by_id', 'id', 'mod_id', 'reference_id', 'updated_by_id'),
    'curation_status_source_association': ('created_by_id', 'curation_status_id', 'id', 'tag_source_id',
                                           'updated_by_id'),
    'dataset': ('created_by_id', 'id', 'mod_id', 'updated_by_id'),
    'dataset_entry': ('dataset_id', 'id', 'supporting_topic_entity_tag_id', 'supporting_workflow_tag_id'),
    'editor': ('created_by_id', 'id', 'resource_id', 'updated_by_id'),
    'embedding_file': ('id', 'parquet_referencefile_id', 'reference_id', 'source_referencefile_id'),
    'image_permission': ('created_by_id', 'id', 'updated_by_id'),
    'indexing_priority': ('created_by_id', 'id', 'mod_id', 'reference_id', 'updated_by_id'),
    'laboratory': ('created_by_id', 'id', 'updated_by_id'),
    'laboratory_allele_designation': ('created_by_id', 'id', 'laboratory_id', 'mod_id', 'updated_by_id'),
    'laboratory_cross_reference': ('created_by_id', 'id', 'laboratory_id', 'updated_by_id'),
    'laboratory_person': ('created_by_id', 'id', 'lab_position_vocab_term_abc_id', 'laboratory_id', 'person_id',
                          'updated_by_id'),
    'manual_indexing_tag': ('created_by_id', 'id', 'mod_id', 'reference_id', 'updated_by_id'),
    'mesh_detail': ('id', 'reference_id'),
    'ml_model': ('created_by_id', 'dataset_id', 'id', 'mod_id', 'updated_by_id'),
    'mod': ('created_by_id', 'id', 'updated_by_id'),
    'mod_corpus_association': ('created_by_id', 'id', 'mod_id', 'reference_id', 'updated_by_id'),
    'mod_referencetype': ('id', 'mod_id', 'referencetype_id'),
    'obsolete_reference_curie': ('id', 'new_id'),
    'person': ('created_by_id', 'id', 'updated_by_id'),
    'person_cross_reference': ('created_by_id', 'id', 'person_id', 'updated_by_id'),
    'person_email': ('created_by_id', 'id', 'person_id', 'updated_by_id'),
    'person_institution': ('created_by_id', 'id', 'person_id', 'updated_by_id'),
    'person_lineage': ('created_by_id', 'id', 'person_object_id', 'person_subject_id',
                       'relationship_vocab_term_abc_id', 'updated_by_id'),
    'person_lineage_submission': ('created_by_id', 'id', 'person_lineage_id', 'person_object_id',
                                  'person_subject_id', 'relationship_vocab_term_abc_id', 'updated_by_id'),
    'person_name': ('created_by_id', 'id', 'person_id', 'updated_by_id'),
    'person_note': ('created_by_id', 'id', 'person_id', 'updated_by_id'),
    'person_setting': ('created_by_id', 'id', 'person_id', 'updated_by_id'),
    'reference': ('citation_id', 'copyright_license_id', 'created_by_id', 'id', 'resource_id', 'updated_by_id'),
    'reference_email': ('created_by_id', 'id', 'reference_id', 'updated_by_id'),
    'reference_mod_md5sum': ('id', 'mod_id', 'reference_id'),
    'reference_mod_referencetype': ('created_by_id', 'id', 'mod_referencetype_id', 'reference_id',
                                    'updated_by_id'),
    'reference_relation': ('id', 'reference_id_from', 'reference_id_to'),
    'referencefile': ('created_by_id', 'id', 'reference_id', 'updated_by_id'),
    'referencefile_mod': ('created_by_id', 'id', 'mod_id', 'referencefile_id', 'updated_by_id'),
    'referencetype': ('id',),
    'resource': ('copyright_license_id', 'created_by_id', 'id', 'updated_by_id'),
    'resource_image_permission': ('created_by_id', 'id', 'image_permission_id', 'resource_id', 'updated_by_id'),
    'tag_source': ('created_by_id', 'id', 'secondary_data_provider_id', 'updated_by_id'),
    'topic_entity_tag': ('created_by_id', 'id', 'ml_model_id', 'reference_id', 'tag_source_id', 'updated_by_id'),
    'topic_entity_tag_validation': ('validated_topic_entity_tag_id', 'validating_topic_entity_tag_id'),
    'transaction': ('user_id',),
    'user_id_migration_backup': ('person_id',),
    'vocabulary_abc': ('created_by_id', 'id', 'updated_by_id'),
    'vocabulary_term_abc': ('created_by_id', 'id', 'updated_by_id', 'vocabulary_abc_id'),
    'vocabulary_term_synonym_abc': ('created_by_id', 'id', 'updated_by_id', 'vocabulary_term_abc_id'),
    'workflow_tag': ('created_by_id', 'id', 'mod_id', 'reference_id', 'updated_by_id'),
    'workflow_tag_topic': ('created_by_id', 'id', 'updated_by_id'),
    'workflow_transition': ('created_by_id', 'id', 'mod_id', 'updated_by_id'),
}

# Exact edits of pg_get_functiondef(), applied in order on upgrade and undone in reverse order
# on downgrade. Each must match exactly once. Counts and years stay integer
# (compute_reference_image_count's result, p_publication_year, author_count).
FUNCTION_TYPE_EDITS: Dict[str, List[Tuple[str, str]]] = {
    'compute_can_display_image': [
        ("p_copyright_license_id integer,", "p_copyright_license_id bigint,"),
        ("p_resource_id integer,", "p_resource_id bigint,")],
    'compute_reference_can_display_image': [
        ("(p_reference_id integer)", "(p_reference_id bigint)")],
    'compute_reference_image_count': [
        ("(p_reference_id integer)", "(p_reference_id bigint)")],
    'get_most_current_email': [
        ("(p_person_id integer)", "(p_person_id bigint)")],
    'get_next_citation_id': [
        ("RETURNS integer", "RETURNS bigint"),
        ("cit_id integer;", "cit_id bigint;")],
    'refresh_can_display_image_for_resource': [
        ("(p_resource_id integer)", "(p_resource_id bigint)")],
    # declared as reference.id%type, which Postgres resolves once, when the procedure is created
    'update_citations': [
        ("(IN ref_id integer)", "(IN ref_id bigint)"),
        ("citation_identifier integer;", "citation_identifier bigint;")],
}


# Tables that are not ABC's and keep their integer keys (see the docstring)
NOT_ABC_TABLES = ('flyway_schema_history',)


def _rows(statement: str, **params) -> List[tuple]:
    return [tuple(row) for row in op.get_bind().execute(text(statement), params)]


def _columns() -> List[Tuple[str, str]]:
    """(table, column) for every listed column and its history twin that exists in this database."""
    existing = set(_rows("SELECT c.relname, a.attname FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
                         "WHERE c.relnamespace = 'public'::regnamespace AND c.relkind = 'r' "
                         "AND a.attnum > 0 AND NOT a.attisdropped"))
    columns: List[Tuple[str, str]] = []
    for table, names in BIGINT_COLUMNS.items():
        for target in (table, f'{table}_version'):
            columns.extend((target, name) for name in names if (target, name) in existing)
    return columns


def _attnums(columns: List[Tuple[str, str]]) -> str:
    """A VALUES list of (relation oid, attnum) for columns, to join against the catalogs."""
    return ', '.join(f"('public.{table}'::regclass, (SELECT attnum FROM pg_attribute WHERE attrelid = "
                     f"'public.{table}'::regclass AND attname = '{name}'))" for table, name in columns)


def _unlisted_integer_ids() -> List[str]:
    """Integer primary keys, foreign keys and *_id columns that BIGINT_COLUMNS does not name."""
    listed = {(table, name) for table, names in BIGINT_COLUMNS.items() for name in names}
    found = _rows(
        "SELECT c.relname, a.attname FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
        "WHERE c.relnamespace = 'public'::regnamespace AND c.relkind = 'r' AND c.relname NOT LIKE '%\\_version' "
        "AND a.attnum > 0 AND NOT a.attisdropped AND a.atttypid = 'int4'::regtype "
        "AND (a.attname ~ '(^id$|_id$)' OR EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conrelid = c.oid "
        "AND k.contype IN ('p', 'f') AND a.attnum = ANY (k.conkey)))")
    return sorted(f'{table}.{name}' for table, name in found
                  if (table, name) not in listed and table not in NOT_ABC_TABLES)


def _drop_dependents(columns: List[Tuple[str, str]]) -> Tuple[List[Tuple[str, str]], List[str]]:
    """Drop the foreign keys and WHEN triggers on columns; return what re-creates them."""
    values = _attnums(columns)
    views = _rows(f"SELECT DISTINCT r.ev_class::regclass::text FROM pg_depend d JOIN pg_rewrite r ON r.oid = d.objid "
                  f"WHERE d.classid = 'pg_rewrite'::regclass AND (d.refobjid, d.refobjsubid) IN ({values})")
    if views:
        raise RuntimeError(f'these views or rules use an id column; drop and re-create them by hand: {views}')
    foreign_keys = _rows(
        f"SELECT k.conrelid::regclass::text, k.conname, pg_get_constraintdef(k.oid) FROM pg_constraint k "
        f"WHERE k.contype = 'f' AND (EXISTS (SELECT 1 FROM unnest(k.conkey) n WHERE (k.conrelid, n) IN ({values})) "
        f"OR EXISTS (SELECT 1 FROM unnest(k.confkey) n WHERE (k.confrelid, n) IN ({values}))) ORDER BY 1, 2")
    triggers = _rows(
        f"SELECT DISTINCT t.tgrelid::regclass::text, t.tgname, pg_get_triggerdef(t.oid) FROM pg_trigger t "
        f"JOIN pg_depend d ON d.classid = 'pg_trigger'::regclass AND d.objid = t.oid "
        f"WHERE NOT t.tgisinternal AND (d.refobjid, d.refobjsubid) IN ({values}) ORDER BY 1, 2")
    for table, name, _ in foreign_keys:
        op.execute(f'ALTER TABLE {table} DROP CONSTRAINT "{name}"')
    for table, name, _ in triggers:
        op.execute(f'DROP TRIGGER "{name}" ON {table}')
    logger.info("dropped %d foreign keys and %d triggers", len(foreign_keys), len(triggers))
    return ([(table, f'ALTER TABLE {table} ADD CONSTRAINT "{name}" {definition}')
             for table, name, definition in foreign_keys],
            [definition for _, _, definition in triggers])


def _alter_types(columns: List[Tuple[str, str]], new_type: str) -> None:
    old_type = 'integer' if new_type == 'bigint' else 'bigint'
    current = dict(((table, name), kind) for table, name, kind in _rows(
        "SELECT c.relname, a.attname, a.atttypid::regtype::text FROM pg_attribute a JOIN pg_class c "
        "ON c.oid = a.attrelid WHERE c.relnamespace = 'public'::regnamespace AND a.attnum > 0"))
    by_table: Dict[str, List[str]] = {}
    for table, name in columns:
        if current.get((table, name)) == old_type:
            by_table.setdefault(table, []).append(name)
    for table, names in by_table.items():
        start = time.monotonic()
        op.execute(f'ALTER TABLE {table} ' + ', '.join(f'ALTER COLUMN {name} TYPE {new_type}' for name in names))
        logger.info("%s: %d columns to %s in %.0f s", table, len(names), new_type, time.monotonic() - start)
    logger.info("changed %d columns on %d tables", sum(len(names) for names in by_table.values()), len(by_table))


def _alter_sequences(columns: List[Tuple[str, str]], new_type: str) -> None:
    """The sequence behind each primary key (serial or identity); ALTER TYPE does not change a serial's."""
    for table, name in columns:
        if name != 'id' or table.endswith('_version'):
            continue
        sequence = _rows("SELECT pg_get_serial_sequence(:t, 'id')", t=f'public.{table}')[0][0]
        if sequence and _rows("SELECT format_type(seqtypid, NULL) FROM pg_sequence WHERE seqrelid = "
                              "CAST(:s AS regclass)", s=sequence)[0][0] != new_type:
            op.execute(f'ALTER SEQUENCE {sequence} AS {new_type}')


def _retype_functions(undo: bool) -> None:
    for name, edits in FUNCTION_TYPE_EDITS.items():
        found = _rows("SELECT p.oid::regprocedure::text, p.prokind, pg_get_functiondef(p.oid) FROM pg_proc p "
                      "WHERE p.pronamespace = 'public'::regnamespace AND p.proname = :n", n=name)
        if not found:
            logger.warning("function %s does not exist; nothing to change", name)
            continue
        if len(found) > 1:
            raise RuntimeError(f'function {name} is overloaded ({[f[0] for f in found]}); drop the stale one by hand')
        signature, kind, definition = found[0]
        original = definition
        for old, new in ([(new, old) for old, new in reversed(edits)] if undo else edits):
            count = definition.count(old)
            if count == 1:
                definition = definition.replace(old, new)
            elif not (count == 0 and new in definition):
                raise RuntimeError(f'{name}: expected {old!r} once, found it {count} times; '
                                   'the function changed since this migration was written')
        if definition != original:
            op.execute(f"DROP {'PROCEDURE' if kind == 'p' else 'FUNCTION'} {signature}")
            op.execute(text(definition))


def _convert(new_type: str) -> None:
    columns = _columns()
    foreign_keys, triggers = _drop_dependents(columns)
    _alter_types(columns, new_type)
    _alter_sequences(columns, new_type)
    _retype_functions(undo=new_type != 'bigint')
    for definition in triggers:
        op.execute(definition)
    start = time.monotonic()
    for _, definition in foreign_keys:
        op.execute(definition)
    logger.info("re-added %d foreign keys in %.0f s", len(foreign_keys), time.monotonic() - start)
    start = time.monotonic()
    for table in sorted({table for table, _ in columns}):
        op.execute(f'ANALYZE {table}')
    logger.info("analyzed in %.0f s", time.monotonic() - start)


def upgrade():
    unlisted = _unlisted_integer_ids()
    if unlisted:
        raise RuntimeError(f'these integer id columns are not in BIGINT_COLUMNS: {unlisted}; '
                           'add them (or explain why they stay integer)')
    _convert('bigint')


def downgrade():
    _convert('integer')
