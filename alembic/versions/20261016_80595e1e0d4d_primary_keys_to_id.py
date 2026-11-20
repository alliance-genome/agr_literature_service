"""ABC primary keys -> id

Revision ID: 80595e1e0d4d
Revises: 1e8243e92015
Create Date: 2026-10-16 12:20:00.000000

SCRUM-6719: A-team conventions before the merge (see linkml_audited_object).

A-team names every primary key id. This renames the single-column primary key of every ABC
table (author.author_id, reference.reference_id, workflow_tag.reference_workflow_tag_id, ...)
to id, in the table and in its Continuum history twin (<table>_version, where the key is the
first column of (<key>, transaction_id)). Foreign-key columns keep their names
(author.reference_id), as in A-team, and follow the rename by themselves; so do indexes,
constraints and trigger WHEN clauses. Sequences keep their names (get_next_citation_id reads
citation_citation_id_seq).

Function bodies do not follow a rename, so the eleven functions that name a renamed key on its
own table are rewritten here, in the same transaction, from exact edits of their current
definition: an edit that does not match exactly once stops the migration. The pgsync leftover
_view (read by the table_notify triggers) is rebuilt with the new key names. Every other
function in public only names foreign-key columns.

Not changed: topic_entity_tag_validation (composite key of two foreign keys), agent (previous
revision), Continuum's transaction, and the A-team tables.

The models keep their attribute names, mapped onto the new column (author_id =
Column('id', ...)); Continuum copies the attribute key onto the history table, so version
objects keep author_id too.

All renames are metadata-only.
"""
import logging
import re
from typing import Dict, List, Tuple

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '80595e1e0d4d'
down_revision = '1e8243e92015'
branch_labels = None
depends_on = None

logger = logging.getLogger('alembic.runtime.migration')

PK_RENAMES: Dict[str, str] = {
    'author': 'author_id',
    'citation': 'citation_id',
    'copyright_license': 'copyright_license_id',
    'cross_reference': 'cross_reference_id',
    'curation_status': 'curation_status_id',
    'curation_status_source_association': 'curation_status_source_association_id',
    'dataset': 'dataset_id',
    'dataset_entry': 'dataset_entry_id',
    'editor': 'editor_id',
    'embedding_file': 'embedding_file_id',
    'image_permission': 'image_permission_id',
    'indexing_priority': 'indexing_priority_id',
    'laboratory': 'laboratory_id',
    'laboratory_allele_designation': 'laboratory_allele_designation_id',
    'laboratory_cross_reference': 'laboratory_cross_reference_id',
    'laboratory_person': 'laboratory_person_id',
    'manual_indexing_tag': 'manual_indexing_tag_id',
    'mesh_detail': 'mesh_detail_id',
    'ml_model': 'ml_model_id',
    'mod': 'mod_id',
    'mod_corpus_association': 'mod_corpus_association_id',
    'mod_referencetype': 'mod_referencetype_id',
    'obsolete_reference_curie': 'obsolete_id',
    'person': 'person_id',
    'person_cross_reference': 'person_cross_reference_id',
    'person_email': 'person_email_id',
    'person_institution': 'person_institution_id',
    'person_lineage': 'person_lineage_id',
    'person_lineage_submission': 'person_lineage_submission_id',
    'person_name': 'person_name_id',
    'person_note': 'person_note_id',
    'person_setting': 'person_setting_id',
    'reference': 'reference_id',
    'reference_email': 'reference_email_id',
    'reference_mod_md5sum': 'reference_mod_md5sum_id',
    'reference_mod_referencetype': 'reference_mod_referencetype_id',
    'reference_relation': 'reference_relation_id',
    'referencefile': 'referencefile_id',
    'referencefile_mod': 'referencefile_mod_id',
    'referencetype': 'referencetype_id',
    'resource': 'resource_id',
    'resource_image_permission': 'resource_image_permission_id',
    'tag_source': 'tag_source_id',
    'topic_entity_tag': 'topic_entity_tag_id',
    'vocabulary_abc': 'vocabulary_abc_id',
    'vocabulary_term_abc': 'vocabulary_term_abc_id',
    'vocabulary_term_synonym_abc': 'vocabulary_term_synonym_abc_id',
    'workflow_tag': 'reference_workflow_tag_id',
    'workflow_tag_topic': 'workflow_tag_topic_id',
    'workflow_transition': 'workflow_transition_id',
}

# Exact edits of pg_get_functiondef(), applied in order on upgrade and undone in reverse order
# on downgrade. Each must match exactly once; an edit whose old text is gone but whose new text
# is present counts as already applied.
FUNCTION_REWRITES: Dict[str, List[Tuple[str, str]]] = {
    'compute_can_display_image': [
        ("WHERE cl.copyright_license_id = p_copyright_license_id;", "WHERE cl.id = p_copyright_license_id;"),
        ("ON cl.copyright_license_id = r.copyright_license_id", "ON cl.id = r.copyright_license_id"),
        ("WHERE r.resource_id = p_resource_id;", "WHERE r.id = p_resource_id;"),
        ("ON ip.image_permission_id = rip.image_permission_id", "ON ip.id = rip.image_permission_id"),
        ("rip.resource_image_permission_id", "rip.id")],
    'compute_reference_can_display_image': [
        ("WHERE r.reference_id = p_reference_id;", "WHERE r.id = p_reference_id;")],
    'copyright_license_refresh_can_display_image': [
        ("SELECT reference_id, compute_reference_can_display_image(reference_id) AS new_value",
         "SELECT id, compute_reference_can_display_image(id) AS new_value"),
        ("WHERE copyright_license_id = NEW.copyright_license_id", "WHERE copyright_license_id = NEW.id"),
        ("WHERE r.reference_id = sub.reference_id", "WHERE r.id = sub.id"),
        ("refresh_can_display_image_for_resource(res.resource_id)", "refresh_can_display_image_for_resource(res.id)"),
        ("WHERE res.copyright_license_id = NEW.copyright_license_id;", "WHERE res.copyright_license_id = NEW.id;")],
    'get_most_current_email': [
        ("person_email_id DESC", "id DESC")],
    'image_permission_refresh_can_display_image': [
        ("WHERE image_permission_id = NEW.image_permission_id", "WHERE image_permission_id = NEW.id")],
    'reference_update_citation': [
        ("NEW.reference_id, NEW.title", "NEW.id, NEW.title"),
        ("call update_citations(NEW.reference_id);", "call update_citations(NEW.id);")],
    'referencefile_update_image_count': [
        ("WHERE reference_id = NEW.reference_id;", "WHERE id = NEW.reference_id;"),
        ("WHERE reference_id = OLD.reference_id;", "WHERE id = OLD.reference_id;")],
    'refresh_can_display_image_for_resource': [
        ("SELECT reference_id, compute_reference_can_display_image(reference_id) AS new_value",
         "SELECT id, compute_reference_can_display_image(id) AS new_value"),
        ("WHERE r.reference_id = sub.reference_id", "WHERE r.id = sub.id")],
    'resource_refresh_can_display_image': [
        ("refresh_can_display_image_for_resource(NEW.resource_id)", "refresh_can_display_image_for_resource(NEW.id)")],
    'resource_update_citation': [
        ("'resource_update_citation: %', NEW.resource_id;", "'resource_update_citation: %', NEW.id;"),
        ("for refs in SELECT reference_id FROM reference", "for refs in SELECT id FROM reference"),
        ("WHERE reference.resource_id = NEW.resource_id", "WHERE reference.resource_id = NEW.id"),
        ("call update_citations(refs.reference_id);", "call update_citations(refs.id);")],
    'update_citations': [
        ("WHERE ref.resource_id = res.resource_id AND", "WHERE ref.resource_id = res.id AND"),
        ("ref.reference_id = ref_id;", "ref.id = ref_id;"),
        ("WHERE reference_id = ref_id;", "WHERE ref.id = ref_id;"),
        ("SELECT citation_id from reference where reference_id = ref_id into",
         "SELECT citation_id from reference where id = ref_id into"),
        ("RETURNING citation_id into citation_identifier;", "RETURNING id into citation_identifier;"),
        ("WHERE reference.reference_id = ref_id;", "WHERE reference.id = ref_id;"),
        ("WHERE citation.citation_id = citation_identifier;", "WHERE citation.id = citation_identifier;")],
}

# Reviewed and left alone: they name only foreign-key columns (author.reference_id,
# reference.resource_id, resource_image_permission.resource_id, referencefile.reference_id),
# a sequence (get_next_citation_id: citation_citation_id_seq, not renamed) or no column at all
# (table_notify reads its keys from _view). Any other function that names a renamed key stops
# the upgrade until it is reviewed.
FUNCTIONS_UNCHANGED = {'author_update_citation', 'reference_set_can_display_image',
                       'resource_image_permission_refresh_can_display_image', 'compute_reference_image_count',
                       'get_next_citation_id', 'table_notify', 'extract_publication_year',
                       'get_long_citation_author_string', 'get_short_author_string'}

PGSYNC_VIEW = '_view'


def _rows(statement: str, **params) -> List[tuple]:
    return [tuple(row) for row in op.get_bind().execute(text(statement), params)]


def _scalar(statement: str, **params):
    return op.get_bind().execute(text(statement), params).scalar()


def _first_pk_column(table: str):
    return _scalar("SELECT a.attname FROM pg_index i JOIN pg_attribute a "
                   "ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0] "
                   "WHERE i.indrelid = to_regclass(:t) AND i.indisprimary", t=f'public.{table}')


def _rename_keys(renames: Dict[str, Tuple[str, str]]) -> int:
    """{table: (from, to)} on each table and its history twin, where the key is still named from."""
    done = 0
    for table, (old, new) in renames.items():
        for target in (table, f'{table}_version'):
            if _first_pk_column(target) == old:
                op.execute(f'ALTER TABLE {target} RENAME COLUMN {old} TO {new}')
                done += 1
    return done


def _rewrite_functions(undo: bool) -> None:
    for name, edits in FUNCTION_REWRITES.items():
        definitions = _rows("SELECT pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n "
                            "ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname = :n", n=name)
        if not definitions:
            logger.warning("function %s does not exist; nothing to rewrite", name)
            continue
        if len(definitions) > 1:
            raise RuntimeError(f'function {name} is overloaded; rewrite it by hand')
        original = definition = definitions[0][0]
        steps = [(new, old) for old, new in reversed(edits)] if undo else edits
        for old, new in steps:
            count = definition.count(old)
            if count == 1:
                definition = definition.replace(old, new)
            elif not (count == 0 and new in definition):
                raise RuntimeError(f'{name}: expected {old!r} once, found it {count} times; '
                                   'the function changed since this migration was written')
        if definition != original:
            op.execute(text(definition))


def _rebuild_pgsync_view(renames: Dict[str, Tuple[str, str]]) -> None:
    """_view rows name pgsync tables, singular or plural; swap each table's own key name."""
    if not _scalar("SELECT to_regclass(:v) IS NOT NULL", v=f'public.{PGSYNC_VIEW}'):
        return
    rows = _rows(f'SELECT table_name, primary_keys, foreign_keys FROM public.{PGSYNC_VIEW}')
    new_rows = []
    for table_name, primary_keys, foreign_keys in rows:
        base = table_name if table_name in renames else table_name[:-1]
        old, new = renames.get(base, ('', ''))
        new_rows.append((table_name, [new if key == old else key for key in primary_keys],
                         [new if key == old else key for key in foreign_keys]))
    if new_rows == [(t, list(p), list(f)) for t, p, f in rows]:
        return

    def array(values: List[str]) -> str:
        return "ARRAY[" + ", ".join(f"'{value}'" for value in values) + "]::text[]" if values else "'{}'::text[]"

    values = ', '.join(f"('{t}', {array(p)}, {array(f)})" for t, p, f in new_rows)
    # the indexes (pgsync's unique _idx on table_name) go with the view: recreate them
    indexes = [definition for (definition,) in _rows(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = 'public' AND tablename = :v", v=PGSYNC_VIEW)]
    op.execute(f'DROP MATERIALIZED VIEW public.{PGSYNC_VIEW}')
    op.execute(f'CREATE MATERIALIZED VIEW public.{PGSYNC_VIEW} AS SELECT table_name, primary_keys, foreign_keys '
               f'FROM (VALUES {values}) t(table_name, primary_keys, foreign_keys)')
    for definition in indexes:
        op.execute(definition)


def upgrade():
    clashes = [table for table in PK_RENAMES
               if _first_pk_column(table) != 'id' and _scalar(
                   "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' "
                   "AND table_name = :t AND column_name = 'id'", t=table)]
    if clashes:
        raise RuntimeError(f'these tables already have an id column: {clashes}')
    pattern = re.compile(r'\b(' + '|'.join(re.escape(pk) for pk in sorted(set(PK_RENAMES.values()))) + r')\b')
    unreviewed = sorted(name for name, source in _rows(
        "SELECT p.proname, p.prosrc FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
        "JOIN pg_language l ON l.oid = p.prolang WHERE n.nspname = 'public' AND l.lanname IN ('plpgsql', 'sql') "
        "AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = p.oid AND d.deptype = 'e')")
        if name not in FUNCTION_REWRITES and name not in FUNCTIONS_UNCHANGED and pattern.search(source))
    if unreviewed:
        raise RuntimeError(f'these functions name a renamed primary key and were not reviewed: {unreviewed}; '
                           'add them to FUNCTION_REWRITES or FUNCTIONS_UNCHANGED')
    renames = {table: (old, 'id') for table, old in PK_RENAMES.items()}
    logger.info("renamed %d primary keys to id", _rename_keys(renames))
    _rewrite_functions(undo=False)
    _rebuild_pgsync_view(renames)


def downgrade():
    # columns first: CREATE FUNCTION ... LANGUAGE sql checks its body against the tables
    renames = {table: ('id', old) for table, old in PK_RENAMES.items()}
    logger.info("renamed %d primary keys back", _rename_keys(renames))
    _rewrite_functions(undo=True)
    _rebuild_pgsync_view(renames)
