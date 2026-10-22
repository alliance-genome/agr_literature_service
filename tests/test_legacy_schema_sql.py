"""Guard raw SQL against the schema before the A-team conventions (SCRUM-6719).

The revisions 47d74bfbbe87 (LinkML audit columns), 1e8243e92015 (users -> agent) and
80595e1e0d4d (primary keys -> id) rename columns and a table that raw SQL names by hand. The
ORM follows them (models keep their attribute names); a string of SQL does not. This scans
every SQL-looking string literal in the code base and fails on any that still names:

  own-pk        a renamed primary key on its own table (r.reference_id with reference r)
  users-table   the users table (now agent)
  audit-string  created_by / updated_by (now created_by_id / updated_by_id)
  is-obsolete   is_obsolete (now obsolete)

Historical code is not scanned: alembic revisions and lit_processing/oneoff_scripts.

Run it as a script to print the hits without pytest:  python -I tests/test_legacy_schema_sql.py
"""
import ast
import re
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

ROOT = Path(__file__).resolve().parents[1]
PK_REVISION = ROOT / 'alembic' / 'versions' / '20261016_80595e1e0d4d_primary_keys_to_id.py'
SCANNED = ('agr_literature_service', 'tests')
EXCLUDED = ('agr_literature_service/lit_processing/oneoff_scripts/', 'tests/test_legacy_schema_sql.py')

SQL_KEYWORD = re.compile(r'\b(select|update|insert|delete|join|from)\b', re.IGNORECASE)
TABLE_REF = re.compile(r'\b(?:from|join|update|into)\s+(\w+)(?:\s+(?:as\s+)?(\w+))?', re.IGNORECASE)
# further tables of a comma-separated FROM list (FROM a x, b y); only known tables count
COMMA_REF = re.compile(r',\s*(\w+)(?:\s+(?:as\s+)?(\w+))?', re.IGNORECASE)
NOT_ALIAS = {'on', 'where', 'join', 'left', 'right', 'inner', 'outer', 'full', 'cross', 'natural', 'set',
             'using', 'group', 'order', 'limit', 'as', 'and', 'or', 'returning', 'values', 'having', 'union',
             'window', 'lateral', 'select', 'offset', 'for', 'default'}
RULES = {
    'users-table': re.compile(r'(?:\b(?:from|join|into|update)\s+|,\s*)users\b', re.IGNORECASE),
    # an output alias (AS created_by) keeps the API's key and is allowed
    'audit-string': re.compile(r'(?<![Aa][Ss] )\b(?:created_by|updated_by)\b(?!_id)'),
    'is-obsolete': re.compile(r'(?<![Aa][Ss] )\bis_obsolete\b'),
}


def _pk_renames() -> Dict[str, str]:
    """PK_RENAMES of the primary_keys_to_id revision, read without importing alembic."""
    tree = ast.parse(PK_REVISION.read_text())
    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and getattr(node.target, 'id', None) == 'PK_RENAMES' and node.value:
            return ast.literal_eval(node.value)
    raise RuntimeError(f'PK_RENAMES not found in {PK_REVISION}')


PK_RENAMES = _pk_renames()

# SCRUM-6720 (ateam_merge): ontology curie columns that are now <column>_id foreign keys to
# ontologyterm (workflow_tag.workflow_tag_id is workflow_tag_term_id). Raw SQL must join
# ontologyterm for the curie; naming the dropped column on its own table is flagged 'curie-column'.
_CURIE_COLUMNS: Dict[str, Tuple[str, ...]] = {
    'manual_indexing_tag': ('curation_tag',),
    'indexing_priority': ('predicted_indexing_priority', 'curator_indexing_priority'),
    'ml_model': ('topic', 'species', 'data_novelty', 'data_context'),
    'workflow_tag_topic': ('workflow_tag', 'topic'),
    'dataset': ('data_type',),
    'workflow_transition': ('transition_from', 'transition_to'),
    'workflow_tag': ('workflow_tag_id', 'curation_tag'),
    'curation_status': ('topic', 'curation_status', 'curation_tag'),
    'curation_status_source_association': ('curation_status', 'curation_tag'),
    'tag_source': ('source_evidence_assertion',),
    'reference': ('retraction_status',),
}
CURIE_COLUMNS: Dict[str, Tuple[str, ...]] = dict(
    _CURIE_COLUMNS, **{f'{table}_version': columns for table, columns in _CURIE_COLUMNS.items()})


def scan_text(sql: str) -> List[Tuple[int, str]]:
    """(line offset within the literal, rule) for every rule the SQL text breaks."""
    if not SQL_KEYWORD.search(sql):
        return []
    found: Set[Tuple[int, str]] = set()

    def line_of(position: int) -> int:
        return sql.count('\n', 0, position)

    for rule, pattern in RULES.items():
        for match in pattern.finditer(sql):
            found.add((line_of(match.start()), rule))
    references = [(table.lower(), (alias or '').lower()) for table, alias in TABLE_REF.findall(sql)]
    references += [(table.lower(), (alias or '').lower()) for table, alias in COMMA_REF.findall(sql)
                   if table.lower() in PK_RENAMES or table.lower() in CURIE_COLUMNS]
    tables = {table for table, _alias in references}
    for table, pk in PK_RENAMES.items():
        if table not in tables:
            continue
        names = {table} | {alias for name, alias in references if name == table and alias and alias not in NOT_ALIAS}
        for name in names:
            for match in re.finditer(rf'\b{re.escape(name)}\.{pk}\b', sql, re.IGNORECASE):
                found.add((line_of(match.start()), 'own-pk'))
        if tables == {table}:
            # a statement on this table alone: an unqualified key is its own
            for match in re.finditer(rf'(?<![.\w:])(?<![Aa][Ss] ){pk}\b', sql):
                found.add((line_of(match.start()), 'own-pk'))
        # a subquery selecting the key straight from its table
        for match in re.finditer(rf'\bselect\s+(?:distinct\s+)?{pk}\s+from\s+{table}\b', sql, re.IGNORECASE):
            found.add((line_of(match.start()), 'own-pk'))
    found.update((line_of(position), 'curie-column') for position in _curie_column_hits(sql, references, tables))
    return sorted(found)


def _curie_column_hits(sql: str, references: List[Tuple[str, str]], tables: Set[str]) -> List[int]:
    """Positions where the SQL names a dropped curie column (CURIE_COLUMNS) on its own table: qualified
    by the table or one of its aliases, or unqualified in a statement on that table alone."""
    positions = []
    for table, columns in CURIE_COLUMNS.items():
        if table not in tables:
            continue
        names = {table} | {alias for name, alias in references if name == table and alias and alias not in NOT_ALIAS}
        for column in columns:
            for name in names:
                positions += [m.start() for m in re.finditer(rf'\b{re.escape(name)}\.{column}\b(?!_)', sql,
                                                             re.IGNORECASE)]
            if tables == {table}:
                positions += [m.start() for m in re.finditer(rf'(?<![.\w:])(?<![Aa][Ss] ){column}\b(?!_)', sql)]
    return positions


def _literals(tree: ast.AST) -> Iterable[Tuple[int, str]]:
    """(line, text) of every string literal except bare string statements (docstrings, code commented out
    as a string); an f-string's expressions become a placeholder."""
    skipped: Set[int] = set()
    for node in ast.walk(tree):
        # a bare string statement is a docstring or a block of commented-out code
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
            skipped.add(id(node.value))
        # an assertion message is prose
        if isinstance(node, ast.Assert) and node.msg is not None:
            skipped.update(id(part) for part in ast.walk(node.msg))
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr) and id(node) not in skipped:
            parts = []
            for value in node.values:
                skipped.add(id(value))
                parts.append(value.value if isinstance(value, ast.Constant) else '{x}')
            yield node.lineno, ''.join(parts)
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in skipped:
            yield node.lineno, node.value


def _scan_file(path: Path, root: Path) -> List[Tuple[str, int, str]]:
    tree = ast.parse(path.read_text(), filename=str(path))
    relative = str(path.relative_to(root))
    return [(relative, line + offset, rule) for line, text in _literals(tree) for offset, rule in scan_text(text)]


def scan(paths: Iterable[Path]) -> List[Tuple[str, int, str]]:
    return sorted({hit for path in paths for hit in _scan_file(path, ROOT)})


def source_files() -> List[Path]:
    files = []
    for top in SCANNED:
        for path in sorted((ROOT / top).rglob('*.py')):
            relative = str(path.relative_to(ROOT))
            if not relative.startswith(EXCLUDED):
                files.append(path)
    return files


def test_no_sql_names_pre_conventions_schema():
    hits = scan(source_files())
    assert hits == [], '\n'.join(f'{file}:{line}: {rule}' for file, line, rule in hits)


def test_scanner_detects_each_rule():
    sql = {
        'own-pk': 'SELECT r.reference_id FROM reference r',
        'users-table': 'SELECT 1 FROM transaction t, users u',
        'audit-string': 'SELECT created_by FROM author',
        'is-obsolete': 'SELECT 1 FROM cross_reference WHERE is_obsolete',
    }
    for rule, statement in sql.items():
        assert {found for _line, found in scan_text(statement)} == {rule}


def test_scanner_detects_unqualified_own_key():
    assert scan_text('SELECT reference_id FROM reference WHERE curie = :c') == [(0, 'own-pk')]


def test_scanner_detects_own_key_in_comma_join():
    sql = 'SELECT cr.curie FROM cross_reference cr, mod_corpus_association mca, mod m WHERE mca.mod_id = m.mod_id'
    assert scan_text(sql) == [(0, 'own-pk')]


def test_scanner_detects_own_key_in_subquery():
    sql = 'SELECT * FROM topic_entity_tag WHERE tag_source_id IN (SELECT tag_source_id FROM tag_source)'
    assert scan_text(sql) == [(0, 'own-pk')]


def test_scanner_ignores_foreign_keys_and_new_names():
    assert scan_text('SELECT a.reference_id FROM author a JOIN reference r ON r.id = a.reference_id') == []
    assert scan_text('SELECT created_by_id, obsolete FROM author') == []
    assert scan_text('created_by') == []
    assert scan_text('SELECT COALESCE(a.automation_username, p.curie) AS updated_by FROM agent a') == []
    assert scan_text('SELECT a.id AS author_id FROM author a') == []
    assert scan_text('DELETE FROM reference WHERE id = :reference_id') == []
    assert scan_text('SELECT obsolete AS is_obsolete FROM cross_reference') == []


def test_scanner_skips_docstrings(tmp_path):
    source = tmp_path / 'module.py'
    source.write_text('def f():\n    """Copy created_by from the reference."""\n    return "SELECT created_by FROM x"\n')
    assert [rule for _file, _line, rule in _scan_file(source, tmp_path)] == ['audit-string']


if __name__ == '__main__':
    found = scan(source_files())
    for file, line, rule in found:
        print(f'{file}:{line}: {rule}')
    print(f'TOTAL {len(found)}')
    sys.exit(1 if found else 0)
