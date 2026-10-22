"""A-team tables for the test database (SCRUM-6720, Phase 0).

After the merge, ABC reads ontology terms, their hierarchy and biological entities from the A-team
tables in the same database instead of the A-team API. The test database has no A-team tables,
so this creates the subset of them that ABC uses and fills them with the same ATP hierarchy the
in-memory mock gives ateam_db_helpers (tests/fixtures.atp_test_hierarchy), so tests keep seeing
one hierarchy whichever way it is read.

Only the columns ABC reads are created. Their names follow the A-team migrations
(agr_curation src/main/resources/db/migration):
  ontologyterm          id, curie (unique), name, ontologytermtype (ATPTerm, NCBITaxonTerm,
                        ECOTerm, ...), namespace, obsolete
  ontologytermclosure   v0.40.0.2: closuresubject_id (descendant), closureobject_id (ancestor),
                        distance, closuretypes (jsonb)
  ontologyterm_subsets  v0.31.0.1: ontologyterm_id, subsets
  biologicalentity      v0.38.0.25: primaryexternalid (the MOD curie, unique), modinternalid,
                        curie (AGRKB:), taxon_id -> ontologyterm, obsolete
These tables are not in Base.metadata, so the per-test cleanup leaves them alone: they hold
static reference data, created and filled once per test process.
"""
from typing import Dict, Iterable, List, Set, Tuple

from sqlalchemy import text

ATEAM_FIXTURE_DDL = (
    """CREATE TABLE IF NOT EXISTS ontologyterm (
        id bigint PRIMARY KEY,
        curie varchar(255) NOT NULL CONSTRAINT ontologyterm_curie_uk UNIQUE,
        name varchar(2000),
        ontologytermtype varchar(64) NOT NULL,
        namespace varchar(255),
        obsolete boolean NOT NULL DEFAULT false)""",
    """CREATE TABLE IF NOT EXISTS ontologytermclosure (
        id bigint PRIMARY KEY,
        closuresubject_id bigint REFERENCES ontologyterm (id),
        closureobject_id bigint REFERENCES ontologyterm (id),
        distance integer,
        closuretypes jsonb,
        obsolete boolean NOT NULL DEFAULT false)""",
    """CREATE TABLE IF NOT EXISTS ontologyterm_subsets (
        ontologyterm_id bigint NOT NULL REFERENCES ontologyterm (id),
        subsets varchar(255))""",
    """CREATE TABLE IF NOT EXISTS biologicalentity (
        id bigint PRIMARY KEY,
        curie varchar(255) CONSTRAINT biologicalentity_curie_uk UNIQUE,
        primaryexternalid varchar(255) CONSTRAINT biologicalentity_primaryexternalid_uk UNIQUE,
        modinternalid varchar(255),
        taxon_id bigint REFERENCES ontologyterm (id),
        obsolete boolean NOT NULL DEFAULT false)""",
)

TERM_TYPES = {'ATP': 'ATPTerm', 'NCBITaxon': 'NCBITaxonTerm', 'ECO': 'ECOTerm'}
# Terms tests use as valid values that the ATP test hierarchy does not contain (made-up or
# deliberately invalid curies, e.g. ATP:9999999, stay out so the tests that expect a 422 keep it)
EXTRA_TEST_TERMS = ('ATP:0000061', 'ATP:0000062', 'ATP:0000110',
                    # workflow tags tests set directly (SCRUM-6720 workflow_tag)
                    'ATP:0000103', 'ATP:0000198', 'ATP:0000214', 'ATP:0000303', 'ATP:0000306', 'ATP:0000342',
                    'ATP:0000380', 'ATP:0000383', 'ATP:0000387', 'ATP:0001111', 'ATP:0002222', 'ATP:0003333',
                    'ATP:NEW', 'ATP:task4_needed',
                    'ATP:0000344')  # retracted curation_tag written by the retraction cleanup
ENTITY_TAXA = {'WB': 'NCBITaxon:6239', 'FB': 'NCBITaxon:7227', 'SGD': 'NCBITaxon:559292',
               'ZFIN': 'NCBITaxon:7955', 'MGI': 'NCBITaxon:10090', 'RGD': 'NCBITaxon:10116',
               'Xenbase': 'NCBITaxon:8355'}


def closure_rows(children: Dict[str, List[str]]) -> List[Tuple[str, str, int]]:
    """(descendant, ancestor, distance) for every ancestor of every term, at the shortest
    distance. A term may have several parents (the test hierarchy puts ATP:0000141 under two),
    which the closure keeps and the single-parent cache does not. No self rows."""
    parents: Dict[str, Set[str]] = {}
    for parent, kids in children.items():
        for kid in kids:
            parents.setdefault(kid, set()).add(parent)
    rows: List[Tuple[str, str, int]] = []
    for term in set(parents):
        best: Dict[str, int] = {}
        frontier, distance = {term}, 0
        while frontier:
            distance += 1
            frontier = {p for t in frontier for p in parents.get(t, ())} - set(best) - {term}
            for ancestor in frontier:
                best[ancestor] = distance
        rows.extend((term, ancestor, d) for ancestor, d in best.items())
    return sorted(rows)


def _prefix(curie: str) -> str:
    return curie.split(':', 1)[0] if ':' in curie else ''


def create_ateam_fixture_tables(conn) -> None:
    for statement in ATEAM_FIXTURE_DDL:
        conn.execute(text(statement))


def _upsert(conn, table: str, key: str, key_value: str, values: Dict[str, object]) -> int:
    """Insert a row identified by key (a unique column), or update it; return its id. Existing rows
    keep their id, so rows that reference them (e.g. data a test or populate_test_db.py wrote)
    stay valid."""
    columns = ', '.join([key] + list(values))
    params = ', '.join([':key'] + [f':{c}' for c in values])
    updates = ', '.join(f'{c} = EXCLUDED.{c}' for c in values)
    return conn.execute(text(
        f"INSERT INTO {table} (id, {columns}) SELECT coalesce(max(id), 0) + 1, {params} FROM {table} "
        f"ON CONFLICT ({key}) DO UPDATE SET {updates} RETURNING id"), dict(values, key=key_value)).scalar()


def seed_ateam_fixture(conn, children: Dict[str, List[str]], names: Dict[str, str],
                       extra_curies: Iterable[str] = ()) -> None:
    """Make the fixture tables hold the terms and entities of the given hierarchy and names.

    Idempotent and non-destructive for terms and entities: they are inserted or updated by curie,
    never deleted, because ABC rows may already reference them (the Debezium integration test seeds
    again after populate_test_db.py has written data). The closure, which nothing references, is
    rebuilt."""
    curies = set(children) | {k for kids in children.values() for k in kids} | set(names) | set(extra_curies)
    terms = sorted(c for c in curies if _prefix(c) in TERM_TYPES)
    entities = sorted(c for c in curies if _prefix(c) in ENTITY_TAXA)
    term_ids = {}
    for curie in terms + sorted(set(ENTITY_TAXA.values()) - set(terms)):
        term_ids[curie] = _upsert(conn, 'ontologyterm', 'curie', curie,
                                  {'name': names.get(curie, curie), 'ontologytermtype': TERM_TYPES[_prefix(curie)]})
    conn.execute(text("DELETE FROM ontologytermclosure"))
    for row_id, (descendant, ancestor, distance) in enumerate(
            (r for r in closure_rows(children) if r[0] in term_ids and r[1] in term_ids), start=1):
        conn.execute(text("INSERT INTO ontologytermclosure (id, closuresubject_id, closureobject_id, distance, "
                          "closuretypes) VALUES (:i, :s, :o, :d, '[\"is_a\"]'::jsonb)"),
                     {"i": row_id, "s": term_ids[descendant], "o": term_ids[ancestor], "d": distance})
    for curie in entities:
        _upsert(conn, 'biologicalentity', 'primaryexternalid', curie, {'taxon_id': term_ids[ENTITY_TAXA[_prefix(curie)]]})


def add_ontology_terms(conn, curies: Iterable[str], termtype: str = 'ATPTerm') -> None:
    """Add terms a test needs (e.g. made-up ATP:curation_tag1) to the fixture ontologyterm table.
    Existing curies are left as they are."""
    for curie in curies:
        conn.execute(text(
            "INSERT INTO ontologyterm (id, curie, name, ontologytermtype) "
            "SELECT coalesce(max(id), 0) + 1, :c, :c, :t FROM ontologyterm "
            "ON CONFLICT (curie) DO NOTHING"), {"c": curie, "t": termtype})
