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
ATEAM_FIXTURE_TABLES = ('ontologytermclosure', 'ontologyterm_subsets', 'biologicalentity', 'ontologyterm')

TERM_TYPES = {'ATP': 'ATPTerm', 'NCBITaxon': 'NCBITaxonTerm', 'ECO': 'ECOTerm'}
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


def seed_ateam_fixture(conn, children: Dict[str, List[str]], names: Dict[str, str],
                       extra_curies: Iterable[str] = ()) -> None:
    """Replace the fixture rows with the terms and entities of the given hierarchy and names."""
    for table in ATEAM_FIXTURE_TABLES:
        conn.execute(text(f"DELETE FROM {table}"))
    curies = set(children) | {k for kids in children.values() for k in kids} | set(names) | set(extra_curies)
    terms = sorted(c for c in curies if _prefix(c) in TERM_TYPES)
    entities = sorted(c for c in curies if _prefix(c) in ENTITY_TAXA)
    term_ids = {curie: i for i, curie in enumerate(
        terms + sorted(set(ENTITY_TAXA.values()) - set(terms)), start=1)}
    for curie, term_id in term_ids.items():
        conn.execute(text("INSERT INTO ontologyterm (id, curie, name, ontologytermtype) VALUES (:i, :c, :n, :t)"),
                     {"i": term_id, "c": curie, "n": names.get(curie, curie), "t": TERM_TYPES[_prefix(curie)]})
    for row_id, (descendant, ancestor, distance) in enumerate(
            (r for r in closure_rows(children) if r[0] in term_ids and r[1] in term_ids), start=1):
        conn.execute(text("INSERT INTO ontologytermclosure (id, closuresubject_id, closureobject_id, distance, "
                          "closuretypes) VALUES (:i, :s, :o, :d, '[\"is_a\"]'::jsonb)"),
                     {"i": row_id, "s": term_ids[descendant], "o": term_ids[ancestor], "d": distance})
    for entity_id, curie in enumerate(entities, start=1):
        conn.execute(text("INSERT INTO biologicalentity (id, primaryexternalid, taxon_id) VALUES (:i, :c, :t)"),
                     {"i": entity_id, "c": curie, "t": term_ids[ENTITY_TAXA[_prefix(curie)]]})
