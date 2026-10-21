"""SCRUM-6720 Phase 0: the A-team fixture tables and the ateam_merge guard revision."""
import importlib.util
import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from .ateam_fixture_schema import ATEAM_FIXTURE_DDL, closure_rows
from .fixtures import atp_test_hierarchy, db  # noqa: F401

GUARD = Path(__file__).resolve().parents[1] / 'alembic' / 'versions' / '20261021_c4a1e7b93d20_ateam_merge_guard.py'


def _guard_revision():
    spec = importlib.util.spec_from_file_location('ateam_merge_guard', GUARD)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _descendants(children, term):
    found, frontier = set(), {term}
    while frontier:
        frontier = {kid for t in frontier for kid in children.get(t, ())} - found
        found |= frontier
    return found


def test_closure_rows_keep_every_parent_at_shortest_distance():
    children = {'A': ['B', 'C'], 'B': ['D'], 'C': ['D'], 'D': ['E'], 'X': ['E']}
    rows = closure_rows(children)
    assert ('D', 'B', 1) in rows and ('D', 'C', 1) in rows and ('D', 'A', 2) in rows
    assert ('E', 'X', 1) in rows and ('E', 'A', 3) in rows
    assert not any(descendant == ancestor for descendant, ancestor, _ in rows)
    assert len(rows) == len(set((d, a) for d, a, _ in rows))


def test_fixture_tables_hold_the_test_hierarchy(db):  # noqa: F811
    atp_to_name, _name_to_atp, children, _parent = atp_test_hierarchy()
    for root in ('ATP:0000177', 'ATP:0000002', 'ATP:0000323', 'ATP:0000335'):
        rows = db.execute(text(
            "SELECT d.curie FROM ontologytermclosure c "
            "JOIN ontologyterm d ON d.id = c.closuresubject_id "
            "JOIN ontologyterm a ON a.id = c.closureobject_id WHERE a.curie = :root"), {"root": root}).scalars()
        assert set(rows) == _descendants(children, root), root
    parent_of_file_needed = set(db.execute(text(
        "SELECT a.curie FROM ontologytermclosure c JOIN ontologyterm d ON d.id = c.closuresubject_id "
        "JOIN ontologyterm a ON a.id = c.closureobject_id WHERE d.curie = 'ATP:0000141' AND c.distance = 1")).scalars())
    assert parent_of_file_needed == {'ATP:0000140', 'ATP:fileupload'}
    assert db.execute(text("SELECT name, ontologytermtype FROM ontologyterm WHERE curie = 'ATP:0000141'")).one() == \
        ('file needed', 'ATPTerm')
    assert db.execute(text("SELECT ontologytermtype FROM ontologyterm WHERE curie = 'NCBITaxon:6239'")).scalar() == \
        'NCBITaxonTerm'
    assert db.execute(text(
        "SELECT t.curie FROM biologicalentity be JOIN ontologyterm t ON t.id = be.taxon_id "
        "WHERE be.primaryexternalid = 'WB:WBGene00003001'")).scalar() == 'NCBITaxon:6239'


def test_guard_refuses_a_database_without_the_ateam_tables(db):  # noqa: F811
    guard = _guard_revision()
    assert guard.down_revision == '80595e1e0d4d'
    # the test database has the fixture tables but no renamed A-team person / reference
    assert guard.missing_ateam_tables(db.connection()) == ['ateam_reference', 'ateam_person']


@pytest.mark.skipif(not os.environ.get('ATEAM_MERGED_DB_URL'),
                    reason='set ATEAM_MERGED_DB_URL to a merged ABC + A-team database to compare columns')
def test_fixture_columns_exist_in_the_merged_database():
    """Every fixture column exists, with the same name, in the real A-team tables."""
    fixture = {}
    for statement in ATEAM_FIXTURE_DDL:
        table = statement.split('EXISTS', 1)[1].split('(', 1)[0].strip()
        body = statement.split('(', 1)[1]
        fixture[table] = {line.strip().split()[0] for line in body.split(',\n') if line.strip()}
    engine = create_engine(os.environ['ATEAM_MERGED_DB_URL'])
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        for table, columns in fixture.items():
            real = set(conn.execute(text(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' "
                "AND table_name = :t"), {"t": table}).scalars())
            assert columns <= real, (table, sorted(columns - real))
