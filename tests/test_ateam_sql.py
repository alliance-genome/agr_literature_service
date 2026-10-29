"""SCRUM-6720 Phase 3: ateam_db_helpers reads the A-team tables with SQL on ABC's own database.

These run the real queries against the fixture tables (tests/ateam_fixture_schema.py), which hold the
same ATP hierarchy the in-memory mock injects (tests/fixtures.atp_test_hierarchy).
"""
import json

import pytest
from fastapi import HTTPException
from sqlalchemy import text

from agr_literature_service.api.crud import ateam_db_helpers as helpers

from .ateam_fixture_schema import add_ontology_terms
from .fixtures import atp_test_hierarchy, db  # noqa: F401

ROOTS = ['ATP:0000002', 'ATP:0000323', 'ATP:0000177', 'ATP:0000335']


@pytest.fixture
def sql_cache():
    """The ATP cache built from the fixture tables; the mocked cache is put back afterwards."""
    saved = (dict(helpers.atp_to_name), dict(helpers.name_to_atp),
             dict(helpers.atp_to_children), dict(helpers.atp_to_parent))
    helpers.load_name_to_atp_and_relationships()
    yield helpers
    helpers.set_globals(*saved)


def _reachable(children, roots):
    seen, frontier = set(), list(roots)
    while frontier:
        for child in children.get(frontier.pop(), []):
            if child not in seen:
                seen.add(child)
                frontier.append(child)
    return seen


def test_cache_built_from_sql_matches_the_hierarchy(db, sql_cache):  # noqa: F811
    names, _name_to_atp, children, parent = atp_test_hierarchy()
    reachable = _reachable(children, ROOTS)
    assert reachable, "the test hierarchy has terms under the default roots"
    for term in reachable:
        assert sql_cache.atp_to_name[term] == names.get(term, term)
        assert sorted(sql_cache.atp_to_children.get(term, [])) == sorted(children.get(term, [])), term
        parents = [p for p, kids in children.items() if term in kids]
        # a term with several parents (ATP:0000141) keeps one of them, as the BFS always did; the
        # mock's choice for it is arbitrary
        assert sql_cache.atp_to_parent[term] in parents, term
        if len(parents) == 1 and term in parent:
            assert sql_cache.atp_to_parent[term] == parent[term], term


def test_ancestors_and_descendants_from_the_cache(db, sql_cache):  # noqa: F811
    assert sql_cache.atp_get_all_ancestors('ATP:0000325') == ['ATP:0000324', 'ATP:0000323']
    assert sorted(sql_cache.atp_get_all_descendants('ATP:0000323')) == sorted(
        ['ATP:0000324', 'ATP:0000326', 'ATP:0000360', 'ATP:0000325', 'ATP:0000328', 'ATP:0000327'])


def test_closure_ancestors_and_descendants_of_any_term(db):  # noqa: F811
    assert set(helpers._sql_ontology_ancestors_or_descendants('ATP:0000325', 'ancestors')) >= {
        'ATP:0000324', 'ATP:0000323'}
    assert set(helpers._sql_ontology_ancestors_or_descendants('ATP:0000324', 'descendants')) == {
        'ATP:0000360', 'ATP:0000325'}


def test_invalid_ids_and_names(db, sql_cache):  # noqa: F811
    assert sql_cache.atp_return_invalid_ids(['ATP:0000325', 'ATP:9999999']) == ['ATP:9999999']
    assert sql_cache.atp_get_name('ATP:0000325') == 'experimentally studied data'


def test_topic_search_and_mod_subsets(db):  # noqa: F811
    connection = db.connection()
    # ATP:0000123 "not sure" is a topic (under ATP:0000002) in the test hierarchy
    topic_id = connection.execute(text("SELECT id FROM ontologyterm WHERE curie = 'ATP:0000123'")).scalar()
    connection.execute(text("INSERT INTO ontologyterm_subsets (ontologyterm_id, subsets) VALUES (:i, 'WB_tag')"),
                       {"i": topic_id})
    db.commit()  # the helpers read on their own connection
    try:
        assert helpers.search_topic_list(topic='not su') == [{"curie": "ATP:0000123", "name": "not sure"}]
        assert helpers.search_topic_list(topic='not su', mod_abbr='WB') == [
            {"curie": "ATP:0000123", "name": "not sure"}]
        assert helpers.search_topic_list(topic='not su', mod_abbr='SGD') == []
        assert helpers.search_topic_list(topic='phenotype') == [], "ATP:0000009 is not under ATP:0000002 here"
        assert helpers._sql_filter_atp_by_mod_subset(['ATP:0000123', 'ATP:0000079'], 'WB') == ['ATP:0000123']
    finally:
        db.execute(text("DELETE FROM ontologyterm_subsets WHERE ontologyterm_id = :i"), {"i": topic_id})
        db.commit()


def test_species_search_and_names(db):  # noqa: F811
    add_ontology_terms(db.connection(), ['NCBITaxon:6239'], termtype='NCBITaxonTerm')
    db.execute(text("UPDATE ontologyterm SET name = 'Caenorhabditis elegans' WHERE curie = 'NCBITaxon:6239'"))
    db.commit()
    assert {"curie": "NCBITaxon:6239", "name": "Caenorhabditis elegans"} in helpers._sql_search_species('elegans', 10)
    assert {"curie": "NCBITaxon:6239", "name": "Caenorhabditis elegans"} in helpers._sql_search_species(
        'ncbitaxon:623', 10)
    assert helpers.map_curies_to_names('species', ['NCBITaxon:6239']) == {'NCBITaxon:6239': 'Caenorhabditis elegans'}
    assert helpers.map_curies_to_names('unknown category', ['X:1']) == {'X:1': 'X:1'}


class _FakeClient:
    """Stands in for the A-team API client: records the calls, returns an imported term."""
    def __init__(self, error=None):
        self.calls, self.error = [], error

    def get_or_create_species(self, curie):
        self.calls.append(curie)
        if self.error:
            raise self.error
        return type("Term", (), {"curie": curie, "name": "imported from NCBI"})()


def _species(response):
    return json.loads(response.body)


def test_get_or_create_species_reads_an_existing_taxon_without_the_api(db, monkeypatch):  # noqa: F811
    add_ontology_terms(db.connection(), ['NCBITaxon:6239'], termtype='NCBITaxonTerm')
    db.execute(text("UPDATE ontologyterm SET name = 'Caenorhabditis elegans' WHERE curie = 'NCBITaxon:6239'"))
    db.commit()
    client = _FakeClient()
    monkeypatch.setattr(helpers, "_get_client", lambda: client)
    expected = {"curie": "NCBITaxon:6239", "name": "Caenorhabditis elegans"}
    assert _species(helpers.get_or_create_species('6239')) == expected
    assert _species(helpers.get_or_create_species('NCBITaxon:6239')) == expected
    assert client.calls == []


def test_get_or_create_species_asks_the_api_for_a_missing_taxon(db, monkeypatch):  # noqa: F811
    client = _FakeClient()
    monkeypatch.setattr(helpers, "_get_client", lambda: client)
    assert _species(helpers.get_or_create_species('999999999')) == {
        "curie": "NCBITaxon:999999999", "name": "imported from NCBI"}
    assert client.calls == ['NCBITaxon:999999999']


def test_get_or_create_species_asks_the_api_when_the_query_fails(monkeypatch):
    def broken(curie):
        raise helpers.AGRAPIError("Database query failed")

    client = _FakeClient()
    monkeypatch.setattr(helpers, "_sql_species", broken)
    monkeypatch.setattr(helpers, "_get_client", lambda: client)
    assert _species(helpers.get_or_create_species('6239'))["curie"] == "NCBITaxon:6239"
    assert client.calls == ['NCBITaxon:6239']


def test_get_or_create_species_api_failure_is_a_502(db, monkeypatch):  # noqa: F811
    monkeypatch.setattr(helpers, "_get_client", lambda: _FakeClient(helpers.AGRAPIError("A-team down")))
    with pytest.raises(HTTPException) as exc_info:
        helpers.get_or_create_species('999999999')
    assert exc_info.value.status_code == 502


def test_a_failed_query_is_an_agr_api_error(db):  # noqa: F811
    # callers turn AGRAPIError into a 502, as when the A-team client failed
    with pytest.raises(helpers.AGRAPIError):
        helpers._rows("SELECT no_such_column FROM ontologyterm")
