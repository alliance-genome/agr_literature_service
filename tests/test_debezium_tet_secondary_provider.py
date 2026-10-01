"""The search index must carry each topic entity tag's owning MOD (SCRUM-6338).

A tag belongs to the MOD that is its source's secondary_data_provider; data_provider
records where the data came from and can be a third party (GEO, PDB). The advanced
search facets scope by MOD, so they need the secondary provider in the nested
``topic_entity_tags`` documents. These checks guard the ksql pipeline and the
Elasticsearch mapping that put it there; nothing else in the test suite runs them.
"""
import json
import re
from pathlib import Path

DEBEZIUM = Path(__file__).resolve().parents[1] / "debezium"


def _ksql_statements():
    """The CREATE statements of ksql_queries.ksql, keyed by object name, in file order."""
    text = (DEBEZIUM / "ksql_queries.ksql").read_text().replace('\\"', '"')
    statements = {}
    for stmt in text.split(";"):
        match = re.search(r"CREATE\s+(?:TABLE|STREAM)\s+(\w+)", stmt, re.IGNORECASE)
        if match:
            statements[match.group(1).lower()] = re.sub(r"\s+", " ", stmt)
    return statements


def test_tag_source_declares_the_secondary_provider_column():
    assert "secondary_data_provider_id string" in _ksql_statements()["tag_source"]


def test_tag_source_with_mod_resolves_the_owning_mod():
    stmt = _ksql_statements()["tag_source_with_mod"]
    assert re.search(r"JOIN mod ON tag_source\.secondary_data_provider_id = mod\.mod_id", stmt)
    assert re.search(r"mod\.abbreviation AS secondary_data_provider\b", stmt)


def test_tags_are_joined_to_their_owning_mod():
    stmt = _ksql_statements()["topic_entity_tag_with_source"]
    assert re.search(r"JOIN tag_source_with_mod tag_source ON", stmt)
    assert "tag_source.secondary_data_provider" in stmt


def test_nested_tag_documents_include_the_secondary_provider():
    assert "'secondary_data_provider':=secondary_data_provider" in _ksql_statements()["topic_entity_tags"]


def test_tag_source_with_mod_is_created_after_its_inputs_and_before_its_use():
    order = list(_ksql_statements())
    position = order.index("tag_source_with_mod")
    assert order.index("mod") < position
    assert order.index("tag_source") < position
    assert position < order.index("topic_entity_tag_with_source")


def test_elasticsearch_maps_the_secondary_provider_as_a_keyword():
    settings = json.loads((DEBEZIUM / "elasticsearch-settings.json").read_text())
    tet_fields = settings["mappings"]["properties"]["topic_entity_tags"]["properties"]
    # same shape as data_provider, so exact-match MOD filters behave the same way
    assert tet_fields["secondary_data_provider"] == tet_fields["data_provider"]
