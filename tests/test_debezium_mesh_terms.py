"""Both search indexes must carry a reference's MeSH terms (SCRUM-6490).

The ABC search looks up mesh_terms.heading_term with the other text fields, so the
private index needs the terms the public one already has. These checks guard the ksql
pipeline and the Elasticsearch mappings that put them there; nothing else in the test
suite runs them.
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


def _mesh_mapping(settings_file):
    settings = json.loads((DEBEZIUM / settings_file).read_text())
    return settings["mappings"]["properties"]["mesh_terms"]


def test_mesh_terms_keys_are_heading_and_qualifier():
    assert "map('heading_term':=heading_term, 'qualifier_term':=qualifier_term)" in \
        _ksql_statements()["mesh_terms"]


def test_private_index_joins_the_mesh_terms():
    reference_joined = _ksql_statements()["reference_joined"]
    assert 'mesh_terms.MESH_TERMS "mesh_terms"' in reference_joined
    assert "LEFT OUTER JOIN mesh_terms mesh_terms ON spine_tet.REFERENCE_ID = mesh_terms.REFERENCE_ID" \
        in reference_joined


def test_mesh_terms_are_created_before_the_private_index():
    order = list(_ksql_statements())
    assert order.index("mesh_terms") < order.index("reference_joined")


def test_private_index_maps_mesh_terms_as_searchable_text():
    # an object, not nested: the all-fields multi_match reads mesh_terms.heading_term directly
    mapping = _mesh_mapping("elasticsearch-settings.json")
    assert mapping.get("type", "object") == "object"
    assert set(mapping["properties"]) == {"heading_term", "qualifier_term"}
    assert mapping["properties"]["heading_term"]["type"] == "text"


def test_public_index_maps_the_fields_the_documents_carry():
    assert set(_mesh_mapping("elasticsearch-settings-public.json")["properties"]) == \
        {"heading_term", "qualifier_term"}
