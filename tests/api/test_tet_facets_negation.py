"""Unit tests for TET (topic-entity-tag) nested facet negation query building.

These exercise ``add_tet_facets_values`` directly against an in-memory ``es_body``
dict, so they need neither Elasticsearch nor a database. They cover the
source-method / source-evidence-assertion *whole-reference* exclusion (top-level
``must_not`` nested clauses) added for SCRUM-5899, alongside the unchanged
confidence-level exclusion (``must_not`` within the positive nested query).
"""
from agr_literature_service.api.crud.search_crud import (
    add_tet_facets_values,
    ensure_structure,
)


def _new_es_body():
    es_body = {}
    ensure_structure(es_body)
    return es_body


def _filter_bool(es_body):
    return es_body["query"]["bool"]["filter"]["bool"]


class TestTetFacetsNegation:

    def test_source_method_exclusion_adds_whole_reference_must_not(self):
        es_body = _new_es_body()
        add_tet_facets_values(
            es_body,
            {
                "tet_facets_values": [],
                "tet_facets_negative_values": [
                    {"topic_entity_tags.source_method.keyword": ["ACKnowledge"]}
                ],
            },
            apply_to_single_tet=False,
        )
        must_not = _filter_bool(es_body).get("must_not", [])
        assert must_not == [
            {
                "nested": {
                    "path": "topic_entity_tags",
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"topic_entity_tags.source_method.keyword": "ACKnowledge"}}
                            ]
                        }
                    },
                }
            }
        ]

    def test_sea_exclusion_normal_value_uses_sea_field(self):
        es_body = _new_es_body()
        add_tet_facets_values(
            es_body,
            {
                "tet_facets_values": [],
                "tet_facets_negative_values": [
                    {"topic_entity_tags.source_evidence_assertion.keyword": ["ECO:0000302"]}
                ],
            },
            apply_to_single_tet=False,
        )
        must_not = _filter_bool(es_body).get("must_not", [])
        term = must_not[0]["nested"]["query"]["bool"]["must"][0]["term"]
        assert term == {"topic_entity_tags.source_evidence_assertion.keyword": "ECO:0000302"}

    def test_sea_exclusion_group_value_remaps_to_group_field(self):
        es_body = _new_es_body()
        add_tet_facets_values(
            es_body,
            {
                "tet_facets_values": [],
                "tet_facets_negative_values": [
                    {"topic_entity_tags.source_evidence_assertion.keyword": ["ECO:0007669"]}
                ],
            },
            apply_to_single_tet=False,
        )
        must_not = _filter_bool(es_body).get("must_not", [])
        term = must_not[0]["nested"]["query"]["bool"]["must"][0]["term"]
        assert term == {"topic_entity_tags.source_evidence_assertion_group.keyword": "ECO:0007669"}

    def test_empty_negation_list_adds_no_clause(self):
        es_body = _new_es_body()
        add_tet_facets_values(
            es_body,
            {
                "tet_facets_values": [],
                "tet_facets_negative_values": [
                    {"topic_entity_tags.source_method.keyword": []}
                ],
            },
            apply_to_single_tet=False,
        )
        assert _filter_bool(es_body).get("must_not", []) == []

    def test_confidence_level_negation_stays_within_positive_nested_query(self):
        """Confidence-level exclusion is per-matching-tag: a ``must_not`` *inside* the
        positive nested query, never a top-level whole-reference clause."""
        es_body = _new_es_body()
        add_tet_facets_values(
            es_body,
            {
                "tet_facets_values": [
                    {"topic_entity_tags.topic.keyword": "ATP:0000005"}
                ],
                "tet_facets_negative_values": [
                    {"topic_entity_tags.confidence_level.keyword": ["NEG"]}
                ],
            },
            apply_to_single_tet=True,
        )
        filter_bool = _filter_bool(es_body)
        # No top-level must_not for confidence level
        assert filter_bool.get("must_not", []) == []
        # The positive nested query carries the confidence-level must_not
        nested = filter_bool["must"][-1]["nested"]["query"]["bool"]
        assert nested["must"] == [{"term": {"topic_entity_tags.topic.keyword": "ATP:0000005"}}]
        assert nested["must_not"] == [{"term": {"topic_entity_tags.confidence_level.keyword": "NEG"}}]

    def test_multiple_source_facets_produce_independent_must_not_clauses(self):
        """A single negated object with several source/SEA keys must yield one
        *independent* top-level must_not nested clause per key -- whole-reference,
        drop-if-ANY-tag-matches -- NOT a single AND-within-one-tag clause.

        Note the frontend↔backend contract: the backend reads only index [0] of
        tet_facets_negative_values, so all negated TET keys are intentionally
        carried in one merged object; splitting them into separate array entries
        would silently drop everything past [0]."""
        es_body = _new_es_body()
        add_tet_facets_values(
            es_body,
            {
                "tet_facets_values": [],
                "tet_facets_negative_values": [
                    {
                        "topic_entity_tags.source_method.keyword": ["ACKnowledge"],
                        "topic_entity_tags.source_evidence_assertion.keyword": ["ECO:0000302"],
                    }
                ],
            },
            apply_to_single_tet=False,
        )
        must_not = _filter_bool(es_body).get("must_not", [])
        # Two separate clauses, each constraining a single field on its own tag.
        assert len(must_not) == 2
        for clause in must_not:
            assert len(clause["nested"]["query"]["bool"]["must"]) == 1
        terms = [c["nested"]["query"]["bool"]["must"][0]["term"] for c in must_not]
        assert {"topic_entity_tags.source_method.keyword": "ACKnowledge"} in terms
        assert {"topic_entity_tags.source_evidence_assertion.keyword": "ECO:0000302"} in terms

    def test_source_method_and_confidence_level_coexist(self):
        es_body = _new_es_body()
        add_tet_facets_values(
            es_body,
            {
                "tet_facets_values": [
                    {"topic_entity_tags.topic.keyword": "ATP:0000005"}
                ],
                "tet_facets_negative_values": [
                    {
                        "topic_entity_tags.confidence_level.keyword": ["NEG"],
                        "topic_entity_tags.source_method.keyword": ["ACKnowledge"],
                    }
                ],
            },
            apply_to_single_tet=True,
        )
        filter_bool = _filter_bool(es_body)
        # Whole-reference source-method exclusion at the top level
        sm_terms = [
            clause["nested"]["query"]["bool"]["must"][0]["term"]
            for clause in filter_bool.get("must_not", [])
        ]
        assert {"topic_entity_tags.source_method.keyword": "ACKnowledge"} in sm_terms
        # Confidence level still nested within the positive query
        nested = filter_bool["must"][-1]["nested"]["query"]["bool"]
        assert nested["must_not"] == [{"term": {"topic_entity_tags.confidence_level.keyword": "NEG"}}]
