"""Unit tests for the advanced Topic query builder (SCRUM-6228).

These exercise ``build_tet_advanced_query`` / ``add_tet_advanced_query`` directly
against an in-memory ``es_body`` dict, so they need neither Elasticsearch nor a
database. The advanced builder compiles an AND/OR tree over TET sub-facets into a
nested Elasticsearch bool query:

* a leaf ``{"type": "tet", "match": {short_name: [values], ...}, "negate": bool}``
  becomes a ``nested`` query where all sub-facets must co-occur on ONE tag
  (bool.must) and values within a sub-facet are OR-ed (terms);
* an internal node ``{"operator": "AND"|"OR", "children": [...]}`` combines its
  children with bool.must (AND) or bool.should + minimum_should_match:1 (OR);
* a ``negate`` leaf is wrapped in bool.must_not (whole-reference exclusion).
"""
from agr_literature_service.api.crud.search_crud import (
    add_tet_advanced_query,
    build_tet_advanced_query,
    ensure_structure,
)


def _new_es_body():
    es_body = {}
    ensure_structure(es_body)
    return es_body


def _filter_bool(es_body):
    return es_body["query"]["bool"]["filter"]["bool"]


def _leaf(match, negate=False):
    node = {"type": "tet", "match": match}
    if negate:
        node["negate"] = True
    return node


class TestBuildTetAdvancedQuery:

    def test_single_leaf_becomes_nested_query(self):
        tree = _leaf({"topic": ["ATP:0000018"]})
        assert build_tet_advanced_query(tree) == {
            "nested": {
                "path": "topic_entity_tags",
                "query": {
                    "bool": {
                        "must": [
                            {"terms": {"topic_entity_tags.topic.keyword": ["ATP:0000018"]}}
                        ]
                    }
                },
            }
        }

    def test_leaf_ands_sub_facets_on_one_tag(self):
        tree = _leaf({
            "topic": ["ATP:0000018"],
            "source_method": ["ACKnowledge form"],
            "confidence_level": ["POS"],
        })
        must = build_tet_advanced_query(tree)["nested"]["query"]["bool"]["must"]
        assert must == [
            {"terms": {"topic_entity_tags.topic.keyword": ["ATP:0000018"]}},
            {"terms": {"topic_entity_tags.source_method.keyword": ["ACKnowledge form"]}},
            {"terms": {"topic_entity_tags.confidence_level.keyword": ["POS"]}},
        ]

    def test_and_of_two_leaves(self):
        tree = {
            "operator": "AND",
            "children": [
                _leaf({"topic": ["ATP:0000018"]}),
                _leaf({"entity_type": ["ATP:0000110"]}),
            ],
        }
        compiled = build_tet_advanced_query(tree)
        assert set(compiled["bool"].keys()) == {"must"}
        assert len(compiled["bool"]["must"]) == 2

    def test_or_of_two_leaves(self):
        tree = {
            "operator": "OR",
            "children": [
                _leaf({"source_method": ["ACKnowledge form"]}),
                _leaf({"source_method": ["ABC classifier"]}),
            ],
        }
        compiled = build_tet_advanced_query(tree)
        assert compiled["bool"]["minimum_should_match"] == 1
        assert len(compiled["bool"]["should"]) == 2
        assert "must" not in compiled["bool"]

    def test_ticket_example_nested_and_of_or(self):
        """WB corpus AND Topic=disease model from (ACKnowledge form positive OR
        ABC classifier positive) AND Topic=Allele entity=Allele."""
        tree = {
            "operator": "AND",
            "children": [
                {
                    "operator": "OR",
                    "children": [
                        _leaf({
                            "topic": ["ATP:0000018"],
                            "source_method": ["ACKnowledge form"],
                            "confidence_level": ["POS"],
                        }),
                        _leaf({
                            "topic": ["ATP:0000018"],
                            "source_method": ["ABC classifier"],
                            "confidence_level": ["POS"],
                        }),
                    ],
                },
                _leaf({
                    "topic": ["ATP:0000012"],
                    "entity_type": ["ATP:0000110"],
                    "entity": ["WB:WBGene00000001"],
                }),
            ],
        }
        compiled = build_tet_advanced_query(tree)
        # Top level: AND of two children.
        top_must = compiled["bool"]["must"]
        assert len(top_must) == 2
        # First child is the OR group of two source leaves.
        or_group = top_must[0]["bool"]
        assert or_group["minimum_should_match"] == 1
        assert len(or_group["should"]) == 2
        # Second child is a single nested leaf (Allele).
        allele_leaf = top_must[1]["nested"]["query"]["bool"]["must"]
        assert {"terms": {"topic_entity_tags.entity.keyword": ["WB:WBGene00000001"]}} in allele_leaf

    def test_negated_leaf_wraps_in_must_not(self):
        tree = _leaf({"confidence_level": ["NEG"]}, negate=True)
        assert build_tet_advanced_query(tree) == {
            "bool": {
                "must_not": [
                    {
                        "nested": {
                            "path": "topic_entity_tags",
                            "query": {
                                "bool": {
                                    "must": [
                                        {"terms": {"topic_entity_tags.confidence_level.keyword": ["NEG"]}}
                                    ]
                                }
                            },
                        }
                    }
                ]
            }
        }

    def test_confidence_score_becomes_range(self):
        tree = _leaf({"confidence_score": [0.5, 1.0]})
        must = build_tet_advanced_query(tree)["nested"]["query"]["bool"]["must"]
        assert must == [
            {"range": {"topic_entity_tags.confidence_score": {"gte": 0.5, "lte": 1.0}}}
        ]

    def test_has_data_yes_maps_to_negated_false(self):
        tree = _leaf({"has_data": ["yes"]})
        must = build_tet_advanced_query(tree)["nested"]["query"]["bool"]["must"]
        assert must == [
            {"terms": {"topic_entity_tags.negated": [False]}}
        ]

    def test_has_data_no_maps_to_negated_true(self):
        tree = _leaf({"has_data": ["no"]})
        must = build_tet_advanced_query(tree)["nested"]["query"]["bool"]["must"]
        assert must == [
            {"terms": {"topic_entity_tags.negated": [True]}}
        ]

    def test_has_data_combines_on_one_tag(self):
        """Ticket example: disease model from the ACKnowledge form, with data."""
        tree = _leaf({
            "topic": ["ATP:0000018"],
            "source_method": ["ACKnowledge form"],
            "has_data": ["yes"],
        })
        must = build_tet_advanced_query(tree)["nested"]["query"]["bool"]["must"]
        assert {"terms": {"topic_entity_tags.negated": [False]}} in must
        assert {"terms": {"topic_entity_tags.topic.keyword": ["ATP:0000018"]}} in must

    def test_has_data_both_values_match_either_polarity(self):
        tree = _leaf({"has_data": ["yes", "no"]})
        must = build_tet_advanced_query(tree)["nested"]["query"]["bool"]["must"]
        assert must == [
            {"terms": {"topic_entity_tags.negated": [False, True]}}
        ]

    def test_has_data_unknown_token_adds_no_condition(self):
        # An unrecognised token contributes no negated filter; with nothing else on
        # the leaf the match is empty and the whole leaf collapses to None.
        assert build_tet_advanced_query(_leaf({"has_data": ["maybe"]})) is None

    def test_sea_group_value_remaps_to_group_field(self):
        tree = _leaf({"source_evidence_assertion": ["ECO:0007669"]})
        must = build_tet_advanced_query(tree)["nested"]["query"]["bool"]["must"]
        assert must == [
            {"terms": {"topic_entity_tags.source_evidence_assertion_group.keyword": ["ECO:0007669"]}}
        ]

    def test_sea_normal_value_uses_raw_field(self):
        tree = _leaf({"source_evidence_assertion": ["ECO:0000302"]})
        must = build_tet_advanced_query(tree)["nested"]["query"]["bool"]["must"]
        assert must == [
            {"terms": {"topic_entity_tags.source_evidence_assertion.keyword": ["ECO:0000302"]}}
        ]

    def test_single_child_group_collapses(self):
        tree = {"operator": "AND", "children": [_leaf({"topic": ["ATP:0000018"]})]}
        # Collapses to the leaf's nested query -- no wrapping bool.
        assert build_tet_advanced_query(tree) == build_tet_advanced_query(
            _leaf({"topic": ["ATP:0000018"]})
        )

    def test_empty_tree_returns_none(self):
        assert build_tet_advanced_query(None) is None
        assert build_tet_advanced_query({}) is None
        assert build_tet_advanced_query({"operator": "AND", "children": []}) is None
        assert build_tet_advanced_query({"type": "tet", "match": {}}) is None

    def test_empty_children_collapse_away(self):
        tree = {
            "operator": "AND",
            "children": [
                _leaf({"topic": ["ATP:0000018"]}),
                {"operator": "OR", "children": []},
                {"type": "tet", "match": {}},
            ],
        }
        # Only the one non-empty leaf survives, so the group collapses to it.
        assert build_tet_advanced_query(tree) == build_tet_advanced_query(
            _leaf({"topic": ["ATP:0000018"]})
        )


class TestAddTetAdvancedQuery:

    def test_appends_clause_and_returns_true(self):
        es_body = _new_es_body()
        added = add_tet_advanced_query(es_body, _leaf({"topic": ["ATP:0000018"]}))
        assert added is True
        must = _filter_bool(es_body)["must"]
        assert len(must) == 1
        assert must[0]["nested"]["path"] == "topic_entity_tags"

    def test_empty_tree_adds_nothing_and_returns_false(self):
        es_body = _new_es_body()
        added = add_tet_advanced_query(es_body, {"operator": "AND", "children": []})
        assert added is False
        assert _filter_bool(es_body)["must"] == []
