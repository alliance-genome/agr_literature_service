"""SCRUM-6720: search facet buckets are named from the labels indexed next to each curie."""
from agr_literature_service.api.crud import search_crud


def _bucket(key, label=None):
    return {"key": key, "doc_count": 1, "label": {"buckets": [{"key": label, "doc_count": 1}] if label else []}}


def test_buckets_are_named_from_their_label_sub_aggregation():
    agg = {"buckets": [_bucket("atp:0000005", "gene"), _bucket("ATP:0000123")]}
    search_crud.add_curie_to_name_values(agg)
    assert [(b["key"], b["name"]) for b in agg["buckets"]] == [("atp:0000005", "gene"), ("ATP:0000123", "Unknown")]
    assert all("label" not in b for b in agg["buckets"])


def test_display_names_override_the_ontology_label():
    agg = {"buckets": [_bucket("ECO:0006155", "documented statement evidence used in manual assertion"),
                       _bucket("ATP:0000036", "documented statement evidence used in manual assertion by "
                                              "professional biocurator")]}
    search_crud.add_curie_to_name_values(agg)
    assert [b["name"] for b in agg["buckets"]] == ["manual", "professional biocurator"]


def test_curie_facets_ask_for_their_label():
    topic = search_crud.create_filtered_aggregation(
        "topic_entity_tags", {}, "topic_entity_tags.topic.keyword", "topics")
    assert topic["aggs"]["topics"]["aggs"]["label"] == {
        "terms": {"field": "topic_entity_tags.topic_name", "size": 1}}
    # a field without a label (the synthetic SEA group) has none
    group = search_crud.create_filtered_aggregation(
        "topic_entity_tags", {}, "topic_entity_tags.source_evidence_assertion_group.keyword", "sea")
    assert "label" not in group["aggs"]["sea"]["aggs"]
