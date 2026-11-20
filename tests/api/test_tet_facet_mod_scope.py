"""The advanced-search TET facets are scoped to the selected MODs (SCRUM-6338).

A tag belongs to the MOD that is its source's secondary_data_provider. data_provider
records where the data came from and can be a third party (GEO, PDB), so scoping on it
hid, for example, FB's GEO-sourced "high throughput assay" tags from FB's facets.
"""
from agr_literature_service.api.crud.search_crud import (
    TET_FACETS_FILTER,
    TET_SCOPED_AGGREGATION,
    TET_SEA_FACETS_FILTER,
    apply_all_tags_tet_aggregations,
    create_mod_scoped_tet_aggregation,
    process_topic_entity_tags_aggregations,
    tet_mod_scope_filter,
)

SCOPED_FACET_KEYS = {
    "topics", "confidence_levels", "confidence_scores", "data_novelty", "data_context",
    "source_methods", "validation_by_professional_biocurator",
}
SEA_FACET_KEYS = {"source_evidence_assertions", "source_evidence_assertion_groups"}


def _scope_filter(agg):
    return agg["aggs"]["filtered"]["filter"]


def _owning_mod_terms(scope):
    # the primary clause of the scope filter: tags owned by one of the MODs
    return scope["bool"]["should"][0]["terms"]


def test_facets_are_scoped_by_the_owning_mod():
    agg = create_mod_scoped_tet_aggregation(tet_facets={}, facets_limits={}, allowed_mods=["FB", "WB"])

    assert _owning_mod_terms(_scope_filter(agg)) == {"topic_entity_tags.secondary_data_provider": ["FB", "WB"]}


def test_tags_without_an_owning_mod_fall_back_to_data_provider():
    # Tags indexed before secondary_data_provider existed (an older index slot, e.g.
    # the rollback slot after a failed rebuild) have no such field. Without a fallback
    # they match nothing and every TET facet comes back empty on that slot (2026-10-03
    # prod incident), so they are scoped by data_provider as before SCRUM-6338.
    scope = tet_mod_scope_filter("topic_entity_tags", ["FB", "WB"])

    assert scope["bool"]["minimum_should_match"] == 1
    owned, legacy = scope["bool"]["should"]
    assert owned == {"terms": {"topic_entity_tags.secondary_data_provider": ["FB", "WB"]}}
    assert legacy["bool"]["must_not"] == {"exists": {"field": "topic_entity_tags.secondary_data_provider"}}
    assert legacy["bool"]["filter"] == {"terms": {"topic_entity_tags.data_provider": ["FB", "WB"]}}


def test_scoping_wraps_the_cross_facet_filter():
    agg = create_mod_scoped_tet_aggregation(
        tet_facets={"topic": "ATP:0000150"}, facets_limits={}, allowed_mods=["FB"])

    assert agg["nested"] == {"path": "topic_entity_tags"}
    # the cross-facet filter and every facet's bucketing sit under the one MOD filter
    inner = agg["aggs"]["filtered"]["aggs"][TET_FACETS_FILTER]
    assert inner["filter"]["bool"]["must"] == [{"term": {"topic_entity_tags.topic.keyword": "ATP:0000150"}}]
    assert set(inner["aggs"]) == SCOPED_FACET_KEYS


def test_the_scope_filter_is_shared_by_every_scoped_facet():
    # SCRUM-6886: one scope filter for all MOD-scoped facets, so Elasticsearch evaluates
    # it once over the nested tags instead of once per facet.
    es_body = {"aggregations": {}}
    apply_all_tags_tet_aggregations(es_body, tet_facets={}, facets_limits={}, tet_data_providers=["fb", "WB"])

    scoped = [agg for agg in es_body["aggregations"].values() if "filtered" in agg.get("aggs", {})]
    assert len(scoped) == 1
    agg = es_body["aggregations"][TET_SCOPED_AGGREGATION]
    assert _scope_filter(agg) == tet_mod_scope_filter("topic_entity_tags", ["FB", "WB"])
    assert set(agg["aggs"]["filtered"]["aggs"]) == SCOPED_FACET_KEYS | SEA_FACET_KEYS


def test_large_scale_tag_facet_is_not_mod_scoped():
    es_body = {"aggregations": {}}
    apply_all_tags_tet_aggregations(es_body, tet_facets={}, facets_limits={}, tet_data_providers=["FB"])

    large_scale = es_body["aggregations"]["large_scale_tag_aggregation"]
    assert "filtered" not in large_scale["aggs"]
    assert "large_scale_tag" in large_scale["aggs"]


def test_sea_facets_are_not_filtered_by_their_own_value():
    agg = create_mod_scoped_tet_aggregation(
        tet_facets={"topic": "ATP:0000150", "source_evidence_assertion": "ECO:0000302"},
        facets_limits={}, allowed_mods=["FB"])

    aggs = agg["aggs"]["filtered"]["aggs"]
    assert set(aggs) == {TET_FACETS_FILTER, TET_SEA_FACETS_FILTER}
    assert len(aggs[TET_FACETS_FILTER]["filter"]["bool"]["must"]) == 2
    assert aggs[TET_SEA_FACETS_FILTER]["filter"]["bool"]["must"] == [
        {"term": {"topic_entity_tags.topic.keyword": "ATP:0000150"}}]
    assert set(aggs[TET_SEA_FACETS_FILTER]["aggs"]) == SEA_FACET_KEYS


def test_sea_facets_sit_directly_under_the_scope_when_only_sea_is_selected():
    agg = create_mod_scoped_tet_aggregation(
        tet_facets={"source_evidence_assertion": "ECO:0000302"}, facets_limits={}, allowed_mods=["FB"])

    aggs = agg["aggs"]["filtered"]["aggs"]
    assert TET_SEA_FACETS_FILTER not in aggs
    assert SEA_FACET_KEYS <= set(aggs)
    assert set(aggs[TET_FACETS_FILTER]["aggs"]) == SCOPED_FACET_KEYS


def test_confidence_levels_keep_empty_buckets_only_when_cross_filtered():
    plain = create_mod_scoped_tet_aggregation(tet_facets={}, facets_limits={}, allowed_mods=["FB"])
    crossed = create_mod_scoped_tet_aggregation(
        tet_facets={"topic": "ATP:0000150"}, facets_limits={}, allowed_mods=["FB"])

    assert "min_doc_count" not in plain["aggs"]["filtered"]["aggs"]["confidence_levels"]["terms"]
    levels = crossed["aggs"]["filtered"]["aggs"][TET_FACETS_FILTER]["aggs"]["confidence_levels"]
    assert levels["terms"]["min_doc_count"] == 0


def test_facet_limits_are_applied_per_facet():
    agg = create_mod_scoped_tet_aggregation(
        tet_facets={}, facets_limits={"topics": 50, "source_evidence_assertions": 7}, allowed_mods=["FB"])

    aggs = agg["aggs"]["filtered"]["aggs"]
    assert aggs["topics"]["terms"]["size"] == 50
    assert aggs["data_novelty"]["terms"]["size"] == 10
    assert aggs["source_evidence_assertions"]["terms"]["size"] == 7
    assert aggs["source_evidence_assertion_groups"]["terms"]["size"] == 7
    assert aggs["confidence_scores"] == {
        "histogram": {"field": "topic_entity_tags.confidence_score", "interval": 0.05}}


def _bucket(key, count):
    return {"key": key, "doc_count": count, "docs_count": {"doc_count": count}}


def test_response_is_read_back_into_the_same_facet_keys(monkeypatch):
    # The UI receives the same facet keys as before the aggregations were combined.
    monkeypatch.setattr(
        "agr_literature_service.api.crud.search_crud.get_map_ateam_curies_to_names",
        lambda category, curies: {c: c.lower() for c in curies})
    res = {"aggregations": {
        TET_SCOPED_AGGREGATION: {"doc_count": 9, "filtered": {
            "doc_count": 5,
            TET_FACETS_FILTER: {
                "doc_count": 3,
                "topics": {"buckets": [_bucket("ATP:0000150", 3)]},
                "data_novelty": {"buckets": [_bucket("ATP:0000321", 2)]},
                "source_methods": {"buckets": [_bucket("abc_classifier", 1)]},
            },
            TET_SEA_FACETS_FILTER: {
                "doc_count": 4,
                "source_evidence_assertions": {"buckets": [_bucket("ECO:0008004", 4)]},
                "source_evidence_assertion_groups": {"buckets": [_bucket("ECO:0007669", 4)]},
            },
        }},
        "large_scale_tag_aggregation": {"doc_count": 9, TET_FACETS_FILTER: {
            "large_scale_tag": {"buckets": [_bucket("true", 1)]}}},
        "mods_in_corpus.keyword": {"buckets": []},
    }}

    out = process_topic_entity_tags_aggregations(res)

    assert out["topics"]["buckets"][0]["key"] == "ATP:0000150"
    assert out["topics"]["buckets"][0]["name"] == "atp:0000150"
    assert out["data_novelty"]["buckets"][0]["doc_count"] == 2
    assert out["source_methods"]["buckets"][0]["key"] == "abc_classifier"
    assert [b["name"] for b in out["source_evidence_assertions"]["buckets"]] == ["automated", "machine learning"]
    assert out["large_scale_tag"]["buckets"][0]["key"] == "true"
    assert out["confidence_levels"] == {}
    # the combined aggregations are consumed; the other facets are left alone
    assert set(res["aggregations"]) == {"mods_in_corpus.keyword"}
