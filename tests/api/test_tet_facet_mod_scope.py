"""The advanced-search TET facets are scoped to the selected MODs (SCRUM-6338).

A tag belongs to the MOD that is its source's secondary_data_provider. data_provider
records where the data came from and can be a third party (GEO, PDB), so scoping on it
hid, for example, FB's GEO-sourced "high throughput assay" tags from FB's facets.
"""
from agr_literature_service.api.crud.search_crud import (
    apply_all_tags_tet_aggregations,
    create_filtered_aggregation_with_dp,
)


def _scope_filter(agg):
    return agg["aggs"]["filtered"]["filter"]


def test_facets_are_scoped_by_the_owning_mod():
    agg = create_filtered_aggregation_with_dp(
        path="topic_entity_tags", tet_facets={}, term_field="topic_entity_tags.source_method.keyword",
        term_key="source_methods", allowed_mods=["FB", "WB"])

    assert _scope_filter(agg) == {"terms": {"topic_entity_tags.secondary_data_provider": ["FB", "WB"]}}


def test_scoping_wraps_the_unscoped_facet_aggregation():
    agg = create_filtered_aggregation_with_dp(
        path="topic_entity_tags", tet_facets={"topic": "ATP:0000150"},
        term_field="topic_entity_tags.source_method.keyword", term_key="source_methods", allowed_mods=["FB"])

    assert agg["nested"] == {"path": "topic_entity_tags"}
    # the per-facet aggregation (bucketing, cross-facet filter) sits under the MOD filter
    inner = agg["aggs"]["filtered"]["aggs"]["filter_by_other_tet_values"]
    assert inner["filter"]["bool"]["must"] == [{"term": {"topic_entity_tags.topic.keyword": "ATP:0000150"}}]
    assert "source_methods" in inner["aggs"]


def test_every_scoped_facet_in_the_search_body_uses_the_owning_mod():
    # Builds the whole TET facet body without Elasticsearch. A facet added with the
    # old allowed_dp= keyword raised NameError only at search time, so only the
    # live-ES tests caught it; this catches it, and any facet scoped another way.
    es_body = {"aggregations": {}}
    apply_all_tags_tet_aggregations(es_body, tet_facets={}, facets_limits={}, tet_data_providers=["fb", "WB"])

    scoped = {name: agg for name, agg in es_body["aggregations"].items() if "filtered" in agg.get("aggs", {})}
    assert len(scoped) >= 9
    for name, agg in scoped.items():
        assert _scope_filter(agg) == {"terms": {"topic_entity_tags.secondary_data_provider": ["FB", "WB"]}}, name
