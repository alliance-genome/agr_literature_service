"""Unit tests for the per-MOD workflow_tag_ids aggregation size (SCRUM-6583).

The Workflow facet categories (file workflow, reference classification, ...) are
built from each MOD's workflow_tag_ids buckets. The UI sends its generic initial
facet limit (10) for ``workflow_tags.workflow_tag_id.keyword`` on every search;
honoring it kept only a MOD's 10 most frequent tags and emptied whole categories
(e.g. WB reference classification). The limit may raise the size for the
advanced-query vocab fetch (SCRUM-6398) but must never lower it below 100.

Pure function tests: no Elasticsearch or database needed.
"""
from agr_literature_service.api.crud.search_crud import (
    MIN_WORKFLOW_TAG_IDS_AGG_SIZE,
    get_workflow_tag_ids_agg_size,
)

KEY = "workflow_tags.workflow_tag_id.keyword"


class TestWorkflowTagIdsAggSize:

    def test_default_when_not_requested(self):
        assert get_workflow_tag_ids_agg_size({}) == MIN_WORKFLOW_TAG_IDS_AGG_SIZE

    def test_ui_initial_limit_does_not_shrink_the_aggregation(self):
        # The exact value the UI sends on a normal search (INITIAL_FACETS_LIMIT).
        assert get_workflow_tag_ids_agg_size({KEY: 10}) == MIN_WORKFLOW_TAG_IDS_AGG_SIZE

    def test_advanced_vocab_fetch_can_raise_the_size(self):
        # The advanced builder vocab fetch asks for ADVANCED_VOCAB_LIMIT (1000).
        assert get_workflow_tag_ids_agg_size({KEY: 1000}) == 1000

    def test_zero_or_none_falls_back_to_minimum(self):
        assert get_workflow_tag_ids_agg_size({KEY: 0}) == MIN_WORKFLOW_TAG_IDS_AGG_SIZE
        assert get_workflow_tag_ids_agg_size({KEY: None}) == MIN_WORKFLOW_TAG_IDS_AGG_SIZE

    def test_other_facet_limits_are_ignored(self):
        assert get_workflow_tag_ids_agg_size({"topics": 1000}) == MIN_WORKFLOW_TAG_IDS_AGG_SIZE
