"""Unit tests for the large-scale summary-tag extraction on search hits
(SCRUM-6614).

The ksql pipeline emits topic_entity_tags as an array of per-group arrays;
the sort_authors_by_order default pipeline flattens it at index time, so
_source normally holds a flat list -- but the extractor must accept both
shapes (a review found the original flat-only extractor silently returned []
against the nested shape).
"""
from unittest.mock import patch

from agr_literature_service.api.crud.search_crud import (
    add_names_to_large_scale_tags,
    extract_large_scale_tags,
)

SUMMARY = {
    "topic": "ATP:0000005", "entity_type": "ATP:0000005", "entity": None,
    "large_scale_tag": "true", "entity_count": "4460",
}
NORMAL = {"topic": "ATP:0000005", "entity_type": "ATP:0000005", "entity": "ZFIN:ZDB-GENE-1"}


class TestExtractLargeScaleTags:

    def test_flat_source_extracts_summaries_only(self):
        source = {"topic_entity_tags": [NORMAL, SUMMARY, NORMAL]}
        assert extract_large_scale_tags(source) == [
            {"topic": "ATP:0000005", "entity_type": "ATP:0000005", "entity_count": 4460},
        ]

    def test_array_of_arrays_source_is_descended_one_level(self):
        # The shape ksql sends (one inner array per group) in case a document
        # was indexed without the flattening pipeline.
        source = {"topic_entity_tags": [[NORMAL, NORMAL], [SUMMARY]]}
        assert extract_large_scale_tags(source) == [
            {"topic": "ATP:0000005", "entity_type": "ATP:0000005", "entity_count": 4460},
        ]

    def test_mixed_shapes_during_resnapshot(self):
        source = {"topic_entity_tags": [NORMAL, [SUMMARY]]}
        assert len(extract_large_scale_tags(source)) == 1

    def test_no_tags_or_missing_field(self):
        assert extract_large_scale_tags({}) == []
        assert extract_large_scale_tags({"topic_entity_tags": None}) == []
        assert extract_large_scale_tags({"topic_entity_tags": [NORMAL]}) == []

    def test_non_numeric_entity_count_becomes_none(self):
        source = {"topic_entity_tags": [{**SUMMARY, "entity_count": None}]}
        assert extract_large_scale_tags(source)[0]["entity_count"] is None


class TestAddNamesToLargeScaleTags:

    @patch("agr_literature_service.api.crud.search_crud.get_map_ateam_curies_to_names",
           return_value={"ATP:0000005": "gene"})
    def test_names_resolved_in_one_batched_lookup(self, mock_map):
        hits = [
            {"large_scale_tags": [{"topic": "ATP:0000005", "entity_type": "ATP:0000005",
                                   "entity_count": 4460}]},
            {"large_scale_tags": []},
        ]
        add_names_to_large_scale_tags(hits)
        tag = hits[0]["large_scale_tags"][0]
        assert tag["topic_name"] == "gene"
        assert tag["entity_type_name"] == "gene"
        mock_map.assert_called_once_with(category="atpterm", curies=["ATP:0000005"])

    @patch("agr_literature_service.api.crud.search_crud.get_map_ateam_curies_to_names")
    def test_no_lookup_when_no_summaries(self, mock_map):
        hits = [{"large_scale_tags": []}]
        add_names_to_large_scale_tags(hits)
        mock_map.assert_not_called()
