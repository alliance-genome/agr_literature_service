"""Unit tests for the large-scale summary-tag extraction on search hits
(SCRUM-6614).

The ksql pipeline emits topic_entity_tags as an array of per-group arrays;
the sort_authors_by_order default pipeline flattens it at index time, so
_source normally holds a flat list -- but the extractor must accept both
shapes (a review found the original flat-only extractor silently returned []
against the nested shape).
"""
from agr_literature_service.api.crud.search_crud import extract_large_scale_tags

# ksql indexes each term's label next to its curie (SCRUM-6720)
SUMMARY = {
    "topic": "ATP:0000005", "topic_name": "gene", "entity_type": "ATP:0000005", "entity_type_name": "gene",
    "entity": None, "large_scale_tag": "true", "entity_count": "4460",
}
EXPECTED = {"topic": "ATP:0000005", "topic_name": "gene", "entity_type": "ATP:0000005", "entity_type_name": "gene",
            "entity_count": 4460}
NORMAL = {"topic": "ATP:0000005", "entity_type": "ATP:0000005", "entity": "ZFIN:ZDB-GENE-1"}


class TestExtractLargeScaleTags:

    def test_flat_source_extracts_summaries_only(self):
        source = {"topic_entity_tags": [NORMAL, SUMMARY, NORMAL]}
        assert extract_large_scale_tags(source) == [EXPECTED]

    def test_array_of_arrays_source_is_descended_one_level(self):
        # The shape ksql sends (one inner array per group) in case a document
        # was indexed without the flattening pipeline.
        source = {"topic_entity_tags": [[NORMAL, NORMAL], [SUMMARY]]}
        assert extract_large_scale_tags(source) == [EXPECTED]

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


class TestLargeScaleTagNames:

    def test_names_come_from_the_indexed_labels(self):
        tag = extract_large_scale_tags({"topic_entity_tags": [SUMMARY]})[0]
        assert (tag["topic_name"], tag["entity_type_name"]) == ("gene", "gene")

    def test_a_tag_indexed_without_labels_falls_back_to_the_curie(self):
        unlabelled = {k: v for k, v in SUMMARY.items() if not k.endswith("_name")}
        tag = extract_large_scale_tags({"topic_entity_tags": [unlabelled]})[0]
        assert (tag["topic_name"], tag["entity_type_name"]) == ("ATP:0000005", "ATP:0000005")
