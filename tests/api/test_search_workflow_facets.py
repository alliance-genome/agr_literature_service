"""Workflow facet grouping in search (DB- and ES-free)."""
from unittest.mock import patch

from agr_literature_service.api.crud import search_crud

# ATP root -> descendants, standing in for the A-Team ontology lookup.
ROOT_DESCENDANTS = {
    "ATP:0000379": ["ATP:0000380", "ATP:0000381", "ATP:0000382", "ATP:0000383",
                    "ATP:0000384", "ATP:0000385", "ATP:0000386", "ATP:0000387"],
    "ATP:0000311": ["ATP:0000312", "ATP:0000313"],
}
NAMES = {
    "ATP:0000386": "abstract classification complete",
    "ATP:0000380": "molecular probe classification needed",
    "ATP:0000313": "curation classification needed",
}


def _get_atp_ids(root_atp_ids):
    return [child for root in root_atp_ids for child in ROOT_DESCENDANTS.get(root, [])]


def _search_response(mod_to_tag_counts):
    return {"aggregations": {"workflow_tags": {"by_mod_abbreviation": {"buckets": [
        {"key": mod, "workflow_tag_ids": {"buckets": [
            {"key": tag, "doc_count": count} for tag, count in tag_counts.items()]}}
        for mod, tag_counts in mod_to_tag_counts.items()]}}}}


def _group(res, wft_mod_abbreviations):
    with patch.object(search_crud, "get_atp_ids", side_effect=_get_atp_ids), \
            patch.object(search_crud, "get_map_ateam_curies_to_names", return_value=NAMES):
        return search_crud.process_workflow_tags_aggregations(res, wft_mod_abbreviations)


def test_abstract_classification_is_a_workflow_facet():
    # SCRUM-6467: selecting one of its values filters on the nested workflow tags
    assert "abstract_classification" in search_crud.WORKFLOW_FACETS


def test_abstract_classification_facet_groups_its_states():
    res = _search_response({
        "ZFIN": {"ATP:0000386": 3, "ATP:0000380": 2, "ATP:0000313": 4},
        "SGD": {"ATP:0000386": 5},
    })
    aggs = _group(res, ["ZFIN"])

    buckets = aggs["abstract_classification"]["buckets"]
    # generic and molecular probe states both land here; SGD is outside the MOD scope
    assert [(b["key"], b["doc_count"], b["name"]) for b in buckets] == [
        ("ATP:0000386", 3, "abstract classification complete"),
        ("ATP:0000380", 2, "molecular probe classification needed"),
    ]
    # and they do not leak into the curation classification facet
    assert [b["key"] for b in aggs["curation_classification"]["buckets"]] == ["ATP:0000313"]


def test_abstract_classification_counts_sum_across_mods():
    res = _search_response({"ZFIN": {"ATP:0000386": 3}, "SGD": {"ATP:0000386": 5}})
    aggs = _group(res, ["ZFIN", "SGD"])
    assert [(b["key"], b["doc_count"]) for b in aggs["abstract_classification"]["buckets"]] == [
        ("ATP:0000386", 8)
    ]
