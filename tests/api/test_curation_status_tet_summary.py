"""
Unit tests for curation_status_crud.get_tet_list_summary.

Pure-function tests: build lightweight tet / tet_source stand-ins with only the
attributes the summary reads, so no database or A-team access is needed.
"""
from datetime import datetime
from types import SimpleNamespace

from agr_literature_service.api.crud.curation_status_crud import get_tet_list_summary

AUTHOR = "ATP:0000035"
BIOCURATOR = "ATP:0000036"
NEW_DATA_NOVELTY = "ATP:0000321"


def _tet(date_created, negated=False, data_novelty=None):
    return SimpleNamespace(date_created=date_created, negated=negated,
                           data_novelty=data_novelty)


def _source(assertion):
    return SimpleNamespace(source_evidence_assertion=assertion)


def test_topic_missing_returns_defaults():
    summary = get_tet_list_summary("ATP:9999999", {})
    assert summary == {
        "tet_info_date_created": None,
        "tet_info_topic_source": [],
        "tet_info_has_data": False,
        "tet_info_new_data": False,
        "tet_info_no_data": False,
    }


def test_topic_present_but_empty_list_returns_defaults():
    summary = get_tet_list_summary("ATP:0000001", {"ATP:0000001": []})
    assert summary["tet_info_date_created"] is None
    assert summary["tet_info_has_data"] is False


def test_negated_tet_sets_no_data_with_biocurator_source():
    topic = "ATP:0000001"
    rows = {topic: [(_tet(datetime(2025, 3, 5), negated=True), _source(BIOCURATOR))]}
    summary = get_tet_list_summary(topic, rows)
    assert summary["tet_info_no_data"] is True
    assert summary["tet_info_has_data"] is False
    assert summary["tet_info_topic_source"] == ["biocurator"]
    assert summary["tet_info_date_created"] == datetime(2025, 3, 5).isoformat()


def test_positive_new_data_from_string_date_and_author_source():
    topic = "ATP:0000001"
    rows = {topic: [(_tet("2025-03-05 12:00:00", negated=False,
                          data_novelty=NEW_DATA_NOVELTY), _source(AUTHOR))]}
    summary = get_tet_list_summary(topic, rows)
    assert summary["tet_info_has_data"] is True
    assert summary["tet_info_new_data"] is True
    assert summary["tet_info_no_data"] is False
    assert summary["tet_info_topic_source"] == ["author"]
    assert summary["tet_info_date_created"] == datetime(2025, 3, 5).isoformat()


def test_positive_without_new_data_novelty():
    topic = "ATP:0000001"
    rows = {topic: [(_tet(datetime(2024, 1, 1), negated=False,
                          data_novelty="ATP:0000000"), _source(BIOCURATOR))]}
    summary = get_tet_list_summary(topic, rows)
    assert summary["tet_info_has_data"] is True
    assert summary["tet_info_new_data"] is False


def test_unknown_assertion_maps_to_computational_and_earliest_date_wins():
    topic = "ATP:0000001"
    rows = {topic: [
        (_tet(datetime(2025, 6, 1), negated=False), _source("ATP:9999998")),
        (_tet("2023-02-02 00:00:00", negated=True), _source(AUTHOR)),
    ]}
    summary = get_tet_list_summary(topic, rows)
    # earliest of the two dates is 2023-02-02
    assert summary["tet_info_date_created"] == datetime(2023, 2, 2).isoformat()
    assert summary["tet_info_has_data"] is True
    assert summary["tet_info_no_data"] is True
    assert summary["tet_info_topic_source"] == ["author", "computational"]
