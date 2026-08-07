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


def _tet(date_created, negated=False, data_novelty=None,
         confidence_score=None, confidence_level=None,
         entity=None, entity_type=None,
         validation_by_professional_biocurator=None):
    return SimpleNamespace(date_created=date_created, negated=negated,
                           data_novelty=data_novelty,
                           confidence_score=confidence_score,
                           confidence_level=confidence_level,
                           entity=entity, entity_type=entity_type,
                           validation_by_professional_biocurator=validation_by_professional_biocurator)


def _source(assertion, source_method=None):
    return SimpleNamespace(source_evidence_assertion=assertion,
                           source_method=source_method)


def test_topic_missing_returns_defaults():
    summary = get_tet_list_summary("ATP:9999999", {})
    assert summary == {
        "tet_info_date_created": None,
        "tet_info_topic_source": [],
        "tet_info_has_data": False,
        "tet_info_new_data": False,
        "tet_info_no_data": False,
        "tet_info_manual_has_data": False,
        "tet_info_manual_new_data": False,
        "tet_info_manual_no_data": False,
        "tet_info_source_predictions": [],
        "tet_info_manual_assessments": [],
        "tet_info_assessment_states": {
            "has_data": None, "new_data": None, "new_to_db": None,
            "new_to_field": None, "no_data": None,
        },
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
    # A biocurator tag is a manual assessment, not a computed prediction.
    assert summary["tet_info_manual_no_data"] is True
    assert summary["tet_info_source_predictions"] == []


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
    # An author tag is manual: both has and new manual flags set, no predictions.
    assert summary["tet_info_manual_has_data"] is True
    assert summary["tet_info_manual_new_data"] is True
    assert summary["tet_info_source_predictions"] == []


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
    # The computational tag surfaces as a prediction; the author (negated) tag
    # is a manual "no data" assessment.
    assert summary["tet_info_manual_no_data"] is True
    assert summary["tet_info_manual_has_data"] is False
    assert len(summary["tet_info_source_predictions"]) == 1
    assert summary["tet_info_source_predictions"][0]["assessment"] == "has"


def test_classifier_prediction_carries_method_and_confidence():
    topic = "ATP:0000001"
    rows = {topic: [
        (_tet(datetime(2025, 5, 1), negated=False, confidence_score=0.92,
              confidence_level="high", entity="WB:WBGene00000912",
              entity_type="ATP:0000005"),
         _source("ECO:0008004", source_method="abc_document_classifier")),
    ]}
    summary = get_tet_list_summary(topic, rows)
    assert summary["tet_info_manual_has_data"] is False
    preds = summary["tet_info_source_predictions"]
    assert len(preds) == 1
    assert preds[0] == {
        "source_method": "abc_document_classifier",
        "source_evidence_assertion": "ECO:0008004",
        "confidence_score": 0.92,
        "confidence_level": "high",
        "negated": False,
        "assessment": "has",
        "data_novelty": None,
        "entity": "WB:WBGene00000912",
        "entity_type": "ATP:0000005",
    }


def test_assessment_states_biocurator_assertion_is_validated_without_vpb():
    """Loaded biocurator data (source_evidence_assertion = biocurator) shows a
    green check even when validation_by_professional_biocurator is not set
    (e.g. ZFIN load_zfin_allele_reference_tags)."""
    topic = "ATP:0000001"
    rows = {topic: [
        (_tet(datetime(2025, 5, 1), negated=False, data_novelty="ATP:0000335",
              validation_by_professional_biocurator="not_validated"),
         _source(BIOCURATOR)),
    ]}
    states = get_tet_list_summary(topic, rows)["tet_info_assessment_states"]
    assert states == {
        "has_data": "validated", "new_data": None, "new_to_db": None,
        "new_to_field": None, "no_data": None,
    }


def test_assessment_states_validated_positive_suppresses_opposite_no_data():
    topic = "ATP:0000001"
    rows = {topic: [
        # biocurator validated a positive "new to field" tag -> Has data + New to Field ✓
        (_tet(datetime(2025, 5, 1), negated=False, data_novelty="ATP:0000229",
              validation_by_professional_biocurator="validated_right_self"),
         _source(BIOCURATOR)),
        # an unvalidated computational "no data" -> suppressed (opposite polarity
        # of a biocurator-validated positive)
        (_tet(datetime(2025, 5, 2), negated=True,
              validation_by_professional_biocurator="not_validated"),
         _source("ECO:0008004")),
    ]}
    states = get_tet_list_summary(topic, rows)["tet_info_assessment_states"]
    assert states == {
        "has_data": "validated", "new_data": None, "new_to_db": None,
        "new_to_field": "validated", "no_data": None,
    }


def test_assessment_states_unvalidated_positive_shown_when_other_column_validated():
    """Polarity guard: a validated positive on one column must NOT hide an
    unvalidated prediction on a different positive column."""
    topic = "ATP:0000001"
    rows = {topic: [
        # biocurator validated a bare "has data" tag (no specific novelty)
        (_tet(datetime(2025, 5, 1), negated=False, data_novelty="ATP:0000335",
              validation_by_professional_biocurator="validated_right_self"),
         _source(BIOCURATOR)),
        # unvalidated computational "new to field" prediction -> still shows "?"
        (_tet(datetime(2025, 5, 2), negated=False, data_novelty="ATP:0000229",
              validation_by_professional_biocurator="not_validated"),
         _source("ECO:0008004")),
    ]}
    states = get_tet_list_summary(topic, rows)["tet_info_assessment_states"]
    assert states == {
        "has_data": "validated", "new_data": None, "new_to_db": None,
        "new_to_field": "unvalidated", "no_data": None,
    }


def test_assessment_states_validated_no_data_suppresses_positive_predictions():
    topic = "ATP:0000001"
    rows = {topic: [
        # biocurator validated "no data"
        (_tet(datetime(2025, 5, 1), negated=True,
              validation_by_professional_biocurator="validated_right_self"),
         _source(BIOCURATOR)),
        # unvalidated computational positive -> suppressed (opposite polarity)
        (_tet(datetime(2025, 5, 2), negated=False, data_novelty="ATP:0000321",
              validation_by_professional_biocurator="not_validated"),
         _source("ECO:0008004")),
    ]}
    states = get_tet_list_summary(topic, rows)["tet_info_assessment_states"]
    assert states == {
        "has_data": None, "new_data": None, "new_to_db": None,
        "new_to_field": None, "no_data": "validated",
    }


def test_assessment_states_unvalidated_shown_and_wrong_excluded():
    topic = "ATP:0000001"
    rows = {topic: [
        # unvalidated computational positive (generic new) -> ? on Has data + New data
        (_tet(datetime(2025, 5, 1), negated=False, data_novelty="ATP:0000321",
              validation_by_professional_biocurator="not_validated"),
         _source("ECO:0008004")),
        # unvalidated "no data" -> ? on No Data
        (_tet(datetime(2025, 5, 2), negated=True,
              validation_by_professional_biocurator="not_validated"),
         _source("ECO:0008004")),
        # validated_wrong is excluded entirely (should not add New to DB)
        (_tet(datetime(2025, 5, 3), negated=False, data_novelty="ATP:0000228",
              validation_by_professional_biocurator="validated_wrong"),
         _source(BIOCURATOR)),
    ]}
    states = get_tet_list_summary(topic, rows)["tet_info_assessment_states"]
    assert states == {
        "has_data": "unvalidated", "new_data": "unvalidated", "new_to_db": None,
        "new_to_field": None, "no_data": "unvalidated",
    }


def test_manual_assessments_capture_source_negated_and_novelty():
    topic = "ATP:0000001"
    rows = {topic: [
        (_tet(datetime(2025, 5, 1), negated=True, data_novelty="ATP:0000229"),
         _source(BIOCURATOR)),
        (_tet(datetime(2025, 5, 2), negated=False, data_novelty="ATP:0000228"),
         _source(AUTHOR)),
        (_tet(datetime(2025, 5, 3), negated=False, data_novelty="ATP:0000228"),
         _source("ECO:0008004", source_method="abc_document_classifier")),
    ]}
    summary = get_tet_list_summary(topic, rows)
    ma = summary["tet_info_manual_assessments"]
    assert len(ma) == 2
    assert {"source": "biocurator", "negated": True, "assessment": "no",
            "data_novelty": "ATP:0000229"} in ma
    assert {"source": "author", "negated": False, "assessment": "new",
            "data_novelty": "ATP:0000228"} in ma
    # The computed tag stays in source_predictions (not manual_assessments).
    preds = summary["tet_info_source_predictions"]
    assert len(preds) == 1
    assert preds[0]["data_novelty"] == "ATP:0000228"
