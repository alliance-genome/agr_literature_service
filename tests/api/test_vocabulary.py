"""Unit tests for the static-enum vocabulary registry (vocabulary_crud).

These are pure unit tests against the crud layer (no TestClient / DB / auth) so they
run locally; the router is a thin pass-through covered by the containerized suite.
"""
import pytest
from fastapi import HTTPException

from agr_literature_service.api.crud import vocabulary_crud as vc


def _terms(values):
    return [{"value": v, "label": v, "is_obsolete": False} for v in values]


def test_person_active_status_values():
    assert vc.get_vocabulary("person_active_status") == _terms(
        ["active", "retired", "deceased"])


def test_person_privacy_values():
    assert vc.get_vocabulary("person_privacy") == _terms(
        ["show_all", "logged_in_only", "fully_hidden", "hide_email"])


def test_laboratory_status_values():
    assert vc.get_vocabulary("laboratory_status") == _terms(
        ["active", "closed", "unknown"])


def test_laboratory_email_visibility_values():
    assert vc.get_vocabulary("laboratory_email_visibility") == _terms(
        ["public", "logged_in_user", "not_shown"])


def test_vocabulary_term_shape():
    term = vc.get_vocabulary("person_active_status")[0]
    assert set(term) == {"value", "label", "is_obsolete"}
    assert term["value"] == term["label"] == "active"
    assert term["is_obsolete"] is False


def test_unknown_vocabulary_raises_404():
    with pytest.raises(HTTPException) as exc_info:
        vc.get_vocabulary("does_not_exist")
    assert exc_info.value.status_code == 404


def test_list_vocabularies():
    names = vc.list_vocabularies()
    assert names == sorted(names)  # stable, sorted
    assert {"person_active_status", "person_privacy",
            "laboratory_status", "laboratory_email_visibility"} <= set(names)
