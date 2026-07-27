"""Vocabulary tests.

The static-enum ``vocabulary_crud`` unit tests are pure (no TestClient / DB / auth)
so they run locally. The table-backed cases exercise the full endpoint through a
TestClient and need DB + Cognito, so they run in the containerized CI suite. The
DB/auth fixtures pull in config deps that are absent from the bare local shell, so
their import is guarded: locally the table-backed tests skip, in CI they run.
"""
import pytest
from fastapi import status

from agr_literature_service.api.crud import vocabulary_crud as vc

try:
    from ..fixtures import db  # noqa
    from .fixtures import auth_headers  # noqa
except ModuleNotFoundError:  # pragma: no cover - CI has these deps; local shell may not
    @pytest.fixture
    def db():  # noqa
        pytest.skip("DB fixtures unavailable outside the container")

    @pytest.fixture
    def auth_headers():  # noqa
        pytest.skip("auth fixtures unavailable outside the container")


def _terms(values):
    return [{"value": v, "label": v, "is_obsolete": False} for v in values]


def test_person_active_status_values():
    assert vc.get_vocabulary(None, "person_active_status") == _terms(
        ["active", "retired", "deceased"])


def test_person_privacy_values():
    assert vc.get_vocabulary(None, "person_privacy") == _terms(
        ["show_all", "logged_in_only", "fully_hidden", "hide_email"])


def test_laboratory_status_values():
    assert vc.get_vocabulary(None, "laboratory_status") == _terms(
        ["active", "closed", "unknown"])


def test_laboratory_email_visibility_values():
    assert vc.get_vocabulary(None, "laboratory_email_visibility") == _terms(
        ["public", "logged_in_user", "not_shown"])


def test_vocabulary_term_shape():
    term = vc.get_vocabulary(None, "person_active_status")[0]
    assert set(term) == {"value", "label", "is_obsolete"}
    assert term["value"] == term["label"] == "active"
    assert term["is_obsolete"] is False


def test_list_vocabularies():
    names = vc.list_vocabularies()
    assert names == sorted(names)  # stable, sorted
    assert {"person_active_status", "person_privacy",
            "laboratory_status", "laboratory_email_visibility"} <= set(names)


class TestTableBackedVocabulary:
    """DB-backed: a table vocabulary is served under the same endpoint and
    autocomplete matches term names and synonyms (runs in CI)."""

    def test_show_and_search_table_backed(self, db, auth_headers):  # noqa
        from starlette.testclient import TestClient
        from agr_literature_service.api.main import app
        with TestClient(app) as client:
            vid = client.post(
                "/vocabulary_abc/", json={"vocabulary": "Laboratory Role"},
                headers=auth_headers).json()
            tid = client.post(
                "/vocabulary_term_abc/",
                json={"vocabulary_abc_id": vid, "name": "Post-Doc"},
                headers=auth_headers).json()
            client.post(
                "/vocabulary_term_synonym_abc/",
                json={"vocabulary_term_abc_id": tid, "synonym_name": "postdoc"},
                headers=auth_headers)

            g = client.get("/vocabulary/Laboratory Role", headers=auth_headers)
            assert g.status_code == status.HTTP_200_OK
            terms = g.json()
            assert {"value": tid, "label": "Post-Doc", "is_obsolete": False} in terms

            # synonym "postdoc" resolves to the canonical "Post-Doc" term
            s = client.get("/vocabulary/Laboratory Role/search?q=postd",
                           headers=auth_headers)
            assert s.status_code == status.HTTP_200_OK
            hits = s.json()
            assert any(h["value"] == tid and h["label"] == "Post-Doc" for h in hits)

    def test_unknown_vocabulary_raises_404(self, db, auth_headers):  # noqa
        from starlette.testclient import TestClient
        from agr_literature_service.api.main import app
        with TestClient(app) as client:
            r = client.get("/vocabulary/does_not_exist", headers=auth_headers)
            assert r.status_code == status.HTTP_404_NOT_FOUND
