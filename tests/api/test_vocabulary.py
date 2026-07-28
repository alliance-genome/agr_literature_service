"""Table-backed vocabulary tests.

These exercise the full ``/vocabulary`` endpoint through a TestClient and need
DB + Cognito, so they run in the containerized CI suite. The pure static-enum
unit tests live in ``test_vocabulary_static.py``.
"""
from fastapi import status
from starlette.testclient import TestClient

from agr_literature_service.api.main import app
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


class TestTableBackedVocabulary:
    """DB-backed: a table vocabulary is served under the same endpoint and
    autocomplete matches term names and synonyms (runs in CI)."""

    def test_show_and_search_table_backed(self, db, auth_headers):  # noqa
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
        with TestClient(app) as client:
            r = client.get("/vocabulary/does_not_exist", headers=auth_headers)
            assert r.status_code == status.HTTP_404_NOT_FOUND
