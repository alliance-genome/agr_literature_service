import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


@pytest.fixture
def test_vocabulary(db, auth_headers):  # noqa
    with TestClient(app) as client:
        r = client.post("/vocabulary_abc/", json={"vocabulary": "Laboratory Role"},
                        headers=auth_headers)
        yield r


class TestVocabularyAbc:
    def test_create(self, test_vocabulary):  # noqa
        assert test_vocabulary.status_code == status.HTTP_201_CREATED

    def test_show(self, db, test_vocabulary, auth_headers):  # noqa
        vid = test_vocabulary.json()
        with TestClient(app) as client:
            r = client.get(f"/vocabulary_abc/{vid}", headers=auth_headers)
            assert r.status_code == status.HTTP_200_OK
            assert r.json()["vocabulary"] == "Laboratory Role"

    def test_duplicate_conflict(self, db, test_vocabulary, auth_headers):  # noqa
        with TestClient(app) as client:
            r = client.post("/vocabulary_abc/", json={"vocabulary": "Laboratory Role"},
                            headers=auth_headers)
            assert r.status_code == status.HTTP_409_CONFLICT

    def test_blank_rejected(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            r = client.post("/vocabulary_abc/", json={"vocabulary": "  "}, headers=auth_headers)
            assert r.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_destroy_unused_vocabulary(self, db, test_vocabulary, auth_headers):  # noqa
        vid = test_vocabulary.json()
        with TestClient(app) as client:
            d = client.delete(f"/vocabulary_abc/{vid}", headers=auth_headers)
            assert d.status_code == status.HTTP_204_NO_CONTENT
            g = client.get(f"/vocabulary_abc/{vid}", headers=auth_headers)
            assert g.status_code == status.HTTP_404_NOT_FOUND


class TestVocabularyTermAbc:
    def test_create_and_show(self, db, test_vocabulary, auth_headers):  # noqa
        vid = test_vocabulary.json()
        with TestClient(app) as client:
            r = client.post("/vocabulary_term_abc/",
                            json={"vocabulary_abc_id": vid, "name": "Post-Doc"},
                            headers=auth_headers)
            assert r.status_code == status.HTTP_201_CREATED
            tid = r.json()
            g = client.get(f"/vocabulary_term_abc/{tid}", headers=auth_headers)
            assert g.status_code == status.HTTP_200_OK
            assert g.json()["name"] == "Post-Doc"
            assert g.json()["is_obsolete"] is False

    def test_duplicate_name_in_vocab_conflict(self, db, test_vocabulary, auth_headers):  # noqa
        vid = test_vocabulary.json()
        with TestClient(app) as client:
            client.post("/vocabulary_term_abc/",
                        json={"vocabulary_abc_id": vid, "name": "Technician"}, headers=auth_headers)
            r = client.post("/vocabulary_term_abc/",
                            json={"vocabulary_abc_id": vid, "name": "Technician"}, headers=auth_headers)
            assert r.status_code == status.HTTP_409_CONFLICT

    def test_destroy_unused_term(self, db, test_vocabulary, auth_headers):  # noqa
        vid = test_vocabulary.json()
        with TestClient(app) as client:
            tid = client.post("/vocabulary_term_abc/",
                              json={"vocabulary_abc_id": vid, "name": "Volunteer"},
                              headers=auth_headers).json()
            d = client.delete(f"/vocabulary_term_abc/{tid}", headers=auth_headers)
            assert d.status_code == status.HTTP_204_NO_CONTENT
            g = client.get(f"/vocabulary_term_abc/{tid}", headers=auth_headers)
            assert g.status_code == status.HTTP_404_NOT_FOUND


class TestVocabularyTermSynonymAbc:
    def test_create_and_show(self, db, test_vocabulary, auth_headers):  # noqa
        vid = test_vocabulary.json()
        with TestClient(app) as client:
            tid = client.post("/vocabulary_term_abc/",
                              json={"vocabulary_abc_id": vid, "name": "Post-Doc"},
                              headers=auth_headers).json()
            r = client.post("/vocabulary_term_synonym_abc/",
                            json={"vocabulary_term_abc_id": tid, "synonym_name": "postdoc"},
                            headers=auth_headers)
            assert r.status_code == status.HTTP_201_CREATED
            sid = r.json()
            g = client.get(f"/vocabulary_term_synonym_abc/{sid}", headers=auth_headers)
            assert g.status_code == status.HTTP_200_OK
            assert g.json()["synonym_name"] == "postdoc"
