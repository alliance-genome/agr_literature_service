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
