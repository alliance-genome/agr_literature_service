# flake8: noqa: F811
from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import PersonLineageModel, PersonModel
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


LineageTestData = namedtuple("LineageTestData", ["response", "new_id", "person_one_id", "person_two_id"])


@pytest.fixture
def two_people(db):  # noqa
    p1 = PersonModel(display_name="Canon One", curie="AGRKB:test-canon-1")
    p2 = PersonModel(display_name="Canon Two", curie="AGRKB:test-canon-2")
    db.add(p1)
    db.add(p2)
    db.commit()
    db.refresh(p1)
    db.refresh(p2)
    return {"person_one_id": p1.person_id, "person_two_id": p2.person_id}


@pytest.fixture
def test_lineage(db, auth_headers, two_people):  # noqa
    with TestClient(app) as client:
        payload = {
            "person_one_id": two_people["person_one_id"],
            "person_two_id": two_people["person_two_id"],
            "relationship": "phd",
        }
        response = client.post("/person_lineage/", json=payload, headers=auth_headers)
        body = response.json() if response.status_code == status.HTTP_201_CREATED else {}
        yield LineageTestData(
            response=response,
            new_id=body.get("person_lineage_id"),
            person_one_id=two_people["person_one_id"],
            person_two_id=two_people["person_two_id"],
        )


class TestPersonLineage:

    def test_create_lineage(self, db, test_lineage):  # noqa
        assert test_lineage.response.status_code == status.HTTP_201_CREATED
        obj = (
            db.query(PersonLineageModel)
            .filter(PersonLineageModel.person_lineage_id == test_lineage.new_id)
            .one()
        )
        assert obj.person_one_id == test_lineage.person_one_id
        assert obj.person_two_id == test_lineage.person_two_id
        assert obj.relationship == "phd"

    def test_duplicate_rejected(self, auth_headers, test_lineage):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/person_lineage/",
                json={
                    "person_one_id": test_lineage.person_one_id,
                    "person_two_id": test_lineage.person_two_id,
                    "relationship": "phd",
                },
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_reversed_pair_allowed(self, auth_headers, test_lineage):  # noqa
        # Directional: B->A is a different fact from A->B.
        with TestClient(app) as client:
            res = client.post(
                "/person_lineage/",
                json={
                    "person_one_id": test_lineage.person_two_id,
                    "person_two_id": test_lineage.person_one_id,
                    "relationship": "phd",
                },
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_201_CREATED

    def test_invalid_person_rejected(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/person_lineage/",
                json={"person_one_id": 9999999, "person_two_id": 9999998, "relationship": "phd"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_show_and_patch(self, auth_headers, test_lineage):  # noqa
        with TestClient(app) as client:
            res = client.get(f"/person_lineage/{test_lineage.new_id}", headers=auth_headers)
            assert res.status_code == status.HTTP_200_OK

            res = client.patch(
                f"/person_lineage/{test_lineage.new_id}",
                json={"relationship": "collaborated"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            assert res.json()["relationship"] == "collaborated"

    def test_destroy(self, auth_headers, test_lineage):  # noqa
        with TestClient(app) as client:
            res = client.delete(f"/person_lineage/{test_lineage.new_id}", headers=auth_headers)
            assert res.status_code == status.HTTP_204_NO_CONTENT
            res = client.get(f"/person_lineage/{test_lineage.new_id}", headers=auth_headers)
            assert res.status_code == status.HTTP_404_NOT_FOUND
