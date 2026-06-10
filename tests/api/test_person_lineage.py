# flake8: noqa: F811
from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import PersonLineageModel, PersonModel
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


LineageTestData = namedtuple(
    "LineageTestData",
    ["response", "new_id"],
)


@pytest.fixture
def test_lineage(db, auth_headers):  # noqa
    with TestClient(app) as client:
        # Only names, relationship and who_sent_this — no person FKs or dates.
        payload = {
            "person_one_name": "Alice Advisor",
            "person_two_name": "Bob Trainee",
            "relationship": "phd",
            "who_sent_this": "curator1",
        }
        response = client.post("/person_lineage/", json=payload, headers=auth_headers)
        body = response.json() if response.status_code == status.HTTP_201_CREATED else {}
        yield LineageTestData(
            response=response,
            new_id=body.get("person_lineage_id"),
        )


class TestPersonLineage:

    def test_create_lineage_names_only(self, db, test_lineage):  # noqa
        assert test_lineage.response.status_code == status.HTTP_201_CREATED
        obj = (
            db.query(PersonLineageModel)
            .filter(PersonLineageModel.person_lineage_id == test_lineage.new_id)
            .one()
        )
        assert obj.person_one_name == "Alice Advisor"
        assert obj.person_two_name == "Bob Trainee"
        assert obj.relationship == "phd"
        assert obj.who_sent_this == "curator1"
        assert obj.person_one is None
        assert obj.person_two is None

    def test_missing_required_rejected(self, auth_headers):  # noqa
        with TestClient(app) as client:
            # missing who_sent_this
            res = client.post(
                "/person_lineage/",
                json={
                    "person_one_name": "A",
                    "person_two_name": "B",
                    "relationship": "phd",
                },
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_bad_relationship_rejected(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/person_lineage/",
                json={
                    "person_one_name": "A",
                    "person_two_name": "B",
                    "relationship": "bogus",
                    "who_sent_this": "x",
                },
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_create_with_person_links(self, db, auth_headers):  # noqa
        person = PersonModel(display_name="Linked Person", curie="AGRKB:test-lineage-person")
        db.add(person)
        db.commit()
        db.refresh(person)
        with TestClient(app) as client:
            res = client.post(
                "/person_lineage/",
                json={
                    "person_one_name": "Linked Person",
                    "person_two_name": "Unknown Trainee",
                    "relationship": "postdoc",
                    "who_sent_this": "curator2",
                    "person_one": person.person_id,
                },
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_201_CREATED
            assert res.json()["person_one"] == person.person_id

    def test_invalid_person_link_rejected(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/person_lineage/",
                json={
                    "person_one_name": "A",
                    "person_two_name": "B",
                    "relationship": "phd",
                    "who_sent_this": "x",
                    "person_one": 9999999,
                },
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_patch_lineage(self, auth_headers, test_lineage):  # noqa
        with TestClient(app) as client:
            res = client.patch(
                f"/person_lineage/{test_lineage.new_id}",
                json={"relationship": "collaborated"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            assert res.json()["relationship"] == "collaborated"

    def test_patch_person_ids(self, db, auth_headers, test_lineage):  # noqa
        # Person object links are late-bound: created names-only, then linked via PATCH.
        p1 = PersonModel(display_name="Patched One", curie="AGRKB:test-lineage-patch-1")
        p2 = PersonModel(display_name="Patched Two", curie="AGRKB:test-lineage-patch-2")
        db.add(p1)
        db.add(p2)
        db.commit()
        db.refresh(p1)
        db.refresh(p2)
        with TestClient(app) as client:
            res = client.patch(
                f"/person_lineage/{test_lineage.new_id}",
                json={"person_one": p1.person_id, "person_two": p2.person_id},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            body = res.json()
            assert body["person_one"] == p1.person_id
            assert body["person_two"] == p2.person_id

    def test_patch_start_and_end_dates(self, auth_headers, test_lineage):  # noqa
        with TestClient(app) as client:
            res = client.patch(
                f"/person_lineage/{test_lineage.new_id}",
                json={
                    "start_date": "2020-01-15T00:00:00",
                    "end_date": "2023-06-30T00:00:00",
                },
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            body = res.json()
            assert body["start_date"] is not None
            assert body["start_date"].startswith("2020-01-15")
            assert body["end_date"] is not None
            assert body["end_date"].startswith("2023-06-30")

    def test_patch_null_required_rejected(self, auth_headers, test_lineage):  # noqa
        with TestClient(app) as client:
            res = client.patch(
                f"/person_lineage/{test_lineage.new_id}",
                json={"who_sent_this": None},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_destroy_lineage(self, auth_headers, test_lineage):  # noqa
        with TestClient(app) as client:
            res = client.delete(
                f"/person_lineage/{test_lineage.new_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_204_NO_CONTENT
            res = client.get(
                f"/person_lineage/{test_lineage.new_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_404_NOT_FOUND
