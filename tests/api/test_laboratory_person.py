# flake8: noqa: F811
from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import (
    LaboratoryModel,
    LaboratoryPersonModel,
    PersonModel,
)
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


LabPersonTestData = namedtuple(
    "LabPersonTestData",
    ["response", "new_id", "laboratory_id", "person_id"],
)


@pytest.fixture
def seeded_lab_and_person(db):  # noqa
    lab = LaboratoryModel(name="People Lab", status="active", lab_is_open=False)
    person = PersonModel(display_name="Lab Member", curie="AGRKB:test-lab-person")
    db.add(lab)
    db.add(person)
    db.commit()
    db.refresh(lab)
    db.refresh(person)
    return {"laboratory_id": lab.laboratory_id, "person_id": person.person_id}


@pytest.fixture
def test_lab_person(db, auth_headers, seeded_lab_and_person):  # noqa
    with TestClient(app) as client:
        response = client.post(
            "/laboratory_person/",
            json={
                "laboratory_curie": str(seeded_lab_and_person["laboratory_id"]),
                "person_curie": str(seeded_lab_and_person["person_id"]),
                "lab_position": "postdoc",
                "is_lab_contact": True,
            },
            headers=auth_headers,
        )
        body = response.json() if response.status_code == status.HTTP_201_CREATED else {}
        yield LabPersonTestData(
            response=response,
            new_id=body.get("laboratory_person_id"),
            laboratory_id=seeded_lab_and_person["laboratory_id"],
            person_id=seeded_lab_and_person["person_id"],
        )


class TestLaboratoryPerson:

    def test_create_lab_person(self, db, test_lab_person):  # noqa
        assert test_lab_person.response.status_code == status.HTTP_201_CREATED
        obj = (
            db.query(LaboratoryPersonModel)
            .filter(LaboratoryPersonModel.laboratory_person_id == test_lab_person.new_id)
            .one()
        )
        assert obj.person_id == test_lab_person.person_id
        assert obj.lab_position == "postdoc"
        assert obj.is_lab_contact is True
        assert obj.can_edit_lab is False

    def test_create_for_invalid_laboratory(self, auth_headers, seeded_lab_and_person):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/laboratory_person/",
                json={
                    "laboratory_curie": "9999999",
                    "person_curie": str(seeded_lab_and_person["person_id"]),
                },
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_create_with_invalid_person(self, auth_headers, seeded_lab_and_person):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/laboratory_person/",
                json={
                    "laboratory_curie": str(seeded_lab_and_person["laboratory_id"]),
                    "person_curie": "9999999",
                },
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_bad_lab_position_rejected(self, auth_headers, seeded_lab_and_person):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/laboratory_person/",
                json={
                    "laboratory_curie": str(seeded_lab_and_person["laboratory_id"]),
                    "person_curie": str(seeded_lab_and_person["person_id"]),
                    "lab_position": "wizard",
                },
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_patch_lab_person(self, auth_headers, test_lab_person):  # noqa
        with TestClient(app) as client:
            res = client.patch(
                f"/laboratory_person/{test_lab_person.new_id}",
                json={"can_edit_lab": True, "lab_position": "co_pi"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            body = res.json()
            assert body["can_edit_lab"] is True
            assert body["lab_position"] == "co_pi"

    def test_show_includes_curies(self, auth_headers, test_lab_person):  # noqa
        with TestClient(app) as client:
            res = client.get(f"/laboratory_person/{test_lab_person.new_id}", headers=auth_headers)
            assert res.status_code == status.HTTP_200_OK
            body = res.json()
            assert "laboratory_curie" in body and "person_curie" in body
            assert body["person_curie"] == "AGRKB:test-lab-person"

    def test_list_for_person(self, auth_headers, test_lab_person):  # noqa
        with TestClient(app) as client:
            res = client.get(
                f"/laboratory_person/person/{test_lab_person.person_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            rows = res.json()
            assert any(r["laboratory_person_id"] == test_lab_person.new_id for r in rows)
            assert all("laboratory_curie" in r and "person_curie" in r for r in rows)

    def test_list_for_nonexistent_person(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.get("/laboratory_person/person/9999999", headers=auth_headers)
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_laboratory_show_includes_lab_persons(self, auth_headers, test_lab_person):  # noqa
        with TestClient(app) as client:
            res = client.get(f"/laboratory/{test_lab_person.laboratory_id}", headers=auth_headers)
            assert res.status_code == status.HTTP_200_OK
            lps = res.json().get("lab_persons") or []
            assert any(lp["laboratory_person_id"] == test_lab_person.new_id for lp in lps)

    def test_person_show_includes_lab_persons(self, auth_headers, test_lab_person):  # noqa
        with TestClient(app) as client:
            res = client.get(f"/person/{test_lab_person.person_id}", headers=auth_headers)
            assert res.status_code == status.HTTP_200_OK
            lps = res.json().get("lab_persons") or []
            assert any(lp["laboratory_person_id"] == test_lab_person.new_id for lp in lps)

    def test_destroy_lab_person(self, auth_headers, test_lab_person):  # noqa
        with TestClient(app) as client:
            res = client.delete(
                f"/laboratory_person/{test_lab_person.new_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_204_NO_CONTENT
