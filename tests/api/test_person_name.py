# flake8: noqa: F811
from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import (
    PersonModel,
    PersonNameModel,
)
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


PersonNameTestData = namedtuple(
    "PersonNameTestData",
    [
        "response",
        "new_person_name_id",
        "person_id",
    ],
)


@pytest.fixture
def seeded_person(db):
    """Create a Person for name tests."""
    person = PersonModel(
        display_name="Test Person",
        curie="AGRKB:test-name-person",
    )
    db.add(person)
    db.commit()
    db.refresh(person)
    return {"person_id": person.person_id}


@pytest.fixture
def test_person_name(db, auth_headers, seeded_person):  # noqa
    """Create a baseline person_name row to reuse across tests."""
    with TestClient(app) as client:
        payload = {
            "first_name": "Alice",
            "middle_name": "M",
            "last_name": "Smith",
        }
        response = client.post(
            f"/person_name/person/{seeded_person['person_id']}",
            json=payload,
            headers=auth_headers,
        )
        body = response.json() if response.status_code == status.HTTP_201_CREATED else {}
        yield PersonNameTestData(
            response=response,
            new_person_name_id=body.get("person_name_id"),
            person_id=seeded_person["person_id"],
        )


class TestPersonName:

    def test_get_bad_person_name(self, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get("/person_name/-1", headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_create_person_name(self, db, test_person_name):  # noqa
        assert test_person_name.response.status_code == status.HTTP_201_CREATED

        pn = (
            db.query(PersonNameModel)
            .filter(PersonNameModel.person_name_id == test_person_name.new_person_name_id)
            .one()
        )
        assert pn.person_id == test_person_name.person_id
        assert pn.first_name == "Alice"
        assert pn.middle_name == "M"
        assert pn.last_name == "Smith"
        # First name for person should auto-set primary=True
        assert pn.primary is True

    def test_create_person_name_invalid_person(self, auth_headers):  # noqa
        with TestClient(app) as client:
            payload = {
                "first_name": "Bad",
                "last_name": "Person",
            }
            res = client.post("/person_name/person/9999999", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_create_second_name_no_primary(self, db, auth_headers, test_person_name):  # noqa
        """Second name without explicit primary should not become primary."""
        with TestClient(app) as client:
            payload = {
                "first_name": "Bob",
                "last_name": "Jones",
            }
            res = client.post(
                f"/person_name/person/{test_person_name.person_id}",
                json=payload,
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_201_CREATED
            body = res.json()
            # Second name should not be primary
            assert body.get("primary") is not True

            # Original should still be primary
            orig = (
                db.query(PersonNameModel)
                .filter(PersonNameModel.person_name_id == test_person_name.new_person_name_id)
                .one()
            )
            assert orig.primary is True

    def test_create_with_explicit_primary_demotes_old(self, db, auth_headers, test_person_name):  # noqa
        """Creating a name with primary=true should demote the existing primary."""
        with TestClient(app) as client:
            payload = {
                "first_name": "Carlos",
                "last_name": "Garcia",
                "primary": True,
            }
            res = client.post(
                f"/person_name/person/{test_person_name.person_id}",
                json=payload,
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_201_CREATED
            new_body = res.json()
            assert new_body["primary"] is True

            # Old primary should be demoted
            orig = (
                db.query(PersonNameModel)
                .filter(PersonNameModel.person_name_id == test_person_name.new_person_name_id)
                .one()
            )
            db.refresh(orig)
            assert orig.primary is False

    def test_list_for_person(self, auth_headers, test_person_name):  # noqa
        with TestClient(app) as client:
            res = client.get(
                f"/person_name/person/{test_person_name.person_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            rows = res.json()
            assert isinstance(rows, list)
            assert len(rows) >= 1
            assert any(r["person_name_id"] == test_person_name.new_person_name_id for r in rows)

    def test_list_for_nonexistent_person(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.get("/person_name/person/9999999", headers=auth_headers)
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_show_person_name(self, test_person_name, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get(
                f"/person_name/{test_person_name.new_person_name_id}",
                headers=auth_headers,
            )
            assert response.status_code == status.HTTP_200_OK
            body = response.json()
            assert body["person_name_id"] == test_person_name.new_person_name_id
            assert body["first_name"] == "Alice"
            assert body["middle_name"] == "M"
            assert body["last_name"] == "Smith"

    def test_patch_person_name_fields(self, auth_headers, test_person_name):  # noqa
        with TestClient(app) as client:
            patch_payload = {
                "first_name": "Alicia",
                "middle_name": "Marie",
                "last_name": "Johnson",
            }
            res = client.patch(
                f"/person_name/{test_person_name.new_person_name_id}",
                json=patch_payload,
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_202_ACCEPTED
            assert res.json().get("message") == "updated"

            fetched = client.get(
                f"/person_name/{test_person_name.new_person_name_id}",
                headers=auth_headers,
            )
            assert fetched.status_code == status.HTTP_200_OK
            body = fetched.json()
            assert body["first_name"] == "Alicia"
            assert body["middle_name"] == "Marie"
            assert body["last_name"] == "Johnson"

    def test_patch_primary_flag(self, db, auth_headers, test_person_name):  # noqa
        """Patching primary=true on a second name should demote the old primary."""
        with TestClient(app) as client:
            # Create a second non-primary name
            second = client.post(
                f"/person_name/person/{test_person_name.person_id}",
                json={"first_name": "Diana", "last_name": "Prince"},
                headers=auth_headers,
            )
            assert second.status_code == status.HTTP_201_CREATED
            second_id = second.json()["person_name_id"]

            # Patch it to primary
            res = client.patch(
                f"/person_name/{second_id}",
                json={"primary": True},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_202_ACCEPTED

            # Verify new one is primary
            fetched = client.get(f"/person_name/{second_id}", headers=auth_headers)
            assert fetched.json()["primary"] is True

            # Verify old one is demoted
            orig = (
                db.query(PersonNameModel)
                .filter(PersonNameModel.person_name_id == test_person_name.new_person_name_id)
                .one()
            )
            db.refresh(orig)
            assert orig.primary is False

    def test_patch_primary_to_false(self, auth_headers, test_person_name):  # noqa
        """Patching primary=false should be allowed — no enforcement that one must stay primary."""
        with TestClient(app) as client:
            res = client.patch(
                f"/person_name/{test_person_name.new_person_name_id}",
                json={"primary": False},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_202_ACCEPTED

            fetched = client.get(
                f"/person_name/{test_person_name.new_person_name_id}",
                headers=auth_headers,
            )
            assert fetched.json()["primary"] is False

    def test_patch_null_last_name_rejected(self, auth_headers, test_person_name):  # noqa
        """PATCH with last_name=null should be rejected at Pydantic layer with 422."""
        with TestClient(app) as client:
            res = client.patch(
                f"/person_name/{test_person_name.new_person_name_id}",
                json={"last_name": None},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_destroy_person_name(self, auth_headers, seeded_person):  # noqa
        """Delete a non-primary name."""
        with TestClient(app) as client:
            # Create primary name
            client.post(
                f"/person_name/person/{seeded_person['person_id']}",
                json={"first_name": "Keep", "last_name": "Me"},
                headers=auth_headers,
            )
            # Create a second (non-primary) name
            res = client.post(
                f"/person_name/person/{seeded_person['person_id']}",
                json={"first_name": "Delete", "last_name": "Me"},
                headers=auth_headers,
            )
            delete_id = res.json()["person_name_id"]

            # Delete it
            res = client.delete(f"/person_name/{delete_id}", headers=auth_headers)
            assert res.status_code == status.HTTP_204_NO_CONTENT

            # Verify it's gone
            res = client.get(f"/person_name/{delete_id}", headers=auth_headers)
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_destroy_primary_person_name(self, db, auth_headers, seeded_person):  # noqa
        """Delete a primary name — should succeed, remaining names have no primary."""
        with TestClient(app) as client:
            # Create primary name (auto-primary as first)
            res1 = client.post(
                f"/person_name/person/{seeded_person['person_id']}",
                json={"first_name": "Primary", "last_name": "Name"},
                headers=auth_headers,
            )
            primary_id = res1.json()["person_name_id"]

            # Create second name
            res2 = client.post(
                f"/person_name/person/{seeded_person['person_id']}",
                json={"first_name": "Secondary", "last_name": "Name"},
                headers=auth_headers,
            )
            secondary_id = res2.json()["person_name_id"]

            # Delete the primary
            res = client.delete(f"/person_name/{primary_id}", headers=auth_headers)
            assert res.status_code == status.HTTP_204_NO_CONTENT

            # Verify secondary still exists but has no primary
            remaining = (
                db.query(PersonNameModel)
                .filter(PersonNameModel.person_name_id == secondary_id)
                .one()
            )
            db.refresh(remaining)
            assert remaining.primary is not True

    def test_destroy_nonexistent(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.delete("/person_name/-1", headers=auth_headers)
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_create_person_with_inline_names(self, auth_headers):  # noqa
        """POST /person/ with inline names, second marked primary."""
        with TestClient(app) as client:
            payload = {
                "display_name": "Inline Test",
                "curie": "AGRKB:test-inline-names",
                "names": [
                    {"first_name": "John", "last_name": "Smith"},
                    {"first_name": "Juan", "middle_name": "Carlos", "last_name": "Garcia", "primary": True},
                ],
            }
            res = client.post("/person/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            person_id = client.get(f"/person/{res.json()}", headers=auth_headers).json()["person_id"]

            # Fetch person to check names
            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            assert fetched.status_code == status.HTTP_200_OK
            body = fetched.json()
            names = body.get("names", [])
            assert len(names) == 2

            # Find which is primary
            primary_names = [n for n in names if n.get("primary") is True]
            assert len(primary_names) == 1
            assert primary_names[0]["last_name"] == "Garcia"

    def test_create_person_with_inline_names_default_primary(self, auth_headers):  # noqa
        """POST /person/ with inline names, none marked primary — first should auto-become primary."""
        with TestClient(app) as client:
            payload = {
                "display_name": "Default Primary Test",
                "curie": "AGRKB:test-default-primary",
                "names": [
                    {"first_name": "Alpha", "last_name": "First"},
                    {"first_name": "Beta", "last_name": "Second"},
                ],
            }
            res = client.post("/person/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            person_id = client.get(f"/person/{res.json()}", headers=auth_headers).json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            body = fetched.json()
            names = body.get("names", [])
            assert len(names) == 2

            primary_names = [n for n in names if n.get("primary") is True]
            assert len(primary_names) == 1
            assert primary_names[0]["last_name"] == "First"
