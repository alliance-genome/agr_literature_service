# flake8: noqa: F811
from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import (
    PersonModel,
    PersonNoteModel,
)
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


PersonNoteTestData = namedtuple(
    "PersonNoteTestData",
    [
        "response",
        "new_person_note_id",
        "person_id",
    ],
)


@pytest.fixture
def seeded_person(db):
    """Create a Person for note tests."""
    person = PersonModel(
        display_name="Note Test Person",
        curie="AGRKB:test-note-person",
    )
    db.add(person)
    db.commit()
    db.refresh(person)
    return {"person_id": person.person_id}


@pytest.fixture
def test_person_note(db, auth_headers, seeded_person):  # noqa
    """Create a baseline person_note row to reuse across tests."""
    with TestClient(app) as client:
        payload = {"note": "Initial note text"}
        response = client.post(
            f"/person_note/person/{seeded_person['person_id']}",
            json=payload,
            headers=auth_headers,
        )
        body = response.json() if response.status_code == status.HTTP_201_CREATED else {}
        yield PersonNoteTestData(
            response=response,
            new_person_note_id=body.get("person_note_id"),
            person_id=seeded_person["person_id"],
        )


class TestPersonNote:

    def test_get_bad_person_note(self, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get("/person_note/-1", headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_create_person_note(self, db, test_person_note):  # noqa
        assert test_person_note.response.status_code == status.HTTP_201_CREATED

        pn = (
            db.query(PersonNoteModel)
            .filter(PersonNoteModel.person_note_id == test_person_note.new_person_note_id)
            .one()
        )
        assert pn.person_id == test_person_note.person_id
        assert pn.note == "Initial note text"

    def test_create_person_note_invalid_person(self, auth_headers):  # noqa
        with TestClient(app) as client:
            payload = {"note": "Some note"}
            res = client.post("/person_note/person/9999999", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_create_note_with_linebreaks(self, auth_headers, seeded_person):  # noqa
        with TestClient(app) as client:
            multiline = "Line one\nLine two\nLine three"
            res = client.post(
                f"/person_note/person/{seeded_person['person_id']}",
                json={"note": multiline},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_201_CREATED
            note_id = res.json()["person_note_id"]

            fetched = client.get(f"/person_note/{note_id}", headers=auth_headers)
            assert fetched.json()["note"] == multiline

    def test_list_for_person(self, auth_headers, test_person_note):  # noqa
        with TestClient(app) as client:
            res = client.get(
                f"/person_note/person/{test_person_note.person_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            rows = res.json()
            assert isinstance(rows, list)
            assert len(rows) >= 1
            assert any(r["person_note_id"] == test_person_note.new_person_note_id for r in rows)

    def test_list_for_nonexistent_person(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.get("/person_note/person/9999999", headers=auth_headers)
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_show_person_note(self, test_person_note, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get(
                f"/person_note/{test_person_note.new_person_note_id}",
                headers=auth_headers,
            )
            assert response.status_code == status.HTTP_200_OK
            body = response.json()
            assert body["person_note_id"] == test_person_note.new_person_note_id
            assert body["note"] == "Initial note text"

    def test_patch_person_note(self, auth_headers, test_person_note):  # noqa
        with TestClient(app) as client:
            res = client.patch(
                f"/person_note/{test_person_note.new_person_note_id}",
                json={"note": "Updated note text"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_202_ACCEPTED
            assert res.json().get("message") == "updated"

            fetched = client.get(
                f"/person_note/{test_person_note.new_person_note_id}",
                headers=auth_headers,
            )
            assert fetched.json()["note"] == "Updated note text"

    def test_patch_note_with_linebreaks(self, auth_headers, test_person_note):  # noqa
        with TestClient(app) as client:
            multiline = "Updated\nwith\nlinebreaks"
            res = client.patch(
                f"/person_note/{test_person_note.new_person_note_id}",
                json={"note": multiline},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_202_ACCEPTED

            fetched = client.get(
                f"/person_note/{test_person_note.new_person_note_id}",
                headers=auth_headers,
            )
            assert fetched.json()["note"] == multiline

    def test_patch_null_note_rejected(self, auth_headers, test_person_note):  # noqa
        """PATCH with note=null should be rejected at Pydantic layer with 422."""
        with TestClient(app) as client:
            res = client.patch(
                f"/person_note/{test_person_note.new_person_note_id}",
                json={"note": None},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_destroy_person_note(self, test_person_note, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.delete(
                f"/person_note/{test_person_note.new_person_note_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_204_NO_CONTENT

            res = client.get(
                f"/person_note/{test_person_note.new_person_note_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_destroy_nonexistent(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.delete("/person_note/-1", headers=auth_headers)
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_multiple_notes_per_person(self, auth_headers, seeded_person):  # noqa
        """A person can have multiple notes."""
        with TestClient(app) as client:
            for i in range(3):
                res = client.post(
                    f"/person_note/person/{seeded_person['person_id']}",
                    json={"note": f"Note number {i}"},
                    headers=auth_headers,
                )
                assert res.status_code == status.HTTP_201_CREATED

            res = client.get(
                f"/person_note/person/{seeded_person['person_id']}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            notes = res.json()
            assert len(notes) == 3
