# flake8: noqa: F811
import importlib.util
from collections import namedtuple
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
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
        payload = {
            "person_curie": str(seeded_person["person_id"]),
            "note": "Initial note text",
        }
        response = client.post(
            "/person_note/",
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
            payload = {"person_curie": "9999999", "note": "Some note"}
            res = client.post("/person_note/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_create_note_with_linebreaks(self, auth_headers, seeded_person):  # noqa
        with TestClient(app) as client:
            multiline = "Line one\nLine two\nLine three"
            res = client.post(
                "/person_note/",
                json={"person_curie": str(seeded_person["person_id"]), "note": multiline},
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
            assert res.status_code == status.HTTP_200_OK
            assert res.json().get("person_note_id") == test_person_note.new_person_note_id

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
            assert res.status_code == status.HTTP_200_OK

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
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

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
                    "/person_note/",
                    json={"person_curie": str(seeded_person["person_id"]), "note": f"Note number {i}"},
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


class TestPersonNoteFlags:
    """internal (a private curator note) and obsolete (SCRUM-6889)."""

    def _post(self, client, auth_headers, person_id, **flags):
        res = client.post("/person_note/",
                          json={"person_curie": str(person_id), "note": "flag note", **flags},
                          headers=auth_headers)
        assert res.status_code == status.HTTP_201_CREATED
        return res.json()

    def test_create_defaults_to_public_and_current(self, auth_headers, seeded_person):  # noqa
        with TestClient(app) as client:
            body = self._post(client, auth_headers, seeded_person["person_id"])
            assert (body["internal"], body["obsolete"]) == (False, False)

    def test_create_with_flags(self, auth_headers, seeded_person):  # noqa
        with TestClient(app) as client:
            body = self._post(client, auth_headers, seeded_person["person_id"], internal=True, obsolete=True)
            assert (body["internal"], body["obsolete"]) == (True, True)
            fetched = client.get(f"/person_note/{body['person_note_id']}", headers=auth_headers).json()
            assert (fetched["internal"], fetched["obsolete"]) == (True, True)

    def test_patch_flags(self, auth_headers, test_person_note):  # noqa
        note_id = test_person_note.new_person_note_id
        with TestClient(app) as client:
            res = client.patch(f"/person_note/{note_id}", json={"internal": True}, headers=auth_headers)
            assert res.status_code == status.HTTP_200_OK
            assert (res.json()["internal"], res.json()["obsolete"], res.json()["note"]) == (
                True, False, "Initial note text")
            res = client.patch(f"/person_note/{note_id}", json={"obsolete": True, "internal": False},
                               headers=auth_headers)
            assert (res.json()["internal"], res.json()["obsolete"]) == (False, True)

    @pytest.mark.parametrize("flag", ["internal", "obsolete"])
    def test_patch_null_flag_rejected(self, auth_headers, test_person_note, flag):  # noqa
        with TestClient(app) as client:
            res = client.patch(f"/person_note/{test_person_note.new_person_note_id}", json={flag: None},
                               headers=auth_headers)
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_list_and_person_show_return_flags(self, auth_headers, seeded_person):  # noqa
        person_id = seeded_person["person_id"]
        with TestClient(app) as client:
            self._post(client, auth_headers, person_id, internal=True)
            self._post(client, auth_headers, person_id)
            listed = client.get(f"/person_note/person/{person_id}", headers=auth_headers).json()
            assert [n["internal"] for n in listed] == [True, False]
            person = client.get(f"/person/{person_id}", headers=auth_headers).json()
            assert sorted(n["internal"] for n in person["notes"]) == [False, True]

    def test_nested_notes_on_person_create(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.post("/person/", json={"display_name": "Nested Note Person", "notes": [
                {"note": "private", "internal": True}, {"note": "public"}]}, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            person = client.get(f"/person/{res.json()['curie']}", headers=auth_headers).json()
            assert sorted((n["note"], n["internal"], n["obsolete"]) for n in person["notes"]) == [
                ("private", True, False), ("public", False, False)]


def _existing_notes_revision():
    path = (Path(__file__).resolve().parents[2] / "alembic" / "versions"
            / "20261117_c4e1a7d90b23_person_note_existing_notes_internal.py")
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_migration_makes_existing_notes_internal_once(db, seeded_person):  # noqa
    """Revision c4e1a7d90b23 marks every note internal, unless one already is."""
    revision = _existing_notes_revision()
    person_id = seeded_person["person_id"]
    connection = db.connection()

    def run():
        with Operations.context(MigrationContext.configure(connection)):
            revision.upgrade()
        db.expire_all()
        return sorted((n.note, n.internal) for n in db.query(PersonNoteModel).filter_by(person_id=person_id))

    try:
        db.add_all([PersonNoteModel(person_id=person_id, note="a"), PersonNoteModel(person_id=person_id, note="b")])
        db.flush()
        assert run() == [("a", True), ("b", True)]
        db.query(PersonNoteModel).filter_by(person_id=person_id, note="b").one().internal = False
        db.flush()
        assert run() == [("a", True), ("b", False)]
    finally:
        db.rollback()
