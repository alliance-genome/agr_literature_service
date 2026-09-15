# flake8: noqa: F811
"""Tests for the person_institution child table.

Split in two:

* ``TestPersonInstitutionModel`` / ``TestPersonInstitutionCrud`` use only the
  ``db`` fixture and run anywhere the test database is reachable.
* ``TestPersonInstitutionApi`` needs the Cognito ``auth_headers`` fixture and
  therefore only runs in CI / ``make run-test-bash``, where the admin client
  credentials are available.
"""
from datetime import datetime

import pytest
from fastapi import HTTPException
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.crud import person_crud, person_institution_crud
from agr_literature_service.api.models import (
    PersonModel,
    PersonInstitutionModel,
)
from agr_literature_service.api.schemas import (
    PersonInstitutionSchemaCreate,
    PersonSchemaCreate,
)
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


@pytest.fixture
def seeded_person(db):  # noqa
    """Create a Person for institution tests."""
    person = PersonModel(
        display_name="Institution Test Person",
        curie="AGRKB:test-institution-person",
    )
    db.add(person)
    db.commit()
    db.refresh(person)
    return person


class TestPersonInstitutionModel:

    def test_institution_row_defaults_to_active(self, db, seeded_person):  # noqa
        """A new row with no date_made_old_institution is an active one."""
        row = PersonInstitutionModel(
            person_id=seeded_person.person_id,
            institution="Caltech",
        )
        db.add(row)
        db.commit()
        db.refresh(row)

        assert row.person_institution_id is not None
        assert row.date_made_old_institution is None
        assert str(row) == "Caltech [active]"

    def test_marking_a_row_old_sets_the_timestamp(self, db, seeded_person):  # noqa
        row = PersonInstitutionModel(
            person_id=seeded_person.person_id,
            institution="Caltech",
        )
        db.add(row)
        db.commit()

        row.date_made_old_institution = datetime(2020, 1, 1)
        db.commit()
        db.refresh(row)

        assert row.date_made_old_institution == datetime(2020, 1, 1)
        assert str(row) == "Caltech [old]"

    def test_same_institution_may_be_both_old_and_active(self, db, seeded_person):  # noqa
        """Regression test for the deliberate divergence from person_email.

        person_email carries a (person_id, lower(email_address)) unique index.
        person_institution must NOT, because people return to institutions:
        Caltech -> MIT -> Caltech. The old Caltech row and the new active one
        have to coexist.
        """
        db.add(
            PersonInstitutionModel(
                person_id=seeded_person.person_id,
                institution="Caltech",
                date_made_old_institution=datetime(2015, 6, 1),
            )
        )
        db.add(
            PersonInstitutionModel(
                person_id=seeded_person.person_id,
                institution="MIT",
                date_made_old_institution=datetime(2020, 6, 1),
            )
        )
        db.add(
            PersonInstitutionModel(
                person_id=seeded_person.person_id,
                institution="Caltech",
            )
        )
        db.commit()

        rows = (
            db.query(PersonInstitutionModel)
            .filter(PersonInstitutionModel.person_id == seeded_person.person_id)
            .all()
        )
        assert len(rows) == 3
        caltech = [r for r in rows if r.institution == "Caltech"]
        assert len(caltech) == 2
        assert sum(1 for r in caltech if r.date_made_old_institution is None) == 1

    def test_person_relationship_and_curie(self, db, seeded_person):  # noqa
        row = PersonInstitutionModel(
            person_id=seeded_person.person_id,
            institution="Caltech",
        )
        db.add(row)
        db.commit()
        db.refresh(seeded_person)

        assert [r.institution for r in seeded_person.institutions] == ["Caltech"]
        assert row.person_curie == "AGRKB:test-institution-person"

    def test_deleting_the_person_removes_its_institutions(self, db, seeded_person):  # noqa
        db.add(
            PersonInstitutionModel(
                person_id=seeded_person.person_id,
                institution="Caltech",
            )
        )
        db.commit()
        person_id = seeded_person.person_id

        db.delete(seeded_person)
        db.commit()

        remaining = (
            db.query(PersonInstitutionModel)
            .filter(PersonInstitutionModel.person_id == person_id)
            .count()
        )
        assert remaining == 0

    def test_person_no_longer_has_an_institution_column(self, db):  # noqa
        """The array column is replaced by the child table, not kept alongside."""
        assert not hasattr(PersonModel, "institution")


class TestPersonInstitutionCrud:

    def test_create_for_person(self, db, seeded_person):  # noqa
        obj = person_institution_crud.create_for_person(
            db, seeded_person.person_id, {"institution": "Caltech"}
        )
        assert obj.person_institution_id is not None
        assert obj.institution == "Caltech"
        assert obj.date_made_old_institution is None

    def test_create_trims_surrounding_whitespace(self, db, seeded_person):  # noqa
        obj = person_institution_crud.create_for_person(
            db, seeded_person.person_id, {"institution": "  Caltech  "}
        )
        assert obj.institution == "Caltech"

    def test_create_rejects_blank_institution(self, db, seeded_person):  # noqa
        with pytest.raises(HTTPException) as exc:
            person_institution_crud.create_for_person(
                db, seeded_person.person_id, {"institution": "   "}
            )
        assert exc.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_create_for_unknown_person_is_404(self, db):  # noqa
        with pytest.raises(HTTPException) as exc:
            person_institution_crud.create_for_person(
                db, -1, {"institution": "Caltech"}
            )
        assert exc.value.status_code == status.HTTP_404_NOT_FOUND

    def test_create_allows_a_duplicate_institution(self, db, seeded_person):  # noqa
        """No uniqueness constraint -- see the model-level regression test."""
        person_institution_crud.create_for_person(
            db, seeded_person.person_id,
            {"institution": "Caltech", "date_made_old_institution": datetime(2015, 6, 1)},
        )
        second = person_institution_crud.create_for_person(
            db, seeded_person.person_id, {"institution": "Caltech"}
        )
        assert second.person_institution_id is not None

    def test_list_for_person(self, db, seeded_person):  # noqa
        person_institution_crud.create_for_person(
            db, seeded_person.person_id, {"institution": "Caltech"}
        )
        person_institution_crud.create_for_person(
            db, seeded_person.person_id, {"institution": "MIT"}
        )
        rows = person_institution_crud.list_for_person(db, seeded_person.person_id)
        assert {r.institution for r in rows} == {"Caltech", "MIT"}

    def test_list_for_unknown_person_is_404(self, db):  # noqa
        with pytest.raises(HTTPException) as exc:
            person_institution_crud.list_for_person(db, -1)
        assert exc.value.status_code == status.HTTP_404_NOT_FOUND

    def test_show(self, db, seeded_person):  # noqa
        created = person_institution_crud.create_for_person(
            db, seeded_person.person_id, {"institution": "Caltech"}
        )
        found = person_institution_crud.show(db, created.person_institution_id)
        assert found.institution == "Caltech"

    def test_show_unknown_is_404(self, db):  # noqa
        with pytest.raises(HTTPException) as exc:
            person_institution_crud.show(db, -1)
        assert exc.value.status_code == status.HTTP_404_NOT_FOUND

    def test_patch_institution_text(self, db, seeded_person):  # noqa
        created = person_institution_crud.create_for_person(
            db, seeded_person.person_id, {"institution": "Caltech"}
        )
        person_institution_crud.patch(
            db, created.person_institution_id, {"institution": "MIT"}
        )
        assert person_institution_crud.show(
            db, created.person_institution_id
        ).institution == "MIT"

    def test_patch_can_mark_old_and_clear_it_again(self, db, seeded_person):  # noqa
        created = person_institution_crud.create_for_person(
            db, seeded_person.person_id, {"institution": "Caltech"}
        )
        person_institution_crud.patch(
            db,
            created.person_institution_id,
            {"date_made_old_institution": datetime(2020, 1, 1)},
        )
        assert person_institution_crud.show(
            db, created.person_institution_id
        ).date_made_old_institution == datetime(2020, 1, 1)

        person_institution_crud.patch(
            db, created.person_institution_id, {"date_made_old_institution": None}
        )
        assert person_institution_crud.show(
            db, created.person_institution_id
        ).date_made_old_institution is None

    def test_patch_rejects_blank_institution(self, db, seeded_person):  # noqa
        created = person_institution_crud.create_for_person(
            db, seeded_person.person_id, {"institution": "Caltech"}
        )
        with pytest.raises(HTTPException) as exc:
            person_institution_crud.patch(
                db, created.person_institution_id, {"institution": "  "}
            )
        assert exc.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_patch_unknown_is_404(self, db):  # noqa
        with pytest.raises(HTTPException) as exc:
            person_institution_crud.patch(db, -1, {"institution": "MIT"})
        assert exc.value.status_code == status.HTTP_404_NOT_FOUND

    def test_destroy(self, db, seeded_person):  # noqa
        created = person_institution_crud.create_for_person(
            db, seeded_person.person_id, {"institution": "Caltech"}
        )
        person_institution_crud.destroy(db, created.person_institution_id)
        with pytest.raises(HTTPException) as exc:
            person_institution_crud.show(db, created.person_institution_id)
        assert exc.value.status_code == status.HTTP_404_NOT_FOUND

    def test_destroy_unknown_is_404(self, db):  # noqa
        with pytest.raises(HTTPException) as exc:
            person_institution_crud.destroy(db, -1)
        assert exc.value.status_code == status.HTTP_404_NOT_FOUND


class TestPersonCrudInstitutions:
    """person_crud must treat institutions as a child relationship, the way it
    already treats emails/names/notes — not as a column on person."""

    def test_create_person_with_inline_institutions(self, db):  # noqa
        person = person_crud.create(
            db,
            PersonSchemaCreate(
                display_name="Inline Institution Person",
                institutions=[
                    PersonInstitutionSchemaCreate(institution="Caltech"),
                    PersonInstitutionSchemaCreate(institution="MIT"),
                ],
            ),
        )
        fetched = person_crud.show(db, str(person.person_id))
        assert {i.institution for i in fetched.institutions} == {"Caltech", "MIT"}
        assert all(i.date_made_old_institution is None for i in fetched.institutions)

    def test_create_person_without_institutions(self, db):  # noqa
        person = person_crud.create(
            db, PersonSchemaCreate(display_name="No Institution Person")
        )
        assert person_crud.show(db, str(person.person_id)).institutions == []

    def test_inline_institutions_may_be_marked_old(self, db):  # noqa
        person = person_crud.create(
            db,
            PersonSchemaCreate(
                display_name="Old Institution Person",
                institutions=[
                    PersonInstitutionSchemaCreate(
                        institution="Caltech",
                        date_made_old_institution=datetime(2015, 6, 1),
                    ),
                ],
            ),
        )
        fetched = person_crud.show(db, str(person.person_id))
        assert fetched.institutions[0].date_made_old_institution == datetime(2015, 6, 1)

    def test_patch_person_ignores_institutions(self, db, seeded_person):  # noqa
        """institutions is a relationship, so PATCH /person must not try to set
        it as a column (the same guard emails/names/notes already have)."""
        person_crud.patch(
            db,
            str(seeded_person.person_id),
            {"display_name": "Renamed", "institutions": [{"institution": "Caltech"}]},
        )
        refreshed = person_crud.show(db, str(seeded_person.person_id))
        assert refreshed.display_name == "Renamed"
        assert refreshed.institutions == []


class TestPersonInstitutionApi:
    """HTTP layer -- runs in CI, where Cognito admin credentials are set."""

    def test_create_and_fetch(self, auth_headers, seeded_person):  # noqa
        with TestClient(app) as client:
            res = client.post(
                f"/person_institution/person/{seeded_person.curie}",
                json={"institution": "Caltech"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_201_CREATED
            new_id = res.json()["person_institution_id"]

            fetched = client.get(
                f"/person_institution/{new_id}", headers=auth_headers
            )
            assert fetched.status_code == status.HTTP_200_OK
            assert fetched.json()["institution"] == "Caltech"
            assert fetched.json()["person_curie"] == seeded_person.curie

    def test_blank_institution_is_rejected(self, auth_headers, seeded_person):  # noqa
        with TestClient(app) as client:
            res = client.post(
                f"/person_institution/person/{seeded_person.curie}",
                json={"institution": "   "},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_list_for_person(self, auth_headers, seeded_person):  # noqa
        with TestClient(app) as client:
            for name in ("Caltech", "MIT"):
                client.post(
                    f"/person_institution/person/{seeded_person.curie}",
                    json={"institution": name},
                    headers=auth_headers,
                )
            res = client.get(
                f"/person_institution/person/{seeded_person.curie}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            assert {r["institution"] for r in res.json()} == {"Caltech", "MIT"}

    def test_patch_and_delete(self, auth_headers, seeded_person):  # noqa
        with TestClient(app) as client:
            new_id = client.post(
                f"/person_institution/person/{seeded_person.curie}",
                json={"institution": "Caltech"},
                headers=auth_headers,
            ).json()["person_institution_id"]

            res = client.patch(
                f"/person_institution/{new_id}",
                json={"institution": "MIT"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            assert res.json()["institution"] == "MIT"

            res = client.delete(
                f"/person_institution/{new_id}", headers=auth_headers
            )
            assert res.status_code == status.HTTP_204_NO_CONTENT
            assert client.get(
                f"/person_institution/{new_id}", headers=auth_headers
            ).status_code == status.HTTP_404_NOT_FOUND

    def test_person_create_accepts_inline_institutions(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/person/",
                json={
                    "display_name": "Inline Institution Person",
                    "institutions": [
                        {"institution": "Caltech"},
                        {"institution": "MIT"},
                    ],
                },
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_201_CREATED
            fetched = client.get(f"/person/{res.json()['curie']}", headers=auth_headers)
            assert {i["institution"] for i in fetched.json()["institutions"]} == {
                "Caltech", "MIT"
            }

    def test_person_rejects_the_old_institution_field(self, auth_headers):  # noqa
        """The array column is gone; extra='forbid' makes this a 422, not a no-op."""
        with TestClient(app) as client:
            res = client.post(
                "/person/",
                json={
                    "display_name": "Legacy Field Person",
                    "institution": ["Caltech"],
                },
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
