# flake8: noqa: F811
from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status, HTTPException

from agr_literature_service.api.main import app
from agr_literature_service.api.crud import laboratory_person_crud
from agr_literature_service.api.models import (
    LaboratoryModel,
    LaboratoryPersonModel,
    PersonModel,
    VocabularyAbcModel,
    VocabularyTermAbcModel,
)
from agr_literature_service.lit_processing.tests.vocabulary_populate_load import (
    populate_test_vocabularies,
)
from agr_literature_service.api.crud import vocabulary_crud
from agr_literature_service.api.crud import vocabulary_seed_data as sd
from agr_literature_service.api.crud.vocabulary_crud import VocabularyTermRefSchema
from agr_literature_service.api.schemas import (
    LaboratoryPersonSchemaShow,
    LaboratoryPersonSchemaRelated,
)
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


LabPersonTestData = namedtuple(
    "LabPersonTestData",
    ["response", "new_id", "laboratory_id", "person_id", "term_id"],
)


def _term_id_by_label(db, label):  # noqa
    populate_test_vocabularies(db)
    return next(
        t["value"]
        for t in vocabulary_crud.get_vocabulary(db, sd.LAB_POSITION_VOCAB)
        if t["label"] == label
    )


@pytest.fixture
def seeded_lab_and_person(db):  # noqa
    lab = LaboratoryModel(
        curie="AGRKB:104test-people", name="People Lab", strain_designation="PL",
        status="active", lab_is_open=False
    )
    person = PersonModel(display_name="Lab Member", curie="AGRKB:test-lab-person")
    db.add(lab)
    db.add(person)
    db.commit()
    db.refresh(lab)
    db.refresh(person)
    return {"laboratory_id": lab.laboratory_id, "person_id": person.person_id}


@pytest.fixture
def test_lab_person(db, auth_headers, seeded_lab_and_person):  # noqa
    term_id = _term_id_by_label(db, "Lab Member")
    with TestClient(app) as client:
        response = client.post(
            "/laboratory_person/",
            json={
                "laboratory_curie": str(seeded_lab_and_person["laboratory_id"]),
                "person_curie": str(seeded_lab_and_person["person_id"]),
                "lab_position": term_id,
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
            term_id=term_id,
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
        assert obj.lab_position_vocabulary_term_abc_id == test_lab_person.term_id
        assert obj.is_lab_contact is True
        assert obj.can_edit_lab is False

    def test_lab_position_roundtrip_as_term_ref(self, auth_headers, test_lab_person):  # noqa
        with TestClient(app) as client:
            res = client.get(
                f"/laboratory_person/{test_lab_person.new_id}", headers=auth_headers
            )
            assert res.status_code == status.HTTP_200_OK
            assert res.json()["lab_position"] == {
                "value": test_lab_person.term_id,
                "label": "Lab Member",
                "is_obsolete": False,
            }

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

    def test_lab_position_bad_id_is_422(self, db, auth_headers, seeded_lab_and_person):  # noqa
        populate_test_vocabularies(db)
        with TestClient(app) as client:
            res = client.post(
                "/laboratory_person/",
                json={
                    "laboratory_curie": str(seeded_lab_and_person["laboratory_id"]),
                    "person_curie": str(seeded_lab_and_person["person_id"]),
                    "lab_position": 999999,
                },
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_patch_lab_person(self, db, auth_headers, test_lab_person):  # noqa
        new_term = _term_id_by_label(db, "Post-Doc")
        with TestClient(app) as client:
            res = client.patch(
                f"/laboratory_person/{test_lab_person.new_id}",
                json={"can_edit_lab": True, "lab_position": new_term},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            body = res.json()
            assert body["can_edit_lab"] is True
            assert body["lab_position"] == {
                "value": new_term, "label": "Post-Doc", "is_obsolete": False,
            }

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

    def _assert_enriched(self, row):
        # The derived display fields surfaced for the UI.
        assert row["person_display_name"] == "Lab Member"
        assert row["laboratory_name"] == "People Lab"
        assert row["laboratory_strain_designation"] == "PL"

    def test_show_includes_display_fields(self, auth_headers, test_lab_person):  # noqa
        with TestClient(app) as client:
            res = client.get(f"/laboratory_person/{test_lab_person.new_id}", headers=auth_headers)
            assert res.status_code == status.HTTP_200_OK
            self._assert_enriched(res.json())

    def test_list_for_person_includes_display_fields(self, auth_headers, test_lab_person):  # noqa
        with TestClient(app) as client:
            res = client.get(
                f"/laboratory_person/person/{test_lab_person.person_id}", headers=auth_headers
            )
            assert res.status_code == status.HTTP_200_OK
            row = next(r for r in res.json() if r["laboratory_person_id"] == test_lab_person.new_id)
            self._assert_enriched(row)

    def test_list_for_laboratory_includes_display_fields(self, auth_headers, test_lab_person):  # noqa
        with TestClient(app) as client:
            res = client.get(
                f"/laboratory_person/laboratory/{test_lab_person.laboratory_id}", headers=auth_headers
            )
            assert res.status_code == status.HTTP_200_OK
            row = next(r for r in res.json() if r["laboratory_person_id"] == test_lab_person.new_id)
            self._assert_enriched(row)

    def test_nested_lab_persons_include_display_fields(self, auth_headers, test_lab_person):  # noqa
        # The lab_persons nested under Laboratory Show also carry the display fields.
        with TestClient(app) as client:
            res = client.get(f"/laboratory/{test_lab_person.laboratory_id}", headers=auth_headers)
            assert res.status_code == status.HTTP_200_OK
            lps = res.json().get("lab_persons") or []
            row = next(lp for lp in lps if lp["laboratory_person_id"] == test_lab_person.new_id)
            self._assert_enriched(row)

    def test_destroy_lab_person(self, auth_headers, test_lab_person):  # noqa
        with TestClient(app) as client:
            res = client.delete(
                f"/laboratory_person/{test_lab_person.new_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_204_NO_CONTENT


class TestLaboratoryPersonCrud:
    """DB-level tests exercising laboratory_person_crud directly (no HTTP/auth).

    These run in environments without Cognito credentials (the HTTP tests above need
    a real admin token via the session-scoped auth_headers fixture). They cover the
    lab_position controlled-vocabulary cutover: the stored FK, the serialized term-ref
    on every read (and on the create response), and fail-closed validation.
    """

    def test_create_stores_fk_and_returns_term_ref(self, db, seeded_lab_and_person):  # noqa
        term_id = _term_id_by_label(db, "Lab Member")
        created = laboratory_person_crud.create_for_laboratory(
            db, seeded_lab_and_person["laboratory_id"],
            {"person_id": seeded_lab_and_person["person_id"],
             "lab_position": term_id, "is_lab_contact": True},
        )
        # The create response echoes the {value,label,is_obsolete} object.
        assert created["lab_position"] == {
            "value": term_id, "label": "Lab Member", "is_obsolete": False}
        obj = (
            db.query(LaboratoryPersonModel)
            .filter(LaboratoryPersonModel.laboratory_person_id
                    == created["laboratory_person_id"])
            .one()
        )
        assert obj.lab_position_vocabulary_term_abc_id == term_id
        assert obj.is_lab_contact is True

        # The read-side serialization path is real: the crud dict validates against
        # the response schema (exercises the model_rebuild() forward-ref resolution),
        # and lab_position deserializes into a VocabularyTermRefSchema.
        validated = LaboratoryPersonSchemaShow.model_validate(created)
        assert isinstance(validated.lab_position, VocabularyTermRefSchema)
        assert validated.lab_position.value == term_id
        assert validated.lab_position.label == "Lab Member"
        assert validated.lab_position.is_obsolete is False

    def test_show_and_list_return_term_ref(self, db, seeded_lab_and_person):  # noqa
        term_id = _term_id_by_label(db, "Lab Member")
        created = laboratory_person_crud.create_for_laboratory(
            db, seeded_lab_and_person["laboratory_id"],
            {"person_id": seeded_lab_and_person["person_id"], "lab_position": term_id},
        )
        lp_id = created["laboratory_person_id"]
        expected = {"value": term_id, "label": "Lab Member", "is_obsolete": False}

        shown = laboratory_person_crud.show(db, lp_id)
        assert shown["lab_position"] == expected
        # show() output validates against the Show response schema.
        validated_show = LaboratoryPersonSchemaShow.model_validate(shown)
        assert isinstance(validated_show.lab_position, VocabularyTermRefSchema)
        assert validated_show.lab_position.value == term_id

        by_person = laboratory_person_crud.list_for_person(
            db, seeded_lab_and_person["person_id"])
        row = next(r for r in by_person if r["laboratory_person_id"] == lp_id)
        assert row["lab_position"] == expected
        # A list item validates against the Related schema — the type embedded in the
        # parent Person/Laboratory Show schemas.
        validated_related = LaboratoryPersonSchemaRelated.model_validate(row)
        assert isinstance(validated_related.lab_position, VocabularyTermRefSchema)
        assert validated_related.lab_position.label == "Lab Member"
        assert validated_related.lab_position.is_obsolete is False

        by_lab = laboratory_person_crud.list_for_laboratory(
            db, seeded_lab_and_person["laboratory_id"])
        row = next(r for r in by_lab if r["laboratory_person_id"] == lp_id)
        assert row["lab_position"] == expected
        LaboratoryPersonSchemaRelated.model_validate(row)

    def test_patch_updates_and_clears_fk(self, db, seeded_lab_and_person):  # noqa
        term_id = _term_id_by_label(db, "Lab Member")
        created = laboratory_person_crud.create_for_laboratory(
            db, seeded_lab_and_person["laboratory_id"],
            {"person_id": seeded_lab_and_person["person_id"], "lab_position": term_id},
        )
        lp_id = created["laboratory_person_id"]

        new_term = _term_id_by_label(db, "Post-Doc")
        laboratory_person_crud.patch(db, lp_id, {"lab_position": new_term})
        assert laboratory_person_crud.show(db, lp_id)["lab_position"] == {
            "value": new_term, "label": "Post-Doc", "is_obsolete": False}

        laboratory_person_crud.patch(db, lp_id, {"lab_position": None})
        assert laboratory_person_crud.show(db, lp_id)["lab_position"] is None

    def test_patch_unknown_term_id_raises_422(self, db, seeded_lab_and_person):  # noqa
        term_id = _term_id_by_label(db, "Lab Member")
        created = laboratory_person_crud.create_for_laboratory(
            db, seeded_lab_and_person["laboratory_id"],
            {"person_id": seeded_lab_and_person["person_id"], "lab_position": term_id},
        )
        lp_id = created["laboratory_person_id"]
        with pytest.raises(HTTPException) as exc_info:
            laboratory_person_crud.patch(db, lp_id, {"lab_position": 999999})
        assert exc_info.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        # The rejected patch left the original term id in place (fail-closed).
        assert laboratory_person_crud.show(db, lp_id)["lab_position"] == {
            "value": term_id, "label": "Lab Member", "is_obsolete": False}

    def test_create_unknown_term_id_raises_422(self, db, seeded_lab_and_person):  # noqa
        populate_test_vocabularies(db)
        with pytest.raises(HTTPException) as exc_info:
            laboratory_person_crud.create_for_laboratory(
                db, seeded_lab_and_person["laboratory_id"],
                {"person_id": seeded_lab_and_person["person_id"],
                 "lab_position": 999999},
            )
        assert exc_info.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_create_wrong_vocabulary_term_raises_422(self, db, seeded_lab_and_person):  # noqa
        populate_test_vocabularies(db)
        # A term id that belongs to a DIFFERENT vocabulary must be rejected.
        wrong = vocabulary_crud.get_vocabulary(db, sd.PERSON_LINEAGE_VOCAB)[0]["value"]
        with pytest.raises(HTTPException) as exc_info:
            laboratory_person_crud.create_for_laboratory(
                db, seeded_lab_and_person["laboratory_id"],
                {"person_id": seeded_lab_and_person["person_id"],
                 "lab_position": wrong},
            )
        assert exc_info.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_create_obsolete_term_raises_422(self, db, seeded_lab_and_person):  # noqa
        populate_test_vocabularies(db)
        vocab = (
            db.query(VocabularyAbcModel)
            .filter(VocabularyAbcModel.vocabulary == sd.LAB_POSITION_VOCAB)
            .one()
        )
        obsolete = VocabularyTermAbcModel(
            vocabulary_abc_id=vocab.vocabulary_abc_id,
            name="Retired Role", is_obsolete=True)
        db.add(obsolete)
        db.commit()
        db.refresh(obsolete)
        with pytest.raises(HTTPException) as exc_info:
            laboratory_person_crud.create_for_laboratory(
                db, seeded_lab_and_person["laboratory_id"],
                {"person_id": seeded_lab_and_person["person_id"],
                 "lab_position": obsolete.vocabulary_term_abc_id},
            )
        assert exc_info.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
