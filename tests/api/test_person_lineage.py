# flake8: noqa: F811
import pytest
from fastapi import status, HTTPException
from pydantic import ValidationError

from agr_literature_service.api.crud import person_lineage_crud
from agr_literature_service.api.models import (
    PersonLineageModel,
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
from agr_literature_service.api.schemas import PersonLineageSchemaShow, PersonLineageSchemaUpdate
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


class TestPersonLineageUpdateSchema:
    """PersonLineageSchemaUpdate rejects an explicit null relationship (fail-loud,
    matching PersonLineageSubmissionSchemaUpdate) rather than silently ignoring it."""

    def test_null_relationship_rejected(self):
        with pytest.raises(ValidationError):
            PersonLineageSchemaUpdate.model_validate({"relationship": None})

    def test_omitted_relationship_ok(self):
        m = PersonLineageSchemaUpdate.model_validate({"start_date": None})
        assert m.relationship is None

    def test_present_relationship_ok(self):
        m = PersonLineageSchemaUpdate.model_validate({"relationship": 5})
        assert m.relationship == 5


def _ppr_id(db, label):  # noqa
    """Term id of a person_person_relationship term by its human-readable label."""
    populate_test_vocabularies(db)
    return next(
        t["value"]
        for t in vocabulary_crud.get_vocabulary(db, sd.PERSON_LINEAGE_VOCAB)
        if t["label"] == label
    )


@pytest.fixture
def two_people(db):  # noqa
    p1 = PersonModel(display_name="Canon One", curie="AGRKB:test-canon-1")
    p2 = PersonModel(display_name="Canon Two", curie="AGRKB:test-canon-2")
    db.add(p1)
    db.add(p2)
    db.commit()
    db.refresh(p1)
    db.refresh(p2)
    return {"person_subject_id": p1.person_id, "person_object_id": p2.person_id}


def _create(db, subject_id, object_id, term_id):  # noqa
    return person_lineage_crud.create(
        db,
        {
            "person_subject_curie_or_id": subject_id,
            "person_object_curie_or_id": object_id,
            "relationship": term_id,
        },
    )


class TestPersonLineageCrud:
    """DB-level tests exercising person_lineage_crud directly (no HTTP/auth).

    These run in environments without Cognito credentials. They cover the
    person_person_relationship controlled-vocabulary cutover: the stored FK, the
    serialized term-ref on every read (and on the create response), symmetric-
    relationship id normalization resolved by term NAME, and fail-closed validation.
    """

    def test_create_stores_fk_and_returns_term_ref(self, db, two_people):  # noqa
        term_id = _ppr_id(db, "PhD Supervisor of")
        created = _create(
            db, two_people["person_subject_id"], two_people["person_object_id"], term_id
        )
        # create response echoes the {value,label,is_obsolete} object
        assert created["relationship"] == {
            "value": term_id, "label": "PhD Supervisor of", "is_obsolete": False}
        obj = (
            db.query(PersonLineageModel)
            .filter(PersonLineageModel.person_lineage_id == created["person_lineage_id"])
            .one()
        )
        assert obj.person_subject_id == two_people["person_subject_id"]
        assert obj.person_object_id == two_people["person_object_id"]
        assert obj.relationship_vocab_term_abc_id == term_id

        # The read-side serialization path is real: the crud dict validates against
        # the response schema (exercises model_rebuild() forward-ref resolution) and
        # relationship deserializes into a VocabularyTermRefSchema.
        validated = PersonLineageSchemaShow.model_validate(created)
        assert isinstance(validated.relationship, VocabularyTermRefSchema)
        assert validated.relationship.value == term_id
        assert validated.relationship.label == "PhD Supervisor of"
        assert validated.relationship.is_obsolete is False

    def test_show_and_list_return_term_ref(self, db, two_people):  # noqa
        term_id = _ppr_id(db, "PhD Supervisor of")
        created = _create(
            db, two_people["person_subject_id"], two_people["person_object_id"], term_id
        )
        pl_id = created["person_lineage_id"]
        expected = {"value": term_id, "label": "PhD Supervisor of", "is_obsolete": False}

        shown = person_lineage_crud.show(db, pl_id)
        assert shown["relationship"] == expected
        validated_show = PersonLineageSchemaShow.model_validate(shown)
        assert isinstance(validated_show.relationship, VocabularyTermRefSchema)
        assert validated_show.relationship.value == term_id

        rows = person_lineage_crud.list_for_person(db, two_people["person_subject_id"])
        row = next(r for r in rows if r["person_lineage_id"] == pl_id)
        assert row["relationship"] == expected
        PersonLineageSchemaShow.model_validate(row)

    def test_show_unknown_404(self, db):  # noqa
        with pytest.raises(HTTPException) as exc_info:
            person_lineage_crud.show(db, 9999999)
        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

    def test_duplicate_rejected(self, db, two_people):  # noqa
        term_id = _ppr_id(db, "PhD Supervisor of")
        _create(db, two_people["person_subject_id"], two_people["person_object_id"], term_id)
        with pytest.raises(HTTPException) as exc_info:
            _create(
                db, two_people["person_subject_id"], two_people["person_object_id"], term_id
            )
        assert exc_info.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_reversed_pair_allowed_directional(self, db, two_people):  # noqa
        # Directional: B->A is a different fact from A->B.
        term_id = _ppr_id(db, "PhD Supervisor of")
        _create(db, two_people["person_subject_id"], two_people["person_object_id"], term_id)
        created = _create(
            db, two_people["person_object_id"], two_people["person_subject_id"], term_id
        )
        assert created["person_lineage_id"] is not None

    def test_invalid_person_rejected(self, db):  # noqa
        term_id = _ppr_id(db, "PhD Supervisor of")
        with pytest.raises(HTTPException) as exc_info:
            _create(db, 9999999, 9999998, term_id)
        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

    def test_self_pair_rejected(self, db, two_people):  # noqa
        term_id = _ppr_id(db, "PhD Supervisor of")
        with pytest.raises(HTTPException) as exc_info:
            _create(
                db, two_people["person_subject_id"], two_people["person_subject_id"], term_id
            )
        assert exc_info.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_collaborator_of_is_symmetric_normalized(self, db, two_people):  # noqa
        # collaborator_of is non-directional: created with subject>object ids, the
        # stored pair must be normalized to ascending id order, and GET returns the
        # term-ref object.
        collab = _ppr_id(db, "Collaborator of")
        lo = min(two_people["person_subject_id"], two_people["person_object_id"])
        hi = max(two_people["person_subject_id"], two_people["person_object_id"])
        created = _create(db, hi, lo, collab)  # deliberately reversed (hi, lo)
        obj = (
            db.query(PersonLineageModel)
            .filter(PersonLineageModel.person_lineage_id == created["person_lineage_id"])
            .one()
        )
        assert obj.person_subject_id == lo
        assert obj.person_object_id == hi
        assert created["relationship"] == {
            "value": collab, "label": "Collaborator of", "is_obsolete": False}

    def test_symmetric_reversed_duplicate_rejected(self, db, two_people):  # noqa
        # (A,B) and (B,A) normalize to the same canonical row -> the reversed insert
        # hits the unique constraint -> 422; exactly one row remains.
        collab = _ppr_id(db, "Collaborator of")
        _create(db, two_people["person_subject_id"], two_people["person_object_id"], collab)
        with pytest.raises(HTTPException) as exc_info:
            _create(
                db, two_people["person_object_id"], two_people["person_subject_id"], collab
            )
        assert exc_info.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        count = (
            db.query(PersonLineageModel)
            .filter(PersonLineageModel.relationship_vocab_term_abc_id == collab)
            .count()
        )
        assert count == 1

    def test_patch_relationship_updates_term_ref(self, db, two_people):  # noqa
        term_id = _ppr_id(db, "PhD Supervisor of")
        created = _create(
            db, two_people["person_subject_id"], two_people["person_object_id"], term_id
        )
        pl_id = created["person_lineage_id"]
        new_term = _ppr_id(db, "Postdoc Supervisor of")
        person_lineage_crud.patch(db, pl_id, {"relationship": new_term})
        assert person_lineage_crud.show(db, pl_id)["relationship"] == {
            "value": new_term, "label": "Postdoc Supervisor of", "is_obsolete": False}

    def test_patch_to_symmetric_normalizes_ids(self, db, two_people):  # noqa
        lo = min(two_people["person_subject_id"], two_people["person_object_id"])
        hi = max(two_people["person_subject_id"], two_people["person_object_id"])
        phd = _ppr_id(db, "PhD Supervisor of")
        collab = _ppr_id(db, "Collaborator of")
        # directional row deliberately stored in reverse (hi, lo) order
        created = _create(db, hi, lo, phd)
        pl_id = created["person_lineage_id"]
        # patch to the symmetric relationship -> ids must re-normalize to (lo, hi)
        person_lineage_crud.patch(db, pl_id, {"relationship": collab})
        shown = person_lineage_crud.show(db, pl_id)
        assert shown["person_subject_id"] == lo
        assert shown["person_object_id"] == hi

    def test_patch_to_symmetric_collision_rejected(self, db, two_people):  # noqa
        lo = min(two_people["person_subject_id"], two_people["person_object_id"])
        hi = max(two_people["person_subject_id"], two_people["person_object_id"])
        phd = _ppr_id(db, "PhD Supervisor of")
        collab = _ppr_id(db, "Collaborator of")
        # existing normalized collaborator_of (lo, hi)
        _create(db, lo, hi, collab)
        # directional (hi, lo) phd row; patching it to collaborator_of would
        # normalize to (lo, hi) and collide with the row above -> 422
        created = _create(db, hi, lo, phd)
        with pytest.raises(HTTPException) as exc_info:
            person_lineage_crud.patch(db, created["person_lineage_id"], {"relationship": collab})
        assert exc_info.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_relationship_bad_id_422_on_create(self, db, two_people):  # noqa
        populate_test_vocabularies(db)
        with pytest.raises(HTTPException) as exc_info:
            _create(
                db, two_people["person_subject_id"], two_people["person_object_id"], 999999
            )
        assert exc_info.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_relationship_bad_id_422_on_patch(self, db, two_people):  # noqa
        term_id = _ppr_id(db, "PhD Supervisor of")
        created = _create(
            db, two_people["person_subject_id"], two_people["person_object_id"], term_id
        )
        pl_id = created["person_lineage_id"]
        with pytest.raises(HTTPException) as exc_info:
            person_lineage_crud.patch(db, pl_id, {"relationship": 999999})
        assert exc_info.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        # fail-closed: the original term id is untouched
        assert person_lineage_crud.show(db, pl_id)["relationship"] == {
            "value": term_id, "label": "PhD Supervisor of", "is_obsolete": False}

    def test_relationship_wrong_vocabulary_422(self, db, two_people):  # noqa
        populate_test_vocabularies(db)
        # a term id from a DIFFERENT vocabulary (lab_position) must be rejected
        wrong = vocabulary_crud.get_vocabulary(db, sd.LAB_POSITION_VOCAB)[0]["value"]
        with pytest.raises(HTTPException) as exc_info:
            _create(
                db, two_people["person_subject_id"], two_people["person_object_id"], wrong
            )
        assert exc_info.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_relationship_obsolete_term_422(self, db, two_people):  # noqa
        populate_test_vocabularies(db)
        vocab = (
            db.query(VocabularyAbcModel)
            .filter(VocabularyAbcModel.vocabulary == sd.PERSON_LINEAGE_VOCAB)
            .one()
        )
        obsolete = VocabularyTermAbcModel(
            vocabulary_abc_id=vocab.vocabulary_abc_id,
            name="Retired Relationship", is_obsolete=True)
        db.add(obsolete)
        db.commit()
        db.refresh(obsolete)
        with pytest.raises(HTTPException) as exc_info:
            _create(
                db, two_people["person_subject_id"], two_people["person_object_id"],
                obsolete.vocabulary_term_abc_id,
            )
        assert exc_info.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_list_for_person_matches_either_side(self, db, two_people):  # noqa
        # A person appearing as subject in one PPR and object in another is returned
        # by both directions.
        s_id = two_people["person_subject_id"]
        o_id = two_people["person_object_id"]
        phd = _ppr_id(db, "PhD Supervisor of")
        postdoc = _ppr_id(db, "Postdoc Supervisor of")
        _create(db, s_id, o_id, phd)      # s is subject
        _create(db, o_id, s_id, postdoc)  # s is object (distinct directional fact)

        rows = person_lineage_crud.list_for_person(db, s_id)
        assert len(rows) == 2
        assert {r["person_subject_id"] for r in rows} == {s_id, o_id}

    def test_destroy(self, db, two_people):  # noqa
        term_id = _ppr_id(db, "PhD Supervisor of")
        created = _create(
            db, two_people["person_subject_id"], two_people["person_object_id"], term_id
        )
        pl_id = created["person_lineage_id"]
        person_lineage_crud.destroy(db, pl_id)
        with pytest.raises(HTTPException) as exc_info:
            person_lineage_crud.show(db, pl_id)
        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

    def test_patch_corrects_object_person(self, db, two_people):  # noqa
        # A curator can correct a mis-resolved person on the canonical (object B -> C).
        term_id = _ppr_id(db, "PhD Supervisor of")
        created = _create(
            db, two_people["person_subject_id"], two_people["person_object_id"], term_id
        )
        person_c = PersonModel(display_name="Canon Three", curie="AGRKB:test-canon-3")
        db.add(person_c)
        db.commit()
        db.refresh(person_c)
        person_lineage_crud.patch(
            db, created["person_lineage_id"], {"person_object_curie_or_id": person_c.curie}
        )
        shown = person_lineage_crud.show(db, created["person_lineage_id"])
        assert shown["person_subject_id"] == two_people["person_subject_id"]
        assert shown["person_object_id"] == person_c.person_id
        assert shown["person_object_curie"] == person_c.curie

    def test_patch_person_collision_rejected(self, db, two_people):  # noqa
        # Correcting a person into an existing (subject, object, relationship) triple
        # must be rejected, not 500.
        term_id = _ppr_id(db, "PhD Supervisor of")
        person_c = PersonModel(display_name="Canon Three", curie="AGRKB:test-canon-3b")
        db.add(person_c)
        db.commit()
        db.refresh(person_c)
        s_id = two_people["person_subject_id"]
        r1 = _create(db, s_id, two_people["person_object_id"], term_id)  # A -> B
        _create(db, s_id, person_c.person_id, term_id)                   # A -> C
        # Patching A->B's object to C would collide with A->C.
        with pytest.raises(HTTPException) as exc_info:
            person_lineage_crud.patch(
                db, r1["person_lineage_id"], {"person_object_curie_or_id": person_c.person_id}
            )
        assert exc_info.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_patch_person_and_relationship_collision_rejected(self, db, two_people):  # noqa
        # A single patch that BOTH reassigns a person into an existing triple AND
        # carries a relationship id (so validate_term_id runs while the row is
        # already dirtied). The pre-commit query must not autoflush the dirty row:
        # the collision must surface as a clean 422, never an uncaught 500.
        term_id = _ppr_id(db, "PhD Supervisor of")
        person_c = PersonModel(display_name="Canon Three", curie="AGRKB:test-canon-3c")
        db.add(person_c)
        db.commit()
        db.refresh(person_c)
        s_id = two_people["person_subject_id"]
        r1 = _create(db, s_id, two_people["person_object_id"], term_id)  # A -> B
        _create(db, s_id, person_c.person_id, term_id)                   # A -> C
        # Reassign A->B's object to C (collides with A->C) AND resend the relationship
        # id, forcing validate_term_id to query with the row already dirty.
        with pytest.raises(HTTPException) as exc_info:
            person_lineage_crud.patch(
                db,
                r1["person_lineage_id"],
                {"person_object_curie_or_id": person_c.person_id, "relationship": term_id},
            )
        assert exc_info.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_find_or_create_symmetric_matches_reversed(self, db, two_people):  # noqa
        # find_or_create normalizes symmetric pairs by term id, so a reversed lookup
        # finds the existing canonical instead of creating a duplicate.
        collab = _ppr_id(db, "Collaborator of")
        s_id = two_people["person_subject_id"]
        o_id = two_people["person_object_id"]
        first, created1 = person_lineage_crud.find_or_create(
            db, person_subject_id=s_id, person_object_id=o_id, relationship_term_id=collab
        )
        db.commit()
        assert created1 is True
        second, created2 = person_lineage_crud.find_or_create(
            db, person_subject_id=o_id, person_object_id=s_id, relationship_term_id=collab
        )
        assert created2 is False
        assert second.person_lineage_id == first.person_lineage_id
