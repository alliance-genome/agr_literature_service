# flake8: noqa: F811
import pytest
from fastapi import HTTPException, status

from agr_literature_service.api.crud import (
    person_lineage_submission_crud,
    person_lineage_crud,
)
from agr_literature_service.api.models import (
    PersonLineageSubmissionModel,
    PersonLineageModel,
    PersonModel,
)
from agr_literature_service.lit_processing.tests.vocabulary_populate_load import (
    populate_test_vocabularies,
)
from agr_literature_service.api.crud import vocabulary_crud
from agr_literature_service.api.crud import vocabulary_seed_data as sd
from agr_literature_service.api.schemas import PersonLineageSubmissionSchemaShow
from ..fixtures import db  # noqa


def _ppr_id(db, label):  # noqa
    """Term id of a person_person_relationship term by its human-readable label."""
    populate_test_vocabularies(db)
    return next(
        t["value"]
        for t in vocabulary_crud.get_vocabulary(db, sd.PERSON_LINEAGE_VOCAB)
        if t["label"] == label
    )


def _lab_position_id(db, label):  # noqa
    """Term id of a lab_position term — used to prove wrong-vocabulary rejection."""
    populate_test_vocabularies(db)
    return next(
        t["value"]
        for t in vocabulary_crud.get_vocabulary(db, sd.LAB_POSITION_VOCAB)
        if t["label"] == label
    )


@pytest.fixture
def two_people(db):  # noqa
    p1 = PersonModel(display_name="Sub One", curie="AGRKB:test-sub-1")
    p2 = PersonModel(display_name="Sub Two", curie="AGRKB:test-sub-2")
    db.add(p1)
    db.add(p2)
    db.commit()
    db.refresh(p1)
    db.refresh(p2)
    return {"person_subject_id": p1.person_id, "person_object_id": p2.person_id}


def _submit(db, term_id, who="curator1", subject=None, obj=None, names=("Alice", "Bob")):  # noqa
    """Create a submission through the CRUD, returning its show-dict."""
    payload = {
        "person_subject_name": names[0],
        "person_object_name": names[1],
        "relationship": term_id,
        "who_sent_this": who,
    }
    if subject is not None:
        payload["person_subject_curie_or_id"] = subject
    if obj is not None:
        payload["person_object_curie_or_id"] = obj
    return person_lineage_submission_crud.create(db, payload)


class TestPersonLineageSubmissionCrud:
    """DB-level tests exercising person_lineage_submission_crud directly (no HTTP/auth).

    These run in environments without Cognito credentials and cover the
    person_person_relationship controlled-vocabulary cutover: the stored FK, the
    {value,label,is_obsolete} read object, id validation, and the promote/validate
    flow that carries the term id onto the canonical person_lineage.
    """

    def test_create_stores_fk_and_returns_ref(self, db, two_people):  # noqa
        term_id = _ppr_id(db, "PhD Supervisor of")
        created = _submit(
            db, term_id,
            subject=two_people["person_subject_id"],
            obj=two_people["person_object_id"],
        )

        # The FK column stores the raw term id.
        row = (
            db.query(PersonLineageSubmissionModel)
            .filter(
                PersonLineageSubmissionModel.person_lineage_submission_id
                == created["person_lineage_submission_id"]
            )
            .one()
        )
        assert row.relationship_vocabulary_term_abc_id == term_id
        assert row.status == "pending"
        assert row.person_lineage_id is None

        # The read shape expands the id to {value,label,is_obsolete}.
        assert created["relationship"] == {
            "value": term_id, "label": "PhD Supervisor of", "is_obsolete": False,
        }

        # model_validate exercises the VocabularyTermRefSchema forward reference.
        show = PersonLineageSubmissionSchemaShow.model_validate(created)
        assert show.relationship.value == term_id
        assert show.relationship.label == "PhD Supervisor of"
        assert show.relationship.is_obsolete is False

    def test_create_bad_term_id_rejected(self, db):  # noqa
        populate_test_vocabularies(db)
        with pytest.raises(HTTPException) as exc:
            _submit(db, 999999999)
        assert exc.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_create_wrong_vocabulary_term_rejected(self, db):  # noqa
        # A valid term id from a DIFFERENT vocabulary must be refused.
        wrong = _lab_position_id(db, "Post-Doc")
        with pytest.raises(HTTPException) as exc:
            _submit(db, wrong)
        assert exc.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_validate_creates_canonical_matching_term_id(self, db, two_people):  # noqa
        term_id = _ppr_id(db, "PhD Supervisor of")
        created = _submit(
            db, term_id,
            subject=two_people["person_subject_id"],
            obj=two_people["person_object_id"],
        )
        result = person_lineage_submission_crud.validate(
            db, created["person_lineage_submission_id"], {}
        )
        assert result["status"] == "validated"
        canonical_id = result["person_lineage_id"]
        assert canonical_id is not None

        canonical = (
            db.query(PersonLineageModel)
            .filter(PersonLineageModel.person_lineage_id == canonical_id)
            .one()
        )
        assert canonical.relationship_vocabulary_term_abc_id == term_id
        assert canonical.person_subject_id == two_people["person_subject_id"]
        assert canonical.person_object_id == two_people["person_object_id"]

    def test_validate_then_dedups(self, db, two_people):  # noqa
        term_id = _ppr_id(db, "PhD Supervisor of")
        first = _submit(
            db, term_id, who="curator1",
            subject=two_people["person_subject_id"],
            obj=two_people["person_object_id"],
        )
        v1 = person_lineage_submission_crud.validate(
            db, first["person_lineage_submission_id"], {}
        )
        canonical_id = v1["person_lineage_id"]

        second = _submit(
            db, term_id, who="curator2",
            subject=two_people["person_subject_id"],
            obj=two_people["person_object_id"],
        )
        v2 = person_lineage_submission_crud.validate(
            db, second["person_lineage_submission_id"], {}
        )
        assert v2["status"] == "duplicate"
        assert v2["person_lineage_id"] == canonical_id

        count = (
            db.query(PersonLineageModel)
            .filter(
                PersonLineageModel.person_subject_id == two_people["person_subject_id"],
                PersonLineageModel.person_object_id == two_people["person_object_id"],
                PersonLineageModel.relationship_vocabulary_term_abc_id == term_id,
            )
            .count()
        )
        assert count == 1

    def test_validate_requires_both_ids(self, db):  # noqa
        term_id = _ppr_id(db, "PhD Supervisor of")
        created = _submit(db, term_id)  # names only, unresolved
        with pytest.raises(HTTPException) as exc:
            person_lineage_submission_crud.validate(
                db, created["person_lineage_submission_id"], {}
            )
        assert exc.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_validate_relationship_override_steers_canonical(self, db, two_people):  # noqa
        submitted = _ppr_id(db, "PhD Supervisor of")
        override = _ppr_id(db, "Postdoc Supervisor of")
        created = _submit(
            db, submitted,
            subject=two_people["person_subject_id"],
            obj=two_people["person_object_id"],
        )
        result = person_lineage_submission_crud.validate(
            db, created["person_lineage_submission_id"], {"relationship": override}
        )
        assert result["status"] == "validated"
        # The submission's own claimed relationship is preserved (submitted term).
        assert result["relationship"]["value"] == submitted

        canonical = person_lineage_crud.show(db, result["person_lineage_id"])
        # The canonical reflects the curator's corrected relationship.
        assert canonical["relationship"]["value"] == override

    def test_validate_bad_override_term_rejected(self, db, two_people):  # noqa
        term_id = _ppr_id(db, "PhD Supervisor of")
        created = _submit(
            db, term_id,
            subject=two_people["person_subject_id"],
            obj=two_people["person_object_id"],
        )
        with pytest.raises(HTTPException) as exc:
            person_lineage_submission_crud.validate(
                db, created["person_lineage_submission_id"], {"relationship": 999999999}
            )
        assert exc.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_patch_relationship_updates_fk(self, db, two_people):  # noqa
        first = _ppr_id(db, "PhD Supervisor of")
        second = _ppr_id(db, "Postdoc Supervisor of")
        created = _submit(db, first)
        person_lineage_submission_crud.patch(
            db, created["person_lineage_submission_id"], {"relationship": second}
        )
        show = person_lineage_submission_crud.show(
            db, created["person_lineage_submission_id"]
        )
        assert show["relationship"]["value"] == second

    def test_patch_bad_relationship_rejected(self, db):  # noqa
        term_id = _ppr_id(db, "PhD Supervisor of")
        created = _submit(db, term_id)
        with pytest.raises(HTTPException) as exc:
            person_lineage_submission_crud.patch(
                db, created["person_lineage_submission_id"], {"relationship": 999999999}
            )
        assert exc.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_validate_rejected_submission_blocked(self, db, two_people):  # noqa
        term_id = _ppr_id(db, "PhD Supervisor of")
        created = _submit(
            db, term_id,
            subject=two_people["person_subject_id"],
            obj=two_people["person_object_id"],
        )
        person_lineage_submission_crud.patch(
            db, created["person_lineage_submission_id"], {"status": "rejected"}
        )
        with pytest.raises(HTTPException) as exc:
            person_lineage_submission_crud.validate(
                db, created["person_lineage_submission_id"], {}
            )
        assert exc.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_revalidate_is_idempotent_noop(self, db, two_people):  # noqa
        term_id = _ppr_id(db, "PhD Supervisor of")
        created = _submit(
            db, term_id,
            subject=two_people["person_subject_id"],
            obj=two_people["person_object_id"],
        )
        v1 = person_lineage_submission_crud.validate(
            db, created["person_lineage_submission_id"], {}
        )
        canonical_id = v1["person_lineage_id"]
        v2 = person_lineage_submission_crud.validate(
            db, created["person_lineage_submission_id"], {}
        )
        assert v2["status"] == "validated"
        assert v2["person_lineage_id"] == canonical_id

    def test_list_for_person_returns_refs(self, db, two_people):  # noqa
        term_id = _ppr_id(db, "PhD Supervisor of")
        _submit(
            db, term_id,
            subject=two_people["person_subject_id"],
            obj=two_people["person_object_id"],
        )
        rows = person_lineage_submission_crud.list_for_person(
            db, two_people["person_subject_id"]
        )
        assert len(rows) == 1
        assert rows[0]["relationship"]["value"] == term_id

    def test_show_unknown_404(self, db):  # noqa
        with pytest.raises(HTTPException) as exc:
            person_lineage_submission_crud.show(db, 999999999)
        assert exc.value.status_code == status.HTTP_404_NOT_FOUND

    def test_destroy(self, db):  # noqa
        term_id = _ppr_id(db, "PhD Supervisor of")
        created = _submit(db, term_id)
        person_lineage_submission_crud.destroy(
            db, created["person_lineage_submission_id"]
        )
        with pytest.raises(HTTPException):
            person_lineage_submission_crud.show(
                db, created["person_lineage_submission_id"]
            )
