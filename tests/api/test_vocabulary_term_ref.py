import pytest
from fastapi import HTTPException

from agr_literature_service.api.crud import vocabulary_crud
from agr_literature_service.api.crud import vocabulary_seed_data as sd
from agr_literature_service.api.models import VocabularyAbcModel, VocabularyTermAbcModel
from ..fixtures import db  # noqa


def _seed_one(db, vocab_key, name, is_obsolete=False):
    v = db.query(VocabularyAbcModel).filter(VocabularyAbcModel.vocabulary == vocab_key).first()
    if v is None:
        v = VocabularyAbcModel(vocabulary=vocab_key)
        db.add(v)
        db.flush()
    t = VocabularyTermAbcModel(vocabulary_abc_id=v.vocabulary_abc_id, name=name, is_obsolete=is_obsolete)
    db.add(t)
    db.commit()
    db.refresh(t)
    return t


class TestTermRefHelpers:
    def test_serialize_none_returns_none(self, db):  # noqa
        assert vocabulary_crud.serialize_term_ref(db, None) is None

    def test_serialize_returns_object(self, db):  # noqa
        t = _seed_one(db, sd.LAB_POSITION_VOCAB, "Lab Member")
        assert vocabulary_crud.serialize_term_ref(db, t.vocabulary_term_abc_id) == {
            "value": t.vocabulary_term_abc_id, "label": "Lab Member", "is_obsolete": False,
        }

    def test_validate_ok(self, db):  # noqa
        t = _seed_one(db, sd.LAB_POSITION_VOCAB, "Technician")
        vocabulary_crud.validate_term_id(db, sd.LAB_POSITION_VOCAB, t.vocabulary_term_abc_id)  # no raise

    def test_validate_unknown_id_422(self, db):  # noqa
        _seed_one(db, sd.LAB_POSITION_VOCAB, "Technician")
        with pytest.raises(HTTPException) as e:
            vocabulary_crud.validate_term_id(db, sd.LAB_POSITION_VOCAB, 999999)
        assert e.value.status_code == 422

    def test_validate_wrong_vocabulary_422(self, db):  # noqa
        t = _seed_one(db, sd.PERSON_LINEAGE_VOCAB, "Collaborator of")
        with pytest.raises(HTTPException) as e:
            vocabulary_crud.validate_term_id(db, sd.LAB_POSITION_VOCAB, t.vocabulary_term_abc_id)
        assert e.value.status_code == 422

    def test_validate_obsolete_422(self, db):  # noqa
        t = _seed_one(db, sd.LAB_POSITION_VOCAB, "Old Role", is_obsolete=True)
        with pytest.raises(HTTPException) as e:
            vocabulary_crud.validate_term_id(db, sd.LAB_POSITION_VOCAB, t.vocabulary_term_abc_id)
        assert e.value.status_code == 422
