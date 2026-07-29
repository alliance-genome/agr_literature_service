"""Seed the ABC controlled vocabularies into a test/dev database.

Mirrors mod_populate_load.populate_test_mods(): tests build their schema with
Base.metadata.create_all (no alembic), so they do not inherit the seed migration
and must call this to have vocabulary terms available.
"""
from sqlalchemy.orm import Session

from agr_literature_service.api.crud import vocabulary_seed_data as sd
from agr_literature_service.api.models import VocabularyAbcModel, VocabularyTermAbcModel


def _ensure_vocabulary(db: Session, key: str) -> VocabularyAbcModel:
    vocab = db.query(VocabularyAbcModel).filter(VocabularyAbcModel.vocabulary == key).one_or_none()
    if vocab is None:
        vocab = VocabularyAbcModel(vocabulary=key)
        db.add(vocab)
        db.flush()
    return vocab


def _ensure_terms(db: Session, vocab: VocabularyAbcModel, labels) -> None:
    for label in labels:
        exists = (
            db.query(VocabularyTermAbcModel)
            .filter(
                VocabularyTermAbcModel.vocabulary_abc_id == vocab.vocabulary_abc_id,
                VocabularyTermAbcModel.name == label,
            )
            .one_or_none()
        )
        if exists is None:
            db.add(VocabularyTermAbcModel(
                vocabulary_abc_id=vocab.vocabulary_abc_id, name=label, is_obsolete=False))


def populate_test_vocabularies(db: Session) -> None:
    lab = _ensure_vocabulary(db, sd.LAB_POSITION_VOCAB)
    _ensure_terms(db, lab, sd.LAB_POSITION_TERMS)
    ppr = _ensure_vocabulary(db, sd.PERSON_LINEAGE_VOCAB)
    _ensure_terms(db, ppr, sd.PERSON_LINEAGE_TERMS)
    db.commit()
