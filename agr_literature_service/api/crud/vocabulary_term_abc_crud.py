from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from agr_literature_service.api.models import VocabularyTermAbcModel
from agr_literature_service.api.crud.user_utils import map_to_user_id


def create(db: Session, term) -> int:
    data = jsonable_encoder(term)
    for f in ("created_by", "updated_by"):
        if data.get(f) is not None:
            data[f] = map_to_user_id(data[f], db)
    obj = VocabularyTermAbcModel(**data)
    try:
        db.add(obj)
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                            detail=f"Term already exists or violates a constraint: {e.orig}")
    db.refresh(obj)
    return int(obj.vocabulary_term_abc_id)


def show(db: Session, vocabulary_term_abc_id: int) -> dict:
    obj = db.query(VocabularyTermAbcModel).filter(
        VocabularyTermAbcModel.vocabulary_term_abc_id == vocabulary_term_abc_id).first()
    if not obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"vocabulary_term_abc {vocabulary_term_abc_id} not found")
    return jsonable_encoder(obj)


def patch(db: Session, vocabulary_term_abc_id: int, patch_data: dict) -> dict:
    obj = db.query(VocabularyTermAbcModel).filter(
        VocabularyTermAbcModel.vocabulary_term_abc_id == vocabulary_term_abc_id).first()
    if not obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"vocabulary_term_abc {vocabulary_term_abc_id} not found")
    for f in ("created_by", "updated_by"):
        if patch_data.get(f) is not None:
            patch_data[f] = map_to_user_id(patch_data[f], db)
    for key, value in patch_data.items():
        setattr(obj, key, value)
    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"{e.orig}")
    return {"message": "updated"}


def destroy(db: Session, vocabulary_term_abc_id: int) -> None:
    obj = db.query(VocabularyTermAbcModel).filter(
        VocabularyTermAbcModel.vocabulary_term_abc_id == vocabulary_term_abc_id).first()
    if not obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"vocabulary_term_abc {vocabulary_term_abc_id} not found")
    db.delete(obj)
    try:
        db.commit()
    except IntegrityError as e:
        # A term still referenced by laboratory_person / person_lineage(_submission)
        # cannot be deleted (FK NO ACTION). Surface a clean 409 instead of the
        # IntegrityError bubbling out of commit() as a 500.
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                            detail=f"Term is in use and cannot be deleted: {e.orig}")
