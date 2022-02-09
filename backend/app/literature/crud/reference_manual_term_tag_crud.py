from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from literature.models import ReferenceManualTermTagModel, ReferenceModel
from literature.schemas import (ReferenceManualTermTagSchemaPatch,
                                ReferenceManualTermTagSchemaPost)


def create(db: Session, reference_manual_term_tag: ReferenceManualTermTagSchemaPost):
    reference_manual_term_tag_data = jsonable_encoder(reference_manual_term_tag)

    reference_curie = reference_manual_term_tag_data['reference_curie']
    reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Reference_curie {reference_curie} does not exist")

    del reference_manual_term_tag_data['reference_curie']

    db_obj = ReferenceManualTermTagModel(**reference_manual_term_tag_data)
    db_obj.reference = reference

    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)

    return db_obj.reference_manual_term_tag_id


def destroy(db: Session, reference_manual_term_tag_id: int):
    db_obj = db.query(ReferenceManualTermTagModel).filter(ReferenceManualTermTagModel.reference_manual_term_tag_id == reference_manual_term_tag_id).first()
    if not db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference Manual Tag Term with reference_manual_term_tag_id {reference_manual_term_tag_id} not found")

    db.delete(db_obj)
    db.commit()

    return None


def patch(db: Session, reference_manual_term_tag_id: int, reference_manual_term_tag_update: ReferenceManualTermTagSchemaPatch):
    db_obj = db.query(ReferenceManualTermTagModel).filter(ReferenceManualTermTagModel.reference_manual_term_tag_id == reference_manual_term_tag_id).first()
    if not db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference Manual Term Tag ID with reference_manual_term_tag_id {reference_manual_term_tag_id} not found")

    for field, value in reference_manual_term_tag_update.dict().items():
        if field == "reference_curie" and value:
            reference_curie_to = value
            reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie_to).first()
            if not reference:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail="Reference with curie {reference_curie_to} does not exist")
            db_obj.reference = reference
        else:
            setattr(db_obj, field, value)

    db.commit()

    return {"message": "updated"}


def show(db: Session, reference_manual_term_tag_id: int):
    db_obj = db.query(ReferenceManualTermTagModel).filter(ReferenceManualTermTagModel.reference_manual_term_tag_id == reference_manual_term_tag_id).first()
    data = jsonable_encoder(db_obj)

    if not db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference Manual Tag Term ID with the reference_manual_term_tag_id {reference_manual_term_tag_id} is not available")

    data['reference_curie'] = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == data['reference_id']).first()[0]
    del data['reference_id']

    return data


def show_changesets(db: Session, reference_manual_term_tag_id: int):
    db_obj = db.query(ReferenceManualTermTagModel).filter(ReferenceManualTermTagModel.reference_manual_term_tag_id == reference_manual_term_tag_id).first()
    if not db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference Manual term Tag with the reference_manual_term_tag_id {reference_manual_term_tag_id} is not available")

    history = []
    for version in db_obj.versions:
        tx = version.transaction
        history.append({'transaction': {'id': tx.id,
                                        'issued_at': tx.issued_at,
                                        'user_id': tx.user_id},
                        'changeset': version.changeset})

    return history
