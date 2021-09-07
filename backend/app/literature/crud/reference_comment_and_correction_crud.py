import sqlalchemy
from sqlalchemy.orm import Session
from datetime import datetime

from fastapi import HTTPException
from fastapi import status
from fastapi.encoders import jsonable_encoder

from literature.schemas import ReferenceCommentAndCorrectionSchemaPost
from literature.schemas import ReferenceCommentAndCorrectionSchemaPatch

from literature.models import ReferenceCommentAndCorrectionModel
from literature.models import ReferenceModel


def create(db: Session, reference_comment_and_correction: ReferenceCommentAndCorrectionSchemaPost):
    reference_comment_and_correction_data = jsonable_encoder(reference_comment_and_correction)
    reference_from_curie = reference_comment_and_correction_data['reference_from_curie']
    reference_to_curie = reference_comment_and_correction_data['reference_to_curie']
    reference_comment_and_correction_type = reference_comment_and_correction_data['reference_comment_and_correction_type']

    reference_from = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_from_curie).first()
    if not reference_from:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Reference_from_curie {reference_from_curie} does not exist")

    reference_to = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_to_curie).first()
    if not reference_to:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Reference_to_curie {reference_to_curie} does not exist")

    reference_from_id = reference_from.reference_id
    reference_to_id = reference_to.reference_id

    db_obj = db.query(ReferenceCommentAndCorrectionModel).filter(ReferenceCommentAndCorrectionModel.reference_from_id == reference_from_id, ReferenceCommentAndCorrectionModel.reference_to_id == reference_to_id).first()
    if db_obj:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                            detail=f"Reference Comment and Correction with reference_from_curie  {reference_from_curie}  and reference_to curie {reference_to_curie} already exists with id {db_obj.reference_comment_and_correction_id}")


    db_obj = ReferenceCommentAndCorrectionModel(reference_comment_and_correction_type=reference_comment_and_correction_type,
                                                reference_from=reference_from,
                                                reference_to=reference_to)
    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)

    return db_obj.reference_comment_and_correction_id


def destroy(db: Session, reference_comment_and_correction_id: int):
    print("inside the matrix")
    db_obj = db.query(ReferenceCommentAndCorrectionModel).filter(ReferenceCommentAndCorrectionModel.reference_comment_and_correction_id == reference_comment_and_correction_id).first()
    if not db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference Comment And Correction with reference_comment_and_correction_id {reference_comment_and_correction_id} not found")

    db.delete(db_obj)
    db.commit()

    return None


def patch(db: Session, reference_comment_and_correction_id: int, reference_comment_and_correction_update: ReferenceCommentAndCorrectionSchemaPatch):
    print(reference_comment_and_correction_id)
    db_obj = db.query(ReferenceCommentAndCorrectionModel).filter(ReferenceCommentAndCorrectionModel.reference_comment_and_correction_id == reference_comment_and_correction_id).first()
    if not db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference Comment And Correction with reference_comment_and_correction_id {reference_comment_and_correction_id} not found")


    for field, value in reference_comment_and_correction_update.items():
        print(field)
        print(value)
        if field == "reference_to_curie" and value:
            reference_to_curie = value
            reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_to_curie).first()
            if not reference:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                  detail=f"Reference with curie {reference_to_curie} does not exist")
            db_obj.reference_to = reference
        elif field == 'reference_from_curie' and value:
            reference_from_curie = value
            reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_from_curie).first()
            if not reference:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                  detail=f"Reference with curie {reference_from_curie} does not exist")
            db_obj.reference_from = reference
        else:
            setattr(db_obj, field, value)

    db.commit()

    return {"message": "updated"}


def show(db: Session, reference_comment_and_correction_id: int):
    db_obj = db.query(ReferenceCommentAndCorrectionModel).filter(ReferenceCommentAndCorrectionModel.reference_comment_and_correction_id == reference_comment_and_correction_id).first()
    data = jsonable_encoder(db_obj)

    if not db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference Comment and Correction with the reference_comment_and_correction_id {reference_comment_and_correction_id} is not available")

    data['reference_from_curie'] = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == data['reference_from_id']).first()[0]
    data['reference_to_curie'] = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == data['reference_to_id']).first()[0]

    del data['reference_from_id']
    del data['reference_to_id']

    return data


def show_changesets(db: Session, reference_comment_and_correction_id: int):
    db_obj = db.query(ReferenceCommentAndCorrectionModel).filter(ReferenceCommentAndCorrectionModel.reference_comment_and_correction_id == reference_comment_and_correction_id).first()
    if not db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference Comment and Correction with the reference_comment_and_correction_id {reference_comment_and_correction_id} is not available")

    history = []
    for version in db_obj.versions:
        tx = version.transaction
        history.append({'transaction': {'id': tx.id,
                                        'issued_at': tx.issued_at,
                                        'user_id': tx.user_id},
                        'changeset': version.changeset})

    return history
