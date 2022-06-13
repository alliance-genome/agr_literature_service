"""
reference_comment_and_correction_crud.py
=========================================
"""


from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.models import (ReferenceCommentAndCorrectionModel,
                                               ReferenceModel)
from agr_literature_service.api.schemas import ReferenceCommentAndCorrectionSchemaPost


def create(db: Session, reference_comment_and_correction: ReferenceCommentAndCorrectionSchemaPost):
    """

    :param db:
    :param reference_comment_and_correction:
    :return:
    """

    reference_comment_and_correction_data = jsonable_encoder(reference_comment_and_correction)
    reference_curie_from = reference_comment_and_correction_data["reference_curie_from"]
    reference_curie_to = reference_comment_and_correction_data["reference_curie_to"]
    reference_comment_and_correction_type = reference_comment_and_correction_data["reference_comment_and_correction_type"]

    reference_from = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie_from).first()
    if not reference_from:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Reference_curie_from {reference_curie_from} does not exist")

    reference_to = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie_to).first()
    if not reference_to:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Reference_curie_to {reference_curie_to} does not exist")

    reference_id_from = reference_from.reference_id
    reference_id_to = reference_to.reference_id

    db_obj = db.query(ReferenceCommentAndCorrectionModel).filter(ReferenceCommentAndCorrectionModel.reference_id_from == reference_id_from, ReferenceCommentAndCorrectionModel.reference_id_to == reference_id_to).first()
    if db_obj:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                            detail=f"Reference Comment and Correction with reference_curie_from  {reference_curie_from} and reference curie_to {reference_curie_to} already exists with id {db_obj.reference_comment_and_correction_id}")

    db_obj = ReferenceCommentAndCorrectionModel(reference_comment_and_correction_type=reference_comment_and_correction_type,
                                                reference_from=reference_from,
                                                reference_to=reference_to)
    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)

    return db_obj.reference_comment_and_correction_id


def destroy(db: Session, reference_comment_and_correction_id: int):
    """

    :param db:
    :param reference_comment_and_correction_id:
    :return:
    """

    db_obj = db.query(ReferenceCommentAndCorrectionModel).filter(ReferenceCommentAndCorrectionModel.reference_comment_and_correction_id == reference_comment_and_correction_id).first()
    if not db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference Comment And Correction with reference_comment_and_correction_id {reference_comment_and_correction_id} not found")

    db.delete(db_obj)
    db.commit()

    return None


def patch(db: Session, reference_comment_and_correction_id: int, reference_comment_and_correction_update):
    """

    :param db:
    :param reference_comment_and_correction_id:
    :param reference_comment_and_correction_update:
    :return:
    """

    db_obj = db.query(ReferenceCommentAndCorrectionModel).filter(ReferenceCommentAndCorrectionModel.reference_comment_and_correction_id == reference_comment_and_correction_id).first()
    if not db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference Comment And Correction with reference_comment_and_correction_id "
                                   f"{reference_comment_and_correction_id} not found")

    for field, value in reference_comment_and_correction_update.dict().items():
        if field == "reference_curie_to" and value:
            reference_curie_to = value
            reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie_to).first()
            if not reference:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail=f"Reference with curie {reference_curie_to} does not exist")
            db_obj.reference_to = reference
        elif field == "reference_curie_from" and value:
            reference_curie_from = value
            reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie_from).first()
            if not reference:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail=f"Reference with curie {reference_curie_from} does not exist")
            db_obj.reference_from = reference
        else:
            setattr(db_obj, field, value)

    db.commit()

    return {"message": "updated"}


def show(db: Session, reference_comment_and_correction_id: int):
    """

    :param db:
    :param reference_comment_and_correction_id:
    :return:
    """

    db_obj = db.query(ReferenceCommentAndCorrectionModel).filter(ReferenceCommentAndCorrectionModel.reference_comment_and_correction_id == reference_comment_and_correction_id).first()
    data = jsonable_encoder(db_obj)

    if not db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference Comment and Correction with the reference_comment_and_correction_id "
                                   f"{reference_comment_and_correction_id} is not available")

    data["reference_curie_from"] = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == data["reference_id_from"]).first()[0]
    data["reference_curie_to"] = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == data["reference_id_to"]).first()[0]

    del data["reference_id_from"]
    del data["reference_id_to"]

    return data


def show_changesets(db: Session, reference_comment_and_correction_id: int):
    """

    :param db:
    :param reference_comment_and_correction_id:
    :return:
    """

    db_obj = db.query(ReferenceCommentAndCorrectionModel).filter(ReferenceCommentAndCorrectionModel.reference_comment_and_correction_id == reference_comment_and_correction_id).first()
    if not db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference Comment and Correction with the reference_comment_and_correction_id "
                                   f"{reference_comment_and_correction_id} is not available")

    history = []
    for version in db_obj.versions:
        tx = version.transaction
        history.append({"transaction": {"id": tx.id,
                                        "issued_at": tx.issued_at,
                                        "user_id": tx.user_id},
                        "changeset": version.changeset})

    return history
