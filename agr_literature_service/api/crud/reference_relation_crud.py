"""
reference_relation_crud.py
=========================================
"""


from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from agr_literature_service.api.models import (ReferenceRelationModel,
                                               ReferenceModel)
from agr_literature_service.api.schemas import ReferenceRelationSchemaPost


def create(db: Session, reference_relation: ReferenceRelationSchemaPost):
    """

    :param db:
    :param reference_relation:
    :return:
    """

    reference_relation_data = jsonable_encoder(reference_relation)
    reference_curie_from = reference_relation_data["reference_curie_from"]
    reference_curie_to = reference_relation_data["reference_curie_to"]
    reference_relation_type = reference_relation_data["reference_relation_type"]

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

    if reference_id_from == reference_id_to:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                            detail=f"Reference Relation with reference_curie_from  {reference_curie_from} and reference curie_to {reference_curie_to} are the same reference")

    db_obj = db.query(ReferenceRelationModel).filter(
        or_(
            and_(ReferenceRelationModel.reference_id_from == reference_id_from,
                 ReferenceRelationModel.reference_id_to == reference_id_to),
            and_(ReferenceRelationModel.reference_id_from == reference_id_to,
                 ReferenceRelationModel.reference_id_to == reference_id_from)
        )
    ).first()
    if db_obj:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                            detail=f"Reference Relation with reference_curie_from  {reference_curie_from} and reference curie_to {reference_curie_to} already exists with id {db_obj.reference_relation_id}")

    db_obj = ReferenceRelationModel(reference_relation_type=reference_relation_type,
                                    reference_from=reference_from,
                                    reference_to=reference_to)
    try:
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj.reference_relation_id
    except (IntegrityError, HTTPException) as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"invalid request: {e}")


def destroy(db: Session, reference_relation_id: int):
    """

    :param db:
    :param reference_relation_id:
    :return:
    """

    db_obj = db.query(ReferenceRelationModel).filter(ReferenceRelationModel.reference_relation_id == reference_relation_id).first()
    if not db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference relation with reference_relation_id {reference_relation_id} not found")

    db.delete(db_obj)
    db.commit()

    return None


def patch(db: Session, reference_relation_id: int, reference_relation_update):
    """

    :param db:
    :param reference_relation_id:
    :param reference_relation_update:
    :return:
    """

    db_obj = db.query(ReferenceRelationModel).filter(
        ReferenceRelationModel.reference_relation_id == reference_relation_id
    ).first()
    if not db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference relation with reference_relation_id "
                                   f"{reference_relation_id} not found")
    # on occasions where patching switches from and to cannot update db_obj while looping through items, or querying
    # reference will trigger an autoflush failure because of constraint a relation cannot connect a reference to itself

    # Start with current values
    new_reference_id_from = db_obj.reference_id_from
    new_reference_id_to = db_obj.reference_id_to
    for field, value in reference_relation_update.items():
        if field == "reference_curie_to" and value:
            reference_curie_to = value
            reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie_to).first()
            if not reference:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail=f"Reference with curie {reference_curie_to} does not exist")
            new_reference_id_to = reference.reference_id
        elif field == "reference_curie_from" and value:
            reference_curie_from = value
            reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie_from).first()
            if not reference:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail=f"Reference with curie {reference_curie_from} does not exist")
            new_reference_id_from = reference.reference_id
        else:
            setattr(db_obj, field, value)
    if new_reference_id_from == new_reference_id_to:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                            detail="A reference relation cannot connect a reference to itself")

    duplicate = db.query(ReferenceRelationModel).filter(
        ReferenceRelationModel.reference_relation_id != reference_relation_id,
        or_(
            and_(
                ReferenceRelationModel.reference_id_from == new_reference_id_from,
                ReferenceRelationModel.reference_id_to == new_reference_id_to
            ),
            and_(
                ReferenceRelationModel.reference_id_from == new_reference_id_to,
                ReferenceRelationModel.reference_id_to == new_reference_id_from
            )
        )
    ).first()

    if duplicate:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                            detail=f"Reference Relation between these references already exists with id {duplicate.reference_relation_id}")

    db_obj.reference_id_from = new_reference_id_from
    db_obj.reference_id_to = new_reference_id_to

    try:
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return {"message": "updated"}
    except (IntegrityError, HTTPException) as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"invalid request: {e}")


def show(db: Session, reference_relation_id: int):
    """

    :param db:
    :param reference_relation_id:
    :return:
    """

    db_obj = db.query(ReferenceRelationModel).filter(ReferenceRelationModel.reference_relation_id == reference_relation_id).first()
    data = jsonable_encoder(db_obj)

    if not db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference Relation with the reference_relation_id "
                                   f"{reference_relation_id} is not available")

    data["reference_curie_from"] = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == data["reference_id_from"]).first()[0]
    data["reference_curie_to"] = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == data["reference_id_to"]).first()[0]

    del data["reference_id_from"]
    del data["reference_id_to"]

    return data


def show_changesets(db: Session, reference_relation_id: int):
    """

    :param db:
    :param reference_relation_id:
    :return:
    """

    db_obj = db.query(ReferenceRelationModel).filter(ReferenceRelationModel.reference_relation_id == reference_relation_id).first()
    if not db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference Relation with the reference_relation_id "
                                   f"{reference_relation_id} is not available")

    history = []
    for version in db_obj.versions:
        tx = version.transaction
        history.append({"transaction": {"id": tx.id,
                                        "issued_at": tx.issued_at,
                                        "user_id": tx.user_id},
                        "changeset": version.changeset})

    return history
