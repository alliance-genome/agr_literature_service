"""
mod_reference_type_crud.py
===========================
"""
from datetime import datetime

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ReferenceModel, ReferenceModReferenceTypeAssociationModel, ModModel, \
    ReferenceTypeModel, ModReferenceTypeAssociationModel
from agr_literature_service.api.schemas import ModReferenceTypeSchemaPost


def create(db: Session, mod_reference_type: ModReferenceTypeSchemaPost) -> int:
    """
    Create a new mod_reference_type
    :param db:
    :param mod_reference_type:
    :return:
    """

    mod_reference_type_data = jsonable_encoder(mod_reference_type)
    reference_curie = mod_reference_type_data["reference_curie"]
    del mod_reference_type_data["reference_curie"]

    reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Reference with curie {reference_curie} does not exist")
    mod = db.query(ModModel).filter(ModModel.abbreviation == mod_reference_type_data["source"]).one_or_none()
    ref_type = db.query(ReferenceTypeModel).filter(ReferenceTypeModel.label == mod_reference_type_data["referenceType"])
    mrt = ModReferenceTypeAssociationModel(mod_id=mod.mod_id, referencetype_id=ref_type.referencetype_id)
    db_obj = ReferenceModReferenceTypeAssociationModel(reference_id=reference.reference_id,
                                                       mod_referencetype_id=mrt.mod_referencetype_id)
    db_obj.reference = reference
    db.add(db_obj)
    db.commit()

    return db_obj.mod_reference_type_id


def destroy(db: Session, mod_reference_type_id: int) -> None:
    """

    :param db:
    :param mod_reference_type_id:
    :return:
    """

    mod_reference_type = db.query(ReferenceModReferenceTypeAssociationModel).filter(
        ReferenceModReferenceTypeAssociationModel.reference_mod_referencetype_id == mod_reference_type_id).first()
    if not mod_reference_type:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModReferenceType with mod_reference_type_id {mod_reference_type_id} not found")
    db.delete(mod_reference_type)
    db.commit()

    return None


def patch(db: Session, mod_reference_type_id: int, mod_reference_type_update):
    """
    Update a mod_reference_type
    :param db:
    :param mod_reference_type_id:
    :param mod_reference_type_update:
    :return:
    """

    mrt_data = jsonable_encoder(mod_reference_type_update)
    ref_mod_ref_type_obj = db.query(ReferenceModReferenceTypeAssociationModel).filter(
        ReferenceModReferenceTypeAssociationModel.reference_mod_referencetype_id == mod_reference_type_id).first()
    if not ref_mod_ref_type_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModReferenceType with mod_reference_type_id {mod_reference_type_id} not found")

    for field, value in mrt_data.items():
        if field == "reference_curie" and value:
            reference_curie = value
            reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
            if not reference:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail=f"Reference with curie {reference_curie} does not exist")
            ref_mod_ref_type_obj.reference = reference
        else:
            setattr(ref_mod_ref_type_obj, field, value)

    db.commit()
    return {"message": "updated"}


def show(db: Session, mod_reference_type_id: int):
    """

    :param db:
    :param mod_reference_type_id:
    :return:
    """

    mod_reference_type = db.query(ReferenceModReferenceTypeAssociationModel).filter(
        ReferenceModReferenceTypeAssociationModel.reference_mod_referencetype_id == mod_reference_type_id).first()
    mod_reference_type_data = jsonable_encoder(mod_reference_type)

    if not mod_reference_type:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModReferenceType with the mod_reference_type_id {mod_reference_type_id} is not available")

    if mod_reference_type_data["reference_id"]:
        mod_reference_type_data["reference_curie"] = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == mod_reference_type_data["reference_id"]).first()[0]
        del mod_reference_type_data["reference_id"]

    return mod_reference_type_data


def show_changesets(db: Session, mod_reference_type_id: int):
    """

    :param db:
    :param mod_reference_type_id:
    :return:
    """

    mod_reference_type = db.query(ReferenceModReferenceTypeAssociationModel).filter(
        ReferenceModReferenceTypeAssociationModel.mod_reference_type_id == mod_reference_type_id).first()
    if not mod_reference_type:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModReferenceType with the mod_reference_type_id {mod_reference_type_id} is not available")

    history = []
    for version in mod_reference_type.versions:
        tx = version.transaction
        history.append({"transaction": {"id": tx.id,
                                        "issued_at": tx.issued_at,
                                        "user_id": tx.user_id},
                        "changeset": version.changeset})

    return history
