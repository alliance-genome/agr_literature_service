"""
mod_reference_type_crud.py
===========================
"""
import math

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ReferenceModel, ReferenceModReferencetypeAssociationModel, ModModel, \
    ReferencetypeModel, ModReferencetypeAssociationModel
from agr_literature_service.api.schemas import ModReferenceTypeSchemaPost


def insert_mod_reference_type_into_db(db_session, pubmed_types, mod_abbreviation, referencetype_label, reference_id):
    mod = db_session.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).one_or_none()
    if mod is None:
        raise ValueError(f"Mod with abbreviation '{mod_abbreviation}' not found.")
    ref_type = db_session.query(ReferencetypeModel).filter(
        ReferencetypeModel.label == referencetype_label).one_or_none()
    mrt = db_session.query(ModReferencetypeAssociationModel).filter(
        ModReferencetypeAssociationModel.mod == mod,
        ModReferencetypeAssociationModel.referencetype == ref_type).one_or_none()
    if (ref_type is None or mrt is None) and mod.abbreviation == "SGD":
        if referencetype_label in pubmed_types:
            if ref_type is None:
                ref_type = ReferencetypeModel(label=referencetype_label)
                db_session.add(ref_type)
                db_session.flush()  # make sure ref_type gets an ID before associating it
            max_display_order = max((mod_ref_type.display_order for mod_ref_type in mod.referencetypes),
                                    default=0)
            mrt = ModReferencetypeAssociationModel(
                mod=mod, referencetype=ref_type,
                display_order=math.ceil((max_display_order + 1) / 10) * 10)
            db_session.add(mrt)
            db_session.flush()
    existing_row = db_session.query(ReferenceModReferencetypeAssociationModel).filter_by(
        reference_id=reference_id,
        mod_referencetype_id=mrt.mod_referencetype_id
    ).one_or_none()
    if existing_row:
        return existing_row.reference_mod_referencetype_id
    if mrt is not None:
        rmrt = ReferenceModReferencetypeAssociationModel(reference_id=reference_id, mod_referencetype=mrt)
        db_session.add(rmrt)
        db_session.commit()
        return rmrt.reference_mod_referencetype_id
    else:
        return None


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
    # this is going to create a new mod_referencetype entry and possibly a new referencetype if the entry is not in the
    # db and mod is SGD and the label is a valid pubmed type
    new_mod_ref_type_id = insert_mod_reference_type_into_db(db, reference.pubmed_types,
                                                            mod_reference_type_data["mod_abbreviation"],
                                                            mod_reference_type_data["reference_type"],
                                                            reference.reference_id)
    if new_mod_ref_type_id is None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="Invalid reference type")
    return new_mod_ref_type_id


def destroy(db: Session, mod_reference_type_id: int) -> None:
    """

    :param db:
    :param mod_reference_type_id:
    :return:
    """

    ref_mod_reference_type = db.query(ReferenceModReferencetypeAssociationModel).filter(
        ReferenceModReferencetypeAssociationModel.reference_mod_referencetype_id == mod_reference_type_id).first()
    if not ref_mod_reference_type:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModReferenceType with mod_reference_type_id {mod_reference_type_id} not found")
    db.delete(ref_mod_reference_type)
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

    # can only patch to an existing mod_referencetype even for SGD
    mrt_data = jsonable_encoder(mod_reference_type_update)
    ref_mod_ref_type_obj = db.query(ReferenceModReferencetypeAssociationModel).filter(
        ReferenceModReferencetypeAssociationModel.reference_mod_referencetype_id == mod_reference_type_id).first()
    if not ref_mod_ref_type_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModReferenceType with mod_reference_type_id {mod_reference_type_id} not found")

    reference = None
    if "reference_curie" in mrt_data:
        reference = db.query(ReferenceModel).filter(ReferenceModel.curie == mrt_data["reference_curie"]).first()
        if not reference:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"Reference with curie {mrt_data['reference_curie']} does not exist")
        ref_mod_ref_type_obj.reference_id = reference.reference_id
    if "reference_type" in mrt_data or "mod_abbreviation" in mrt_data:
        if reference is None:
            reference = db.query(ReferenceModel).filter(
                ReferenceModel.reference_id == ref_mod_ref_type_obj.reference_id).first()
        if "mod_abbreviation" in mrt_data:
            mod = db.query(ModModel).filter(ModModel.abbreviation == mrt_data["mod_abbreviation"]).one_or_none()
            if mod is None:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail=f"Mod with abbreviation {mrt_data['mod_abbreviation']} does not exist")
        else:
            mod = ref_mod_ref_type_obj.mod_referencetype.mod
        if "reference_type" in mrt_data:
            referencetype = db.query(ReferencetypeModel).filter(
                ReferencetypeModel.label == mrt_data["reference_type"]).first()
            if referencetype is None:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail="The provided reference type is not valid")
        else:
            referencetype = ref_mod_ref_type_obj.mod_referencetype.referencetype
        mod_ref_type = db.query(ModReferencetypeAssociationModel).filter(
            ModReferencetypeAssociationModel.mod_id == mod.mod_id,
            ModReferencetypeAssociationModel.referencetype_id == referencetype.referencetype_id).one_or_none()
        if mod_ref_type is None:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail="The provided reference type and mod combination is not valid")
        ref_mod_ref_type_obj.mod_referencetype = mod_ref_type
    db.add(ref_mod_ref_type_obj)
    db.commit()
    return {"message": "updated"}


def show(db: Session, mod_reference_type_id: int):
    """

    :param db:
    :param mod_reference_type_id:
    :return:
    """

    ref_mod_reference_type = db.query(ReferenceModReferencetypeAssociationModel).filter(
        ReferenceModReferencetypeAssociationModel.reference_mod_referencetype_id == mod_reference_type_id).first()
    mod_reference_type_data = {}

    if not ref_mod_reference_type:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModReferenceType with the mod_reference_type_id {mod_reference_type_id} is not available")

    ref_curie = db.query(ReferenceModel.curie).filter(
        ReferenceModel.reference_id == ref_mod_reference_type.reference_id).one_or_none()[0]
    mod_reference_type_data["mod_reference_type_id"] = ref_mod_reference_type.reference_mod_referencetype_id
    mod_reference_type_data["reference_curie"] = ref_curie
    mod_reference_type_data["reference_type"] = ref_mod_reference_type.mod_referencetype.referencetype.label
    mod_reference_type_data["mod_abbreviation"] = ref_mod_reference_type.mod_referencetype.mod.abbreviation
    return mod_reference_type_data


def show_changesets(db: Session, mod_reference_type_id: int):
    """

    :param db:
    :param mod_reference_type_id:
    :return:
    """

    ref_mod_reference_type = db.query(ReferenceModReferencetypeAssociationModel).filter(
        ReferenceModReferencetypeAssociationModel.reference_mod_referencetype_id == mod_reference_type_id).first()
    if not ref_mod_reference_type:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModReferenceType with the mod_reference_type_id {mod_reference_type_id} is not available")

    history = []
    for version in ref_mod_reference_type.versions:
        tx = version.transaction
        history.append({"transaction": {"id": tx.id,
                                        "issued_at": tx.issued_at,
                                        "user_id": tx.user_id},
                        "changeset": version.changeset})

    return history


def show_by_mod(db: Session, mod_abbreviation: str):
    mod_reference_types = db.query(
        ModReferencetypeAssociationModel).filter(
        ModReferencetypeAssociationModel.mod.has(
            abbreviation=mod_abbreviation)
    ).order_by(ModReferencetypeAssociationModel.display_order).all()
    return [mod_reference_type.referencetype.label for mod_reference_type in mod_reference_types]
