"""
reference_ontology_crud.py
===========================
"""

from datetime import datetime

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ReferenceOntologyModel, ReferenceModel, ModModel
from agr_literature_service.api.schemas import ReferenceOntologySchemaCreate


def create(db: Session, reference_ontology: ReferenceOntologySchemaCreate) -> int:
    """
    Create a new reference_ontology
    :param db:
    :param reference_ontology:
    :return:
    """

    reference_ontology_data = jsonable_encoder(reference_ontology)
    print("Create: {}".format(reference_ontology_data))
    reference_curie = reference_ontology_data["reference_curie"]
    del reference_ontology_data["reference_curie"]
    mod_abbreviation = reference_ontology_data["mod_abbreviation"]
    del reference_ontology_data["mod_abbreviation"]
    ontology_id = reference_ontology_data["ontology_id"]

    reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Reference with curie {reference_curie} does not exist")

    mod = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).first()
    if not mod:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Mod with abbreviation {mod_abbreviation} does not exist")
    reference_ontology_db_obj = db.query(ReferenceOntologyModel).filter(
        ReferenceOntologyModel.reference_id == reference.reference_id).filter(
        ReferenceOntologyModel.mod_id == mod.mod_id).filter(
        ReferenceOntologyModel.ontology_id == ontology_id).first()
    if reference_ontology_db_obj:
        print("Exists already?: {}".format(reference_ontology_db_obj))
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"ReferenceOntology with the reference_curie {reference_curie} "
                                   f"and mod_abbreviation {mod_abbreviation} and "
                                   f"{ontology_id} already exist, "
                                   f"with id:{reference_ontology_db_obj.reference_ontology_id} can not "
                                   f"create duplicate record.")

    reference_ontology_data["reference_id"] = reference.reference_id
    reference_ontology_data["mod_id"] = mod.mod_id
    print("Sending data to Model: {}".format(reference_ontology_data))
    db_obj = ReferenceOntologyModel(**reference_ontology_data)
    db_obj.reference = reference
    db_obj.mod = mod
    db.add(db_obj)
    print("Added to db: {}".format(db_obj))
    db.commit()

    return db_obj.reference_ontology_id


def destroy(db: Session, reference_ontology_id: int) -> None:
    """

    :param db:
    :param reference_ontology_id:
    :return:
    """

    reference_ontology = db.query(ReferenceOntologyModel).filter(ReferenceOntologyModel.reference_ontology_id == reference_ontology_id).first()
    if not reference_ontology:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ReferenceOntology with reference_ontology_id {reference_ontology_id} not found")
    db.delete(reference_ontology)
    db.commit()

    return None


def patch(db: Session, reference_ontology_id: int, reference_ontology_update):
    """
    Update a reference_ontology
    :param db:
    :param reference_ontology_id:
    :param reference_ontology_update:
    :return:
    """
    reference_ontology_data = jsonable_encoder(reference_ontology_update)
    reference_ontology_db_obj = db.query(ReferenceOntologyModel).filter(ReferenceOntologyModel.reference_ontology_id == reference_ontology_id).first()
    if not reference_ontology_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ReferenceOntology with reference_ontology_id {reference_ontology_id} not found")

    for field, value in reference_ontology_data.items():
        if field == "reference_curie":
            if value is not None:
                reference_curie = value
                new_reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
                if not new_reference:
                    raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                        detail=f"Reference with curie {reference_curie} does not exist")
                reference_ontology_db_obj.reference = new_reference
        elif field == "mod_abbreviation":
            if value is not None:
                mod_abbreviation = value
                new_mod = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).first()
                if not new_mod:
                    raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                        detail=f"Mod with abbreviation {mod_abbreviation} does not exist")
                reference_ontology_db_obj.mod = new_mod
        else:
            setattr(reference_ontology_db_obj, field, value)

    reference_ontology_db_obj.dateUpdated = datetime.utcnow()
    db.commit()

    return {"message": "updated"}


def show(db: Session, reference_ontology_id: int):
    """

    :param db:
    :param reference_ontology_id:
    :return:
    """

    reference_ontology = db.query(ReferenceOntologyModel).filter(ReferenceOntologyModel.reference_ontology_id == reference_ontology_id).first()
    if not reference_ontology:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ReferenceOntology with the reference_ontology_id {reference_ontology_id} is not available")

    reference_ontology_data = jsonable_encoder(reference_ontology)
    if reference_ontology_data["reference_id"]:
        reference_ontology_data["reference_curie"] = db.query(ReferenceModel).filter(ReferenceModel.reference_id == reference_ontology_data["reference_id"]).first().curie
    del reference_ontology_data["reference_id"]
    if reference_ontology_data["mod_id"]:
        reference_ontology_data["mod_abbreviation"] = db.query(ModModel).filter(ModModel.mod_id == reference_ontology_data["mod_id"]).first().abbreviation
    del reference_ontology_data["mod_id"]

    return reference_ontology_data


def show_by_reference_mod_abbreviation(db: Session, reference_curie: str, mod_abbreviation: str) -> int:
    """

    :param db:
    :param reference_ontology_id:
    :return:
    """
    print("No idea where this is getting called")
    return 200
    mod = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).first()
    reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
    if not mod:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Mod with the abbreviation {mod_abbreviation} is not available")
    elif not reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the curie {reference_curie} is not available")
    else:
        reference_ontology_db_obj = db.query(ReferenceOntologyModel).filter(
            ReferenceOntologyModel.reference_id == reference.reference_id).filter(
            ReferenceOntologyModel.mod_id == mod.mod_id).first()
        if not reference_ontology_db_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"ReferenceOntology with the reference_curie {reference_curie} "
                                       f"and mod_abbreviation {mod_abbreviation} is not available")
        else:
            return reference_ontology_db_obj.reference_ontology_id
    return 200


def show_changesets(db: Session, reference_ontology_id: int):
    """

    :param db:
    :param reference_ontology_id:
    :return:
    """

    reference_ontology = db.query(ReferenceOntologyModel).filter(
        ReferenceOntologyModel.reference_ontology_id == reference_ontology_id).first()
    if not reference_ontology:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ReferenceOntology with the reference_ontology_id {reference_ontology_id} is not available")

    history = []
    for version in reference_ontology.versions:
        tx = version.transaction
        history.append({"transaction": {"id": tx.id,
                                        "issued_at": tx.issued_at,
                                        "user_id": tx.user_id},
                        "changeset": version.changeset})

    return history
