"""
mod_corpus_association_crud.py
===========================
"""

from datetime import datetime

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session
from literature.models import reference_model
from literature.schemas.mod_corpus_association_schemas import ModCorpusAssociationSchemaShowID

from literature.models import ModCorpusAssociationModel, ReferenceModel, ModModel
from literature.schemas import ModCorpusAssociationSchemaPost, ModCorpusAssociationSchemaUpdate
import logging
import logging.config

def create(db: Session, mod_corpus_association: ModCorpusAssociationSchemaPost) -> int:
    """
    Create a new mod_corpus_association
    :param db:
    :param mod_corpus_association:
    :return:
    """

    mod_corpus_association_data = jsonable_encoder(mod_corpus_association)

    reference_curie = mod_corpus_association_data["reference_curie"]
    del mod_corpus_association_data["reference_curie"]
    mod_abbreviation = mod_corpus_association_data["mod_abbreviation"]
    del mod_corpus_association_data["mod_abbreviation"]

    reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Reference with curie {reference_curie} does not exist")

    mod = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).first()
    if not mod:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Mod with abbreviation {mod_abbreviation} does not exist")
    mod_corpus_association_db_obj = db.query(ModCorpusAssociationModel).filter(
        ModCorpusAssociationModel.reference_id == reference.reference_id).filter(
        ModCorpusAssociationModel.mod_id == mod.mod_id).first()
    if mod_corpus_association_db_obj:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"ModCorpusAssociation with the reference_curie {reference_curie}  and mod_abbreviation {mod_abbreviation} already exist, with id:{mod_corpus_association_db_obj.mod_corpus_association_id} can not create duplicate record.")
    db_obj = ModCorpusAssociationModel(**mod_corpus_association_data)
    db_obj.reference = reference
    db_obj.mod = mod
    db.add(db_obj)
    db.commit()

    return db_obj.mod_corpus_association_id


def destroy(db: Session, mod_corpus_association_id: int) -> None:
    """

    :param db:
    :param mod_corpus_association_id:
    :return:
    """

    mod_corpus_association = db.query(ModCorpusAssociationModel).filter(ModCorpusAssociationModel.mod_corpus_association_id == mod_corpus_association_id).first()
    if not mod_corpus_association:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModCorpusAssociation with mod_corpus_association_id {mod_corpus_association_id} not found")
    db.delete(mod_corpus_association)
    db.commit()

    return None


def patch(db: Session, mod_corpus_association_id: int, mod_corpus_association_update: ModCorpusAssociationSchemaUpdate):
    """
    Update a mod_corpus_association
    :param db:
    :param mod_corpus_association_id:
    :param mod_corpus_association_update:
    :return:
    """
    mod_corpus_association_data = jsonable_encoder(mod_corpus_association_update)
    mod_corpus_association_db_obj = db.query(ModCorpusAssociationModel).filter(ModCorpusAssociationModel.mod_corpus_association_id == mod_corpus_association_id).first()
    if not mod_corpus_association_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModCorpusAssociation with mod_corpus_association_id {mod_corpus_association_id} not found")

    for field, value in mod_corpus_association_data.items():
        if field == "reference_curie":
            if value is not None:
                reference_curie = value
                new_reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
                if not new_reference:
                    raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                        detail=f"Reference with curie {reference_curie} does not exist")
                mod_corpus_association_db_obj.reference = new_reference
        elif field == "mod_abbreviation":
            if value is not None:
                mod_abbreviation = value
                new_mod = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).first()
                if not new_mod:
                    raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                        detail=f"Mod with abbreviation {mod_abbreviation} does not exist")
                mod_corpus_association_db_obj.mod = new_mod
        else:
            setattr(mod_corpus_association_db_obj, field, value)

    mod_corpus_association_db_obj.dateUpdated = datetime.utcnow()
    db.commit()

    return {"message": "updated"}


def show(db: Session, mod_corpus_association_id: int):
    """

    :param db:
    :param mod_corpus_association_id:
    :return:
    """

    mod_corpus_association = db.query(ModCorpusAssociationModel).filter(ModCorpusAssociationModel.mod_corpus_association_id == mod_corpus_association_id).first()
    if not mod_corpus_association:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModCorpusAssociation with the mod_corpus_association_id {mod_corpus_association_id} is not available")
    
    mod_corpus_association_data = jsonable_encoder(mod_corpus_association)
    if mod_corpus_association_data["reference_id"]:
        mod_corpus_association_data["reference_curie"] = db.query(ReferenceModel).filter(ReferenceModel.reference_id == mod_corpus_association_data["reference_id"]).first().curie
    del mod_corpus_association_data["reference_id"]
    if mod_corpus_association_data["mod_id"]:
        mod_corpus_association_data["mod_abbreviation"] = db.query(ModModel).filter(ModModel.mod_id == mod_corpus_association_data["mod_id"]).first().abbreviation
    del mod_corpus_association_data["mod_id"]

    return mod_corpus_association_data

#def show_id(db: Session, mod_corpus_association: ModCorpusAssociationSchemaShowID) -> int:
def show_by_reference_mod_abbreviation(db: Session, reference_curie: str, mod_abbreviation: str) -> int:
    """

    :param db:
    :param mod_corpus_association_id:
    :return:
    """
    #logging.basicConfig(level=logging.INFO)
    #logging.info("start to encode data")
    #mod_corpus_association_data = jsonable_encoder(mod_corpus_association)
    #reference_curie = mod_corpus_association_data["reference_curie"]
    #mod_abbreviation = mod_corpus_association_data["mod_abbreviation"]
    
    mod = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).first()
    reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
    if not mod:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Mod with the abbreviation {mod_abbreviation} is not available")
    elif not reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the curie {reference_curie} is not available")
    else:
        #mod_corpus_association_db_obj = db.query(ModCorpusAssociationModel).filter(
        #ModCorpusAssociationModel.reference_id == reference.reference_id
        #and ModCorpusAssociationModel.mod_id == mod.mod_id).first()
        mod_corpus_association_db_obj = db.query(ModCorpusAssociationModel).filter(
        ModCorpusAssociationModel.reference_id == reference.reference_id).filter(
        ModCorpusAssociationModel.mod_id == mod.mod_id).first()
        if not mod_corpus_association_db_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModCorpusAssociation with the reference_curie {reference_curie}  and mod_abbreviation {mod_abbreviation} is not available")
        else:
            return mod_corpus_association_db_obj.mod_corpus_association_id

    return 200  


def show_changesets(db: Session, mod_corpus_association_id: int):
    """

    :param db:
    :param mod_corpus_association_id:
    :return:
    """

    mod_corpus_association = db.query(ModCorpusAssociationModel).filter(
        ModCorpusAssociationModel.mod_corpus_association_id == mod_corpus_association_id).first()
    if not mod_corpus_association:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModCorpusAssociation with the mod_corpus_association_id {mod_corpus_association_id} is not available")

    history = []
    for version in mod_corpus_association.versions:
        tx = version.transaction
        history.append({"transaction": {"id": tx.id,
                                        "issued_at": tx.issued_at,
                                        "user_id": tx.user_id},
                        "changeset": version.changeset})

    return history
