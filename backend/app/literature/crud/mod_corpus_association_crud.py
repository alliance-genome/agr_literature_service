"""
mod_corpus_association_crud.py
===========================
"""

from datetime import datetime

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from literature.models import ModCorpusAssociationModel, ReferenceModel
from literature.schemas import ModCorpusAssociationSchemaPost, ModCorpusAssociationSchemaUpdate


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

    reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Reference with curie {reference_curie} does not exist")

    db_obj = ModCorpusAssociationModel(**mod_corpus_association_data)
    db_obj.reference = reference
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

    mod_corpus_association_db_obj = db.query(ModCorpusAssociationModel).filter(ModCorpusAssociationModel.mod_corpus_association_id == mod_corpus_association_id).first()
    if not mod_corpus_association_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModCorpusAssociation with mod_corpus_association_id {mod_corpus_association_id} not found")

    for field, value in mod_corpus_association_update.dict().items():
        if field == "reference_curie" and value:
            reference_curie = value
            reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
            if not reference:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail=f"Reference with curie {reference_curie} does not exist")
            mod_corpus_association_db_obj.reference = reference
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
    mod_corpus_association_data = jsonable_encoder(mod_corpus_association)

    if not mod_corpus_association:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModCorpusAssociation with the mod_corpus_association_id {mod_corpus_association_id} is not available")

    if mod_corpus_association_data["reference_id"]:
        mod_corpus_association_data["reference_curie"] = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == mod_corpus_association_data["reference_id"]).first()[0]
        del mod_corpus_association_data["reference_id"]

    return mod_corpus_association_data


def show_changesets(db: Session, mod_corpus_association_id: int):
    """

    :param db:
    :param mod_corpus_association_id:
    :return:
    """

    mod_corpus_association = db.query(ModCorpusAssociationModel).filter(ModCorpusAssociationModel.mod_corpus_association_id == mod_corpus_association_id).first()
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
