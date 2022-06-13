"""
person_crud.py
==============
"""

from datetime import datetime

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ModModel
from agr_literature_service.api.schemas import ModSchemaPost, ModSchemaUpdate


def create(db: Session, mod: ModSchemaPost):
    """

    :param db:
    :param mod:
    :return:
    """

    mod_data = jsonable_encoder(mod)
    mod_db_obj = ModModel(**mod_data)
    db.add(mod_db_obj)
    db.commit()

    db.refresh(mod_db_obj)

    return mod_db_obj.mod_id


def destroy(db: Session, mod_id: int):
    """

    :param db:
    :param mod_id:
    :return:
    """

    mod = db.query(ModModel).filter(ModModel.mod_id == mod_id).first()
    if not mod:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Mod with mod_id {mod_id} not found")
    db.delete(mod)
    db.commit()

    return None


def patch(db: Session, mod_id: int, mod_update: ModSchemaUpdate):
    """

    :param db:
    :param mod_id:
    :param mod_update:
    :return:
    """

    patch = mod_update.dict(exclude_unset=True)
    mod_data = jsonable_encoder(patch)
    mod_db_obj = db.query(ModModel).filter(ModModel.mod_id == mod_id).first()
    if not mod_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Mod with mod_id {mod_id} not found")
    for field, value in mod_data.items():
        setattr(mod_db_obj, field, value)

    mod_db_obj.dateUpdated = datetime.utcnow()
    db.commit()

    return {"message": "updated"}


def show(db: Session, abbreviation: str):
    """

    :param db:
    :param mod_id:
    :return:
    """
    mod = db.query(ModModel).filter(ModModel.abbreviation == abbreviation).first()
    if not mod:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Mod with the abbreviation {abbreviation} is not available")
    mod_data = jsonable_encoder(mod)
    return mod_data


def show_changesets(db: Session, mod_id: int):
    """

    :param db:
    :param mod_id:
    :return:
    """

    mod = db.query(ModModel).filter(ModModel.mod_id == mod_id).first()
    if not mod:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Mod with the mod_id {mod_id} is not available")

    history = []
    for version in mod.versions:
        tx = version.transaction
        history.append({"transaction": {"id": tx.id,
                                        "issued_at": tx.issued_at,
                                        "user_id": tx.user_id},
                        "changeset": version.changeset})

    return history
