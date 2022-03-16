"""
person_crud.py
==============
"""

from datetime import datetime

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from literature.crud.reference_resource import add, create_obj, stripout
from literature.models import ModModel, ReferenceModel
from literature.schemas import ModSchemaPost


def create(db: Session, mod: ModSchemaPost):
    """

    :param db:
    :param mod:
    :return:
    """

    mod_data = jsonable_encoder(mod)

    db_obj = create_obj(db, ModModel, mod_data)

    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)

    return db_obj


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


def patch(db: Session, mod_id: int, mod_update: ModSchemaPost):
    """

    :param db:
    :param mod_id:
    :param mod_update:
    :return:
    """

    mod_db_obj = db.query(ModModel).filter(ModModel.mod_id == mod_id).first()
    if not mod_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Mod with mod_id {mod_id} not found")
    res_ref = stripout(db, mod_update)
    add(res_ref, mod_db_obj)
    for field, value in mod_update.dict().items():
        setattr(mod_db_obj, field, value)

    mod_db_obj.dateUpdated = datetime.utcnow()
    db.commit()

    return {"message": "updated"}


def show(db: Session, mod_id: int):
    """

    :param db:
    :param mod_id:
    :return:
    """
    mod = db.query(ModModel).filter(ModModel.mod_id == mod_id).first()
    mod_data = jsonable_encoder(mod)

    if not mod:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Mod with the mod_id {mod_id} is not available")

    if mod_data["reference_id"]:
        mod_data["reference_curie"] = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == mod_data["reference_id"]).first()[0]
    del mod_data["reference_id"]

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
