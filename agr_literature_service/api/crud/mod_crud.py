"""
mod_crud.py
==============
"""

from datetime import datetime
from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ModModel
from agr_literature_service.api.schemas import ModSchemaPost
from agr_literature_service.api.crud.user_utils import map_to_user_id


def create(db: Session, mod: ModSchemaPost):
    """

    :param db:
    :param mod:
    :return:
    """

    mod_data = jsonable_encoder(mod)

    if "created_by" in mod_data and mod_data["created_by"] is not None:
        mod_data["created_by"] = map_to_user_id(mod_data["created_by"], db)
    if "updated_by" in mod_data and mod_data["updated_by"] is not None:
        mod_data["updated_by"] = map_to_user_id(mod_data["updated_by"], db)

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


def patch(db: Session, mod_id: int, mod_update):
    """

    :param db:
    :param mod_id:
    :param mod_update:
    :return:
    """

    mod_data = jsonable_encoder(mod_update)

    if "created_by" in mod_data and mod_data["created_by"] is not None:
        mod_data["created_by"] = map_to_user_id(mod_data["created_by"], db)
    if "updated_by" in mod_data and mod_data["updated_by"] is not None:
        mod_data["updated_by"] = map_to_user_id(mod_data["updated_by"], db)

    mod_db_obj = db.query(ModModel).filter(ModModel.mod_id == mod_id).first()
    if not mod_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Mod with mod_id {mod_id} not found")
    for field, value in mod_data.items():
        setattr(mod_db_obj, field, value)

    mod_db_obj.dateUpdated = datetime.utcnow()
    db.add(mod_db_obj)
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


def taxons(db: Session, type='default'):

    taxons = []
    for mod in db.query(ModModel).all():
        if mod.abbreviation != 'GO':
            if type == 'all':
                taxons.append({'mod_abbreviation': mod.abbreviation,
                               'taxon_ids': mod.taxon_ids})
            else:
                taxon_ids_list = list(mod.taxon_ids)
                first_taxon_id = taxon_ids_list[0] if taxon_ids_list else None
                taxons.append({'mod_abbreviation': mod.abbreviation,
                               'taxon_id': first_taxon_id})
    return taxons


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
