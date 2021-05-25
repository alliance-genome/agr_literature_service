import sqlalchemy
from sqlalchemy.orm import Session
from datetime import datetime

from fastapi import HTTPException
from fastapi import status
from fastapi.encoders import jsonable_encoder
from fastapi_sqlalchemy import db

from literature.schemas import ModReferenceTypeSchemaPost
from literature.schemas import ModReferenceTypeSchemaUpdate

from literature.models import Reference
from literature.models import ModReferenceType


def create(mod_reference_type: ModReferenceTypeSchemaPost):
    mod_reference_type_data = jsonable_encoder(mod_reference_type)

    if 'reference_curie' in mod_reference_type_data:
        reference_curie = mod_reference_type_data['reference_curie']
        del mod_reference_type_data['reference_curie']

    reference = db.session.query(Reference).filter(Reference.curie == reference_curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                               detail=f"Reference with curie {reference_curie} does not exist")

    db_obj = ModReferenceType(**mod_reference_type_data)
    db_obj.reference = reference
    db.session.add(db_obj)
    db.session.commit()

    return db_obj


def destroy(mod_reference_type_id: int):
    mod_reference_type = db.session.query(ModReferenceType).filter(ModReferenceType.mod_reference_type_id == mod_reference_type_id).first()
    if not mod_reference_type:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModReferenceType with mod_reference_type_id {mod_reference_type_id} not found")
    db.session.delete(mod_reference_type)
    db.session.commit()

    return None


def update(mod_reference_type_id: int, mod_reference_type_update: ModReferenceTypeSchemaUpdate):

    mod_reference_type_db_obj = db.session.query(ModReferenceType).filter(ModReferenceType.mod_reference_type_id == mod_reference_type_id).first()
    if not mod_reference_type_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModReferenceType with mod_reference_type_id {mod_reference_type_id} not found")


    if not mod_reference_type_update.reference_curie:
       raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                           detail=f"Only supply either resource_curie or reference_curie")

    for field, value in vars(mod_reference_type_update).items():
        if field == 'reference_curie' and value:
            reference_curie = value
            reference = db.session.query(Reference).filter(Reference.curie == reference_curie).first()
            if not reference:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                  detail=f"Reference with curie {reference_curie} does not exist")
            mod_reference_type_db_obj.reference = reference
            mod_reference_type_db_obj.resource = None
        else:
            setattr(mod_reference_type_db_obj, field, value)

    mod_reference_type_db_obj.dateUpdated = datetime.utcnow()
    db.session.commit()

    return db.session.query(ModReferenceType).filter(ModReferenceType.mod_reference_type_id == mod_reference_type_id).first()


def show(mod_reference_type_id: int):
    mod_reference_type = db.session.query(ModReferenceType).filter(ModReferenceType.mod_reference_type_id == mod_reference_type_id).first()
    mod_reference_type_data = jsonable_encoder(mod_reference_type)

    if not mod_reference_type:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModReferenceType with the mod_reference_type_id {mod_reference_type_id} is not available")

    if mod_reference_type_data['reference_id']:
        mod_reference_type_data['reference_curie'] = db.session.query(Reference.curie).filter(Reference.reference_id == mod_reference_type_data['reference_id']).first()[0]
    del mod_reference_type_data['reference_id']

    return mod_reference_type_data


def show_changesets(mod_reference_type_id: int):
    mod_reference_type = db.session.query(ModReferenceType).filter(ModReferenceType.mod_reference_type_id == mod_reference_type_id).first()
    if not mod_reference_type:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModReferenceType with the mod_reference_type_id {mod_reference_type_id} is not available")

    history = []
    for version in mod_reference_type.versions:
        tx = version.transaction
        history.append({'transaction': {'id': tx.id,
                                        'issued_at': tx.issued_at,
                                        'user_id': tx.user_id},
                        'changeset': version.changeset})

    return history
