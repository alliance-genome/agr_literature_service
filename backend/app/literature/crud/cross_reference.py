import sqlalchemy
from sqlalchemy.orm import Session
from datetime import datetime

from fastapi import HTTPException
from fastapi import status
from fastapi.encoders import jsonable_encoder
from fastapi_sqlalchemy import db

from literature.schemas import CrossReferenceSchema
from literature.schemas import CrossReferenceSchemaUpdate

from literature.models import CrossReference
from literature.models import Reference
from literature.models import Resource


def create(cross_reference: CrossReferenceSchema):
    cross_reference_data = jsonable_encoder(cross_reference_data)

    if 'resource_curie' in cross_reference_data:
        resource_curie = cross_reference_data['resource_curie']
        del cross_reference_data['ressource_curie']

    if 'reference_curie' in author_data:
        reference_curie = author_data['reference_curie']
        del corss_reference_data['reference_curie']

    db_obj = CrossReference(**cross_reference_data)
    if resource_curie and reference_curie:
       raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                           detail=f"Only supply either resource_curie or reference_curie")
    elif resource_curie:
       resource = db.session.query(Resource).filter(Resource.curie == resource_curie).first()
       if not resource:
           raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                               detail=f"Resource with curie {resource_curie} does not exist")
       db_obj.resource = resource
    elif reference_curie:
       reference = db.session.query(Reference).filter(Reference.curie == reference_curie).first()
       if not reference:
           raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                               detail=f"Reference with curie {reference_curie} does not exist")
       db_obj.reference = reference
    else:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Supply one of resource_curie or reference_curie")
    db.session.add(db_obj)
    db.session.commit()
    db.session.refresh(db_obj)

    return db_obj


def destroy(curie: str):
    cross_reference = db.session.query(CrossReference).filter(cross_reference.curie == curie).first()
    if not cross_reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Cross Reference with curie {curie} not found")
    db.session.delete(cross_reference)
    db.session.commit()

    return None


def update(curie: str, cross_reference_update: CrossReferenceSchemaUpdate):

    cross_reference_db_obj = db.session.query(CrossReference).filter(CrossReference.curie == curie).first()
    if not cross_reference_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Cross Reference with curie {curie} not found")


    if cross_reference_update.resource_curie and cross_reference_update.reference_curie:
       raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                           detail=f"Only supply either resource_curie or reference_curie")

    for field, value in vars(cross_reference_update).items():
        if field == "resource_curie" and value:
            resource_curie = value
            resource = db.session.query(Resource).filter(Resource.curie == resource_curie).first()
            if not resource:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                  detail=f"Resource with curie {resource_curie} does not exist")
            cross_reference_db_obj.resource = resource
            cross_reference_db_obj.reference = None
        elif field == 'reference_curie' and value:
            reference_curie = value
            reference = db.session.query(Reference).filter(Reference.curie == reference_curie).first()
            if not reference:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                  detail=f"Reference with curie {reference_curie} does not exist")
            cross_reference_db_obj.reference = reference
            cross_reference_db_obj.resource = None
        else:
            setattr(cross_reference_db_obj, field, value)

    cross_reference_db_obj.dateUpdated = datetime.utcnow()
    db.session.commit()
    db.session.flush()

    return cross_reference_db_obj


def show(curie: str):
    cross_reference = db.session.query(CrossReference).filter(CrossReference.curie == curie).first()

    if not cross_reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"CrossReference with the curie {curie} is not available")

    cross_reference_data = jsonable_encoder(cross_reference)
    if cross_reference_data['resource_id']:
        cross_reference_data['resource_curie'] = db.session.query(Resource.curie).filter(Resource.resource_id == cross_reference_data['resource_id']).first().curie
    del cross_reference_data['resource_id']

    if cross_reference_data['reference_id']:
        cross_reference_data['reference_curie'] = db.session.query(Reference.curie).filter(Reference.reference_id == cross_reference_data['reference_id']).first().curie
    del cross_reference_data['reference_id']

    return cross_reference_data


def show_changesets(curie: str):
    cross_reference = db.session.query(CrossReference).filter(CrossReference.curie == curie).first()
    if not cross_reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Cross Reference with curie {curie} is not available")

    changesets = []
    for version in cross_reference.versions:
        changesets.append(version.changeset)

    return changesets
