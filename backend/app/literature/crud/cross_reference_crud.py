import re

import sqlalchemy
from sqlalchemy.orm import Session
from datetime import datetime

from fastapi import HTTPException
from fastapi import status
from fastapi.encoders import jsonable_encoder

from literature.schemas import CrossReferenceSchema
from literature.schemas import CrossReferenceSchemaUpdate

from literature.models import CrossReference
from literature.models import Reference
from literature.models import Resource
from literature.models import ResourceDescriptor


def create(db: Session, cross_reference: CrossReferenceSchema):
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
       resource = db.query(Resource).filter(Resource.curie == resource_curie).first()
       if not resource:
           raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                               detail=f"Resource with curie {resource_curie} does not exist")
       db_obj.resource = resource
    elif reference_curie:
       reference = db.query(Reference).filter(Reference.curie == reference_curie).first()
       if not reference:
           raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                               detail=f"Reference with curie {reference_curie} does not exist")
       db_obj.reference = reference
    else:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Supply one of resource_curie or reference_curie")
    db.add(db_obj)
    db.commit()

    return "created"


def destroy(db: Session, curie: str):
    cross_reference = db.query(CrossReference).filter(CrossReference.curie == curie).first()
    if not cross_reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Cross Reference with curie {curie} not found")
    db.delete(cross_reference)
    db.commit()

    return None


def update(db: Session, curie: str, cross_reference_update: CrossReferenceSchemaUpdate):

    cross_reference_db_obj = db.query(CrossReference).filter(CrossReference.curie == curie).first()
    if not cross_reference_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Cross Reference with curie {curie} not found")


    if cross_reference_update.resource_curie and cross_reference_update.reference_curie:
       raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                           detail=f"Only supply either resource_curie or reference_curie")

    for field, value in vars(cross_reference_update).items():
        if field == "resource_curie" and value:
            resource_curie = value
            resource = db.query(Resource).filter(Resource.curie == resource_curie).first()
            if not resource:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                  detail=f"Resource with curie {resource_curie} does not exist")
            cross_reference_db_obj.resource = resource
            cross_reference_db_obj.reference = None
        elif field == 'reference_curie' and value:
            reference_curie = value
            reference = db.query(Reference).filter(Reference.curie == reference_curie).first()
            if not reference:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                  detail=f"Reference with curie {reference_curie} does not exist")
            cross_reference_db_obj.reference = reference
            cross_reference_db_obj.resource = None
        else:
            setattr(cross_reference_db_obj, field, value)

    cross_reference_db_obj.date_updated = datetime.utcnow()
    db.commit()

    return "updated"


def show(db: Session, curie: str):
    cross_reference = db.query(CrossReference).filter(CrossReference.curie == curie).first()

    if not cross_reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"CrossReference with the curie {curie} is not available")

    cross_reference_data = jsonable_encoder(cross_reference)
    if cross_reference_data['resource_id']:
        cross_reference_data['resource_curie'] = db.query(Resource.curie).filter(Resource.resource_id == cross_reference_data['resource_id']).first().curie
    del cross_reference_data['resource_id']

    if cross_reference_data['reference_id']:
        cross_reference_data['reference_curie'] = db.query(Reference.curie).filter(Reference.reference_id == cross_reference_data['reference_id']).first().curie
    del cross_reference_data['reference_id']


    [db_prefix, local_id] = curie.split(":", 1)
    resource_descriptor = db.query(ResourceDescriptor).filter(ResourceDescriptor.db_prefix == db_prefix).first()
    if resource_descriptor:
        default_url = resource_descriptor.default_url.replace("[%s]", local_id)
        cross_reference_data['url'] = default_url

        if cross_reference_data['pages']:
            pages_data = []
            for cr_page in cross_reference_data['pages']:
                page_url = ""
                for rd_page in resource_descriptor.pages:
                    if rd_page.name == cr_page:
                        page_url = rd_page.url
                        break
                pages_data.append({"name": cr_page,
                                   "url": page_url.replace("[%s]", local_id)})
            cross_reference_data['pages'] = pages_data
    else:
       pages_data = []
       for cr_page in cross_reference_data['pages']:
           pages_data.append({"name": cr_page})
       cross_reference_data['pages'] = pages_data

    return cross_reference_data


def show_changesets(db: Session, curie: str):
    cross_reference = db.query(CrossReference).filter(CrossReference.curie == curie).first()
    if not cross_reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Cross Reference with curie {curie} is not available")

    history = []
    for version in cross_reference.versions:
        tx = version.transaction
        history.append({'transaction': {'id': tx.id,
                                        'issued_at': tx.issued_at,
                                        'user_id': tx.user_id},
                        'changeset': version.changeset})

    return history
