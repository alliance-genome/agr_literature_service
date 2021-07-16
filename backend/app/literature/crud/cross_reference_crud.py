import re

import json
import sqlalchemy
from sqlalchemy.orm import Session
from datetime import datetime

from fastapi import HTTPException
from fastapi import status
from fastapi.encoders import jsonable_encoder

from literature.schemas import CrossReferenceSchema
from literature.schemas import CrossReferenceSchemaUpdate

from literature.models import CrossReferenceModel
from literature.models import ReferenceModel
from literature.models import ResourceModel
from literature.models import ResourceDescriptorModel


def create(db: Session, cross_reference: CrossReferenceSchema):
    cross_reference_data = jsonable_encoder(cross_reference)

    if db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == cross_reference_data['curie']).first():
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                            detail=f"CrossReference with curie {cross_reference_data['curie']} already exists")

    resource_curie = None
    if cross_reference_data['resource_curie']:
        resource_curie = cross_reference_data['resource_curie']
    del cross_reference_data['resource_curie']

    reference_curie = None
    if cross_reference_data['reference_curie']:
        reference_curie = cross_reference_data['reference_curie']
    del cross_reference_data['reference_curie']

    db_obj = CrossReferenceModel(**cross_reference_data)
    if resource_curie and reference_curie:
       raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                           detail=f"Only supply either resource_curie or reference_curie")
    elif resource_curie:
       resource = db.query(ResourceModel).filter(ResourceModel.curie == resource_curie).first()
       if not resource:
           raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                               detail=f"Resource with curie {resource_curie} does not exist")
       db_obj.resource = resource
    elif reference_curie:
       reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
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
    cross_reference = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == curie).first()
    if not cross_reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Cross Reference with curie {curie} not found")
    db.delete(cross_reference)
    db.commit()

    return None


def patch(db: Session, curie: str, cross_reference_update: CrossReferenceSchemaUpdate):

    cross_reference_db_obj = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == curie).first()
    if not cross_reference_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Cross Reference with curie {curie} not found")


    if cross_reference_update.resource_curie and cross_reference_update.reference_curie:
       raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                           detail=f"Only supply either resource_curie or reference_curie")

    for field, value in cross_reference_update.items():
        if field == "resource_curie" and value:
            resource_curie = value
            resource = db.query(ResourceModel).filter(ResourceModel.curie == resource_curie).first()
            if not resource:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                  detail=f"Resource with curie {resource_curie} does not exist")
            cross_reference_db_obj.resource = resource
            cross_reference_db_obj.reference = None
        elif field == 'reference_curie' and value:
            reference_curie = value
            reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
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
    cross_reference = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == curie).first()
    if not cross_reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"CrossReference with the curie {curie} is not available")

    cross_reference_data = jsonable_encoder(cross_reference)
    if cross_reference_data['resource_id']:
        cross_reference_data['resource_curie'] = db.query(ResourceModel.curie).filter(ResourceModel.resource_id == cross_reference_data['resource_id']).first().curie
    del cross_reference_data['resource_id']

    if cross_reference_data['reference_id']:
        cross_reference_data['reference_curie'] = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == cross_reference_data['reference_id']).first().curie
    del cross_reference_data['reference_id']

    [db_prefix, local_id] = curie.split(":", 1)
    resource_descriptor = db.query(ResourceDescriptorModel).filter(ResourceDescriptorModel.db_prefix == db_prefix).first()
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
    elif cross_reference_data['pages']:
       pages_data = []
       for cr_page in cross_reference_data['pages']:
           pages_data.append({"name": cr_page})
       cross_reference_data['pages'] = pages_data

    return cross_reference_data


def show_changesets(db: Session, curie: str):
    cross_reference = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == curie).first()
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
