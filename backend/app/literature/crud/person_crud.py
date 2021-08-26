import sqlalchemy
from sqlalchemy.orm import Session
from datetime import datetime

from fastapi import HTTPException
from fastapi import status
from fastapi.encoders import jsonable_encoder

from literature.schemas import PersonSchemaPost

from literature.models import ReferenceModel
from literature.models import ResourceModel
from literature.models import PersonModel


def create(db: Session, person: PersonSchemaPost):
    person_data = jsonable_encoder(person)

    if 'resource_curie' in person_data:
        resource_curie = person_data['resource_curie']
        del person_data['resource_curie']

    if 'reference_curie' in person_data:
        reference_curie = person_data['reference_curie']
        del person_data['reference_curie']

    db_obj = PersonModel(**person_data)
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
    db.refresh(db_obj)

    return db_obj


def destroy(db: Session, person_id: int):
    person = db.query(PersonModel).filter(PersonModel.person_id == person_id).first()
    if not person:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Person with person_id {person_id} not found")
    db.delete(person)
    db.commit()

    return None


def patch(db: Session, person_id: int, person_update: PersonSchemaPost):

    person_db_obj = db.query(PersonModel).filter(PersonModel.person_id == person_id).first()
    if not person_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Person with person_id {person_id} not found")


    if person_update.resource_curie and person_update.reference_curie:
       raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                           detail=f"Only supply either resource_curie or reference_curie")

    for field, value in person_update.items():
        if field == "resource_curie" and value:
            resource_curie = value
            resource = db.query(ResourceModel).filter(ResourceModel.curie == resource_curie).first()
            if not resource:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                  detail=f"Resource with curie {resource_curie} does not exist")
            person_db_obj.resource = resource
            person_db_obj.reference = None
        elif field == 'reference_curie' and value:
            reference_curie = value
            reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
            if not reference:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                  detail=f"Reference with curie {reference_curie} does not exist")
            person_db_obj.reference = reference
            person_db_obj.resource = None
        else:
            setattr(person_db_obj, field, value)

    person_db_obj.dateUpdated = datetime.utcnow()
    db.commit()

    return {"message": "updated"}


def show(db: Session, person_id: int):
    person = db.query(PersonModel).filter(PersonModel.person_id == person_id).first()
    person_data = jsonable_encoder(person)

    if not person:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Person with the person_id {person_id} is not available")

    if person_data['reference_id']:
        person_data['reference_curie'] = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == person_data['reference_id']).first()[0]
    del person_data['reference_id']

    return person_data


def show_changesets(db: Session, person_id: int):
    person = db.query(PersonModel).filter(PersonModel.person_id == person_id).first()
    if not person:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Person with the person_id {person_id} is not available")

    history = []
    for version in person.versions:
        tx = version.transaction
        history.append({'transaction': {'id': tx.id,
                                        'issued_at': tx.issued_at,
                                        'user_id': tx.user_id},
                        'changeset': version.changeset})

    return history
