from sqlalchemy.orm import Session
from datetime import datetime

from fastapi import HTTPException
from fastapi import status
from fastapi.encoders import jsonable_encoder

from literature.schemas import PersonSchemaPost

from literature.models import ReferenceModel
from literature.models import PersonModel
from literature.crud.reference_resource import add, stripout, create_obj


def create(db: Session, person: PersonSchemaPost):
    person_data = jsonable_encoder(person)

    db_obj = create_obj(db, PersonModel, person_data)

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
    res_ref = stripout(db, person_update)
    add(res_ref, person_db_obj)
    for field, value in person_update.items():
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
