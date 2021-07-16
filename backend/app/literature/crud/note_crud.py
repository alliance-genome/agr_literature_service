import sqlalchemy
from sqlalchemy.orm import Session
from datetime import datetime

from fastapi import HTTPException
from fastapi import status
from fastapi.encoders import jsonable_encoder

from literature.schemas import NoteSchemaPost
from literature.schemas import NoteSchemaUpdate

from literature.models import ReferenceModel
from literature.models import ResourceModel
from literature.models import NoteModel


def create(db: Session, note: NoteSchemaPost):
    note_data = jsonable_encoder(note)

    resource_curie = None
    if note_data['resource_curie']:
        resource_curie = note_data['resource_curie']
    del note_data['resource_curie']

    reference_curie = None
    if note_data['reference_curie']:
        reference_curie = note_data['reference_curie']
    del note_data['reference_curie']

    db_obj = NoteModel(**note_data)
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

    return db_obj.note_id


def destroy(db: Session, note_id: int):
    note = db.query(NoteModel).filter(NoteModel.note_id == note_id).first()
    if not note:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Note with note_id {note_id} not found")
    db.delete(note)
    db.commit()

    return None


def patch(db: Session, note_id: int, note_update: NoteSchemaUpdate):
    print(note_id)
    note_db_obj = db.query(NoteModel).filter(NoteModel.note_id == note_id).first()
    print(note_db_obj)
    if not note_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Note with note_id {note_id} not found")

    if 'resource_curie' in note_update and note_update.resource_curie and 'reference_curie' in note_update and note_update.reference_curie:
       raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                           detail=f"Only supply either resource_curie or reference_curie")
    for field, value in note_update.items():
        if field == "resource_curie" and value:
            resource_curie = value
            resource = db.query(ResourceModel).filter(ResourceModel.curie == resource_curie).first()
            if not resource:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                  detail=f"Resource with curie {resource_curie} does not exist")
            note_db_obj.resource = resource
            note_db_obj.reference = None
        elif field == 'reference_curie' and value:
            reference_curie = value
            reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
            if not reference:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                  detail=f"Reference with curie {reference_curie} does not exist")
            note_db_obj.reference = reference
            note_db_obj.resource = None
        else:
            setattr(note_db_obj, field, value)

    db.commit()

    return "Updated"


def show(db: Session, note_id: int):
    note = db.query(NoteModel).filter(NoteModel.note_id == note_id).first()
    note_data = jsonable_encoder(note)

    if not note:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Note with the note_id {note_id} is not available")

    if note_data['resource_id']:
        note_data['resource_curie'] = db.query(Resource.curie).filter(Resource.resource_id == note_data['resource_id']).first()[0]
    del note_data['resource_id']

    if note_data['reference_id']:
        note_data['reference_curie'] = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == note_data['reference_id']).first()[0]
    del note_data['reference_id']

    return note_data


def show_changesets(db: Session, note_id: int):
    note = db.query(NoteModel).filter(NoteModel.note_id == note_id).first()
    if not note:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Note with the note_id {note_id} is not available")

    history = []
    for version in note.versions:
        tx = version.transaction
        history.append({'transaction': {'id': tx.id,
                                        'issued_at': tx.issued_at,
                                        'user_id': tx.user_id},
                        'changeset': version.changeset})

    return history
