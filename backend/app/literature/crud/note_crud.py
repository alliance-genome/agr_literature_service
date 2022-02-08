from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from literature.crud.reference_resource import add, create_obj, stripout
from literature.models import NoteModel, ReferenceModel, ResourceModel
from literature.schemas import NoteSchemaPost, NoteSchemaUpdate


def create(db: Session, note: NoteSchemaPost) -> int:
    note_data = jsonable_encoder(note)

    db_obj = create_obj(db, NoteModel, note_data)

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
    note_db_obj = db.query(NoteModel).filter(NoteModel.note_id == note_id).first()
    if not note_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Note with note_id {note_id} not found")
    res_ref = stripout(db, note_update)
    add(res_ref, note_db_obj)

    for field, value in note_update.items():
        setattr(note_db_obj, field, value)

    db.commit()

    return {"message": "updated"}


def show(db: Session, note_id: int):
    note = db.query(NoteModel).filter(NoteModel.note_id == note_id).first()
    note_data = jsonable_encoder(note)

    if not note:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Note with the note_id {note_id} is not available")

    if note_data['resource_id']:
        note_data['resource_curie'] = db.query(ResourceModel.curie).filter(ResourceModel.resource_id == note_data['resource_id']).first()
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
