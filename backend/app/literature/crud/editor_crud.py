from sqlalchemy.orm import Session
from datetime import datetime

from fastapi import HTTPException
from fastapi import status
from fastapi.encoders import jsonable_encoder

from literature.schemas import EditorSchemaPost

from literature.models import ReferenceModel
from literature.models import ResourceModel
from literature.models import EditorModel
from literature.models import CrossReferenceModel
from literature.crud import cross_reference_crud
from lookup import add_reference_resource


def create(db: Session, editor: EditorSchemaPost) -> int:
    editor_data = jsonable_encoder(editor)

    db_obj = EditorModel(**editor_data)
    add_reference_resource(db, editor_data, db_obj)

    orcid = None
    if 'orcid' in editor_data:
        orcid = editor_data['orcid']
        del editor_data['orcid']

    if orcid:
        cross_reference_obj = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == orcid).first()
        if not cross_reference_obj:
            cross_reference_obj = CrossReferenceModel(curie=orcid)
            db.add(cross_reference_obj)
        db_obj.orcid_cross_reference = cross_reference_obj

    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)

    return db_obj.editor_id


def destroy(db: Session, editor_id: int) -> None:
    editor = db.query(EditorModel).filter(EditorModel.editor_id == editor_id).first()
    if not editor:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Editor with editor_id {editor_id} not found")
    db.delete(editor)
    db.commit()

    return None


def patch(db: Session, editor_id: int, editor_update: EditorSchemaPost) -> dict:

    editor_db_obj = db.query(EditorModel).filter(EditorModel.editor_id == editor_id).first()
    if not editor_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Editor with editor_id {editor_id} not found")
    add_reference_resource(db, editor_update, editor_db_obj)

    for field, value in editor_update.items():
        if field == 'orcid' and value:
            orcid = value
            cross_reference_obj = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == orcid).first()
            if not cross_reference_obj:
                cross_reference_obj = CrossReferenceModel(curie=orcid)
                db.add(cross_reference_obj)
            editor_db_obj.orcid_cross_reference = cross_reference_obj
        else:
            setattr(editor_db_obj, field, value)

    editor_db_obj.dateUpdated = datetime.utcnow()
    db.commit()

    return {"message": "updated"}


def show(db: Session, editor_id: int) -> dict:
    editor = db.query(EditorModel).filter(EditorModel.editor_id == editor_id).first()
    editor_data = jsonable_encoder(editor)

    if not editor:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Editor with the editor_id {editor_id} is not available")

    if editor_data['resource_id']:
        editor_data['resource_curie'] = db.query(ResourceModel.curie).filter(ResourceModel.resource_id == editor_data['resource_id']).first()
    del editor_data['resource_id']

    if editor_data['reference_id']:
        editor_data['reference_curie'] = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == editor_data['reference_id']).first()
    del editor_data['reference_id']

    if editor_data['orcid']:
        orcid = editor_data['orcid']
        editor_data['orcid'] = jsonable_encoder(cross_reference_crud.show(db, orcid['curie']))

    return editor_data


def show_changesets(db: Session, editor_id: int):
    editor = db.query(EditorModel).filter(EditorModel.editor_id == editor_id).first()
    if not editor:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Editor with the editor_id {editor_id} is not available")

    history = []
    for version in editor.versions:
        tx = version.transaction
        history.append({'transaction': {'id': tx.id,
                                        'issued_at': tx.issued_at,
                                        'user_id': tx.user_id},
                        'changeset': version.changeset})

    return history
