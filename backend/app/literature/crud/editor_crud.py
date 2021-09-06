import sqlalchemy
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


def create(db: Session, editor: EditorSchemaPost):
    editor_data = jsonable_encoder(editor)

    resource_curie = None
    if 'resource_curie' in editor_data:
        resource_curie = editor_data['resource_curie']
        del editor_data['resource_curie']

    reference_curie = None
    if 'reference_curie' in editor_data:
        reference_curie = editor_data['reference_curie']
        del editor_data['reference_curie']

    orcid = None
    if 'orcid' in editor_data:
        orcid = editor_data['orcid']
        del editor_data['orcid']

    db_obj = EditorModel(**editor_data)
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
       Modelreference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
       if not reference:
           raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                               detail=f"Reference with curie {reference_curie} does not exist")
       db_obj.reference = reference
    else:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Supply one of resource_curie or reference_curie")

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


def destroy(db: Session, editor_id: int):
    editor = db.query(EditorModel).filter(EditorModel.editor_id == editor_id).first()
    if not editor:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Editor with editor_id {editor_id} not found")
    db.delete(editor)
    db.commit()

    return None


def patch(db: Session, editor_id: int, editor_update: EditorSchemaPost):

    editor_db_obj = db.query(EditorModel).filter(EditorModel.editor_id == editor_id).first()
    if not editor_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Editor with editor_id {editor_id} not found")

    if editor_update.resource_curie and editor_update.reference_curie:
       raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                           detail=f"Only supply either resource_curie or reference_curie")

    for field, value in editor_update.items():
        if field == "resource_curie" and value:
            resource_curie = value
            resource = db.query(ResourceModel).filter(ResourceModel.curie == resource_curie).first()
            if not resource:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                  detail=f"Resource with curie {resource_curie} does not exist")
            editor_db_obj.resource = resource
            editor_db_obj.reference = None
        elif field == 'reference_curie' and value:
            reference_curie = value
            reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
            if not reference:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                  detail=f"Reference with curie {reference_curie} does not exist")
            editor_db_obj.reference = reference
            editor_db_obj.resource = None
        elif field == 'orcid' and value:
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


def show(db: Session, editor_id: int):
    editor = db.query(EditorModel).filter(EditorModel.editor_id == editor_id).first()
    editor_data = jsonable_encoder(editor)

    if not editor:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Editor with the editor_id {editor_id} is not available")

    if editor_data['resource_id']:
        editor_data['resource_curie'] = db.query(ResourceModel.curie).filter(ResourceModel.resource_id == editor_data['resource_id']).first()[0]
    del editor_data['resource_id']

    if editor_data['reference_id']:
        editor_data['reference_curie'] = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == editor_data['reference_id']).first()[0]
    del editor_data['reference_id']

    if editor_data['orcid']:
        editor_data['orcid'] = jsonable_encoder(cross_reference_crud.show(db, orcid['curie']))

    return editor_data


def show_changesets(db: Session, editor_id: int):
    editor = db.query(EditorModel).filter(EditorModel.editor_id == editor_id).first()
    if not editor:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Editor with the editor_id {editor_id} is not available")

    history = []
    for version in reference.versions:
        tx = version.transaction
        history.append({'transaction': {'id': tx.id,
                                        'issued_at': tx.issued_at,
                                        'user_id': tx.user_id},
                        'changeset': version.changeset})

    return history
