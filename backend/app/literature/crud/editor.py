import sqlalchemy
from sqlalchemy.orm import Session
from datetime import datetime

from fastapi import HTTPException
from fastapi import status
from fastapi.encoders import jsonable_encoder
from fastapi_sqlalchemy import db

from literature.schemas import EditorSchemaPost
from literature.schemas import EditorSchemaUpdate

from literature.models import Reference
from literature.models import Resource
from literature.models import Editor



def create(editor: EditorSchemaPost):
    editor_data = jsonable_encoder(editor)

    if 'resource_curie' in editor_data:
        resource_curie = editor_data['resource_curie']
        del editor_data['resource_curie']

    if 'reference_curie' in editor_data:
        reference_curie = editor_data['reference_curie']
        del editor_data['reference_curie']

    db_obj = Editor(**editor_data)
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


def destroy(editor_id: int):
    editor = db.session.query(Editor).filter(Editor.editor_id == editor_id).first()
    if not editor:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Editor with editor_id {editor_id} not found")
    db.session.delete(editor)
    db.session.commit()

    return None


def update(editor_id: int, editor_update: EditorSchemaUpdate):

    editor_db_obj = db.session.query(Editor).filter(Editor.editor_id == editor_id).first()
    if not editor_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Editor with editor_id {editor_id} not found")


    if editor_update.resource_curie and editor_update.reference_curie:
       raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                           detail=f"Only supply either resource_curie or reference_curie")

    for field, value in vars(editor_update).items():
        if field == "resource_curie" and value:
            resource_curie = value
            resource = db.session.query(Resource).filter(Resource.curie == resource_curie).first()
            if not resource:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                  detail=f"Resource with curie {resource_curie} does not exist")
            editor_db_obj.resource = resource
            editor_db_obj.reference = None
        elif field == 'reference_curie' and value:
            reference_curie = value
            reference = db.session.query(Reference).filter(Reference.curie == reference_curie).first()
            if not reference:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                  detail=f"Reference with curie {reference_curie} does not exist")
            editor_db_obj.reference = reference
            editor_db_obj.resource = None
        else:
            setattr(editor_db_obj, field, value)

    editor_db_obj.dateUpdated = datetime.utcnow()
    db.session.commit()

    return db.session.query(Editor).filter(Editor.editor_id == editor_id).first()


def show(editor_id: int):
    editor = db.session.query(Editor).filter(Editor.editor_id == editor_id).first()
    editor_data = jsonable_encoder(editor)

    if not editor:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Editor with the editor_id {editor_id} is not available")

    if editor_data['resource_id']:
        editor_data['resource_curie'] = db.session.query(Resource.curie).filter(Resource.resource_id == editor_data['resource_id']).first()[0]
    del editor_data['resource_id']

    if editor_data['reference_id']:
        editor_data['reference_curie'] = db.session.query(Reference.curie).filter(Reference.reference_id == editor_data['reference_id']).first()[0]
    del editor_data['reference_id']

    return editor_data


def show_changesets(editor_id: int):
    editor = db.session.query(Editor).filter(Editor.editor_id == editor_id).first()
    if not editor:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Editor with the editor_id {editor_id} is not available")

    changesets = []
    for version in editor.versions:
        changesets.append(version.changeset)

    return changesets
