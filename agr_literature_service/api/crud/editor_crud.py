"""
editor_crud.py
=============
"""

from datetime import datetime


from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.reference_resource import add, create_obj, stripout
from agr_literature_service.api.models import EditorModel, ResourceModel
from agr_literature_service.api.schemas import EditorSchemaPost


def create(db: Session, editor: EditorSchemaPost) -> int:
    """

    :param db:
    :param editor:
    :return:
    """

    editor_data = jsonable_encoder(editor)

    # orcid = None
    # if "orcid" in editor_data:
    #    orcid = editor_data["orcid"]
    #    del editor_data["orcid"]

    db_obj = create_obj(db, EditorModel, editor_data)
    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)

    return db_obj.editor_id


def destroy(db: Session, editor_id: int) -> None:
    """

    :param db:
    :param editor_id:
    :return:
    """

    editor = db.query(EditorModel).filter(EditorModel.editor_id == editor_id).first()
    if not editor:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Editor with editor_id {editor_id} not found")
    db.delete(editor)
    db.commit()

    return None


def patch(db: Session, editor_id: int, editor_update) -> dict:
    """

    :param db:
    :param editor_id:
    :param editor_update:
    :return:
    """

    editor_data = jsonable_encoder(editor_update)
    editor_db_obj = db.query(EditorModel).filter(EditorModel.editor_id == editor_id).first()
    if not editor_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Editor with editor_id {editor_id} not found")
    res_ref = stripout(db, editor_update, non_fatal=True)
    add(res_ref, editor_db_obj)

    for field, value in editor_data.items():
        setattr(editor_db_obj, field, value)

    editor_db_obj.dateUpdated = datetime.utcnow()
    db.add(editor_db_obj)
    db.commit()

    return {"message": "updated"}


def show(db: Session, editor_id: int) -> dict:
    """

    :param db:
    :param editor_id:
    :return:
    """

    editor = db.query(EditorModel).filter(EditorModel.editor_id == editor_id).first()
    editor_data = jsonable_encoder(editor)

    if not editor:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Editor with the editor_id {editor_id} is not available")

    if editor_data["resource_id"]:
        editor_data["resource_curie"] = db.query(ResourceModel.curie).filter(ResourceModel.resource_id == editor_data["resource_id"]).first()
    del editor_data["resource_id"]

    return editor_data


def show_changesets(db: Session, editor_id: int):
    """

    :param db:
    :param editor_id:
    :return:
    """

    editor = db.query(EditorModel).filter(EditorModel.editor_id == editor_id).first()
    if not editor:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Editor with the editor_id {editor_id} is not available")

    history = []
    for version in editor.versions:
        tx = version.transaction
        history.append({"transaction": {"id": tx.id,
                                        "issued_at": tx.issued_at,
                                        "user_id": tx.user_id},
                        "changeset": version.changeset})

    return history
