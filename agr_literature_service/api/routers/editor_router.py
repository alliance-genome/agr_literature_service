from fastapi import APIRouter, Depends, Response, Security, status
from typing import Dict, Any, Optional

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import editor_crud
from agr_literature_service.api.schemas import (
    EditorSchemaCreate,
    EditorSchemaPost,
    EditorSchemaShow,
)
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user
from agr_literature_service.api.util.resource_urls import editor_url

router = APIRouter(
    prefix="/editor",
    tags=['Editor']
)

get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    response_model=EditorSchemaShow,
)
def create(
    request: EditorSchemaCreate,
    response: Response,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    editor = editor_crud.create(db, request)
    response.headers["Location"] = editor_url(editor.editor_id)
    return editor


@router.delete(
    "/{editor_id}",
    status_code=status.HTTP_204_NO_CONTENT
)
def destroy(
    editor_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session
):
    set_global_user_from_cognito(db, user)
    editor_crud.destroy(db, editor_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch(
    "/{editor_id}",
    status_code=status.HTTP_200_OK,
    response_model=EditorSchemaShow,
)
def patch(
    editor_id: int,
    request: EditorSchemaPost,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    patch_data = request.model_dump(exclude_unset=True)
    return editor_crud.patch(db, editor_id, patch_data)


@router.get(
    "/{editor_id}",
    status_code=status.HTTP_200_OK,
    response_model=EditorSchemaShow
)
def show(
    editor_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session
) -> EditorSchemaShow:
    editor = editor_crud.show(db, editor_id)
    return EditorSchemaShow.model_validate(editor)


@router.get(
    "/{editor_id}/versions",
    status_code=status.HTTP_200_OK
)
def show_versions(
    editor_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session
):
    return editor_crud.show_changesets(db, editor_id)
