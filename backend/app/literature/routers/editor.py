from typing import List

from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi import HTTPException
from fastapi import Response
from fastapi import Security

from fastapi_auth0 import Auth0User

from sqlalchemy.orm import Session

from literature import schemas
from literature.crud import editor
from literature.routers.authentication import auth

router = APIRouter(
    prefix="/editor",
    tags=['Editor']
)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=schemas.EditorSchemaShow,
             dependencies=[Depends(auth.implicit_scheme)])
def create(request: schemas.EditorSchemaCreate,
           user: Auth0User = Security(auth.get_user)):
    return editor.create(request)


@router.delete('/{editor_id}',
               status_code=status.HTTP_204_NO_CONTENT,
               dependencies=[Depends(auth.implicit_scheme)])
def destroy(editor_id: int,
            user: Auth0User = Security(auth.get_user)):
    editor.destroy(editor_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put('/{editor_id}',
            status_code=status.HTTP_202_ACCEPTED,
            response_model=schemas.EditorSchemaShow,
            dependencies=[Depends(auth.implicit_scheme)])
def update(editor_id: int,
           request: schemas.EditorSchemaUpdate,
           user: Auth0User = Security(auth.get_user)):
    return editor.update(editor_id, request)


@router.get('/{editor_id}',
            status_code=200)
def show(editor_id: int):
    return editor.show(editor_id)


@router.get('/{editor_id}/versions',
            status_code=200)
def show(editor_id: int):
    return editor.show_changesets(editor_id)
