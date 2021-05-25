from typing import List

from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi import Response
from fastapi import Security

from fastapi_auth0 import Auth0User

from literature.schemas import EditorSchemaShow
from literature.schemas import EditorSchemaCreate
from literature.schemas import EditorSchemaUpdate

from literature.crud import editor_crud
from literature.routers.authentication import auth

router = APIRouter(
    prefix="/editor",
    tags=['Editor']
)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=EditorSchemaShow,
             dependencies=[Depends(auth.implicit_scheme)])
def create(request: EditorSchemaCreate,
           user: Auth0User = Security(auth.get_user)):
    return editor_crud.create(request)


@router.delete('/{editor_id}',
               status_code=status.HTTP_204_NO_CONTENT,
               dependencies=[Depends(auth.implicit_scheme)])
def destroy(editor_id: int,
            user: Auth0User = Security(auth.get_user)):
    editor_crud.destroy(editor_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put('/{editor_id}',
            status_code=status.HTTP_202_ACCEPTED,
            response_model=EditorSchemaShow,
            dependencies=[Depends(auth.implicit_scheme)])
def update(editor_id: int,
           request: EditorSchemaUpdate,
           user: Auth0User = Security(auth.get_user)):
    return editor_crud.update(editor_id, request)


@router.get('/{editor_id}',
            status_code=200)
def show(editor_id: int):
    return editor_crud.show(editor_id)


@router.get('/{editor_id}/versions',
            status_code=200)
def show(editor_id: int):
    return editor.show_changesets(editor_id)
