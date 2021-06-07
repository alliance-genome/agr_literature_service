from typing import List

from sqlalchemy.orm import Session

from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi import Response
from fastapi import Security

from fastapi_auth0 import Auth0User

from literature import database

from literature.user import set_global_user_id

from literature.schemas import EditorSchemaShow
from literature.schemas import EditorSchemaPost

from literature.crud import editor_crud
from literature.routers.authentication import auth

router = APIRouter(
    prefix="/editor",
    tags=['Editor']
)

get_db = database.get_db

@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=EditorSchemaShow,
             dependencies=[Depends(auth.implicit_scheme)])
def create(request: EditorSchemaPost,
           user: Auth0User = Security(auth.get_user),
           db: Session = Depends(get_db)):
    set_global_user_id(user.id)
    return editor_crud.create(db, request)


@router.delete('/{editor_id}',
               status_code=status.HTTP_204_NO_CONTENT,
               dependencies=[Depends(auth.implicit_scheme)])
def destroy(editor_id: int,
            user: Auth0User = Security(auth.get_user),
            db: Session = Depends(get_db)):
    set_global_user_id(user.id)
    editor_crud.destroy(db, editor_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put('/{editor_id}',
            status_code=status.HTTP_202_ACCEPTED,
            response_model=EditorSchemaShow,
            dependencies=[Depends(auth.implicit_scheme)])
def update(editor_id: int,
           request: EditorSchemaPost,
           user: Auth0User = Security(auth.get_user),
           db: Session = Depends(get_db)):
    set_global_user_id(user.id)
    return editor_crud.update(db, editor_id, request)


@router.get('/{editor_id}',
            status_code=200)
def show(editor_id: int,
         db: Session = Depends(get_db)):
    return editor_crud.show(db, editor_id)


@router.get('/{editor_id}/versions',
            status_code=200)
def show(editor_id: int,
         db: Session = Depends(get_db)):
    return editor.show_changesets(db, editor_id)
