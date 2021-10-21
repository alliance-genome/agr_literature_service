from typing import List

from sqlalchemy.orm import Session

from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi import Response
from fastapi import Security

from fastapi_okta import OktaUser

from literature import database

from literature.user import set_global_user_id

from literature.schemas import EditorSchemaShow
from literature.schemas import EditorSchemaPost
from literature.schemas import ResponseMessageSchema

from literature.crud import editor_crud
from literature.routers.authentication import auth

router = APIRouter(
    prefix="/editor",
    tags=['Editor']
)

get_db = database.get_db

@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=str)
def create(request: EditorSchemaPost,
           user: OktaUser = Security(auth.get_user),
           db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    return editor_crud.create(db, request)


@router.delete('/{editor_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(editor_id: int,
            user: OktaUser= Security(auth.get_user),
            db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    editor_crud.destroy(db, editor_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{editor_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
async def patch(editor_id: int,
                request: EditorSchemaPost,
                user: OktaUser = Security(auth.get_user),
                db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    patch = request.dict(exclude_unset=True)

    return editor_crud.patch(db, editor_id, patch)


@router.get('/{editor_id}',
            status_code=200)
def show(editor_id: int,
         db: Session = Depends(get_db)):
    return editor_crud.show(db, editor_id)


@router.get('/{editor_id}/versions',
            status_code=200)
def show_versions(editor_id: int,
         db: Session = Depends(get_db)):
    return editor_crud.show_changesets(db, editor_id)
