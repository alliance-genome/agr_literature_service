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

from literature.schemas import NoteSchemaShow
from literature.schemas import NoteSchemaPost
from literature.schemas import NoteSchemaUpdate

from literature.crud import note_crud
from literature.routers.authentication import auth


router = APIRouter(
    prefix="/note",
    tags=['Note']
)

get_db = database.get_db


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=int,
             dependencies=[Depends(auth.implicit_scheme)])
def create(request: NoteSchemaPost,
           user: Auth0User = Security(auth.get_user),
           db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    return note_crud.create(db, request)


@router.delete('/{note_id}',
               status_code=status.HTTP_204_NO_CONTENT,
               dependencies=[Depends(auth.implicit_scheme)])
def destroy(note_id: int,
            user: Auth0User = Security(auth.get_user),
            db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    note_crud.destroy(db, note_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{note_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=str,
              dependencies=[Depends(auth.implicit_scheme)])
async def patch(note_id: int,
                request: NoteSchemaUpdate,
                user: Auth0User = Security(auth.get_user),
                db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    patch = request.dict(exclude_unset=True)

    return note_crud.patch(db, note_id, patch)


@router.get('/{note_id}',
            status_code=200)
def show(note_id: int,
         db: Session = Depends(get_db)):
    return note_crud.show(db, note_id)


@router.get('/{note_id}/versions',
            status_code=200)
def show(note_id: int,
         db: Session = Depends(get_db)):
    return note_crud.show_changesets(db, note_id)
