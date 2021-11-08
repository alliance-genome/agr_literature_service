from sqlalchemy.orm import Session

from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi import Response
from fastapi import Security

from fastapi_okta import OktaUser

from literature import database

from literature.user import set_global_user_id

from literature.schemas import NoteSchemaPost
from literature.schemas import NoteSchemaUpdate
from literature.schemas import ResponseMessageSchema

from literature.crud import note_crud
from literature.routers.authentication import auth

router = APIRouter(
    prefix="/note",
    tags=['Note']
)

get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=int
             )
def create(request: NoteSchemaPost,
           user: OktaUser = db_user,
           db: Session = db_session):
    set_global_user_id(db, user.id)
    return note_crud.create(db, request)


@router.delete('/{note_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(note_id: int,
            user: OktaUser = db_user,
            db: Session = db_session):
    set_global_user_id(db, user.id)
    note_crud.destroy(db, note_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{note_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
async def patch(note_id: int,
                request: NoteSchemaUpdate,
                user: OktaUser = db_user,
                db: Session = db_session):
    set_global_user_id(db, user.id)
    patch = request.dict(exclude_unset=True)

    return note_crud.patch(db, note_id, patch)


@router.get('/{note_id}',
            status_code=200)
def show(note_id: int,
         db: Session = db_session):
    return note_crud.show(db, note_id)


@router.get('/{note_id}/versions',
            status_code=200)
def show_versions(note_id: int,
                  db: Session = db_session):
    return note_crud.show_changesets(db, note_id)
