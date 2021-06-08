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

from literature.schemas import PersonSchemaShow
from literature.schemas import PersonSchemaCreate

from literature.crud import person_crud
from literature.routers.authentication import auth


router = APIRouter(
    prefix="/person",
    tags=['Person']
)


get_db = database.get_db


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=PersonSchemaShow,
             dependencies=[Depends(auth.implicit_scheme)])
def create(request: PersonSchemaCreate,
           user: Auth0User = Security(auth.get_user),
           db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    return person_crud.create(db, request)


@router.delete('/{person_id}',
               status_code=status.HTTP_204_NO_CONTENT,
               dependencies=[Depends(auth.implicit_scheme)])
def destroy(person_id: int,
            user: Auth0User = Security(auth.get_user),
            db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    person_crud.destroy(db, person_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put('/{person_id}',
            status_code=status.HTTP_202_ACCEPTED,
            response_model=PersonSchemaShow,
            dependencies=[Depends(auth.implicit_scheme)],)
def update(person_id: int,
           request: PersonSchemaCreate,
           user: Auth0User = Security(auth.get_user),
           db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    return person_crud.update(db, person_id, request)


@router.get('/{person_id}',
            status_code=200)
def show(person_id: int,
         db: Session = Depends(get_db)):
    return person.show(db, person_id)


@router.get('/{person_id}/versions',
            status_code=200)
def show(person_id: int,
         db: Session = Depends(get_db)):
    return person_crud.show_changesets(db, person_id)
