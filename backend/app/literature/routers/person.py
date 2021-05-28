from typing import List

from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi import Response
from fastapi import Security

from fastapi_auth0 import Auth0User
from literature.user import set_global_user_id

from literature.schemas import PersonSchemaShow
from literature.schemas import PersonSchemaCreate

from literature.crud import person_crud
from literature.routers.authentication import auth

router = APIRouter(
    prefix="/person",
    tags=['Person']
)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=PersonSchemaShow,
             dependencies=[Depends(auth.implicit_scheme)])
def create(request: PersonSchemaCreate,
           user: Auth0User = Security(auth.get_user)):
    set_global_user_id(user.id)
    return person_crud.create(request)


@router.delete('/{person_id}',
               status_code=status.HTTP_204_NO_CONTENT,
               dependencies=[Depends(auth.implicit_scheme)])
def destroy(person_id: int,
            user: Auth0User = Security(auth.get_user)):
    set_global_user_id(user.id)
    person_crud.destroy(person_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put('/{person_id}',
            status_code=status.HTTP_202_ACCEPTED,
            response_model=PersonSchemaShow,
            dependencies=[Depends(auth.implicit_scheme)])
def update(person_id: int,
           request: PersonSchemaCreate,
           user: Auth0User = Security(auth.get_user)):
    set_global_user_id(user.id)
    return person_crud.update(person_id, request)


@router.get('/{person_id}',
            status_code=200)
def show(person_id: int):
    return person.show(person_id)


@router.get('/{person_id}/versions',
            status_code=200)
def show(person_id: int):
    return person_crud.show_changesets(person_id)
