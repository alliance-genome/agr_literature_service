from typing import List

from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi import Response
from fastapi import Security

from fastapi_auth0 import Auth0User

from literature.schemas import AuthorSchemaShow
from literature.schemas import AuthorSchemaUpdate
from literature.schemas import AuthorSchemaCreate

from literature.crud import author_crud
from literature.routers.authentication import auth

router = APIRouter(
    prefix="/author",
    tags=['Author']
)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=AuthorSchemaShow,
             dependencies=[Depends(auth.implicit_scheme)])
def create(request: AuthorSchemaCreate,
           user: Auth0User = Security(auth.get_user)):
    return author_crud.create(request)


@router.delete('/{author_id}',
               status_code=status.HTTP_204_NO_CONTENT,
               dependencies=[Depends(auth.implicit_scheme)])
def destroy(author_id: int,
            user: Auth0User = Security(auth.get_user)):
    author_crud.destroy(author_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put('/{author_id}',
            status_code=status.HTTP_202_ACCEPTED,
            response_model=AuthorSchemaShow,
            dependencies=[Depends(auth.implicit_scheme)])
def update(author_id: int,
           request: AuthorSchemaUpdate,
           user: Auth0User = Security(auth.get_user)):
    return author_crud.update(author_id, request)


@router.get('/{author_id}',
            status_code=200)
def show(author_id: int):
    return author.show(author_id)


@router.get('/{author_id}/versions',
            status_code=200)
def show(author_id: int):
    return author_crud.show_changesets(author_id)
