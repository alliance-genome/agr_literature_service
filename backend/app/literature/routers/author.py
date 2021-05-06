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
from literature.crud import author
from literature.routers.authentication import auth

router = APIRouter(
    prefix="/author",
    tags=['Author']
)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=schemas.AuthorSchemaShow,
             dependencies=[Depends(auth.implicit_scheme)])
def create(request: schemas.AuthorSchemaCreate,
           user: Auth0User = Security(auth.get_user)):
    return author.create(request)


@router.delete('/{author_id}',
               status_code=status.HTTP_204_NO_CONTENT,
               dependencies=[Depends(auth.implicit_scheme)])
def destroy(author_id: int,
            user: Auth0User = Security(auth.get_user)):
    author.destroy(author_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put('/{author_id}',
            status_code=status.HTTP_202_ACCEPTED,
            response_model=schemas.AuthorSchemaShow,
            dependencies=[Depends(auth.implicit_scheme)])
def update(author_id: int,
           request: schemas.AuthorSchemaUpdate,
           user: Auth0User = Security(auth.get_user)):
    return author.update(author_id, request)


@router.get('/{author_id}',
            status_code=200)
def show(author_id: int):
    return author.show(author_id)


@router.get('/{author_id}/versions',
            status_code=200)
def show(author_id: int):
    return author.show_changesets(author_id)
