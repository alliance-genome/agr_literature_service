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

from literature.schemas import AuthorSchemaShow
from literature.schemas import AuthorSchemaCreate

from literature.crud import author_crud
from literature.routers.authentication import auth


router = APIRouter(
    prefix="/author",
    tags=['Author']
)

get_db = database.get_db


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=AuthorSchemaShow,
             dependencies=[Depends(auth.implicit_scheme)])
def create(request: AuthorSchemaCreate,
           user: Auth0User = Security(auth.get_user),
           db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    return author_crud.create(db, request)


@router.delete('/{author_id}',
               status_code=status.HTTP_204_NO_CONTENT,
               dependencies=[Depends(auth.implicit_scheme)])
def destroy(author_id: int,
            user: Auth0User = Security(auth.get_user),
            db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    author_crud.destroy(db, author_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put('/{author_id}',
            status_code=status.HTTP_202_ACCEPTED,
            response_model=AuthorSchemaShow,
            dependencies=[Depends(auth.implicit_scheme)])
def update(author_id: int,
           request: AuthorSchemaCreate,
           user: Auth0User = Security(auth.get_user),
           db: Session = Depends(get_db)):
    set_global_user_id(db, user.id)
    return author_crud.update(db, author_id, request)


@router.get('/{author_id}',
            status_code=200)
def show(author_id: int,
         db: Session = Depends(get_db)):
    return author.show(db, author_id)


@router.get('/{author_id}/versions',
            status_code=200)
def show(author_id: int,
         db: Session = Depends(get_db)):
    return author_crud.show_changesets(db, author_id)
