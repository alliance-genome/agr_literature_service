from fastapi import APIRouter, Depends, Response, Security, status
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import author_crud
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas import (AuthorSchemaCreate, AuthorSchemaShow,
                                                ResponseMessageSchema)
from agr_literature_service.api.user import set_global_user_from_okta

router = APIRouter(
    prefix="/author",
    tags=['Author']
)

get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.post('/',
             status_code=status.HTTP_201_CREATED)
def create(request: AuthorSchemaCreate,
           user: OktaUser = db_user,
           db: Session = db_session):
    set_global_user_from_okta(db, user)
    return author_crud.create(db, request)


@router.delete('/{author_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(author_id: int,
            user: OktaUser = db_user,
            db: Session = db_session):
    set_global_user_from_okta(db, user)
    author_crud.destroy(db, author_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{author_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
async def patch(author_id: int,
                request: AuthorSchemaCreate,
                user: OktaUser = db_user,
                db: Session = db_session):
    set_global_user_from_okta(db, user)
    patch = request.dict(exclude_unset=True)
    return author_crud.patch(db, author_id, patch)


@router.get('/{author_id}',
            response_model=AuthorSchemaShow,
            status_code=200)
def show(author_id: int,
         db: Session = db_session):
    return author_crud.show(db, author_id)


@router.get('/{author_id}/versions',  # type: ignore
            status_code=200)
def show_versions(author_id: int,
                  db: Session = db_session):
    return author_crud.show_changesets(db, author_id)
