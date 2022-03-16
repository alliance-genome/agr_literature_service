from fastapi import APIRouter, Depends, Response, Security, status
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from literature import database
from literature.crud import mod_crud
from literature.routers.authentication import auth
from literature.schemas import (ModSchemaCreate, ModSchemaShow,
                                ResponseMessageSchema)
from literature.user import set_global_user_id

router = APIRouter(
    prefix="/mod",
    tags=['Mod']
)


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=ModSchemaShow)
def create(request: ModSchemaCreate,
           user: OktaUser = db_user,
           db: Session = db_session):
    set_global_user_id(db, user.id)
    return mod_crud.create(db, request)


@router.delete('/{mod_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(mod_id: int,
            user: OktaUser = db_user,
            db: Session = db_session):
    set_global_user_id(db, user.id)
    mod_crud.destroy(db, mod_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{mod_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
async def patch(mod_id: int,
                request: ModSchemaCreate,
                user: OktaUser = db_user,
                db: Session = db_session):
    set_global_user_id(db, user.id)
    patch = request.dict(exclude_unset=True)

    return mod_crud.patch(db, mod_id, patch)


@router.get('/{mod_id}',
            status_code=200)
def show(mod_id: int,
         db: Session = db_session):
    return mod_crud.show(db, mod_id)


@router.get('/{mod_id}/versions',
            status_code=200)
def show_versions(mod_id: int,
                  db: Session = db_session):
    return mod_crud.show_changesets(db, mod_id)
