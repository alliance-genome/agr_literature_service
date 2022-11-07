import logging

from fastapi import APIRouter, Depends, Security, status
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.deps import s3_auth
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas import ResponseMessageSchema
from agr_literature_service.api.schemas.referencefile_mod_schemas import ReferencefileModSchemaPost, \
    ReferencefileModSchemaShow, ReferencefileModSchemaUpdate
from agr_literature_service.api.user import set_global_user_from_okta
from agr_literature_service.api.crud import referencefile_mod_crud

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/reference/referencefile_mod",
    tags=['ReferencefileMod'])


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)
s3_session = Depends(s3_auth)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=str)
def create(request: ReferencefileModSchemaPost,
           user: OktaUser = db_user,
           db: Session = db_session):
    set_global_user_from_okta(db, user)
    return referencefile_mod_crud.create(db, request)


@router.get('/{referencefile_mod_id}',
            status_code=status.HTTP_200_OK,
            response_model=ReferencefileModSchemaShow)
def show(referencefile_mod_id: int,
         db: Session = db_session):
    return referencefile_mod_crud.show(db, referencefile_mod_id)


@router.patch('/{referencefile_mod_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
def patch(referencefile_mod_id: int,
          request: ReferencefileModSchemaUpdate,
          user: OktaUser = db_user,
          db: Session = db_session):
    set_global_user_from_okta(db, user)
    return referencefile_mod_crud.patch(db, referencefile_mod_id, request.dict(exclude_unset=True))


@router.delete('/{referencefile_mod_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(referencefile_mod_id: int,
            user: OktaUser = db_user,
            db: Session = db_session):
    set_global_user_from_okta(db, user)
    referencefile_mod_crud.destroy(db, referencefile_mod_id)
