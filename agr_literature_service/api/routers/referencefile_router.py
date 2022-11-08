import logging

from fastapi import APIRouter, Depends, Security, status
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.deps import s3_auth
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas import ResponseMessageSchema
from agr_literature_service.api.schemas.referencefile_schemas import ReferencefileSchemaPost, ReferencefileSchemaShow, \
    ReferencefileSchemaUpdate
from agr_literature_service.api.user import set_global_user_from_okta
from agr_literature_service.api.crud import referencefile_crud

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/reference/referencefile",
    tags=['Reference'])


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)
s3_session = Depends(s3_auth)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=str)
def create(request: ReferencefileSchemaPost,
           user: OktaUser = db_user,
           db: Session = db_session):
    set_global_user_from_okta(db, user)
    return referencefile_crud.create(db, request)


@router.get('/{md5sum_or_referencefile_id}',
            status_code=status.HTTP_200_OK,
            response_model=ReferencefileSchemaShow)
def show(md5sum_or_referencefile_id: str,
         db: Session = db_session):
    return referencefile_crud.show(db, md5sum_or_referencefile_id)


@router.patch('/{md5sum_or_referencefile_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
def patch(md5sum_or_referencefile_id: str,
          request: ReferencefileSchemaUpdate,
          user: OktaUser = db_user,
          db: Session = db_session):
    set_global_user_from_okta(db, user)
    return referencefile_crud.patch(db, md5sum_or_referencefile_id, request.dict(exclude_unset=True))


@router.delete('/{md5sum_or_referencefile_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(md5sum_or_referencefile_id: str,
            user: OktaUser = db_user,
            db: Session = db_session):
    set_global_user_from_okta(db, user)
    referencefile_crud.destroy(db, md5sum_or_referencefile_id)
