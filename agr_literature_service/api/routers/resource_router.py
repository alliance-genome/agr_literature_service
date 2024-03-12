from fastapi import APIRouter, Depends, Response, Security, status
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import resource_crud
from agr_literature_service.api.crud.utils import patterns_check
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas import (ResourceSchemaPost,
                                                ResourceSchemaShow, ResourceSchemaUpdate,
                                                ResponseMessageSchema)
from agr_literature_service.api.user import set_global_user_from_okta

router = APIRouter(
    prefix="/resource",
    tags=['Resource']
)


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.post('/',
             status_code=status.HTTP_201_CREATED,

             response_model=str)
def create(request: ResourceSchemaPost,
           user: OktaUser = db_user,
           db: Session = db_session):
    set_global_user_from_okta(db, user)
    return resource_crud.create(db, request)


@router.delete('/{curie}',

               status_code=status.HTTP_204_NO_CONTENT)
def destroy(curie: str,
            user: OktaUser = db_user,
            db: Session = db_session):
    set_global_user_from_okta(db, user)
    resource_crud.destroy(db, curie)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{curie}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
def patch(curie: str,
          request: ResourceSchemaUpdate,
          user: OktaUser = db_user,
          db: Session = db_session):
    set_global_user_from_okta(db, user)
    patch = request.dict(exclude_unset=True)

    return resource_crud.patch(db, curie, patch)


@router.get('/{curie}',
            status_code=200,
            response_model=ResourceSchemaShow)
def show(curie: str,
         db: Session = db_session):
    return resource_crud.show(db, curie)


@router.get('/{curie}/versions',
            status_code=200)
def show_versions(curie: str,
                  db: Session = db_session):
    return resource_crud.show_changesets(db, curie)


@router.get('/get/patterns/prefixed',
            status_code=200,
            )
def show_resource_patterns_prefixed():
    return patterns_check.get_patterns(with_prefix=True)['resource']


@router.get('/get/patterns',
            status_code=200,
            )
def show_resource_patterns():
    return patterns_check.get_patterns()['resource']


@router.get('/check/{species}/{curie}',
            status_code=200,
            )
def check_pattern(species: str, curie: str):
    ret = patterns_check.check_pattern('resource', species, curie)
    if ret is None:
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    return ret
