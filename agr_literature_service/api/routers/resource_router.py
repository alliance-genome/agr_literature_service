from fastapi import APIRouter, Depends, Response, Security, status
from typing import Dict, Any

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import resource_crud
from agr_literature_service.api.schemas import (ResourceSchemaPost,
                                                ResourceSchemaShow, ResourceSchemaUpdate,
                                                ResponseMessageSchema)
from agr_literature_service.api.user import set_global_user_from_cognito

from agr_cognito_auth import get_cognito_user_swagger

router = APIRouter(
    prefix="/resource",
    tags=['Resource']
)


get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post('/',
             status_code=status.HTTP_201_CREATED,

             response_model=str)
def create(request: ResourceSchemaPost,
           user: Dict[str, Any] = Security(get_cognito_user_swagger),
           db: Session = db_session):
    set_global_user_from_cognito(db, user)
    return resource_crud.create(db, request)


@router.delete('/{curie}',

               status_code=status.HTTP_204_NO_CONTENT)
def destroy(curie: str,
            user: Dict[str, Any] = Security(get_cognito_user_swagger),
            db: Session = db_session):
    set_global_user_from_cognito(db, user)
    resource_crud.destroy(db, curie)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{curie}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
def patch(curie: str,
          request: ResourceSchemaUpdate,
          user: Dict[str, Any] = Security(get_cognito_user_swagger),
          db: Session = db_session):
    set_global_user_from_cognito(db, user)
    patch = request.model_dump(exclude_unset=True)

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
