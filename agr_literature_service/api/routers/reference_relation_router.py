from fastapi import APIRouter, Depends, Response, Security, status
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import reference_relation_crud
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas import (ReferenceRelationSchemaPatch,
                                                ReferenceRelationSchemaPost,
                                                ReferenceRelationSchemaShow,
                                                ResponseMessageSchema)
from agr_literature_service.api.user import set_global_user_from_okta

router = APIRouter(
    prefix="/reference_relation",
    tags=['Reference Relation']
)


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=str)
def create(request: ReferenceRelationSchemaPost,
           user: OktaUser = db_user,
           db: Session = db_session):
    set_global_user_from_okta(db, user)
    return reference_relation_crud.create(db, request)


@router.delete('/{reference_relation_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(reference_relation_id: int,
            user: OktaUser = db_user,
            db: Session = db_session):
    set_global_user_from_okta(db, user)
    reference_relation_crud.destroy(db, reference_relation_id)

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{reference_relation_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
async def patch(reference_relation_id: int,
                request: ReferenceRelationSchemaPatch,
                user: OktaUser = db_user,
                db: Session = db_session):
    set_global_user_from_okta(db, user)
    patch = request.dict(exclude_unset=True)
    return reference_relation_crud.patch(db, reference_relation_id, patch)


@router.get('/{reference_relation_id}',
            response_model=ReferenceRelationSchemaShow,
            status_code=200)
def show(reference_relation_id: int,
         db: Session = db_session):
    return reference_relation_crud.show(db, reference_relation_id)


@router.get('/{reference_relation_id}/versions',
            status_code=200)
def show_versions(reference_relation_id: int,
                  db: Session = db_session):
    return reference_relation_crud.show_changesets(db, reference_relation_id)
