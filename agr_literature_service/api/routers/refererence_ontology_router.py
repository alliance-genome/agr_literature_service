from fastapi import APIRouter, Depends, Response, Security, status
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import reference_ontology_crud
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas import (ReferenceOntologySchemaShow,
                                                ReferenceOntologySchemaUpdate,
                                                ResponseMessageSchema)
from agr_literature_service.api.user import set_global_user_id

router = APIRouter(
    prefix="/reference_ontology",
    tags=['Reference Ontology']
)


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=str)
def create(request: ReferenceOntologySchemaPost,
           user: OktaUser = db_user,
           db: Session = db_session):
    set_global_user_id(db, user.id)
    return reference_ontology_crud.create(db, request)


@router.delete('/{reference_ontology_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(reference_ontology_id: int,
            user: OktaUser = db_user,
            db: Session = db_session):
    set_global_user_id(db, user.id)
    reference_ontology_crud.destroy(db, reference_ontology_id)

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{reference_ontology_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
async def patch(reference_ontology_id: int,
                request: ReferenceOntologySchemaUpdate,
                user: OktaUser = db_user,
                db: Session = db_session):
    set_global_user_id(db, user.id)
    patch = request.dict(exclude_unset=True)
    return reference_ontology_crud.patch(db, reference_ontology_id, patch)


@router.get('/{reference_ontology_id}',
            response_model=ReferenceOntologySchemaShow,
            status_code=200)
def show(reference_ontology_id: int,
         db: Session = db_session):
    return reference_ontology_crud.show(db, reference_ontology_id)


@router.get('/{reference_ontology_id}/versions',
            status_code=200)
def show_versions(reference_ontology_id: int,
                  db: Session = db_session):
    return reference_ontology_crud.show_changesets(db, reference_ontology_id)
