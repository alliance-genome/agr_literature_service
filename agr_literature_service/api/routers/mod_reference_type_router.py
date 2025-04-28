from typing import List

from fastapi import APIRouter, Depends, Response, Security, status
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import mod_reference_type_crud
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas import (ModReferenceTypeSchemaPost,
                                                ModReferenceTypeSchemaShow,
                                                ModReferenceTypeSchemaUpdate,
                                                ResponseMessageSchema)
from agr_literature_service.api.user import set_global_user_from_okta

router = APIRouter(
    prefix="/reference/mod_reference_type",
    tags=['Reference']
)


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=int)
def create(request: ModReferenceTypeSchemaPost,
           user: OktaUser = db_user,
           db: Session = db_session):
    set_global_user_from_okta(db, user)
    return mod_reference_type_crud.create(db, request)


@router.delete('/{mod_reference_type_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(mod_reference_type_id: int,
            user: OktaUser = db_user,
            db: Session = db_session):
    set_global_user_from_okta(db, user)
    mod_reference_type_crud.destroy(db, mod_reference_type_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{mod_reference_type_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
async def patch(mod_reference_type_id: int,
                request: ModReferenceTypeSchemaUpdate,
                user: OktaUser = db_user,
                db: Session = db_session):
    set_global_user_from_okta(db, user)
    patch = request.model_dump(exclude_unset=True)
    return mod_reference_type_crud.patch(db, mod_reference_type_id, patch)


@router.get('/{mod_reference_type_id}',
            response_model=ModReferenceTypeSchemaShow,
            status_code=200)
def show(mod_reference_type_id: int,
         db: Session = db_session):
    return mod_reference_type_crud.show(db, mod_reference_type_id)


@router.get('/{mod_reference_type_id}/versions',
            status_code=200)
def show_versions(mod_reference_type_id: int,
                  db: Session = db_session):
    return mod_reference_type_crud.show_changesets(db, mod_reference_type_id)


@router.get('/by_mod/{abbreviation}',
            response_model=List[str],
            status_code=200)
def show_by_mod(abbreviation: str,
                db: Session = db_session):
    return mod_reference_type_crud.show_by_mod(db=db, mod_abbreviation=abbreviation)
