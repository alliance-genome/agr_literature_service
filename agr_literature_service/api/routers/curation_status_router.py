from typing import List, Dict, Any

from fastapi import APIRouter, Depends, Security, Response

from sqlalchemy.orm import Session
from starlette import status

from agr_literature_service.api import database
from agr_literature_service.api.crud import curation_status_crud
from agr_literature_service.api.schemas.curation_status_schemas import CurationStatusSchemaPost, \
    CurationStatusSchemaUpdate, AggregatedCurationStatusAndTETInfoSchema
from agr_literature_service.api.user import set_global_user_from_cognito

from agr_cognito_auth import get_cognito_user_swagger

router = APIRouter(
    prefix='/curation_status',
    tags=['Curation_status']
)

get_db = database.get_db
db_session: Session = Depends(get_db)


@router.get("/aggregated_curation_status_and_tet_info/{reference_curie}/{mod_abbreviation}",
            status_code=200,
            response_model=List[AggregatedCurationStatusAndTETInfoSchema])
def show_aggregated_curation_status_and_tet_info(reference_curie: str,
                                                 mod_abbreviation: str,
                                                 db: Session = db_session):
    return curation_status_crud.get_aggregated_curation_status_and_tet_info(db, reference_curie, mod_abbreviation)


@router.get("/{curation_status_id}",
            status_code=200)
def show(curation_status_id: int,
         db: Session = db_session):
    return curation_status_crud.show(db, curation_status_id)


@router.post("/",
             status_code=status.HTTP_201_CREATED)
def create_curation_status(request: CurationStatusSchemaPost, user: Dict[str, Any] = Security(get_cognito_user_swagger), db: Session = db_session):
    set_global_user_from_cognito(db, user)
    return curation_status_crud.create(db, curation_status=request)


@router.delete('/{curation_status_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(curation_status_id: int,
            user: Dict[str, Any] = Security(get_cognito_user_swagger),
            db: Session = db_session):
    set_global_user_from_cognito(db, user)
    curation_status_crud.destroy(db, curation_status_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{curation_status_id}',
              status_code=status.HTTP_202_ACCEPTED)
def patch(curation_status_id: int,
          request: CurationStatusSchemaUpdate,
          user: Dict[str, Any] = Security(get_cognito_user_swagger),
          db: Session = db_session):

    set_global_user_from_cognito(db, user)
    return curation_status_crud.patch(db, curation_status_id, request)
