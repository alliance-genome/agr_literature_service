from typing import List, Dict, Any, Optional

from fastapi import APIRouter, Depends, Security, Response

from sqlalchemy.orm import Session
from starlette import status

from agr_literature_service.api import database
from agr_literature_service.api.crud import curation_status_crud, curation_status_source_crud
from agr_literature_service.api.schemas.curation_status_schemas import CurationStatusSchemaPost, \
    CurationStatusSchemaShow, CurationStatusSchemaUpdate, AggregatedCurationStatusAndTETInfoSchema
from agr_literature_service.api.schemas.curation_status_source_schemas import \
    CurationStatusSourceAssociationSchemaPost, CurationStatusSourceAssociationSchemaShow, \
    CurationStatusSourceAssociationSchemaUpdate
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user
from agr_literature_service.api.util.resource_urls import curation_status_url

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
                                                 db: Session = db_session,
                                                 user: Optional[Dict[str, Any]] = Security(get_authenticated_user)):
    return curation_status_crud.get_aggregated_curation_status_and_tet_info(db, reference_curie, mod_abbreviation)


@router.get("/{curation_status_id}",
            status_code=200)
def show(curation_status_id: int,
         db: Session = db_session,
         user: Optional[Dict[str, Any]] = Security(get_authenticated_user)):
    return curation_status_crud.show(db, curation_status_id)


@router.post("/",
             status_code=status.HTTP_201_CREATED,
             response_model=CurationStatusSchemaShow)
def create_curation_status(request: CurationStatusSchemaPost,
                           response: Response,
                           user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                           db: Session = db_session):
    set_global_user_from_cognito(db, user)
    obj = curation_status_crud.create(db, curation_status=request)
    response.headers["Location"] = curation_status_url(obj.curation_status_id)
    return obj


@router.delete('/{curation_status_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(curation_status_id: int,
            user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
            db: Session = db_session):
    set_global_user_from_cognito(db, user)
    curation_status_crud.destroy(db, curation_status_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{curation_status_id}',
              status_code=status.HTTP_200_OK,
              response_model=CurationStatusSchemaShow)
def patch(curation_status_id: int,
          request: CurationStatusSchemaUpdate,
          user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
          db: Session = db_session):

    set_global_user_from_cognito(db, user)
    return curation_status_crud.patch(db, curation_status_id, request)


# --- SCRUM-6518 source attribution -------------------------------------------
# These paths all have two segments or a literal first segment, so none of them
# is shadowed by the single-segment /{curation_status_id} routes above.


@router.post('/add_source_association',
             status_code=status.HTTP_201_CREATED,
             response_model=CurationStatusSourceAssociationSchemaShow)
def add_source_association(request: CurationStatusSourceAssociationSchemaPost,
                           user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                           db: Session = db_session):
    """Record what one source reports for an EXISTING curation_status row.

    404s when no curation_status row exists: it never auto-creates the anchor.
    The base row's own values are never modified.
    """
    set_global_user_from_cognito(db, user)
    values = request.model_dump(exclude_unset=True,
                                exclude={"curation_status_id", "tag_source_id"})
    association = curation_status_source_crud.upsert_association(
        db, request.curation_status_id, request.tag_source_id, values)
    return curation_status_source_crud.show_association(
        db, association.curation_status_source_association_id)


@router.get('/{curation_status_id}/source_associations',
            status_code=200,
            response_model=List[CurationStatusSourceAssociationSchemaShow])
def show_source_associations(curation_status_id: int,
                             db: Session = db_session,
                             user: Optional[Dict[str, Any]] = Security(get_authenticated_user)):
    """Every source's report for this row. aggregated_curation_status_and_tet_info
    is deliberately NOT extended with these; SCRUM-6517 consumes this endpoint."""
    return curation_status_source_crud.show_associations(db, curation_status_id)


@router.patch('/source_association/{curation_status_source_association_id}',
              status_code=status.HTTP_200_OK,
              response_model=CurationStatusSourceAssociationSchemaShow)
def patch_source_association(curation_status_source_association_id: int,
                             request: CurationStatusSourceAssociationSchemaUpdate,
                             user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                             db: Session = db_session):
    set_global_user_from_cognito(db, user)
    curation_status_source_crud.patch_association(
        db, curation_status_source_association_id, request)
    return curation_status_source_crud.show_association(
        db, curation_status_source_association_id)


@router.delete('/source_association/{curation_status_source_association_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def delete_source_association(curation_status_source_association_id: int,
                              user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                              db: Session = db_session):
    set_global_user_from_cognito(db, user)
    curation_status_source_crud.destroy_association(db, curation_status_source_association_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.delete('/source/{tag_source_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def delete_source_associations(tag_source_id: int,
                               user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                               db: Session = db_session):
    """Wipe one source's attributions ahead of a reload (e.g. a GO repopulate).
    The curation_status rows themselves are untouched."""
    set_global_user_from_cognito(db, user)
    curation_status_source_crud.destroy_associations_for_source(db, tag_source_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
