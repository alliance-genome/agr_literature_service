from fastapi import APIRouter, Depends, HTTPException, Response, Security, status
from typing import Dict, Any, List, Optional

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import resource_crud
from agr_literature_service.api.schemas import (ResourceSchemaPost,
                                                ResourceSchemaShow, ResourceSchemaUpdate,
                                                ResponseMessageSchema)
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.resource_lookup import (
    lookup_resource, create_resource_from_external_curie)
from agr_literature_service.lit_processing.utils.generic_utils import split_identifier
from agr_literature_service.api.schemas.external_lookup_schemas import ResourceExternalLookupResponse
from agr_literature_service.api.schemas.resource_schemas import ResourceSchemaAddCurie

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
           user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
           db: Session = db_session):
    set_global_user_from_cognito(db, user)
    return resource_crud.create(db, request)


@router.get('/external_lookup/{external_curie}',
            status_code=200,
            response_model=ResourceExternalLookupResponse)
def external_lookup(external_curie: str,
                    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                    db: Session = db_session):
    set_global_user_from_cognito(db, user)
    prefix, identifier, _ = split_identifier(external_curie, ignore_error=True)
    if not prefix:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="You must enter an NLM, ISSN, or ISBN")
    if prefix.lower() not in ('issn', 'nlm', 'nlmid', 'isbn'):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="You must enter an NLM, ISSN, or ISBN")
    return lookup_resource(identifier, prefix, db)


@router.post('/add/',
             status_code=status.HTTP_201_CREATED,
             response_model=List[str])
def add(request: ResourceSchemaAddCurie,
        user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
        db: Session = db_session):
    set_global_user_from_cognito(db, user)
    prefix, identifier, _ = split_identifier(request.curie, ignore_error=True)
    if not prefix:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="You must enter an NLM, ISSN, or ISBN")
    prefix_lower = prefix.lower()
    if prefix_lower == 'isbn':
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="ISBN not supported yet")
    if prefix_lower not in ('issn', 'nlm', 'nlmid'):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="You must enter an NLM, ISSN, or ISBN")
    field = 'nlmid' if prefix_lower in ('nlm', 'nlmid') else 'issn'
    result = create_resource_from_external_curie(identifier, field, db)
    if not result.get('resource_curies'):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Resource not found in NLM catalog for {request.curie}")
    return result['resource_curies']


@router.delete('/{curie}',

               status_code=status.HTTP_204_NO_CONTENT)
def destroy(curie: str,
            user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
            db: Session = db_session):
    set_global_user_from_cognito(db, user)
    resource_crud.destroy(db, curie)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{curie}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
def patch(curie: str,
          request: ResourceSchemaUpdate,
          user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
          db: Session = db_session):
    set_global_user_from_cognito(db, user)
    patch = request.model_dump(exclude_unset=True)

    return resource_crud.patch(db, curie, patch)


@router.get('/show_all',
            status_code=200,
            response_model=List[ResourceSchemaShow],
            description="Returns all resources with full data. "
                        "WARNING: Response is ~46MB. Swagger UI will fail to render it. "
                        "Use curl or programmatic access instead.")
def show_all(user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
             db: Session = db_session):
    set_global_user_from_cognito(db, user)
    return resource_crud.show_all(db)


@router.get('/{curie}',
            status_code=200,
            response_model=ResourceSchemaShow)
def show(curie: str,
         user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
         db: Session = db_session):
    set_global_user_from_cognito(db, user)
    return resource_crud.show(db, curie)


@router.get('/{curie}/versions',
            status_code=200)
def show_versions(curie: str,
                  user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                  db: Session = db_session):
    set_global_user_from_cognito(db, user)
    return resource_crud.show_changesets(db, curie)
