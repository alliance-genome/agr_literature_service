"""
tag_source_router.py
====================

The six source endpoints, moved off /topic_entity_tag/source by SCRUM-6518 now
that the source table is shared with curation status. No aliases and no
duplicated id field: an alias would preserve the URL but not the payload, so a
caller reading the id breaks either way.

Route order matters and mirrors the old router: /all is declared before
/{tag_source_id}, or "all" would be matched as an int path parameter.
"""
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Response, Security
from sqlalchemy.orm import Session
from starlette import status

from agr_literature_service.api import database
from agr_literature_service.api.auth import get_authenticated_user
from agr_literature_service.api.crud import tag_source_crud
from agr_literature_service.api.schemas.tag_source_schemas import TagSourceSchemaCreate, \
    TagSourceSchemaShow, TagSourceSchemaUpdate
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.util.resource_urls import tag_source_url

router = APIRouter(
    prefix='/tag_source',
    tags=['Tag_source']
)

get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post('',
             status_code=status.HTTP_201_CREATED,
             response_model=TagSourceSchemaShow)
def create_source(request: TagSourceSchemaCreate,
                  response: Response,
                  user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                  db: Session = db_session):
    set_global_user_from_cognito(db, user)
    new_source_id = tag_source_crud.create_source(db, request)
    response.headers["Location"] = tag_source_url(new_source_id)
    return tag_source_crud.show_source(db, new_source_id)


@router.delete('/{tag_source_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def delete_source(tag_source_id: int,
                  user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                  db: Session = db_session):
    set_global_user_from_cognito(db, user)
    tag_source_crud.destroy_source(db, tag_source_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{tag_source_id}',
              status_code=status.HTTP_200_OK,
              response_model=TagSourceSchemaShow)
def patch_source(tag_source_id: int,
                 request: TagSourceSchemaUpdate,
                 user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                 db: Session = db_session):
    set_global_user_from_cognito(db, user)
    tag_source_crud.patch_source(db, tag_source_id, request)
    return tag_source_crud.show_source(db, tag_source_id)


@router.get('/all',
            status_code=200)
def show_all_source(user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                    db: Session = db_session):
    return tag_source_crud.show_all_source(db)


@router.get('/{tag_source_id}',
            response_model=TagSourceSchemaShow,
            status_code=200)
def show_source(tag_source_id: int,
                user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                db: Session = db_session):
    return tag_source_crud.show_source(db, tag_source_id)


@router.get('/{source_evidence_assertion}/{source_method}/{data_provider}/{secondary_data_provider_abbreviation}',
            response_model=TagSourceSchemaShow,
            status_code=200)
def show_source_by_name(source_evidence_assertion: str,
                        source_method: str,
                        data_provider: str,
                        secondary_data_provider_abbreviation: str,
                        user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                        db: Session = db_session):
    return tag_source_crud.show_source_by_name(db, source_evidence_assertion, source_method,
                                               data_provider, secondary_data_provider_abbreviation)
