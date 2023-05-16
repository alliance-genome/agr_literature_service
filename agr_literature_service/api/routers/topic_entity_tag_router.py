from typing import List, Dict, Union

from fastapi import APIRouter, Depends, Response, Security, status, HTTPException
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import topic_entity_tag_crud
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas import (TopicEntityTagSchemaShow,
                                                TopicEntityTagSchemaUpdate,
                                                TopicEntityTagSchemaPost,
                                                ResponseMessageSchema)
from agr_literature_service.api.schemas.topic_entity_tag_schemas import TopicEntityTagSchemaRelated, \
    TopicEntityTagSourceSchemaPost, TopicEntityTagSourceSchemaUpdate
from agr_literature_service.api.user import set_global_user_from_okta

router = APIRouter(
    prefix="/topic_entity_tag",
    tags=['Topic Entity Tag']
)


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=str)
def create(request: TopicEntityTagSchemaPost,
           user: OktaUser = db_user,
           db: Session = db_session):
    set_global_user_from_okta(db, user)
    return topic_entity_tag_crud.create_tag_with_source(db, request)


@router.get('/{topic_entity_tag_id}',
            response_model=TopicEntityTagSchemaShow,
            status_code=200)
def show(topic_entity_tag_id: int,
         db: Session = db_session):
    return topic_entity_tag_crud.show(db, topic_entity_tag_id)


@router.post('/add_source',
             status_code=status.HTTP_201_CREATED,
             response_model=str)
def add_source(topic_entity_tag_id,
               request: TopicEntityTagSourceSchemaPost,
               user: OktaUser = db_user,
               db: Session = db_session):
    set_global_user_from_okta(db, user)
    return topic_entity_tag_crud.add_source_to_tag(db, topic_entity_tag_id, request)


@router.delete('/delete_souce/{topic_entity_tag_source_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def delete_source(topic_entity_tag_source_id,
                  user: OktaUser = db_user,
                  db: Session = db_session):
    set_global_user_from_okta(db, user)
    topic_entity_tag_crud.destroy_source(db, topic_entity_tag_source_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/source/{topic_entity_tag_source_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=str)
def patch_source(topic_entity_tag_source_id,
                 request: TopicEntityTagSourceSchemaUpdate,
                 user: OktaUser = db_user,
                 db: Session = db_session):
    set_global_user_from_okta(db, user)
    return topic_entity_tag_crud.patch_source(db, topic_entity_tag_source_id, request)


@router.get('/by_reference/{curie_or_reference_id}',
            status_code=200)
def show_all_reference_tags(curie_or_reference_id: str, page: int = 1, page_size: int = None, count_only: bool = False,
                            sort_by: str = None, desc_sort: bool = False,
                            db: Session = db_session) -> Union[List[TopicEntityTagSchemaRelated], int]:
    return topic_entity_tag_crud.show_all_reference_tags(db, curie_or_reference_id, page, page_size, count_only,
                                                         sort_by, desc_sort)


@router.get('/map_entity_curie_to_name/',
            response_model=Dict[str, str],
            status_code=200)
def get_map_entity_curie_to_name(curie_or_reference_id: str,
                                 token: str = None,
                                 user: OktaUser = db_user,
                                 db: Session = db_session):
    set_global_user_from_okta(db, user)
    if token is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="no token provided")
    return topic_entity_tag_crud.get_map_entity_curie_to_name(db, curie_or_reference_id, token)
