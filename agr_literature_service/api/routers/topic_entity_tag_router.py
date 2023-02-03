from typing import List, Dict

from fastapi import APIRouter, Depends, Response, Security, status, HTTPException
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session
import jwt

from agr_literature_service.api import database
from agr_literature_service.api.crud import topic_entity_tag_crud
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas import (TopicEntityTagSchemaShow,
                                                TopicEntityTagSchemaUpdate,
                                                TopicEntityTagSchemaPost,
                                                ResponseMessageSchema)
from agr_literature_service.api.schemas.topic_entity_tag_schemas import TopicEntityTagSchemaRelated
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
    return topic_entity_tag_crud.create(db, request)


@router.delete('/{topic_entity_tag_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(topic_entity_tag_id: int,
            user: OktaUser = db_user,
            db: Session = db_session):
    set_global_user_from_okta(db, user)
    topic_entity_tag_crud.destroy(db, topic_entity_tag_id)

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{topic_entity_tag_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
async def patch(topic_entity_tag_id: int,
                request: TopicEntityTagSchemaUpdate,
                user: OktaUser = db_user,
                db: Session = db_session):
    set_global_user_from_okta(db, user)
    return topic_entity_tag_crud.patch(db, topic_entity_tag_id, request)


@router.get('/{topic_entity_tag_id}',
            response_model=TopicEntityTagSchemaShow,
            status_code=200)
def show(topic_entity_tag_id: int,
         db: Session = db_session):
    return topic_entity_tag_crud.show(db, topic_entity_tag_id)


@router.get('/by_reference/{curie_or_reference_id}',
            response_model=List[TopicEntityTagSchemaRelated],
            status_code=200)
def show_all_reference_tags(curie_or_reference_id: str, offset: int = None, limit: int = None,
                            db: Session = db_session):
    return topic_entity_tag_crud.show_all_reference_tags(db, curie_or_reference_id, offset, limit)


@router.get('/curation_id_name_map/{curie_or_reference_id}',
            response_model=Dict[str, str],
            status_code=200)
def get_curation_id_name_map(curie_or_reference_id: str,
                             user: OktaUser = db_user,
                             token: str = None,
                             db: Session = db_session):
    set_global_user_from_okta(db, user)
    if token is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="no token provided")
    return topic_entity_tag_crud.get_curation_id_name_map(db, curie_or_reference_id, token)
