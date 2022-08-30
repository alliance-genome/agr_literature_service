from fastapi import APIRouter, Depends, Response, Security, status
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import topic_entity_tag_crud
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas import (TopicEntityTagPropSchemaShow,
                                                TopicEntityTagPropSchemaUpdate,
                                                TopicEntityTagPropSchemaCreate,
                                                ResponseMessageSchema)
from agr_literature_service.api.user import set_global_user_from_okta

router = APIRouter(
    prefix="/topic_entity_tag_prop",
    tags=['Topic Entity Tag']
)


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=str)
def create(request: TopicEntityTagPropSchemaCreate,
           user: OktaUser = db_user,
           db: Session = db_session):
    set_global_user_from_okta(db, user)
    return topic_entity_tag_crud.create_prop(db, request)


@router.delete('/{topic_entity_tag_prop_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(topic_entity_tag_prop_id: int,
            user: OktaUser = db_user,
            db: Session = db_session):
    set_global_user_from_okta(db, user)
    topic_entity_tag_crud.delete_prop(db, topic_entity_tag_prop_id)

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{topic_entity_tag_prop_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
async def patch(topic_entity_tag_prop_id: int,
                request: TopicEntityTagPropSchemaUpdate,
                user: OktaUser = db_user,
                db: Session = db_session):
    set_global_user_from_okta(db, user)
    return topic_entity_tag_crud.update_prop(db, topic_entity_tag_prop_id, request)


@router.get('/{topic_entity_tag_prop_id}',
            response_model=TopicEntityTagPropSchemaShow,
            status_code=200)
def show(topic_entity_tag_prop_id: int,
         db: Session = db_session):
    return topic_entity_tag_crud.show_prop(db, topic_entity_tag_prop_id)
