"""
topic_entity_tag_crud.py
===========================
"""

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.models import TopicEntityTagModel, ModModel
from agr_literature_service.api.schemas import TopicEntityTagSchemaCreate
from agr_literature_service.api.crud.utils import add_default_update_keys, add_default_create_keys


def create(db: Session, topic_entity_tag: TopicEntityTagSchemaCreate) -> int:
    """
    Create a new topic_entity_tag
    :param db:
    :param topic_entity_tag:
    :return:
    """

    topic_entity_tag_data = jsonable_encoder(topic_entity_tag)
    add_default_create_keys(db, topic_entity_tag_data)


def show(db: Session, topic_entity_tag_id: int):
    """

    :param db:
    :param topic_entity_tag_id:
    :return:
    """

    topic_entity_tag = db.query(TopicEntityTagModel).filter(TopicEntityTagModel.topic_entity_tag == topic_entity_tag_id).first()
    if not topic_entity_tag:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"topic_entityTag with the topic_entity_tag_id {topic_entity_tag_id} is not available")

    topic_entity_tag_data = jsonable_encoder(topic_entity_tag)

    if topic_entity_tag_data["mod_id"]:
        topic_entity_tag_data["mod_abbreviation"] = db.query(ModModel).filter(ModModel.mod_id == topic_entity_tag_data["mod_id"]).first().abbreviation
    del topic_entity_tag_data["mod_id"]


def patch(db: Session, topic_entity_tag_id: int, topic_entity_tag_update):
    """
    Update a topic_entity_tag
    :param db:
    :param topic_entity_tag_id:
    :param topic_entity_tag_update:
    :return:
    """
    topic_entity_tag_data = jsonable_encoder(topic_entity_tag_update)

    add_default_update_keys(topic_entity_tag_data)
    topic_entity_tag_db_obj = db.query(TopicEntityTagModel).filter(TopicEntityTagModel.topic_entity_tag_id == topic_entity_tag_id).first()
    if not topic_entity_tag_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"topic_entity_tag with topic_entity_tag_id {topic_entity_tag_id} not found")

    for field, value in topic_entity_tag_data.items():
        print(field, value)
