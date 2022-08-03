"""
topic_entity_tag_crud.py
===========================
"""

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.models import (
    TopicEntityTagModel,
    TopicEntityTagPropModel,
    ReferenceModel
)
from agr_literature_service.api.schemas import TopicEntityTagSchemaCreate
from agr_literature_service.api.crud.utils import add_default_update_keys, add_default_create_keys


def extra_checks(topic_entity_tag_data):
    count = 0
    okay = True
    details = ""
    # Error if none of the entitys is set.
    if "alliance_entity" in topic_entity_tag_data and topic_entity_tag_data["alliance_entity"]:
        count += 1
    if "mod_entity" in topic_entity_tag_data and topic_entity_tag_data["mod_entity"]:
        count += 1
    if "new_entity" in topic_entity_tag_data and topic_entity_tag_data["new_entity"]:
        count += 1
    if not count:
        details = "One of the XXXX_entity's MUST be set"
        okay = False
    elif count > 1:
        details = "ONLY one of the XXXX_entity's MUST be set"
        okay = False
    return (okay, details)


def create(db: Session, topic_entity_tag: TopicEntityTagSchemaCreate) -> int:
    """
    Create a new topic_entity_tag
    :param db:
    :param topic_entity_tag:
    :return:
    """

    topic_entity_tag_data = jsonable_encoder(topic_entity_tag)
    add_default_create_keys(db, topic_entity_tag_data)

    reference_curie = topic_entity_tag_data["reference_curie"]
    del topic_entity_tag_data["reference_curie"]
    reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Reference with curie {reference_curie} does not exist")
    topic_entity_tag_data["reference_id"] = reference.reference_id

    (okay, details) = extra_checks(topic_entity_tag_data)
    if not okay:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=details)

    props = []
    if "props" in topic_entity_tag_data and topic_entity_tag_data["props"]:
        props = topic_entity_tag_data["props"]
        del topic_entity_tag_data["props"]
    elif "props" in topic_entity_tag_data:
        del topic_entity_tag_data["props"]

    db_obj = TopicEntityTagModel(**topic_entity_tag_data)
    db.add(db_obj)
    db.commit()
    for prop in props:
        xml = {"topic_entity_tag_id": db_obj.topic_entity_tag_id,
               "qualifier": prop['qualifier'],
               "created_by": topic_entity_tag_data["created_by"]}
        add_default_create_keys(db, xml)
        prop_obj = TopicEntityTagPropModel(**xml)
        db.add(prop_obj)
    db.commit()
    return db_obj.topic_entity_tag_id


def show(db: Session, topic_entity_tag_id: int):
    """

    :param db:
    :param topic_entity_tag_id:
    :return:
    """

    topic_entity_tag = db.query(TopicEntityTagModel).filter(TopicEntityTagModel.topic_entity_tag_id == topic_entity_tag_id).first()
    if not topic_entity_tag:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"topic_entityTag with the topic_entity_tag_id {topic_entity_tag_id} is not available")

    topic_entity_tag_data = jsonable_encoder(topic_entity_tag)

    if topic_entity_tag_data["reference_id"]:
        topic_entity_tag_data["reference_curie"] = db.query(ReferenceModel).filter(ReferenceModel.reference_id == topic_entity_tag_data["reference_id"]).first().curie
        del topic_entity_tag_data["reference_id"]

    props = db.query(TopicEntityTagPropModel).filter(TopicEntityTagPropModel.topic_entity_tag_id == topic_entity_tag_id).all()
    topic_entity_tag_data["props"] = []
    prop: TopicEntityTagPropModel
    for prop in props:
        prop_data = jsonable_encoder(prop)
        topic_entity_tag_data["props"].append(prop_data)
    return topic_entity_tag_data


def patch(db: Session, topic_entity_tag_id: int, topic_entity_tag_update):
    """
    Update a topic_entity_tag
    :param db:
    :param topic_entity_tag_id:
    :param topic_entity_tag_update:
    :return:
    """
    topic_entity_tag_data = jsonable_encoder(topic_entity_tag_update)
    add_default_update_keys(db, topic_entity_tag_data)
    topic_entity_tag_db_obj = db.query(TopicEntityTagModel).filter(TopicEntityTagModel.topic_entity_tag_id == topic_entity_tag_id).first()
    if not topic_entity_tag_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"topic_entity_tag with topic_entity_tag_id {topic_entity_tag_id} not found")

    # Loop ONLY on the fields that were passed to patch before pydantic
    # added a bunch of fields with None values etc.
    for field in topic_entity_tag_update.__fields_set__:
        value = topic_entity_tag_data[field]
        if field == "reference_curie":
            if value is not None:
                reference_curie = value
                new_reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
                if not new_reference:
                    raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                        detail=f"Reference with curie {reference_curie} does not exist")
                topic_entity_tag_db_obj.reference = new_reference
        elif field == "props" and value:
            for prop in value:
                add_default_update_keys(db, prop)
                prop_obj = db.query(TopicEntityTagPropModel).filter(TopicEntityTagPropModel.topic_entity_tag_id == prop["topic_entity_tag_id"]).one()
                if prop_obj.qualifier != prop["qualifier"]:
                    prop_obj.qualifier = prop["qualifier"]
                    prop.updated_by = prop["updated_by"]
                    prop.date_updated = prop["date_updated"]
                    db.commit()
        else:
            setattr(topic_entity_tag_db_obj, field, value)

    # Becouse we added updated fields after pydantic they are not in the changed fields list
    # So we want to do these separately now.
    for field in ['updated_by', 'date_updated']:
        setattr(topic_entity_tag_db_obj, field, topic_entity_tag_data[field])
    db.commit()
    return {"message": "updated"}


def destroy(db: Session, topic_entity_tag_id: int):

    topic_entity_tag = db.query(TopicEntityTagModel).filter(TopicEntityTagModel.topic_entity_tag_id == topic_entity_tag_id).first()
    if not topic_entity_tag:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"topic_entityTag with the topic_entity_tag_id {topic_entity_tag_id} is not available")
    db.delete(topic_entity_tag)
    db.commit()

    return None
