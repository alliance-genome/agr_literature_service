"""
topic_entity_tag_crud.py
===========================
"""
import json
import urllib.request
from collections import defaultdict

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import and_
from sqlalchemy.orm import Session, joinedload

from agr_literature_service.api.models import (
    TopicEntityTagModel,
    TopicEntityTagPropModel,
    ReferenceModel
)
from agr_literature_service.api.schemas import (
    TopicEntityTagPropSchemaPost,
    TopicEntityTagPropSchemaUpdate
)
from agr_literature_service.api.schemas.topic_entity_tag_schemas import TopicEntityTagSchemaPost


def get_reference_id_from_curie_or_id(db: Session, curie_or_reference_id):
    reference_id = int(curie_or_reference_id) if curie_or_reference_id.isdigit() else None
    if reference_id is None:
        reference_id = db.query(ReferenceModel.reference_id).filter(
            ReferenceModel.curie == curie_or_reference_id).one_or_none()
    if reference_id is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the reference_id or curie {curie_or_reference_id} is not available")
    return reference_id


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


def create(db: Session, topic_entity_tag: TopicEntityTagSchemaPost) -> int:
    """
    Create a new topic_entity_tag
    :param db:
    :param topic_entity_tag:
    :return:
    """
    topic_entity_tag_data = jsonable_encoder(topic_entity_tag)
    reference_curie = topic_entity_tag_data["reference_curie"]
    if "reference_curie" in topic_entity_tag_data and topic_entity_tag_data["reference_curie"]:
        del topic_entity_tag_data["reference_curie"]
    else:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="reference_curie not within  topic_entity_tag_data")
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
               "qualifier": prop['qualifier']}
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


def show_all_reference_tags(db: Session, curie_or_reference_id, offset: int = None, limit: int = None):
    reference_id = get_reference_id_from_curie_or_id(db, curie_or_reference_id)
    topics_and_entities = db.query(TopicEntityTagModel).options(joinedload(TopicEntityTagModel.props)).filter(
        TopicEntityTagModel.reference_id == reference_id).offset(offset).limit(limit).all()
    return [jsonable_encoder(tet) for tet in topics_and_entities]


def patch(db: Session, topic_entity_tag_id: int, topic_entity_tag_update):
    """
    Update a topic_entity_tag
    :param db:
    :param topic_entity_tag_id:
    :param topic_entity_tag_update:
    :return:
    """
    topic_entity_tag_data = jsonable_encoder(topic_entity_tag_update)
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
        else:
            setattr(topic_entity_tag_db_obj, field, value)
    # Becouse we added updated fields after pydantic they are not in the changed fields list
    # So we want to do these separately now.
    db.commit()
    return {"message": "updated"}


def destroy(db: Session, topic_entity_tag_id: int):

    topic_entity_tag = db.query(TopicEntityTagModel).filter(TopicEntityTagModel.topic_entity_tag_id == topic_entity_tag_id).first()
    if not topic_entity_tag:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"topic_entity_tag with the topic_entity_tag_id {topic_entity_tag_id} is not available")
    db.delete(topic_entity_tag)
    db.commit()

    return None


def create_prop(db: Session, topic_entity_tag_prop: TopicEntityTagPropSchemaPost) -> int:
    """
    Create a new topic_entity_tag
    :param db:
    :param topic_entity_tag:
    :return:
    """

    topic_entity_tag_prop_data = jsonable_encoder(topic_entity_tag_prop)
    topic_entity_tag = db.query(TopicEntityTagModel).\
        filter(TopicEntityTagModel.topic_entity_tag_id == topic_entity_tag_prop_data["topic_entity_tag_id"]).first()
    if not topic_entity_tag:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"topic_entityTag with the topic_entity_tag_id {topic_entity_tag_prop_data['topic_entity_tag_id']} is not available")

    db_obj = TopicEntityTagPropModel(**topic_entity_tag_prop_data)
    db.add(db_obj)
    db.commit()
    return db_obj.topic_entity_tag_prop_id


def delete_prop(db: Session, topic_entity_tag_prop_id: int):
    topic_entity_tag_prop = db.query(TopicEntityTagPropModel).\
        filter(TopicEntityTagPropModel.topic_entity_tag_prop_id == topic_entity_tag_prop_id).first()
    if not topic_entity_tag_prop:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"topic_entity_tag_prop with the topic_entity_tag_id {topic_entity_tag_prop_id} is not available")

    db.delete(topic_entity_tag_prop)
    db.commit()

    return None


def update_prop(db: Session, topic_entity_tag_prop_id: int, topic_entity_tag_prop: TopicEntityTagPropSchemaUpdate):
    prop_data = jsonable_encoder(topic_entity_tag_prop)
    prop_obj = db.query(TopicEntityTagPropModel).\
        filter(TopicEntityTagPropModel.topic_entity_tag_prop_id == topic_entity_tag_prop_id).first()
    if not prop_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"topic_entity_tag_prop with the topic_entity_tag_prop_id {topic_entity_tag_prop_id} is not available")

    for field, value in prop_data.items():
        print("Updating {} {} for {}".format(field, value, prop_obj))
        if value:
            setattr(prop_obj, field, value)
    db.commit()
    return {"message": "updated"}


def show_prop(db: Session, topic_entity_tag_prop_id: int):
    prop = db.query(TopicEntityTagPropModel).\
        filter(TopicEntityTagPropModel.topic_entity_tag_prop_id == topic_entity_tag_prop_id).first()
    if not prop:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"topic_entity_tag_prop with the topic_entity_tag_id {topic_entity_tag_prop_id} is not available")

    prop_data = jsonable_encoder(prop)
    return prop_data


def get_map_entity_curie_to_name(db: Session, curie_or_reference_id: str, token: str):
    allowed_entity_type_map = {'ATP:0000005': 'gene', 'ATP:0000006': 'allele'}
    reference_id = get_reference_id_from_curie_or_id(db, curie_or_reference_id)
    topics_and_entities = db.query(TopicEntityTagModel).filter(
        and_(TopicEntityTagModel.reference_id == reference_id,
             TopicEntityTagModel.entity_type.in_([key for key in allowed_entity_type_map.keys()]),
             TopicEntityTagModel.alliance_entity.isnot(None))).all()
    tags_by_entity_type = defaultdict(set)
    entity_curie_to_name = {}
    for tag in topics_and_entities:
        tags_by_entity_type[allowed_entity_type_map[tag.entity_type]].add(tag.alliance_entity)
    for entity_type, entity_curies in tags_by_entity_type.items():
        ateam_api = f'https://beta-curation.alliancegenome.org/api/{entity_type}/search?limit=1000&page=0'
        request_body = {"searchFilters": {
            "nameFilters": {
                "curie_keyword": {"queryString": " ".join(entity_curies), "tokenOperator": "OR"}
            }

        }}
        request_data_encoded = json.dumps(request_body)
        request_data_encoded_str = str(request_data_encoded)
        request = urllib.request.Request(url=ateam_api, data=request_data_encoded_str.encode('utf-8'))
        request.add_header("Authorization", f"Bearer {token}")
        request.add_header("Content-type", "application/json")
        request.add_header("Accept", "application/json")
        with urllib.request.urlopen(request) as response:
            resp = response.read().decode("utf8")
            resp_obj = json.loads(resp)
            entity_curie_to_name.update({entity["curie"]: entity[entity_type + "Symbol"]["displayText"]
                                         for entity in resp_obj["results"]})
    return entity_curie_to_name





