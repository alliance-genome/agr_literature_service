"""
topic_entity_tag_crud.py
===========================
"""
from collections import defaultdict

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import case
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload

from agr_literature_service.api.crud.topic_entity_tag_utils import get_reference_id_from_curie_or_id, \
    get_source_from_db, add_source_obj_to_db_session, get_sorted_column_values, \
    get_map_ateam_curies_to_names
from agr_literature_service.api.models import (
    TopicEntityTagModel,
    ReferenceModel, TopicEntityTagSourceModel
)
from agr_literature_service.api.schemas.topic_entity_tag_schemas import TopicEntityTagSchemaPost, \
    TopicEntityTagSourceSchemaPost, TopicEntityTagSourceSchemaUpdate


def create_tag_with_source(db: Session, topic_entity_tag: TopicEntityTagSchemaPost) -> int:
    topic_entity_tag_data = jsonable_encoder(topic_entity_tag)
    reference_curie = topic_entity_tag_data.pop("reference_curie", None)
    if reference_curie is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="reference_curie not within topic_entity_tag_data")
    reference_id = get_reference_id_from_curie_or_id(db, reference_curie)
    topic_entity_tag_data["reference_id"] = reference_id
    sources = topic_entity_tag_data.pop("sources", []) or []
    new_db_obj = TopicEntityTagModel(**topic_entity_tag_data)
    existing_topic_entity_tag = db.query(TopicEntityTagModel).filter(
        TopicEntityTagModel.reference_id == reference_id,
        TopicEntityTagModel.topic == new_db_obj.topic,
        TopicEntityTagModel.entity_type == new_db_obj.entity_type,
        TopicEntityTagModel.entity == new_db_obj.entity,
        TopicEntityTagModel.entity_source == new_db_obj.entity_source,
        TopicEntityTagModel.entity_published_as == new_db_obj.entity_published_as,
        TopicEntityTagModel.species == new_db_obj.species
    ).one_or_none()
    try:
        if existing_topic_entity_tag is None:
            db.add(new_db_obj)
            db.flush()
            db.refresh(new_db_obj)
            topic_entity_tag_id = new_db_obj.topic_entity_tag_id
        else:
            topic_entity_tag_id = existing_topic_entity_tag.topic_entity_tag_id
        for source in sources:
            add_source_obj_to_db_session(db, topic_entity_tag_id, source)
        db.commit()
    except (IntegrityError, HTTPException) as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"invalid request: {e}")
    return topic_entity_tag_id


def show(db: Session, topic_entity_tag_id: int):
    topic_entity_tag = db.query(TopicEntityTagModel).get(topic_entity_tag_id)
    if not topic_entity_tag:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"topic_entityTag with the topic_entity_tag_id {topic_entity_tag_id} "
                                   f"is not available")
    topic_entity_tag_data = jsonable_encoder(topic_entity_tag)
    if topic_entity_tag_data["reference_id"]:
        topic_entity_tag_data["reference_curie"] = db.query(ReferenceModel).filter(
            ReferenceModel.reference_id == topic_entity_tag_data["reference_id"]).first().curie
        del topic_entity_tag_data["reference_id"]
    sources = db.query(TopicEntityTagSourceModel).options(joinedload(TopicEntityTagSourceModel.mod)).filter(
        TopicEntityTagSourceModel.topic_entity_tag_id == topic_entity_tag_id).all()
    topic_entity_tag_data["sources"] = [jsonable_encoder(source) for source in sources]
    for source in topic_entity_tag_data["sources"]:
        source["mod_abbreviation"] = source["mod"]["abbreviation"]
        del source["mod"]
        del source["mod_id"]
        del source["topic_entity_tag_id"]
    return topic_entity_tag_data


def add_source_to_tag(db: Session, source: TopicEntityTagSourceSchemaPost):
    topic_entity_tag = db.query(TopicEntityTagModel).filter(
        TopicEntityTagModel.topic_entity_tag_id == source.topic_entity_tag_id).one_or_none()
    if topic_entity_tag is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"topic or entity tag {source.topic_entity_tag_id} not found")
    source_data = jsonable_encoder(source)
    add_source_obj_to_db_session(db, source.topic_entity_tag_id, source_data)
    db.commit()


def destroy_source(db: Session, topic_entity_tag_source_id: int):
    source = get_source_from_db(db, topic_entity_tag_source_id)
    if len(source.topic_entity_tag.sources) == 1:
        db.delete(source.topic_entity_tag)
    else:
        db.delete(source)
    db.commit()


def patch_source(db: Session, topic_entity_tag_source_id: int, source_patch: TopicEntityTagSourceSchemaUpdate):
    source = get_source_from_db(db, topic_entity_tag_source_id)
    for key, value in source_patch.dict(exclude_unset=True).items():
        setattr(source, key, value)
    db.commit()
    return {"message": "updated"}


def show_all_reference_tags(db: Session, curie_or_reference_id, page: int = 1, page_size: int = None,
                            count_only: bool = False, sort_by: str = None, desc_sort: bool = False):
    if page < 1:
        page = 1
    if sort_by == "null":
        sort_by = None
    reference_id = get_reference_id_from_curie_or_id(db, curie_or_reference_id)
    query = db.query(TopicEntityTagModel).options(
        joinedload(TopicEntityTagModel.sources)).options(
        joinedload(TopicEntityTagModel.qualifiers)).filter(
        TopicEntityTagModel.reference_id == reference_id)
    if count_only:
        return query.count()
    else:
        if sort_by:
            column_property = getattr(TopicEntityTagModel, sort_by, None)
            column = column_property.property.columns[0]
            order_expression = case([(column.is_(None), 1 if desc_sort else 0)], else_=0 if desc_sort else 1)
            curie_ordering = case({curie: index for index, curie in enumerate(get_sorted_column_values(db, sort_by,
                                                                                                       desc_sort))},
                                  value=getattr(TopicEntityTagModel, sort_by))
            query = query.order_by(order_expression, curie_ordering)
        return [jsonable_encoder(tet) for tet in query.offset((page - 1) * page_size if page_size else None).limit(
            page_size).all()]


def get_map_entity_curie_to_name(db: Session, curie_or_reference_id: str, token: str):
    reference_id = get_reference_id_from_curie_or_id(db, curie_or_reference_id)
    topics_and_entities = db.query(TopicEntityTagModel).filter(TopicEntityTagModel.reference_id == reference_id).all()
    all_topics_and_entities = []
    all_entities = defaultdict(list)
    for tag in topics_and_entities:
        all_topics_and_entities.append(tag.topic)
        if tag.entity_type is not None:
            all_topics_and_entities.append(tag.entity_type)
            all_entities[tag.entity_type].append(tag.entity)
    entity_curie_to_name = get_map_ateam_curies_to_names(curies_category="atpterm", curies=all_topics_and_entities,
                                                         token=token)
    for atpterm_curie in all_entities.keys():
        entity_curie_to_name.update(get_map_ateam_curies_to_names(curies_category=entity_curie_to_name[atpterm_curie],
                                                                  curies=all_entities[atpterm_curie],
                                                                  token=token))
    return entity_curie_to_name
