"""
topic_entity_tag_crud.py
===========================
"""
from collections import defaultdict

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import case, and_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload

from agr_literature_service.api.crud.topic_entity_tag_utils import get_reference_id_from_curie_or_id, \
    get_source_from_db, add_source_obj_to_db_session, get_sorted_column_values, \
    get_map_ateam_curies_to_names, check_and_set_sgd_display_tag, add_audited_object_users_if_not_exist
from agr_literature_service.api.models import (
    TopicEntityTagModel,
    ReferenceModel, TopicEntityTagSourceModel, ModModel
)
from agr_literature_service.api.schemas.topic_entity_tag_schemas import (TopicEntityTagSchemaPost,
                                                                         TopicEntityTagSourceSchemaUpdate,
                                                                         TopicEntityTagSourceSchemaCreate,
                                                                         TopicEntityTagSchemaUpdate)

ATP_ID_SOURCE_AUTHOR = "author"
ATP_ID_SOURCE_CURATOR = "curator"
ATP_ID_SOURCE_CURATION_TOOLS = "curation_tools"


def create_tag(db: Session, topic_entity_tag: TopicEntityTagSchemaPost) -> int:
    topic_entity_tag_data = jsonable_encoder(topic_entity_tag)
    reference_curie = topic_entity_tag_data.pop("reference_curie", None)
    if reference_curie is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="reference_curie not within topic_entity_tag_data")
    reference_id = get_reference_id_from_curie_or_id(db, reference_curie)
    topic_entity_tag_data["reference_id"] = reference_id
    topic_entity_tag_data["reference_id"] = reference_id
    if topic_entity_tag_data['mod_abbreviation'] == 'SGD':
        check_and_set_sgd_display_tag(topic_entity_tag_data)
    source: TopicEntityTagSourceModel = db.query(TopicEntityTagSourceModel).filter(
        and_(TopicEntityTagSourceModel.source_name == topic_entity_tag_data["source_name"],
             TopicEntityTagSourceModel.mod.has(
                 ModModel.abbreviation == topic_entity_tag_data["mod_abbreviation"]))).one_or_none()
    if source is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cannot find the specified source")
    topic_entity_tag_data["topic_entity_tag_source_id"] = source.topic_entity_tag_source_id
    del topic_entity_tag_data["source_name"]
    del topic_entity_tag_data["mod_abbreviation"]
    add_audited_object_users_if_not_exist(db, topic_entity_tag_data)
    new_db_obj = TopicEntityTagModel(**topic_entity_tag_data)
    try:
        db.add(new_db_obj)
        db.flush()
        db.refresh(new_db_obj)
        topic_entity_tag_id = new_db_obj.topic_entity_tag_id
        validate_tags_on_insertion(db=db, tag_obj=new_db_obj)
        db.commit()
    except (IntegrityError, HTTPException) as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"invalid request: {e}")
    return topic_entity_tag_id


def show_tag(db: Session, topic_entity_tag_id: int):
    topic_entity_tag: TopicEntityTagModel = db.query(TopicEntityTagModel).get(topic_entity_tag_id)
    if not topic_entity_tag:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"topic_entityTag with the topic_entity_tag_id {topic_entity_tag_id} "
                                   f"is not available")
    topic_entity_tag_data = jsonable_encoder(topic_entity_tag)
    if topic_entity_tag_data["reference_id"]:
        topic_entity_tag_data["reference_curie"] = db.query(ReferenceModel).filter(
            ReferenceModel.reference_id == topic_entity_tag_data["reference_id"]).first().curie
        del topic_entity_tag_data["reference_id"]
    topic_entity_tag_data[
        "topic_entity_tag_source_id"] = topic_entity_tag.topic_entity_tag_source.topic_entity_tag_source_id
    topic_entity_tag_data["source_name"] = topic_entity_tag.topic_entity_tag_source.source_name
    topic_entity_tag_data["mod_abbreviation"] = topic_entity_tag.topic_entity_tag_source.mod.abbreviation
    return topic_entity_tag_data


def patch_tag(db: Session, topic_entity_tag_id: int, patch_data: TopicEntityTagSchemaUpdate):
    topic_entity_tag: TopicEntityTagModel = db.query(TopicEntityTagModel).get(topic_entity_tag_id)
    patch_data = patch_data.dict(exclude_unset=True)
    add_audited_object_users_if_not_exist(db, patch_data)
    for key, value in patch_data.items():
        setattr(topic_entity_tag, key, value)
    db.commit()
    return {"message": "updated"}


def destroy_tag(db: Session, topic_entity_tag_id: int):
    topic_entity_tag: TopicEntityTagModel = db.query(TopicEntityTagModel).get(topic_entity_tag_id)
    db.delete(topic_entity_tag)
    db.commit()


def validate_tags_on_insertion(db: Session, tag_obj: TopicEntityTagModel):
    # TODO: rewrite with new schema
    ...
    # all_related_sources = db.query(TopicEntityTagSourceModel).filter(
    #     TopicEntityTagSourceModel.topic_entity_tag_id == source_obj.topic_entity_tag_id).all()
    # for existing_source in all_related_sources:
    #     if source_obj.source in [ATP_ID_SOURCE_AUTHOR, ATP_ID_SOURCE_CURATOR, ATP_ID_SOURCE_CURATION_TOOLS]:
    #         existing_source_valid = existing_source.negated == source_obj.negated
    #         if source_obj.source == ATP_ID_SOURCE_AUTHOR:
    #             existing_source.validation_value_author = existing_source_valid
    #         elif source_obj.source == ATP_ID_SOURCE_CURATOR:
    #             existing_source.validation_value_curator = existing_source_valid
    #         elif source_obj.source == ATP_ID_SOURCE_CURATION_TOOLS:
    #             existing_source.validation_value_curation_tools = existing_source_valid
    #     if existing_source.source in [ATP_ID_SOURCE_AUTHOR, ATP_ID_SOURCE_CURATOR, ATP_ID_SOURCE_CURATION_TOOLS]:
    #         new_source_valid = source_obj.negated == existing_source.negated
    #         if existing_source.source == ATP_ID_SOURCE_AUTHOR:
    #             source_obj.validation_value_author = new_source_valid
    #         elif existing_source.source == ATP_ID_SOURCE_CURATOR:
    #             source_obj.validation_value_curator = new_source_valid
    #         elif existing_source.source == ATP_ID_SOURCE_CURATION_TOOLS:
    #             source_obj.validation_value_curation_tools = new_source_valid
    # db.commit()


def create_source(db: Session, source: TopicEntityTagSourceSchemaCreate):
    source_data = {key: value for key, value in jsonable_encoder(source).items() if value is not None}
    source_obj = add_source_obj_to_db_session(db, source_data)
    try:
        db.commit()
    except (IntegrityError, HTTPException) as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"invalid request: {e}")
    return source_obj.topic_entity_tag_source_id


def destroy_source(db: Session, topic_entity_tag_source_id: int):
    source = get_source_from_db(db, topic_entity_tag_source_id)
    db.delete(source)
    db.commit()


def patch_source(db: Session, topic_entity_tag_source_id: int, source_patch: TopicEntityTagSourceSchemaUpdate):
    source = get_source_from_db(db, topic_entity_tag_source_id)
    source_patch_data = source_patch.dict(exclude_unset=True)
    add_audited_object_users_if_not_exist(db, source_patch_data)
    for key, value in source_patch_data.items():
        setattr(source, key, value)
    db.commit()
    return {"message": "updated"}


def show_source(db: Session, topic_entity_tag_source_id: int):
    source = get_source_from_db(db, topic_entity_tag_source_id)
    source_data = jsonable_encoder(source)
    del source_data["mod_id"]
    source_data["mod_abbreviation"] = source.mod.abbreviation
    return source_data


def show_all_reference_tags(db: Session, curie_or_reference_id, page: int = 1, page_size: int = None,
                            count_only: bool = False, sort_by: str = None, desc_sort: bool = False):
    if page < 1:
        page = 1
    if sort_by == "null":
        sort_by = None
    reference_id = get_reference_id_from_curie_or_id(db, curie_or_reference_id)
    query = db.query(TopicEntityTagModel).options(
        joinedload(TopicEntityTagModel.sources)).filter(
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
            if tag.entity_source == "alliance":
                all_entities[tag.entity_type].append(tag.entity)
    entity_curie_to_name = get_map_ateam_curies_to_names(curies_category="atpterm", curies=all_topics_and_entities,
                                                         token=token)
    for atpterm_curie in all_entities.keys():
        entity_curie_to_name.update(get_map_ateam_curies_to_names(curies_category=entity_curie_to_name[atpterm_curie],
                                                                  curies=all_entities[atpterm_curie],
                                                                  token=token))
    return entity_curie_to_name
