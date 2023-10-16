"""
topic_entity_tag_crud.py
===========================
"""
from collections import defaultdict
from typing import Dict

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import case, and_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload, subqueryload

from agr_literature_service.api.crud.topic_entity_tag_utils import get_reference_id_from_curie_or_id, \
    get_source_from_db, add_source_obj_to_db_session, get_sorted_column_values, \
    get_map_ateam_curies_to_names, check_and_set_sgd_display_tag, add_audited_object_users_if_not_exist, \
    get_ancestors_or_descendants
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
    source: TopicEntityTagSourceModel = db.query(TopicEntityTagSourceModel).filter(
        TopicEntityTagSourceModel.topic_entity_tag_source_id == topic_entity_tag_data["topic_entity_tag_source_id"]
    ).one_or_none()
    if source is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cannot find the specified source")
    if source.mod.abbreviation == "SGD":
        check_and_set_sgd_display_tag(topic_entity_tag_data)
    add_audited_object_users_if_not_exist(db, topic_entity_tag_data)
    new_db_obj = TopicEntityTagModel(**topic_entity_tag_data)
    try:
        db.add(new_db_obj)
        db.flush()
        db.refresh(new_db_obj)
        topic_entity_tag_id = new_db_obj.topic_entity_tag_id
        validate_tags(db=db, new_tag_obj=new_db_obj)
    except (IntegrityError, HTTPException) as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"invalid request: {e}")
    return topic_entity_tag_id


def calculate_validation_value_for_tag(topic_entity_tag_db_obj: TopicEntityTagModel, validation_type: str):
    validating_tags_values = [validating_tag.negated for validating_tag in topic_entity_tag_db_obj.validated_by if
                              validating_tag.topic_entity_tag_source.validation_type == validation_type]
    if len(validating_tags_values) > 0:
        if topic_entity_tag_db_obj.topic_entity_tag_source.validation_type == validation_type:
            validating_tags_values.append(topic_entity_tag_db_obj.negated)
        if len(set(validating_tags_values)) == 1:
            return "validated_right" if topic_entity_tag_db_obj.negated == validating_tags_values[0] else \
                "validated_wrong"
        else:
            return "validation_conflict"
    return "not_validated"


def add_validation_values_to_tag(topic_entity_tag_db_obj: TopicEntityTagModel, tag_data_dict: Dict):
    tag_data_dict["validation_value_author"] = calculate_validation_value_for_tag(topic_entity_tag_db_obj, "author")
    tag_data_dict["validation_value_curator"] = calculate_validation_value_for_tag(topic_entity_tag_db_obj, "curator")
    tag_data_dict["validation_value_curation_tools"] = calculate_validation_value_for_tag(topic_entity_tag_db_obj,
                                                                                          "curation_tools")


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
    add_validation_values_to_tag(topic_entity_tag, topic_entity_tag_data)
    return topic_entity_tag_data


def patch_tag(db: Session, topic_entity_tag_id: int, patch_data: TopicEntityTagSchemaUpdate):
    topic_entity_tag: TopicEntityTagModel = db.query(TopicEntityTagModel).filter(
        TopicEntityTagModel.topic_entity_tag_id == topic_entity_tag_id).one_or_none()
    if topic_entity_tag is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"topic_entityTag with the topic_entity_tag_id {topic_entity_tag_id} "
                                   f"is not available")
    patch_data_dict = patch_data.dict(exclude_unset=True)
    add_audited_object_users_if_not_exist(db, patch_data_dict)
    for key, value in patch_data_dict.items():
        setattr(topic_entity_tag, key, value)
    db.commit()
    return {"message": "updated"}


def destroy_tag(db: Session, topic_entity_tag_id: int):
    topic_entity_tag: TopicEntityTagModel = db.query(TopicEntityTagModel).filter(
        TopicEntityTagModel.topic_entity_tag_id == topic_entity_tag_id).one_or_none()
    if topic_entity_tag is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"topic_entityTag with the topic_entity_tag_id {topic_entity_tag_id} "
                                   f"is not available")
    db.delete(topic_entity_tag)
    db.commit()


def validate_tags_already_in_db(new_tag_obj: TopicEntityTagModel, related_tags_in_db):
    if new_tag_obj.topic_entity_tag_source.validation_type is not None:
        more_generic_topics = set(get_ancestors_or_descendants(onto_node=new_tag_obj.topic))
        more_generic_topics.add(new_tag_obj.topic)
        tag_in_db: TopicEntityTagModel
        for tag_in_db in related_tags_in_db:
            if tag_in_db.topic in more_generic_topics:
                if tag_in_db.entity_type is None or (tag_in_db.entity_type == new_tag_obj.entity_type
                                                     and tag_in_db.entity == new_tag_obj.entity):
                    if tag_in_db.species is None or tag_in_db.species == new_tag_obj.species:
                        tag_in_db.validated_by.append(new_tag_obj)
    # validate pure entity-only tags if the new tag is a mixed topic + entity tag for the same entity
    if new_tag_obj.entity is not None and new_tag_obj.entity_type != new_tag_obj.topic:
        for tag_in_db in related_tags_in_db:
            if (tag_in_db.topic == tag_in_db.entity_type == new_tag_obj.entity_type
                    and new_tag_obj.entity == tag_in_db.entity):
                tag_in_db.validated_by.append(new_tag_obj)


def validate_tag_with_tags_in_db(new_tag_obj: TopicEntityTagModel, related_tags_in_db):
    more_specific_topics = set(get_ancestors_or_descendants(onto_node=new_tag_obj.topic,
                                                            ancestors_or_descendants='descendants'))
    more_specific_topics.add(new_tag_obj.topic)
    tag_in_db: TopicEntityTagModel
    for tag_in_db in related_tags_in_db:
        if tag_in_db.topic_entity_tag_source.validation_type is not None:
            if tag_in_db.topic in more_specific_topics:
                if new_tag_obj.entity_type is None or (tag_in_db.entity_type == new_tag_obj.entity_type
                                                       and tag_in_db.entity == new_tag_obj.entity):
                    if new_tag_obj.species is None or tag_in_db.species == new_tag_obj.species:
                        new_tag_obj.validated_by.append(tag_in_db)
    # if the new tag is a pure entity-only tag and there are mixed topic + entity tags with the same entity
    if new_tag_obj.topic == new_tag_obj.entity_type:
        for tag_in_db in related_tags_in_db:
            if (tag_in_db.entity_type != tag_in_db.topic and new_tag_obj.entity_type == tag_in_db.entity_type
                    and new_tag_obj.entity == tag_in_db.entity):
                tag_in_db.validated_by.append(new_tag_obj)


def validate_tags(db: Session, new_tag_obj: TopicEntityTagModel):
    related_tags_in_db = db.query(TopicEntityTagModel).options(
        subqueryload(TopicEntityTagModel.topic_entity_tag_source)).filter(
        and_(
            TopicEntityTagModel.topic_entity_tag_id != new_tag_obj.topic_entity_tag_id,
            TopicEntityTagModel.reference_id == new_tag_obj.reference_id,
            TopicEntityTagModel.topic_entity_tag_source.has(
                TopicEntityTagSourceModel.mod_id == new_tag_obj.topic_entity_tag_source.mod_id
            )
        )
    ).all()
    if len(related_tags_in_db) == 0:
        return
    # 1. identify more generic tags (or same tag) already in the db to be validated by current tag
    validate_tags_already_in_db(new_tag_obj, related_tags_in_db)
    # 2. identify more specific tags (or same tag) already in the db that validate the current tag
    validate_tag_with_tags_in_db(new_tag_obj, related_tags_in_db)
    # TODO: fix validation for negative tags
    db.commit()


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


def filter_tet_data_by_column(query, column_name, values):
    column = getattr(TopicEntityTagModel, column_name, None)
    query = query.filter(column.in_(values))
    return query


def show_all_reference_tags(db: Session, curie_or_reference_id, page: int = 1,
                            page_size: int = None, count_only: bool = False,
                            sort_by: str = None, desc_sort: bool = False,
                            column_only: str = None, column_filter: str = None,
                            column_values: str = None):

    if page < 1:
        page = 1
    if sort_by == "null":
        sort_by = None
    reference_id = get_reference_id_from_curie_or_id(db, curie_or_reference_id)

    if column_only:
        # species_list = db.query(TopicEntityTagModel.species).filter_by(
        #    reference_id=reference_id).distinct().all()
        # distinct_species_list = [species[0] for species in species_list if species[0] is not None]
        """
        column_only = 'species' or 'display_tag'
        distinct_column_values = a list of species for this paper if column_only = 'species'
        distinct_column_values = a list of display_tag for this paper if column_only = 'display_tag'
        """
        distinct_column_values = db.query(getattr(TopicEntityTagModel, column_only)).filter_by(
            reference_id=reference_id).distinct().all()
        distinct_values = [x[0] for x in distinct_column_values if x[0] is not None]
        return jsonable_encoder(distinct_values)

    query = db.query(TopicEntityTagModel).options(
        joinedload(TopicEntityTagModel.topic_entity_tag_source)).filter(
        TopicEntityTagModel.reference_id == reference_id)

    if column_filter and column_values:
        column_value_list = column_values.split(',')
        query = filter_tet_data_by_column(query, column_filter, column_value_list)

    if count_only:
        return query.count()
    else:
        if sort_by:
            # if sort_by in ['topic', 'entity_type', 'species', 'entity', 'display_tag']:
            if sort_by in ['topic', 'entity_type', 'species', 'display_tag', 'entity']:
                column_property = getattr(TopicEntityTagModel, sort_by, None)
                column = column_property.property.columns[0]
                order_expression = case([(column.is_(None), 1 if desc_sort else 0)], else_=0 if desc_sort else 1)
                sorted_column_values = get_sorted_column_values(reference_id, db,
                                                                sort_by, token, desc_sort)
                curie_ordering = case({curie: index for index, curie in enumerate(sorted_column_values)},
                                      value=getattr(TopicEntityTagModel, sort_by))
                query = query.order_by(order_expression, curie_ordering)
            else:
                if sort_by in ['source_mod_id', 'source_evidence', 'source_validation_type', 'source_description']:
                    sort_by = sort_by.replace('source_', '')

                # check if the column exists in TopicEntityTagModel
                if hasattr(TopicEntityTagModel, sort_by):
                    column_property = getattr(TopicEntityTagModel, sort_by)
                elif hasattr(TopicEntityTagSourceModel, sort_by):
                    column_property = getattr(TopicEntityTagSourceModel, sort_by)
                    # explicitly join the topic_entity_tag_source table for sorting
                    query = query.join(TopicEntityTagSourceModel,
                                       TopicEntityTagModel.topic_entity_tag_source_id == TopicEntityTagSourceModel.topic_entity_tag_source_id)
                else:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                        detail=f"The column '{sort_by}' does not exist in either TopicEntityTagModel or TopicEntityTagSourceModel.")
                if column_property is None:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                        detail=f"Failed to get the column '{sort_by}' from the models.")

                # check for None values and order accordingly
                order_expression = case([(column_property.is_(None), 1 if desc_sort else 0)], else_=0 if desc_sort else 1)
                query = query.order_by(order_expression, column_property.desc() if desc_sort else column_property)
        all_tet = []
        for tet in query.offset((page - 1) * page_size if page_size else None).limit(page_size).all():
            tet_data = jsonable_encoder(tet)
            add_validation_values_to_tag(tet, tet_data)
            all_tet.append(tet_data)
        return all_tet


def get_map_entity_curie_to_name(db: Session, curie_or_reference_id: str):
    reference_id = get_reference_id_from_curie_or_id(db, curie_or_reference_id)
    topics_and_entities = db.query(TopicEntityTagModel).filter(TopicEntityTagModel.reference_id == reference_id).all()
    all_topics_and_entities = []
    all_entities = defaultdict(list)
    for tag in topics_and_entities:
        all_topics_and_entities.append(tag.topic)
        if tag.display_tag is not None:
            all_topics_and_entities.append(tag.display_tag)
        if tag.entity_type is not None:
            all_topics_and_entities.append(tag.entity_type)
            if tag.entity_source == "alliance":
                all_entities[tag.entity_type].append(tag.entity)
    entity_curie_to_name = get_map_ateam_curies_to_names(curies_category="atpterm", curies=all_topics_and_entities)
    for atpterm_curie in all_entities.keys():
        entity_curie_to_name.update(get_map_ateam_curies_to_names(
            curies_category=entity_curie_to_name[atpterm_curie].replace(" ", ""),
            curies=all_entities[atpterm_curie]))
    for curie_without_name in (set(all_entities) | set(all_topics_and_entities)) - set(entity_curie_to_name.keys()):
        entity_curie_to_name[curie_without_name] = curie_without_name
    return entity_curie_to_name


def show_source_by_name(db: Session, source_type: str, source_method: str, mod_abbreviation: str):
    mod = db.query(ModModel.mod_id).filter(ModModel.abbreviation == mod_abbreviation).one_or_none()
    if mod is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cannot find the specified MOD")
    source = db.query(TopicEntityTagSourceModel).filter(
        and_(
            TopicEntityTagSourceModel.source_type == source_type,
            TopicEntityTagSourceModel.source_method == source_method,
            TopicEntityTagSourceModel.mod_id == mod.mod_id
        )
    ).one_or_none()
    if source is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cannot find the specified Source")
    source_data = jsonable_encoder(source)
    del source_data["mod_id"]
    source_data["mod_abbreviation"] = mod_abbreviation
    return source_data
