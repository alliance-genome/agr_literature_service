"""
topic_entity_tag_crud.py
===========================
"""
import copy
import logging
from collections import defaultdict
from os import environ
from typing import Dict, Set

from dateutil import parser as date_parser
from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import case, and_, create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload, sessionmaker

from agr_literature_service.api.crud.topic_entity_tag_utils import get_reference_id_from_curie_or_id, \
    get_source_from_db, add_source_obj_to_db_session, get_sorted_column_values, \
    get_map_ateam_curies_to_names, check_and_set_sgd_display_tag, check_and_set_species, \
    add_audited_object_users_if_not_exist, get_ancestors, get_descendants, \
    check_atp_ids_validity, get_map_entity_curies_to_names
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.models import (
    TopicEntityTagModel,
    ReferenceModel, TopicEntityTagSourceModel, ModModel
)
from agr_literature_service.api.models.audited_model import get_default_user_value
from agr_literature_service.api.routers.okta_utils import OktaAccess, OKTA_ACCESS_MOD_ABBR
from agr_literature_service.api.schemas.topic_entity_tag_schemas import (TopicEntityTagSchemaPost,
                                                                         TopicEntityTagSourceSchemaUpdate,
                                                                         TopicEntityTagSourceSchemaCreate,
                                                                         TopicEntityTagSchemaUpdate)
from agr_literature_service.lit_processing.utils.email_utils import send_email


logger = logging.getLogger(__name__)


ATP_ID_SOURCE_AUTHOR = "author"
ATP_ID_SOURCE_CURATOR = "professional_biocurator"

TET_CURIE_FIELDS = ['topic', 'entity_type', 'display_tag', 'entity', 'species']


def create_tag(db: Session, topic_entity_tag: TopicEntityTagSchemaPost, validate_on_insert: bool = True) -> dict:
    topic_entity_tag_data = jsonable_encoder(topic_entity_tag)
    if topic_entity_tag_data["entity"] is None:
        topic_entity_tag_data["entity_type"] = None
    reference_curie = topic_entity_tag_data.pop("reference_curie", None)
    if reference_curie is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="reference_curie not within topic_entity_tag_data")
    reference_id = get_reference_id_from_curie_or_id(db, reference_curie)
    topic_entity_tag_data["reference_id"] = reference_id
    force_insertion = topic_entity_tag_data.pop("force_insertion", None)
    source: TopicEntityTagSourceModel = db.query(TopicEntityTagSourceModel).filter(
        TopicEntityTagSourceModel.topic_entity_tag_source_id == topic_entity_tag_data["topic_entity_tag_source_id"]
    ).one_or_none()
    if source is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cannot find the specified source")
    if source.secondary_data_provider.abbreviation == "SGD":
        check_and_set_sgd_display_tag(topic_entity_tag_data)
    else:
        check_and_set_species(topic_entity_tag_data)
    # check atp ID's validity
    atp_ids = [topic_entity_tag_data['topic'], topic_entity_tag_data['entity_type']]
    if 'display_tag' in topic_entity_tag_data:
        atp_ids.append(topic_entity_tag_data['display_tag'])
    atp_ids_filtered = [atp_id for atp_id in atp_ids if atp_id is not None]
    (valid_atp_ids, id_to_name) = check_atp_ids_validity(atp_ids_filtered)
    invalid_atp_ids = set(atp_ids_filtered) - valid_atp_ids
    if len(invalid_atp_ids) > 0:
        message = " ".join(f"{id} is not valid." for id in invalid_atp_ids if id is not None)
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"{message}")
    add_audited_object_users_if_not_exist(db, topic_entity_tag_data)
    duplicate_check_result = check_for_duplicate_tags(db, topic_entity_tag_data, reference_id, force_insertion)
    if duplicate_check_result is not None:
        return duplicate_check_result
    new_db_obj = TopicEntityTagModel(**topic_entity_tag_data)
    try:
        db.add(new_db_obj)
        db.flush()
        db.refresh(new_db_obj)
        if validate_on_insert:
            validate_tags(db=db, new_tag_obj=new_db_obj)
        return {
            "status": "success",
            "message": "New tag created successfully.",
            "topic_entity_tag_id": new_db_obj.topic_entity_tag_id
        }
    except (IntegrityError, HTTPException) as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"invalid request: {e}")
    # return topic_entity_tag_id


def calculate_validation_value_for_tag(topic_entity_tag_db_obj: TopicEntityTagModel, validation_type: str):
    validating_tags_values = []
    validating_tags_added_ids = set()
    validating_tags_to_add = [topic_entity_tag_db_obj]
    validating_tags_added_ids.add(topic_entity_tag_db_obj.topic_entity_tag_id)
    while len(validating_tags_to_add) > 0:
        validating_tag = validating_tags_to_add.pop()
        additional_validating_tags = [
            tag for tag in validating_tag.validated_by
            if tag.topic_entity_tag_source.validation_type == validation_type and tag.topic_entity_tag_id not in
            validating_tags_added_ids]
        additional_validating_tag_values = [tag.negated for tag in additional_validating_tags]
        additional_validating_tag_ids = [tag.topic_entity_tag_id for tag in additional_validating_tags]
        validating_tags_values.extend(additional_validating_tag_values)
        validating_tags_added_ids.update(additional_validating_tag_ids)
        validating_tags_to_add.extend(additional_validating_tags)
    if len(validating_tags_values) > 0:
        if topic_entity_tag_db_obj.topic_entity_tag_source.validation_type == validation_type:
            validating_tags_values.append(topic_entity_tag_db_obj.negated)
        if len(set(validating_tags_values)) == 1:
            return "validated_right" if topic_entity_tag_db_obj.negated == validating_tags_values[0] else \
                "validated_wrong"
        else:
            return "validation_conflict"
    elif topic_entity_tag_db_obj.topic_entity_tag_source.validation_type == validation_type:
        return "validated_right_self"
    return "not_validated"


def add_validation_values_to_tag(topic_entity_tag_db_obj: TopicEntityTagModel, tag_data_dict: Dict):
    tag_data_dict["validation_by_author"] = calculate_validation_value_for_tag(topic_entity_tag_db_obj,
                                                                               ATP_ID_SOURCE_AUTHOR)
    tag_data_dict["validation_by_professional_biocurator"] = calculate_validation_value_for_tag(topic_entity_tag_db_obj,
                                                                                                ATP_ID_SOURCE_CURATOR)


def add_list_of_users_who_validated_tag(topic_entity_tag_db_obj: TopicEntityTagModel, tag_data_dict: Dict):
    validating_tag: TopicEntityTagModel
    tag_data_dict["validating_users"] = list({validating_tag.created_by for validating_tag in
                                              topic_entity_tag_db_obj.validated_by})


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
    revalidate_all_tags(curie_or_reference_id=str(topic_entity_tag.reference_id))
    return {"message": "updated"}


def destroy_tag(db: Session, topic_entity_tag_id: int, mod_access: OktaAccess):
    topic_entity_tag: TopicEntityTagModel = db.query(TopicEntityTagModel).filter(
        TopicEntityTagModel.topic_entity_tag_id == topic_entity_tag_id).one_or_none()
    if topic_entity_tag is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"topic_entityTag with the topic_entity_tag_id {topic_entity_tag_id} "
                                   f"is not available")

    """
    If a tag is created by a curator via the API or UI, then `created_by` is set to `okta_user_id`.
    This allows us to set `created_by_mod` based on the mod to which `created_by` is associated,
    assuming person data is available in the database. However, if the tag is added by a script,
    `created_by` is set to `curator_id` (which is not an `okta_user_id`). In this case, we set
    `created_by_mod` based on the mod in the `topic_entity_tag_source` table.
    Currently, `created_by_mod` always defaults to the mod in the `topic_entity_tag_source` table,
    as we lack the database data to map each user's `okta_id` to a mod.
    """
    user_mod = OKTA_ACCESS_MOD_ABBR[mod_access]
    created_by_mod = topic_entity_tag.topic_entity_tag_source.secondary_data_provider.abbreviation
    """
    fixed HTTP_403_Forbidden to HTTP_404_NOT_FOUND in following code since mypy complains
    about "HTTP_403_Forbidden" not found
    """
    if mod_access != OktaAccess.ALL_ACCESS and user_mod != created_by_mod:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"You do not have permission to delete topic_entity_tag with the topic_entity_tag_id {topic_entity_tag_id} created by {created_by_mod}")

    db.delete(topic_entity_tag)
    db.commit()


def validate_tags_already_in_db_with_positive_tag(db, new_tag_obj: TopicEntityTagModel, related_tags_in_db):
    # 1. new tag positive, existing tag positive = validate existing (right) if existing is more generic
    # 2. new tag positive, existing tag negative = validate existing (wrong) if existing is more generic
    more_generic_topics = set(get_ancestors(onto_node=new_tag_obj.topic))
    more_generic_topics.add(new_tag_obj.topic)
    tag_in_db: TopicEntityTagModel
    for tag_in_db in related_tags_in_db:
        if tag_in_db.topic in more_generic_topics:
            if tag_in_db.entity_type is None or (tag_in_db.entity_type == new_tag_obj.entity_type
                                                 and tag_in_db.entity == new_tag_obj.entity):
                if tag_in_db.species is None or tag_in_db.species == new_tag_obj.species:
                    add_validation_to_db(db, tag_in_db, new_tag_obj)
    # validate pure entity-only tags if the new tag is a mixed topic + entity tag for the same entity
    if new_tag_obj.entity is not None and new_tag_obj.entity_type != new_tag_obj.topic:
        for tag_in_db in related_tags_in_db:
            if (tag_in_db.topic == tag_in_db.entity_type == new_tag_obj.entity_type
                    and new_tag_obj.entity == tag_in_db.entity):
                add_validation_to_db(db, tag_in_db, new_tag_obj)


def validate_tags_already_in_db_with_negative_tag(db, new_tag_obj: TopicEntityTagModel, related_tags_in_db):
    # 1. new tag negative, existing tag positive = validate existing (wrong) if existing is more specific
    # 2. new tag negative, existing tag negative = validate existing (right) if existing is more specific
    more_specific_topics = set(get_descendants(onto_node=new_tag_obj.topic))
    more_specific_topics.add(new_tag_obj.topic)
    tag_in_db: TopicEntityTagModel
    for tag_in_db in related_tags_in_db:
        if tag_in_db.topic in more_specific_topics:
            if new_tag_obj.entity_type is None or (tag_in_db.entity_type == new_tag_obj.entity_type
                                                   and tag_in_db.entity == new_tag_obj.entity):
                if new_tag_obj.species is None or tag_in_db.species == new_tag_obj.species:
                    add_validation_to_db(db, tag_in_db, new_tag_obj)
    # if the new tag is a pure entity-only tag and there are mixed topic + entity tags with the same entity
    # validate existing tag only if it is positive
    if new_tag_obj.topic == new_tag_obj.entity_type:
        for tag_in_db in related_tags_in_db:
            if (tag_in_db.negated is False and tag_in_db.entity_type != tag_in_db.topic
                    and new_tag_obj.entity_type == tag_in_db.entity_type
                    and new_tag_obj.entity == tag_in_db.entity):
                add_validation_to_db(db, tag_in_db, new_tag_obj)


def validate_new_tag_with_existing_tags(db, new_tag_obj: TopicEntityTagModel, related_validating_tags_in_db):
    # 1. new tag positive, existing tag positive = validate new tag (right) if existing is more specific
    # 2. new tag negative, existing tag positive = validate new tag (wrong) if existing is more specific
    # 3. new tag positive, existing tag negative = validate new tag (wrong) if existing is more generic
    # 4. new tag negative, existing tag negative = validate new tag (right) if existing is more generic
    more_specific_topics = set(get_descendants(onto_node=new_tag_obj.topic))
    more_specific_topics.add(new_tag_obj.topic)
    more_generic_topics = set(get_ancestors(onto_node=new_tag_obj.topic))
    more_generic_topics.add(new_tag_obj.topic)
    tag_in_db: TopicEntityTagModel
    for tag_in_db in related_validating_tags_in_db:
        if tag_in_db.negated is False and tag_in_db.topic in more_specific_topics:
            if new_tag_obj.entity_type is None or (tag_in_db.entity_type == new_tag_obj.entity_type
                                                   and tag_in_db.entity == new_tag_obj.entity):
                if new_tag_obj.species is None or tag_in_db.species == new_tag_obj.species:
                    add_validation_to_db(db, new_tag_obj, tag_in_db)
        elif tag_in_db.negated is True and tag_in_db.topic in more_generic_topics:
            if tag_in_db.entity_type is None or (tag_in_db.entity_type == new_tag_obj.entity_type
                                                 and tag_in_db.entity == new_tag_obj.entity):
                if tag_in_db.species is None or tag_in_db.species == new_tag_obj.species:
                    add_validation_to_db(db, new_tag_obj, tag_in_db)
    # if the new tag is a pure entity-only tag and there are mixed topic + entity tags with the same entity
    # validate positive or negative new tag only if existing is positive
    if new_tag_obj.topic == new_tag_obj.entity_type:
        for tag_in_db in related_validating_tags_in_db:
            if (tag_in_db.entity_type != tag_in_db.topic and new_tag_obj.entity_type == tag_in_db.entity_type
                    and new_tag_obj.entity == tag_in_db.entity and tag_in_db.negated is False):
                add_validation_to_db(db, new_tag_obj, tag_in_db)
    # if the new tag is a mixed topic + entity tag and there are pure entity-only tags with the same entity
    # validate only positive new tag if existing is negative
    if new_tag_obj.negated is False and new_tag_obj.entity is not None and new_tag_obj.entity_type != new_tag_obj.topic:
        for tag_in_db in related_validating_tags_in_db:
            if (tag_in_db.negated is True and tag_in_db.topic == tag_in_db.entity_type == new_tag_obj.entity_type
                    and new_tag_obj.entity == tag_in_db.entity):
                add_validation_to_db(db, new_tag_obj, tag_in_db)


def add_validation_to_db(db: Session, validated_tag: TopicEntityTagModel, validating_tag: TopicEntityTagModel):
    db.execute(f"INSERT INTO topic_entity_tag_validation (validated_topic_entity_tag_id, "
               f"validating_topic_entity_tag_id) VALUES ({validated_tag.topic_entity_tag_id}, "
               f"{validating_tag.topic_entity_tag_id})")


def validate_tags(db: Session, new_tag_obj: TopicEntityTagModel, validate_new_tag: bool = True,
                  commit_changes: bool = True):
    related_tags_in_db = db.query(
        TopicEntityTagModel.topic_entity_tag_id,
        TopicEntityTagModel.topic,
        TopicEntityTagModel.entity_type,
        TopicEntityTagModel.entity,
        TopicEntityTagModel.species,
        TopicEntityTagModel.negated,
        TopicEntityTagSourceModel.validation_type
    ).join(
        TopicEntityTagSourceModel, TopicEntityTagModel.topic_entity_tag_source
    ).filter(
        TopicEntityTagModel.topic_entity_tag_id != new_tag_obj.topic_entity_tag_id,
        TopicEntityTagModel.reference_id == new_tag_obj.reference_id,
        TopicEntityTagSourceModel.secondary_data_provider_id == new_tag_obj.topic_entity_tag_source.secondary_data_provider_id,
        TopicEntityTagModel.negated.isnot(None)
    ).all()
    # The current tag can validate existing tags or be validated by other tags only if it has a True or False negated
    # value
    if len(related_tags_in_db) == 0 or new_tag_obj.negated is None:
        return
    # Validate existing tags
    if new_tag_obj.topic_entity_tag_source.validation_type is not None:
        if new_tag_obj.negated is False:
            validate_tags_already_in_db_with_positive_tag(db, new_tag_obj, related_tags_in_db)
        else:
            validate_tags_already_in_db_with_negative_tag(db, new_tag_obj, related_tags_in_db)
    # Validate current tag with existing ones
    if validate_new_tag:
        related_validating_tags_in_db = [related_tag for related_tag in related_tags_in_db if
                                         related_tag.validation_type is not None]
        validate_new_tag_with_existing_tags(db, new_tag_obj, related_validating_tags_in_db)
    if commit_changes:
        db.commit()


def revalidate_all_tags(email: str = None, delete_all_first: bool = False, curie_or_reference_id: str = None):
    engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
    new_session = sessionmaker(bind=engine, autoflush=True)
    db = new_session()
    reference_query_filter = ""
    query_tags = db.query(TopicEntityTagModel)
    if curie_or_reference_id:
        delete_all_first = True
        reference_id = int(curie_or_reference_id) if curie_or_reference_id.isdigit() else None
        if not reference_id:
            reference_id = db.query(ReferenceModel.reference_id).filter(ReferenceModel.curie == curie_or_reference_id)
        all_tag_ids_for_reference = [res[0] for res in db.query(
            TopicEntityTagModel.topic_entity_tag_id).filter(TopicEntityTagModel.reference_id == reference_id).all()]
        if not all_tag_ids_for_reference:
            return
        all_tag_ids_str = [str(tag_id) for tag_id in all_tag_ids_for_reference]
        reference_query_filter = (f" WHERE validating_topic_entity_tag_id IN ({', '.join(all_tag_ids_str)}) "
                                  f"OR validated_topic_entity_tag_id IN ({', '.join(all_tag_ids_str)})")
        query_tags = query_tags.filter(TopicEntityTagModel.topic_entity_tag_id.in_(all_tag_ids_for_reference))
    if delete_all_first:
        db.execute("DELETE FROM topic_entity_tag_validation" + reference_query_filter)
        db.commit()
    for tag_counter, tag in enumerate(query_tags.all()):
        if not delete_all_first:
            db.execute(f"DELETE FROM topic_entity_tag_validation "
                       f"WHERE validating_topic_entity_tag_id = {tag.topic_entity_tag_id}")
        validate_tags(db=db, new_tag_obj=tag, validate_new_tag=False, commit_changes=False)
        if tag_counter % 200 == 0:
            db.commit()
    db.commit()
    db.close()

    if email:
        email_recipients = email
        sender_email = environ.get('SENDER_EMAIL', None)
        sender_password = environ.get('SENDER_PASSWORD', None)
        reply_to = environ.get('REPLY_TO', sender_email)
        email_body = "Finished re-validating all tags"
        if curie_or_reference_id:
            email_body += " for reference " + str(curie_or_reference_id)
        send_email("Alliance ABC notification: all tags re-validated", email_recipients, email_body, sender_email,
                   sender_password, reply_to)


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
    del source_data["secondary_data_provider_id"]
    source_data["secondary_data_provider_abbreviation"] = source.secondary_data_provider.abbreviation
    return source_data


def filter_tet_data_by_column(query, column_name, values):
    column = getattr(TopicEntityTagModel, column_name, None)
    query = query.filter(column.in_(values))
    return query


def check_for_duplicate_tags(db: Session, topic_entity_tag_data: dict, reference_id: int, force_insertion: bool = False):
    new_tag_data = copy.copy(topic_entity_tag_data)
    new_tag_data.pop('date_created', None)
    date_updated: str = new_tag_data.pop('date_updated', '')
    note = new_tag_data.pop('note', None)
    created_by_user = get_default_user_value()

    if new_tag_data.get('created_by', None) is None:
        new_tag_data['created_by'] = created_by_user
    if new_tag_data.get('updated_by', None) is None:
        new_tag_data['updated_by'] = created_by_user

    existing_tag = db.query(TopicEntityTagModel).filter_by(**new_tag_data).first()
    if existing_tag:
        existing_date_updated = existing_tag.date_updated
        tag_data = get_tet_with_names(db, tet=new_tag_data, curie_or_reference_id=str(reference_id))
        if note:
            tag_data['note'] = note
        if note == existing_tag.note or note is None:
            return {
                "status": "exists",
                "message": "The tag already exists in the database.",
                "data": tag_data
            }
        else:
            message = "The new note was added to the previously empty note column for the tag already in the database."
            new_note = note
            if existing_tag.note is not None:
                message = "The new note was appended to the existing one for the tag already in the database."
                new_note = existing_tag.note + " | " + note
            try:
                existing_tag.note = new_note
                existing_tag.updated_by = created_by_user
                db.add(existing_tag)
                db.commit()
                if date_updated and date_parser.parse(date_updated) > existing_tag.date_updated:
                    existing_tag.date_updated = date_updated
                else:
                    existing_tag.date_updated = existing_date_updated
                db.commit()
                return {
                    "status": "exists",
                    "message": message,
                    "data": tag_data
                }
            except (IntegrityError, HTTPException) as e:
                db.rollback()
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail=f"invalid request: {e}")

    if force_insertion:
        return
    new_tag_data_wo_creator = copy.copy(new_tag_data)
    new_tag_data_wo_creator.pop('created_by')
    new_tag_data_wo_creator.pop('updated_by')
    existing_tag = db.query(TopicEntityTagModel).filter_by(**new_tag_data_wo_creator).first()
    if existing_tag:
        tag_data = get_tet_with_names(db, tet=new_tag_data, curie_or_reference_id=str(reference_id))
        if note:
            tag_data['note'] = note
        tag_data['topic_entity_tag_id'] = existing_tag.topic_entity_tag_id
        if existing_tag.note == note or note is None:
            return {
                "status": f"exists: {existing_tag.created_by} | {existing_tag.note}",
                "message": "The tag, created by another curator, already exists in the database.",
                "data": tag_data
            }
        else:
            message = "The tag without a note, created by another curator, already exists in the database."
            if existing_tag.note:
                message = "The tag with a different note, created by another curator, already exists in the database."
            note_in_db = existing_tag.note if existing_tag.note else ''
            return {
                "status": f"exists: {existing_tag.created_by} | {note_in_db}",
                "message": message,
                "data": tag_data
            }

    # if no duplicates found, return None
    return None


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
            if sort_by in ['topic', 'entity_type', 'species', 'display_tag', 'entity']:
                column_property = getattr(TopicEntityTagModel, sort_by, None)
                column = column_property.property.columns[0]
                order_expression = case([(column.is_(None), 1 if desc_sort else 0)], else_=0 if desc_sort else 1)
                sorted_column_values = get_sorted_column_values(reference_id, db,
                                                                sort_by, desc_sort)
                curie_ordering = case({curie: index for index, curie in enumerate(sorted_column_values)},
                                      value=getattr(TopicEntityTagModel, sort_by))
                query = query.order_by(order_expression, curie_ordering, TopicEntityTagModel.topic_entity_tag_id)
            else:
                # check if the column exists in TopicEntityTagModel
                if hasattr(TopicEntityTagModel, sort_by):
                    column_property = getattr(TopicEntityTagModel, sort_by)
                elif hasattr(TopicEntityTagSourceModel, sort_by):
                    column_property = getattr(TopicEntityTagSourceModel, sort_by)
                    # explicitly join the topic_entity_tag_source table for sorting
                    query = query.join(TopicEntityTagSourceModel,
                                       TopicEntityTagModel.topic_entity_tag_source_id == TopicEntityTagSourceModel.topic_entity_tag_source_id)
                elif sort_by == 'secondary_data_provider':
                    column_property_name = "abbreviation"
                    column_property = getattr(ModModel, column_property_name)
                    query = query.join(
                        TopicEntityTagSourceModel,
                        TopicEntityTagModel.topic_entity_tag_source_id == TopicEntityTagSourceModel.topic_entity_tag_source_id)
                    query = query.join(
                        ModModel, TopicEntityTagSourceModel.secondary_data_provider_id == ModModel.mod_id)
                else:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                        detail=f"The column '{sort_by}' does not exist in either TopicEntityTagModel "
                                               f"or TopicEntityTagSourceModel.")
                if column_property is None:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                        detail=f"Failed to get the column '{sort_by}' from the models.")

                # check for None values and order accordingly
                order_expression = case([(column_property.is_(None), 1 if desc_sort else 0)], else_=0 if desc_sort else 1)
                query = query.order_by(order_expression, column_property.desc() if desc_sort else column_property,
                                       TopicEntityTagModel.topic_entity_tag_id)

        mod_id_to_mod = dict([(x.mod_id, x.abbreviation) for x in db.query(ModModel).all()])
        all_tet = []
        for tet in query.offset((page - 1) * page_size if page_size else None).limit(page_size).all():
            tet_data = jsonable_encoder(vars(tet), exclude={"validated_by"})
            if "validated_by" in tet_data:
                del tet_data["validated_by"]
            add_validation_values_to_tag(tet, tet_data)
            add_list_of_users_who_validated_tag(tet, tet_data)
            tet_data["topic_entity_tag_source"]["secondary_data_provider_abbreviation"] = mod_id_to_mod[
                tet.topic_entity_tag_source.secondary_data_provider_id]
            all_tet.append(tet_data)
        curie_to_name = get_curie_to_name_from_all_tets(db, curie_or_reference_id)
        return [get_tet_with_names(db, tag, curie_to_name) for tag in all_tet]


def get_curie_to_name_from_all_tets(db: Session, curie_or_reference_id: str):
    reference_id = get_reference_id_from_curie_or_id(db, curie_or_reference_id)
    ref_related_tets = db.query(TopicEntityTagModel).filter(TopicEntityTagModel.reference_id == reference_id).all()
    all_atp_terms = set()
    entity_id_validation_entity_type_entities: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
    all_entity_curies = set()
    tag_species = set()
    for tet in ref_related_tets:
        all_atp_terms.add(tet.topic)
        if tet.display_tag is not None:
            all_atp_terms.add(tet.display_tag)
        if tet.entity_type is not None:
            all_atp_terms.add(tet.entity_type)
            if tet.entity_id_validation:
                entity_id_validation_entity_type_entities[tet.entity_id_validation][tet.entity_type].add(
                    tet.entity)
                all_entity_curies.add(tet.entity)
        if tet.species:
            tag_species.add(tet.species)
    entity_curie_to_name = get_map_ateam_curies_to_names(curies_category="atpterm", curies=list(all_atp_terms))
    entity_curie_to_name.update(get_map_ateam_curies_to_names(curies_category="ncbitaxonterm",
                                                              curies=list(tag_species)))
    for entity_id_validation, entity_type_curies_dict in entity_id_validation_entity_type_entities.items():
        for entity_type, curies in entity_type_curies_dict.items():
            entity_type_name = entity_curie_to_name[entity_type].replace("species", "ncbitaxonterm")
            if "construct" in entity_type_name:
                entity_type_name = "transgenicconstruct"
            entity_curie_to_name.update(get_map_entity_curies_to_names(
                entity_id_validation=entity_id_validation,
                curies_category=entity_type_name,
                curies=list(curies)))
    for curie_without_name in (all_entity_curies | all_atp_terms) - set(entity_curie_to_name.keys()):
        entity_curie_to_name[curie_without_name] = curie_without_name
    return entity_curie_to_name


def get_tet_with_names(db: Session, tet, curie_to_name_mapping: Dict = None, curie_or_reference_id: str = None):
    if curie_to_name_mapping is None:
        curie_to_name_mapping = get_curie_to_name_from_all_tets(db, str(curie_or_reference_id))
    new_tet = {}
    for field in tet:
        new_tet[field] = tet[field]
        if field in TET_CURIE_FIELDS:
            curie = tet[field]
            new_field = f"{field}_name"
            new_tet[new_field] = curie_to_name_mapping.get(curie, curie)
    return new_tet


def show_source_by_name(db: Session, source_evidence_assertion: str, source_method: str,
                        data_provider: str, secondary_data_provider_abbreviation: str):
    secondary_data_provider = db.query(ModModel.mod_id).filter(
        ModModel.abbreviation == secondary_data_provider_abbreviation).one_or_none()
    if secondary_data_provider is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Cannot find the specified secondary data provider")
    source = db.query(TopicEntityTagSourceModel).filter(
        and_(
            TopicEntityTagSourceModel.source_evidence_assertion == source_evidence_assertion,
            TopicEntityTagSourceModel.source_method == source_method,
            TopicEntityTagSourceModel.data_provider == data_provider,
            TopicEntityTagSourceModel.secondary_data_provider_id == secondary_data_provider.mod_id
        )
    ).one_or_none()
    if source is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cannot find the specified Source")
    source_data = jsonable_encoder(source)
    del source_data["secondary_data_provider_id"]
    source_data["secondary_data_provider_abbreviation"] = secondary_data_provider_abbreviation
    return source_data
