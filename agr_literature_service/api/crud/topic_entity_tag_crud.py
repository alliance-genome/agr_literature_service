"""
topic_entity_tag_crud.py
===========================
"""
import copy
import logging
from typing import Optional
from collections import defaultdict
from os import environ
from typing import Dict, Set
from datetime import datetime, timedelta

from dateutil import parser as date_parser
from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import case, and_, create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload, sessionmaker, noload

from agr_literature_service.api.crud.topic_entity_tag_utils import get_reference_id_from_curie_or_id, \
    get_source_from_db, add_source_obj_to_db_session, get_sorted_column_values, \
    check_and_set_sgd_display_tag, check_and_set_species, add_audited_object_users_if_not_exist, \
    get_ancestors, get_descendants, get_map_entity_curies_to_names, \
    id_to_name_cache, get_map_ateam_curies_to_names, get_mod_id_from_mod_abbreviation, \
    get_user_display_name_map
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.models import (
    TopicEntityTagModel, WorkflowTagModel, ModCorpusAssociationModel,
    ReferenceModel, TopicEntityTagSourceModel, ModModel
)
from agr_literature_service.api.models.ml_model_model import MLModel
from agr_literature_service.api.crud.workflow_tag_crud import get_workflow_tags_from_process
from agr_literature_service.api.models.audited_model import (
    get_default_user_value,
    disable_set_updated_by_onupdate,
    disable_set_date_updated_onupdate
)
from agr_literature_service.api.routers.okta_utils import OktaAccess, OKTA_ACCESS_MOD_ABBR
from agr_literature_service.api.schemas.topic_entity_tag_schemas import (TopicEntityTagSchemaPost,
                                                                         TopicEntityTagSourceSchemaUpdate,
                                                                         TopicEntityTagSourceSchemaCreate,
                                                                         TopicEntityTagSchemaUpdate)
from agr_literature_service.lit_processing.utils.email_utils import send_email
from agr_literature_service.api.crud.ateam_db_helpers import atp_return_invalid_ids
from agr_literature_service.api.crud.user_utils import map_to_user_id

logger = logging.getLogger(__name__)


ATP_ID_SOURCE_AUTHOR = "author"
ATP_ID_SOURCE_CURATOR = "professional_biocurator"

TET_CURIE_FIELDS = ['topic', 'entity_type', 'display_tag', 'entity', 'species']
TET_SOURCE_CURIE_FIELDS = ['source_evidence_assertion']


def create_tag(db: Session, topic_entity_tag: TopicEntityTagSchemaPost, validate_on_insert: bool = True) -> Dict:      # noqa: C901
    logger.info("Starting create_tag")
    topic_entity_tag_data = jsonable_encoder(topic_entity_tag)
    for k in ("created_by", "updated_by"):
    if k in topic_entity_tag_data:
        mapped = map_to_user_id(topic_entity_tag_data[k], db)
        if mapped is None:
            topic_entity_tag_data.pop(k)
        else:
            topic_entity_tag_data[k] = mapped
    if topic_entity_tag_data["entity"] is None:
        topic_entity_tag_data["entity_type"] = None
    reference_curie = topic_entity_tag_data.pop("reference_curie", None)
    if reference_curie is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="reference_curie not within topic_entity_tag_data")
    logger.info("Getting reference_id from curie")
    reference_id = get_reference_id_from_curie_or_id(db, reference_curie)
    topic_entity_tag_data["reference_id"] = reference_id
    force_insertion = topic_entity_tag_data.pop("force_insertion", None)
    index_wft = topic_entity_tag_data.pop("index_wft", None)
    logger.info("Querying topic_entity_tag_source")
    source: TopicEntityTagSourceModel = db.query(TopicEntityTagSourceModel).filter(
        TopicEntityTagSourceModel.topic_entity_tag_source_id == topic_entity_tag_data["topic_entity_tag_source_id"]
    ).one_or_none()
    if source is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cannot find the specified source")
    logger.info("Setting display_tag/species based on data provider")
    if source.secondary_data_provider.abbreviation == "SGD":
        check_and_set_sgd_display_tag(topic_entity_tag_data)
        if topic_entity_tag_data['topic'] == topic_entity_tag_data['entity_type']:
            topic_entity_tag_data['data_novelty'] = 'ATP:0000334'
        else:
            topic_entity_tag_data['data_novelty'] = 'ATP:0000335'
    else:
        if topic_entity_tag_data.get('data_novelty') is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="The 'data_novelty' is not passed in")
        check_and_set_species(topic_entity_tag_data)
    # check atp ID's validity
    logger.info("Validating ATP IDs")
    atp_ids = [topic_entity_tag_data['topic'], topic_entity_tag_data['entity_type']]
    if 'display_tag' in topic_entity_tag_data and topic_entity_tag_data['display_tag'] is not None:
        atp_ids.append(topic_entity_tag_data['display_tag'])
    atp_ids_filtered = [atp_id for atp_id in atp_ids if atp_id is not None]
    # (valid_atp_ids, id_to_name) = check_atp_ids_validity(atp_ids_filtered)
    # invalid_atp_ids = set(atp_ids_filtered) - valid_atp_ids
    invalid_atp_ids = atp_return_invalid_ids(atp_ids_filtered)
    if len(invalid_atp_ids) > 0:
        message = " ".join(f"{id} is not valid." for id in invalid_atp_ids if id is not None)
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"{message}")

    # Validate ml_model_id if provided
    if 'ml_model_id' in topic_entity_tag_data and topic_entity_tag_data['ml_model_id']:
        logger.info("Validating ML model ID")
        ml_model_id = topic_entity_tag_data['ml_model_id']
        ml_model = db.query(MLModel).filter(MLModel.ml_model_id == ml_model_id).first()
        if not ml_model:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"ML model with ID {ml_model_id} not found"
            )
        topic_entity_tag_data["ml_model_id"] = ml_model.ml_model_id

    logger.info("Adding audited object users")
    add_audited_object_users_if_not_exist(db, topic_entity_tag_data)
    logger.info("Checking for duplicate tags")
    duplicate_check_result = check_for_duplicate_tags(db, topic_entity_tag_data, source, reference_id, force_insertion)
    if duplicate_check_result is not None:
        logger.info("Duplicate tag found, returning early")
        return duplicate_check_result
    new_db_obj = TopicEntityTagModel(**topic_entity_tag_data)

    try:
        logger.info("Adding new tag to database")
        db.add(new_db_obj)
        db.commit()
        logger.info("Tag committed, refreshing with topic_entity_tag_source")
        # Optimize: Eagerly load topic_entity_tag_source to avoid lazy loading during validation
        db.refresh(new_db_obj, ['topic_entity_tag_source'])

        logger.info("Adding paper to MOD if needed")
        mod_id = get_mod_id_from_mod_abbreviation(db, source.secondary_data_provider.abbreviation)
        add_paper_to_mod_if_not_already(db, reference_id, mod_id)
        logger.info("Updating manual indexing workflow tag")
        update_manual_indexing_workflow_tag(db, mod_id, reference_id, index_wft)
        if validate_on_insert:
            logger.info("Starting tag validation")
            validate_tags(db=db, new_tag_obj=new_db_obj)
            logger.info("Tag validation completed")
        logger.info("create_tag completed successfully")
        return {
            "status": "success",
            "message": "New tag created successfully.",
            "topic_entity_tag_id": new_db_obj.topic_entity_tag_id
        }
    except (IntegrityError, HTTPException) as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"invalid request: {e}")
    return {"topic_entity_tag_id": new_db_obj.topic_entity_tag_id}


def set_indexing_status_for_no_tet_data(db: Session, mod_abbreviation, reference_curie, uid):

    reference_id = get_reference_id_from_curie_or_id(db, reference_curie)
    mod_id = get_mod_id_from_mod_abbreviation(db, mod_abbreviation)
    add_paper_to_mod_if_not_already(db, reference_id, mod_id)
    update_manual_indexing_workflow_tag(db, mod_id, reference_id, "ATP:0000275", uid)


def update_manual_indexing_workflow_tag(db: Session, mod_id, reference_id, index_wft, uid=None):

    if index_wft is None:
        return
    all_manual_indexing_wf_tags = get_workflow_tags_from_process("ATP:0000273")
    wft = db.query(WorkflowTagModel).filter(
        and_(
            WorkflowTagModel.workflow_tag_id.in_(all_manual_indexing_wf_tags),
            WorkflowTagModel.reference_id == reference_id,
            WorkflowTagModel.mod_id == mod_id
        )
    ).one_or_none()
    if wft is None:
        if uid:
            wft_obj = WorkflowTagModel(reference_id=reference_id,
                                       mod_id=mod_id,
                                       created_by=uid,
                                       updated_by=uid,
                                       workflow_tag_id=index_wft)
        else:
            wft_obj = WorkflowTagModel(reference_id=reference_id,
                                       mod_id=mod_id,
                                       workflow_tag_id=index_wft)
        db.add(wft_obj)
        db.commit()
    elif wft.workflow_tag_id != index_wft:
        wft.workflow_tag_id = index_wft
        if uid and uid != wft.updated_by:
            wft.updated_by = uid
        db.add(wft)
        db.commit()


def add_paper_to_mod_if_not_already(db: Session, reference_id, mod_id):

    add_wft_141_bool = False
    mca_db_obj = db.query(ModCorpusAssociationModel).filter_by(
        mod_id=mod_id, reference_id=reference_id).one_or_none()
    if mca_db_obj is None:
        add_wft_141_bool = True
        new_mca = ModCorpusAssociationModel(reference_id=reference_id,
                                            mod_id=mod_id,
                                            corpus=True,
                                            mod_corpus_sort_source='manual_creation')
        db.add(new_mca)
    elif not mca_db_obj.corpus:
        add_wft_141_bool = True
        mca_db_obj.corpus = True
        mca_db_obj.mod_corpus_sort_source = "manual_creation"
        db.add(mca_db_obj)
    if add_wft_141_bool:
        new_wft = WorkflowTagModel(reference_id=reference_id,
                                   mod_id=mod_id,
                                   workflow_tag_id='ATP:0000141')
        db.add(new_wft)
    db.commit()


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


def add_list_of_users_who_validated_tag(topic_entity_tag_db_obj: TopicEntityTagModel, tag_data_dict: Dict):
    validating_tag: TopicEntityTagModel
    tag_data_dict["validating_users"] = list({validating_tag.created_by for validating_tag in
                                              topic_entity_tag_db_obj.validated_by})


def add_list_of_validating_tag_ids(topic_entity_tag_db_obj: TopicEntityTagModel, tag_data_dict: Dict):
    validating_tag: TopicEntityTagModel
    tag_data_dict["validating_tags"] = list({validating_tag.topic_entity_tag_id for validating_tag in
                                            topic_entity_tag_db_obj.validated_by})


def show_tag(db: Session, topic_entity_tag_id: int):      # noqa: C901
    topic_entity_tag: Optional[TopicEntityTagModel] = db.query(TopicEntityTagModel).get(topic_entity_tag_id)
    if not topic_entity_tag:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"topic_entityTag with the topic_entity_tag_id {topic_entity_tag_id} "
                                   f"is not available")
    topic_entity_tag_data = jsonable_encoder(topic_entity_tag)
    if topic_entity_tag_data.get("reference_id"):
        topic_entity_tag_data["reference_curie"] = db.query(ReferenceModel).filter(
            ReferenceModel.reference_id == topic_entity_tag_data["reference_id"]).first().curie
        del topic_entity_tag_data["reference_id"]
    topic_entity_tag_data[
        "topic_entity_tag_source_id"] = topic_entity_tag.topic_entity_tag_source.topic_entity_tag_source_id
    if topic_entity_tag_data.get("entity"):
        name = id_to_name_cache.get(topic_entity_tag_data["entity"])
        if name:
            topic_entity_tag_data["entity_name"] = name
    if topic_entity_tag.validated_by:
        add_list_of_users_who_validated_tag(topic_entity_tag, topic_entity_tag_data)
        add_list_of_validating_tag_ids(topic_entity_tag, topic_entity_tag_data)
    if 'ml_model_id' in topic_entity_tag_data:
        ml = db.query(MLModel).get(topic_entity_tag_data["ml_model_id"])
        if ml:
            topic_entity_tag_data["ml_model_version"] = ml.version_num

    # --- Map users.id -> person.display_name where users.person_id is not null ---
    user_ids: Set[str] = set()

    # top-level created_by / updated_by
    if topic_entity_tag_data.get("created_by"):
        user_ids.add(topic_entity_tag_data["created_by"])
    if topic_entity_tag_data.get("updated_by"):
        user_ids.add(topic_entity_tag_data["updated_by"])

    # nested source created_by / updated_by
    if topic_entity_tag_data.get("topic_entity_tag_source"):
        src = topic_entity_tag_data["topic_entity_tag_source"]
        if src.get("created_by"):
            user_ids.add(src["created_by"])
        if src.get("updated_by"):
            user_ids.add(src["updated_by"])

    # validating_users list (derived from validated_by.created_by)
    validating_user_ids = topic_entity_tag_data.get("validating_users") or []
    for uid in validating_user_ids:
        if uid:
            user_ids.add(uid)

    # Build the map and apply it
    id_to_display = get_user_display_name_map(db, user_ids)

    # Replace top-level created_by/updated_by
    for k in ("created_by", "updated_by"):
        uid = topic_entity_tag_data.get(k)
        if uid and uid in id_to_display:
            topic_entity_tag_data[k] = id_to_display[uid]

    # Replace nested source created_by/updated_by
    if topic_entity_tag_data.get("topic_entity_tag_source"):
        for k in ("created_by", "updated_by"):
            uid = topic_entity_tag_data["topic_entity_tag_source"].get(k)
            if uid and uid in id_to_display:
                topic_entity_tag_data["topic_entity_tag_source"][k] = id_to_display[uid]

    # Replace validating_users list items with display names where available
    if validating_user_ids:
        topic_entity_tag_data["validating_users"] = [
            id_to_display.get(uid, uid) for uid in validating_user_ids
        ]

    return topic_entity_tag_data


def patch_tag(db: Session, topic_entity_tag_id: int, patch_data: TopicEntityTagSchemaUpdate):
    topic_entity_tag: TopicEntityTagModel = db.query(TopicEntityTagModel).filter(
        TopicEntityTagModel.topic_entity_tag_id == topic_entity_tag_id).one_or_none()
    if topic_entity_tag is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"topic_entityTag with the topic_entity_tag_id {topic_entity_tag_id} "
                                   f"is not available")
    patch_data_dict = patch_data.model_dump(exclude_unset=True)
    if patch_data_dict.get("created_by"):
        patch_data_dict["created_by"] = map_to_user_id(patch_data_dict["created_by"], db)
    if patch_data_dict.get("updated_by"):
        patch_data_dict["updated_by"] = map_to_user_id(patch_data_dict["updated_by"], db)
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

    reference_id = topic_entity_tag.reference_id
    db.delete(topic_entity_tag)
    db.commit()
    revalidate_all_tags(curie_or_reference_id=str(reference_id), delete_all_first=False, validation_values_only=False)


def validate_tags_already_in_db_with_positive_tag(db, new_tag_obj: TopicEntityTagModel, related_tags_in_db,
                                                  calculate_validation_values: bool = True):
    # 1. new tag positive, existing tag positive = validate existing (right) if existing is more generic
    # 2. new tag positive, existing tag negative = validate existing (wrong) if existing is more generic
    more_generic_topics = set(get_ancestors(onto_node=new_tag_obj.topic))  # type: ignore
    more_generic_topics.add(new_tag_obj.topic)
    more_generic_novelty = set(get_ancestors(new_tag_obj.data_novelty))
    more_generic_novelty.add(new_tag_obj.data_novelty)
    tag_in_db: TopicEntityTagModel
    for tag_in_db in related_tags_in_db:
        if tag_in_db.topic in more_generic_topics:
            if tag_in_db.entity_type is None or (tag_in_db.entity_type == new_tag_obj.entity_type
                                                 and tag_in_db.entity == new_tag_obj.entity):
                if tag_in_db.species is None or tag_in_db.species == new_tag_obj.species:
                    # Check data novelty
                    if tag_in_db.data_novelty in more_generic_novelty:
                        add_validation_to_db(db, tag_in_db, new_tag_obj,
                                             calculate_validation_values=calculate_validation_values)
    # validate pure entity-only tags if the new tag is a mixed topic + entity tag for the same entity
    if new_tag_obj.entity is not None and new_tag_obj.entity_type != new_tag_obj.topic:
        for tag_in_db in related_tags_in_db:
            if (tag_in_db.topic == tag_in_db.entity_type == new_tag_obj.entity_type
                    and new_tag_obj.entity == tag_in_db.entity):
                if tag_in_db.data_novelty in more_generic_novelty:
                    add_validation_to_db(db, tag_in_db, new_tag_obj,
                                         calculate_validation_values=calculate_validation_values)


def validate_tags_already_in_db_with_negative_tag(db, new_tag_obj: TopicEntityTagModel, related_tags_in_db,
                                                  calculate_validation_values: bool = True):
    # 1. new tag negative, existing tag positive = validate existing (wrong) if existing is more specific
    # 2. new tag negative, existing tag negative = validate existing (right) if existing is more specific
    more_specific_topics = set(get_descendants(onto_node=new_tag_obj.topic))  # type: ignore
    more_specific_topics.add(new_tag_obj.topic)
    more_specific_novelty = set(get_descendants(new_tag_obj.data_novelty))
    more_specific_novelty.add(new_tag_obj.data_novelty)
    tag_in_db: TopicEntityTagModel
    for tag_in_db in related_tags_in_db:
        if tag_in_db.topic in more_specific_topics:
            if new_tag_obj.entity_type is None or (tag_in_db.entity_type == new_tag_obj.entity_type
                                                   and tag_in_db.entity == new_tag_obj.entity):
                if new_tag_obj.species is None or tag_in_db.species == new_tag_obj.species:
                    if tag_in_db.data_novelty in more_specific_novelty:
                        add_validation_to_db(db, tag_in_db, new_tag_obj,
                                             calculate_validation_values=calculate_validation_values)
    # if the new tag is a pure entity-only tag and there are mixed topic + entity tags with the same entity
    # validate existing tag only if it is positive
    if new_tag_obj.topic == new_tag_obj.entity_type:
        for tag_in_db in related_tags_in_db:
            if (tag_in_db.negated is False and tag_in_db.entity_type != tag_in_db.topic
                    and new_tag_obj.entity_type == tag_in_db.entity_type
                    and new_tag_obj.entity == tag_in_db.entity):
                if tag_in_db.data_novelty in more_specific_novelty:
                    add_validation_to_db(db, tag_in_db, new_tag_obj,
                                         calculate_validation_values=calculate_validation_values)


def validate_new_tag_with_existing_tags(db, new_tag_obj: TopicEntityTagModel, related_validating_tags_in_db,
                                        calculate_validation_values: bool = True):
    # 1. new tag positive, existing tag positive = validate new tag (right) if existing is more specific
    # 2. new tag negative, existing tag positive = validate new tag (wrong) if existing is more specific
    # 3. new tag positive, existing tag negative = validate new tag (wrong) if existing is more generic
    # 4. new tag negative, existing tag negative = validate new tag (right) if existing is more generic
    more_specific_topics = set(get_descendants(onto_node=new_tag_obj.topic))  # type: ignore
    more_specific_topics.add(new_tag_obj.topic)
    more_specific_novelty = set(get_descendants(new_tag_obj.data_novelty))
    more_specific_novelty.add(new_tag_obj.data_novelty)
    more_generic_topics = set(get_ancestors(onto_node=new_tag_obj.topic))  # type: ignore
    more_generic_topics.add(new_tag_obj.topic)
    more_generic_novelty = set(get_ancestors(new_tag_obj.data_novelty))
    more_generic_novelty.add(new_tag_obj.data_novelty)
    tag_in_db: TopicEntityTagModel
    for tag_in_db in related_validating_tags_in_db:
        if (tag_in_db.negated is False and tag_in_db.topic in more_specific_topics
                and tag_in_db.data_novelty in more_specific_novelty):
            if new_tag_obj.entity_type is None or (tag_in_db.entity_type == new_tag_obj.entity_type
                                                   and tag_in_db.entity == new_tag_obj.entity):
                if new_tag_obj.species is None or tag_in_db.species == new_tag_obj.species:
                    add_validation_to_db(db, new_tag_obj, tag_in_db,
                                         calculate_validation_values=calculate_validation_values)
        elif (tag_in_db.negated is True and tag_in_db.topic in more_generic_topics
              and tag_in_db.data_novelty in more_generic_novelty):
            if tag_in_db.entity_type is None or (tag_in_db.entity_type == new_tag_obj.entity_type
                                                 and tag_in_db.entity == new_tag_obj.entity):
                if tag_in_db.species is None or tag_in_db.species == new_tag_obj.species:
                    add_validation_to_db(db, new_tag_obj, tag_in_db,
                                         calculate_validation_values=calculate_validation_values)
    # if the new tag is a pure entity-only tag and there are mixed topic + entity tags with the same entity
    # validate positive or negative new tag only if existing is positive
    if new_tag_obj.topic == new_tag_obj.entity_type:
        for tag_in_db in related_validating_tags_in_db:
            if (tag_in_db.entity_type != tag_in_db.topic and new_tag_obj.entity_type == tag_in_db.entity_type
                    and new_tag_obj.entity == tag_in_db.entity and tag_in_db.negated is False):
                if tag_in_db.data_novelty in more_specific_novelty:
                    add_validation_to_db(db, new_tag_obj, tag_in_db,
                                         calculate_validation_values=calculate_validation_values)
    # if the new tag is a mixed topic + entity tag and there are pure entity-only tags with the same entity
    # validate only positive new tag if existing is negative
    if new_tag_obj.negated is False and new_tag_obj.entity is not None and new_tag_obj.entity_type != new_tag_obj.topic:
        for tag_in_db in related_validating_tags_in_db:
            if (tag_in_db.negated is True and tag_in_db.topic == tag_in_db.entity_type == new_tag_obj.entity_type
                    and new_tag_obj.entity == tag_in_db.entity and tag_in_db.data_novelty in more_generic_novelty):
                add_validation_to_db(db, new_tag_obj, tag_in_db,
                                     calculate_validation_values=calculate_validation_values)


def add_validation_to_db(db: Session, validated_tag: TopicEntityTagModel, validating_tag: TopicEntityTagModel,
                         calculate_validation_values: bool = True):
    logger.info(f"Adding validation: tag {validated_tag.topic_entity_tag_id} validated by tag {validating_tag.topic_entity_tag_id}")
    db.execute(text(f"INSERT INTO topic_entity_tag_validation (validated_topic_entity_tag_id, "
                    f"validating_topic_entity_tag_id) VALUES ({validated_tag.topic_entity_tag_id}, "
                    f"{validating_tag.topic_entity_tag_id})"))
    if calculate_validation_values:
        logger.info("Committing validation insert and recalculating validation values")
        db.commit()
        validated_tag_obj = db.query(TopicEntityTagModel).filter(
            TopicEntityTagModel.topic_entity_tag_id == validated_tag.topic_entity_tag_id).first()
        set_validation_values_to_tag(validated_tag_obj)
        logger.info(f"Validation values updated for tag {validated_tag.topic_entity_tag_id}")


def validate_tags(db: Session, new_tag_obj: TopicEntityTagModel, validate_new_tag: bool = True,
                  commit_changes: bool = True, calculate_validation_values: bool = True, related_tags_in_db=None):
    if related_tags_in_db is None:
        logger.info("Reading related tags from db")
        related_tags_in_db = db.query(
            TopicEntityTagModel.topic_entity_tag_id,
            TopicEntityTagModel.topic,
            TopicEntityTagModel.entity_type,
            TopicEntityTagModel.entity,
            TopicEntityTagModel.species,
            TopicEntityTagModel.negated,
            TopicEntityTagModel.data_novelty,
            TopicEntityTagSourceModel.validation_type
        ).join(
            TopicEntityTagSourceModel, TopicEntityTagModel.topic_entity_tag_source
        ).filter(
            TopicEntityTagModel.reference_id == new_tag_obj.reference_id,
            TopicEntityTagSourceModel.secondary_data_provider_id == new_tag_obj.topic_entity_tag_source.secondary_data_provider_id,
            TopicEntityTagModel.negated.isnot(None)
        ).all()
        logger.info("Query for related tags completed")
    all_related_tags = related_tags_in_db
    related_tags_in_db = [tag for tag in related_tags_in_db if
                          tag.topic_entity_tag_id != new_tag_obj.topic_entity_tag_id]
    # The current tag can validate existing tags or be validated by other tags only if it has a True or False negated
    # value
    logger.info(f"Found {str(len(related_tags_in_db))} related tags")
    if len(related_tags_in_db) > 0 and new_tag_obj.negated is not None:
        # Validate existing tags
        if new_tag_obj.topic_entity_tag_source.validation_type is not None:
            logger.info(f"Validating existing tags with new tag (negated={new_tag_obj.negated})")
            if new_tag_obj.negated is False:
                validate_tags_already_in_db_with_positive_tag(db, new_tag_obj, related_tags_in_db,
                                                              calculate_validation_values=calculate_validation_values)
            else:
                validate_tags_already_in_db_with_negative_tag(db, new_tag_obj, related_tags_in_db,
                                                              calculate_validation_values=calculate_validation_values)
            logger.info("Existing tag validation completed")
        # Validate current tag with existing ones
        if validate_new_tag:
            related_validating_tags_in_db = [related_tag for related_tag in related_tags_in_db if
                                             related_tag.validation_type is not None]
            logger.info(f"Validating new tag with {len(related_validating_tags_in_db)} existing validating tags")
            validate_new_tag_with_existing_tags(db, new_tag_obj, related_validating_tags_in_db,
                                                calculate_validation_values=calculate_validation_values)
            logger.info("New tag validation completed")
    if calculate_validation_values:
        logger.info("Calculating validation values for new tag")
        set_validation_values_to_tag(new_tag_obj)
    if commit_changes:
        logger.info("Committing validation changes")
        db.commit()
    if new_tag_obj.validation_by_professional_biocurator == "validation_conflict" or \
            new_tag_obj.validation_by_author == "validation_conflict":
        logger.info("Validation conflict detected, batch loading related tags for revalidation")
        # Optimize: Batch load all related tags at once with eager loading
        related_tag_ids = [related_tag.topic_entity_tag_id for related_tag in related_tags_in_db]
        if related_tag_ids:
            related_tag_objs = db.query(TopicEntityTagModel).options(
                joinedload(TopicEntityTagModel.topic_entity_tag_source),
                joinedload(TopicEntityTagModel.validated_by).joinedload(TopicEntityTagModel.topic_entity_tag_source)
            ).filter(TopicEntityTagModel.topic_entity_tag_id.in_(related_tag_ids)).all()
            logger.info(f"Setting validation values for {len(related_tag_objs)} conflicting tags")
            for related_tag_obj in related_tag_objs:
                set_validation_values_to_tag(related_tag_obj)
        if commit_changes:
            logger.info("Committing conflict resolution changes")
            db.commit()
    return all_related_tags


def set_validation_values_to_tag(tag: TopicEntityTagModel):
    disable_set_updated_by_onupdate(tag)
    disable_set_date_updated_onupdate(tag)
    tag.validation_by_professional_biocurator = calculate_validation_value_for_tag(tag, ATP_ID_SOURCE_CURATOR)
    tag.validation_by_author = calculate_validation_value_for_tag(tag, ATP_ID_SOURCE_AUTHOR)


def revalidate_all_tags(email: str = None, delete_all_first: bool = False, curie_or_reference_id: str = None,
                        validation_values_only: bool = False):
    engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
    new_session = sessionmaker(bind=engine, autoflush=True)
    db = new_session()
    reference_query_filter = ""
    query_tags = (db.query(TopicEntityTagModel)
                  .join(TopicEntityTagModel.topic_entity_tag_source)
                  .options(joinedload(TopicEntityTagModel.topic_entity_tag_source))
                  .options(joinedload(TopicEntityTagModel.validated_by))
                  .options(noload(TopicEntityTagModel.reference))
                  .order_by(TopicEntityTagModel.reference_id,
                            TopicEntityTagModel.topic_entity_tag_source_id,
                            TopicEntityTagSourceModel.secondary_data_provider_id))
    if not validation_values_only:
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
            db.execute(text("DELETE FROM topic_entity_tag_validation" + reference_query_filter))
            db.commit()
        curr_ref_tags_in_db = None
        curr_reference_id = None
        curr_mod_id = None
        for tag_counter, tag in enumerate(query_tags.all()):
            if tag.reference_id != curr_reference_id or tag.topic_entity_tag_source.secondary_data_provider_id != curr_mod_id:
                curr_reference_id = tag.reference_id
                curr_mod_id = tag.topic_entity_tag_source.secondary_data_provider_id
                curr_ref_tags_in_db = None
            logger.info(f"Processing tag # {str(tag_counter)}")
            if not delete_all_first:
                db.execute(text(f"DELETE FROM topic_entity_tag_validation "
                                f"WHERE validating_topic_entity_tag_id = {tag.topic_entity_tag_id}"))
            curr_ref_tags_in_db = validate_tags(db=db, new_tag_obj=tag, validate_new_tag=False, commit_changes=False,
                                                calculate_validation_values=False,
                                                related_tags_in_db=curr_ref_tags_in_db)
            if tag_counter > 0 and tag_counter % 200 == 0:
                db.commit()
        db.commit()
    offset = 0
    batch_size = 200
    tag_counter = 0
    while True:
        batch_tags = query_tags.offset(offset).limit(batch_size).all()
        if not batch_tags:
            break  # All tags processed
        for tag in batch_tags:
            tag_counter += 1
            logger.info(f"Setting validation values for tag #{tag_counter}")
            set_validation_values_to_tag(tag)
        db.commit()
        offset += batch_size
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
    source_patch_data = source_patch.model_dump(exclude_unset=True)
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


def check_for_duplicate_tags(db: Session, topic_entity_tag_data: dict, source: TopicEntityTagSourceModel,
                             reference_id: int, force_insertion: bool = False):
    logger.info("Starting duplicate tag check")
    new_tag_data = copy.copy(topic_entity_tag_data)
    new_tag_data.pop('validation_by_author', None)
    new_tag_data.pop('validation_by_professional_biocurator', None)
    new_tag_data.pop('date_created', None)
    date_updated: str = new_tag_data.pop('date_updated', '')
    note = new_tag_data.pop('note', None)
    created_by_user = get_default_user_value()

    if new_tag_data.get('created_by', None) is None:
        new_tag_data['created_by'] = created_by_user
    if new_tag_data.get('updated_by', None) is None:
        new_tag_data['updated_by'] = created_by_user

    logger.info("Checking for exact duplicate with same creator")
    # Optimize: Build a single query with filters instead of filter_by for better performance
    query = db.query(TopicEntityTagModel)
    for key, value in new_tag_data.items():
        query = query.filter(getattr(TopicEntityTagModel, key) == value)
    existing_tag = query.first()
    if existing_tag:
        existing_date_updated = existing_tag.date_updated
        tag_data = get_tet_with_names(db, tet=new_tag_data, curie_or_reference_id=str(reference_id))
        if note:
            tag_data['note'] = note
        existing_note_list = existing_tag.note.split(" | ") if existing_tag.note else []
        if (note and note in existing_note_list) or note is None:
            return {
                "status": "exists",
                "message": "The tag already exists in the database.",
                "data": tag_data,
                "topic_entity_tag_id": existing_tag.topic_entity_tag_id
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
                    "data": tag_data,
                    "topic_entity_tag_id": existing_tag.topic_entity_tag_id
                }
            except (IntegrityError, HTTPException) as e:
                db.rollback()
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail=f"invalid request: {e}")

    if source.source_method == "abc_literature_system" and source.validation_type == "professional_biocurator":
        negation_check_data = copy.deepcopy(new_tag_data)
        negation_check_data.pop('data_novelty')
        negation_check_data['negated'] = not negation_check_data['negated']  # look for tags with opposite negated value
        similar_tags = db.query(TopicEntityTagModel).filter_by(**negation_check_data).all()
        if similar_tags:
            tag_data = get_tet_with_names(db, tet=topic_entity_tag_data, curie_or_reference_id=str(reference_id))
            return {
                "status": "exists",
                "message": "One or more tags already exist in the database with the opposite 'negated' value.",
                "data": tag_data,
                "conflicting_tag_ids": [tag.topic_entity_tag_id for tag in similar_tags]
            }

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
                "data": tag_data,
                "topic_entity_tag_id": existing_tag.topic_entity_tag_id
            }
        else:
            message = "The tag without a note, created by another curator, already exists in the database."
            if existing_tag.note:
                message = "The tag with a different note, created by another curator, already exists in the database."
            note_in_db = existing_tag.note if existing_tag.note else ''
            return {
                "status": f"exists: {existing_tag.created_by} | {note_in_db}",
                "message": message,
                "data": tag_data,
                "topic_entity_tag_id": existing_tag.topic_entity_tag_id
            }

    # if no duplicates found, return None
    return None


def show_all_reference_tags(db: Session, curie_or_reference_id, page: int = 1, page_size: int = None, count_only: bool = False, sort_by: str = None, desc_sort: bool = False, column_only: str = None, column_filter: str = None, column_values: str = None):      # noqa: C901

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
        joinedload(TopicEntityTagModel.topic_entity_tag_source),
        joinedload(TopicEntityTagModel.ml_model)).filter(
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
                order_expression = case(
                    {column.is_(None): 1 if desc_sort else 0},
                    else_=0 if desc_sort else 1
                )
                # order_expression = case([(column.is_(None), 1 if desc_sort else 0)], else_=0 if desc_sort else 1)
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
                # order_expression = case([(column_property.is_(None), 1 if desc_sort else 0)], else_=0 if desc_sort else 1)
                order_expression = case(
                    {column_property.is_(None): 1 if desc_sort else 0},
                    else_=0 if desc_sort else 1
                )
                query = query.order_by(order_expression, column_property.desc() if desc_sort else column_property,
                                       TopicEntityTagModel.topic_entity_tag_id)

        # build a bulk map of users.id -> person.display_name
        page_q = query.offset((page - 1) * page_size if page_size else None).limit(page_size)
        rows = page_q.all()
        user_ids: Set[str] = set()
        for tet in rows:
            if tet.created_by:
                user_ids.add(tet.created_by)
            if tet.updated_by:
                user_ids.add(tet.updated_by)
            if tet.topic_entity_tag_source:
                if tet.topic_entity_tag_source.created_by:
                    user_ids.add(tet.topic_entity_tag_source.created_by)
                if tet.topic_entity_tag_source.updated_by:
                    user_ids.add(tet.topic_entity_tag_source.updated_by)
        id_to_display_name = get_user_display_name_map(db, user_ids)

        mod_id_to_mod = dict([(x.mod_id, x.abbreviation) for x in db.query(ModModel).all()])
        all_tet = []
        for tet in rows:
            tet_data = jsonable_encoder(vars(tet), exclude={"validated_by"})
            if "validated_by" in tet_data:
                del tet_data["validated_by"]
            # Replace top-level created_by/updated_by if we have a display name
            for k in ("created_by", "updated_by"):
                uid = tet_data.get(k)
                if uid and uid in id_to_display_name:
                    tet_data[k] = id_to_display_name[uid]
            # Nested source object: replace created_by/updated_by if present
            if "topic_entity_tag_source" in tet_data and tet_data["topic_entity_tag_source"]:
                for k in ("created_by", "updated_by"):
                    uid = tet_data["topic_entity_tag_source"].get(k)
                    if uid and uid in id_to_display_name:
                        tet_data["topic_entity_tag_source"][k] = id_to_display_name[uid]
            add_list_of_users_who_validated_tag(tet, tet_data)
            add_list_of_validating_tag_ids(tet, tet_data)
            tet_data["topic_entity_tag_source"]["secondary_data_provider_abbreviation"] = mod_id_to_mod[
                tet.topic_entity_tag_source.secondary_data_provider_id]
            # Add ML model version if associated with an ML model
            if tet.ml_model:
                tet_data["ml_model_version"] = tet.ml_model.version_num
            all_tet.append(tet_data)
        curie_to_name = get_curie_to_name_from_all_tets(db, curie_or_reference_id)
        return [get_tet_with_names(db, tag, curie_to_name) for tag in all_tet]


def get_all_topic_entity_tags_by_mod(db: Session, mod_abbreviation: str, days_updated: int = 7):

    current_date = datetime.now()
    past_date = current_date - timedelta(days=int(days_updated))
    last_date_updated = past_date.strftime("%Y-%m-%d")

    rows = db.execute(text("SELECT cr.curie, tet.*, u.email "
                           "FROM cross_reference cr "
                           "JOIN topic_entity_tag tet ON cr.reference_id = tet.reference_id AND cr.curie_prefix = :mod_abbreviation "
                           "JOIN topic_entity_tag_source tets ON tet.topic_entity_tag_source_id = tets.topic_entity_tag_source_id "
                           "JOIN users u ON tet.updated_by = u.id "
                           "JOIN mod m ON tets.secondary_data_provider_id = m.mod_id "
                           "WHERE m.abbreviation = :mod_abbreviation "
                           "AND tet.date_updated >= :last_date_updated"),
                      {'mod_abbreviation': mod_abbreviation, 'last_date_updated': last_date_updated}).mappings().fetchall()

    # tags = [dict(row) for row in rows]
    # there are duplicate rows returned
    tags = []
    processed_ids = set()
    for row in rows:
        row_dict = dict(row)
        topic_entity_tag_id = row_dict['topic_entity_tag_id']
        if topic_entity_tag_id not in processed_ids:
            tags.append(row_dict)
            processed_ids.add(topic_entity_tag_id)

    curie_to_name_mapping = get_curie_to_name_mapping_for_mod(db, mod_abbreviation, last_date_updated)

    data = [get_tet_with_names(db, tag, curie_to_name_mapping) for tag in tags]

    src_rows = db.execute(text("SELECT tets.* "
                               "FROM topic_entity_tag_source tets "
                               "JOIN mod m ON tets.secondary_data_provider_id = m.mod_id "
                               "WHERE m.abbreviation = :mod_abbreviation"),
                          {'mod_abbreviation': mod_abbreviation}).mappings().fetchall()
    metadata = [dict(row) for row in src_rows]

    return {"metadata": metadata, "data": data}


def get_curie_to_name_mapping_for_mod(db, mod_abbreviation, last_date_updated):

    curie_to_name_mapping = {}

    rows = db.execute(text("SELECT DISTINCT tet.reference_id "
                           "FROM topic_entity_tag tet "
                           "JOIN topic_entity_tag_source tets ON tet.topic_entity_tag_source_id = tets.topic_entity_tag_source_id "
                           "JOIN mod m ON tets.secondary_data_provider_id = m.mod_id "
                           "WHERE m.abbreviation = :mod_abbreviation "
                           "AND tet.date_updated >= :last_date_updated"),
                      {'mod_abbreviation': mod_abbreviation, 'last_date_updated': last_date_updated}).mappings().fetchall()
    for x in rows:
        curie_to_name_mapping.update(get_curie_to_name_from_all_tets(db, str(x['reference_id'])))
    return curie_to_name_mapping


def get_curie_to_name_from_all_tets(db: Session, curie_or_reference_id: str):
    reference_id = get_reference_id_from_curie_or_id(db, curie_or_reference_id)
    ref_related_tets = db.query(TopicEntityTagModel).filter(TopicEntityTagModel.reference_id == reference_id).all()
    all_atp_terms = set()
    entity_id_validation_entity_type_entities: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
    all_entity_curies = set()
    tag_species = set()
    source_eco_codes = set()
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
        if tet.topic_entity_tag_source.source_evidence_assertion:
            if tet.topic_entity_tag_source.source_evidence_assertion.startswith("ECO:"):
                source_eco_codes.add(tet.topic_entity_tag_source.source_evidence_assertion)
            elif tet.topic_entity_tag_source.source_evidence_assertion.startswith("ATP:"):
                all_atp_terms.add(tet.topic_entity_tag_source.source_evidence_assertion)
    entity_curie_to_name = get_map_ateam_curies_to_names(category="atpterm", curies=list(all_atp_terms))
    entity_curie_to_name.update(get_map_ateam_curies_to_names(category="ecoterm",
                                                              curies=list(source_eco_codes)))
    entity_curie_to_name.update(get_map_ateam_curies_to_names(category="species",
                                                              curies=list(tag_species)))
    for entity_id_validation, entity_type_curies_dict in entity_id_validation_entity_type_entities.items():
        for entity_type, curies in entity_type_curies_dict.items():
            entity_type_name = entity_curie_to_name[entity_type]
            entity_curie_to_name.update(get_map_entity_curies_to_names(
                db, entity_id_validation=entity_id_validation,
                curies_category=entity_type_name,
                curies=list(curies)))
    for curie_without_name in (all_entity_curies | all_atp_terms) - set(entity_curie_to_name.keys()):
        entity_curie_to_name[curie_without_name] = curie_without_name
    return entity_curie_to_name


def get_tet_with_names(db: Session, tet, curie_to_name_mapping: Dict = None, curie_or_reference_id: str = None):
    if curie_to_name_mapping is None:
        curie_to_name_mapping = get_curie_to_name_from_all_tets(db, str(curie_or_reference_id))
    new_tet = copy.deepcopy(tet)
    for tet_field_name, tet_field_value in tet.items():
        if tet_field_name == "topic_entity_tag_source":
            for source_field_name, source_field_value in tet_field_value.items():
                if source_field_name in TET_SOURCE_CURIE_FIELDS:
                    new_field = f"{source_field_name}_name"
                    new_tet[tet_field_name][new_field] = curie_to_name_mapping.get(source_field_value, source_field_value)
        else:
            if tet_field_name in TET_CURIE_FIELDS:
                new_field = f"{tet_field_name}_name"
                new_tet[new_field] = curie_to_name_mapping.get(tet_field_value, tet_field_value)
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


def show_all_source(db: Session):
    return [jsonable_encoder(source) for source in db.query(TopicEntityTagSourceModel).all()]
