"""
topic_entity_tag_crud.py
===========================
"""
import copy
import logging
from typing import Optional
from collections import defaultdict
from os import environ
from typing import Any, Dict, List, Set, Tuple
from datetime import datetime, timedelta
from time import perf_counter

from dateutil import parser as date_parser
from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import case, and_, or_, func, create_engine, text, inspect as sa_inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload, selectinload, sessionmaker, noload

from agr_literature_service.api.crud.topic_entity_tag_utils import get_reference_id_from_curie_or_id, \
    get_source_from_db, add_source_obj_to_db_session, get_sorted_column_values, \
    check_and_set_sgd_display_tag, check_and_set_species, add_audited_object_users_if_not_exist, \
    get_ancestors, get_descendants, get_map_entity_curies_to_names, \
    id_to_name_cache, get_map_ateam_curies_to_names, get_mod_id_from_mod_abbreviation, \
    get_user_display_name_map
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.models import (
    TopicEntityTagModel, WorkflowTagModel, ModCorpusAssociationModel,
    ReferenceModel, TopicEntityTagSourceModel, ModModel, CrossReferenceModel
)
from agr_literature_service.api.models.ml_model_model import MLModel
from agr_literature_service.api.crud.workflow_tag_crud import get_workflow_tags_from_process, \
    get_current_workflow_status
from agr_literature_service.api.models.audited_model import (
    get_default_user_value,
    impute_audit_user_ids,
    disable_set_updated_by_onupdate,
    disable_set_date_updated_onupdate
)
from agr_cognito_py import ModAccess, MOD_ACCESS_ABBR
from agr_literature_service.api.schemas.topic_entity_tag_schemas import (TopicEntityTagSchemaPost,
                                                                         TopicEntityTagSourceSchemaUpdate,
                                                                         TopicEntityTagSourceSchemaCreate,
                                                                         TopicEntityTagSchemaUpdate)
from agr_literature_service.lit_processing.utils.email_utils import send_email
from agr_literature_service.api.crud.ateam_db_helpers import atp_return_invalid_ids
from agr_literature_service.api.crud.user_utils import map_to_user_id

logger = logging.getLogger(__name__)


def _tet_batch_timing_enabled():
    # Re-read each call so it can be toggled without a code change.
    return environ.get("DEBUG_TET_BATCH_TIMING", "").lower() in ("1", "true", "yes")


def _log_tet_batch_timing(message, *args):
    # TET batch phase timing (resolve / query / names / serialize / total).
    # Off by default to keep production logs quiet; set DEBUG_TET_BATCH_TIMING=true
    # to print the per-phase breakdown to stdout when diagnosing slow Topic-grid
    # loads. The same numbers are also returned in the endpoint response under
    # "debug_timing" (also gated by the flag), which is readable from the browser
    # Network tab even when the container ships stdout to a remote log driver.
    if _tet_batch_timing_enabled():
        print(message % args, flush=True)


ATP_ID_SOURCE_AUTHOR = "author"
ATP_ID_SOURCE_CURATOR = "professional_biocurator"

TET_CURIE_FIELDS = ['topic', 'entity_type', 'display_tag', 'entity', 'species']
TET_SOURCE_CURIE_FIELDS = ['source_evidence_assertion']

# SCRUM-6183: data_novelty term "existing data" used on the companion pure entity
# tag auto-created from a positive mixed topic+entity tag.
EXISTING_DATA_NOVELTY_ATP = "ATP:0000334"


def create_tag(db: Session, topic_entity_tag: TopicEntityTagSchemaPost,
               validate_on_insert: bool = True) -> Tuple[int, bool]:
    """
    Create a new topic entity tag.

    Returns ``(topic_entity_tag_id, was_upsert)``:
      - ``(new_id, False)``       — a new tag was created (caller should return 201).
      - ``(existing_id, True)``   — an existing tag was updated in place (note appended);
                                    caller should return 200.

    Raises ``HTTPException(409)`` for true duplicate / conflict cases. See
    ``check_for_duplicate_tags`` for the exact branches.
    """
    logger.info("Starting create_tag")
    topic_entity_tag_data = jsonable_encoder(topic_entity_tag)
    if "created_by" in topic_entity_tag_data and topic_entity_tag_data["created_by"] is not None:
        topic_entity_tag_data["created_by"] = map_to_user_id(topic_entity_tag_data["created_by"], db)
    if "updated_by" in topic_entity_tag_data and topic_entity_tag_data["updated_by"] is not None:
        topic_entity_tag_data["updated_by"] = map_to_user_id(topic_entity_tag_data["updated_by"], db)
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
    # check_for_duplicate_tags raises HTTPException(409) on conflict branches, or
    # returns the existing tag id when the request was absorbed as an in-place note
    # append (upsert). Returns None when no duplicate exists.
    duplicate_check_result = check_for_duplicate_tags(db, topic_entity_tag_data, source, reference_id, force_insertion)
    if duplicate_check_result is not None:
        logger.info("Duplicate tag absorbed as upsert, returning existing id")
        return (duplicate_check_result, True)
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
        add_paper_to_mod_if_not_already(db, reference_curie, reference_id,
                                        source.secondary_data_provider.abbreviation,
                                        mod_id)
        logger.info("Updating manual indexing workflow tag")
        update_manual_indexing_workflow_tag(db, mod_id, reference_id, index_wft)
        if validate_on_insert:
            logger.info("Starting tag validation")
            validate_tags(db=db, new_tag_obj=new_db_obj)
            logger.info("Tag validation completed")
            # SCRUM-6183: when a curator creates a positive mixed topic+entity tag, also
            # create a companion pure entity tag (topic == entity_type) so they don't have
            # to add it by hand. Gated on validate_on_insert so it only fires for the
            # interactive create path (the router) and not for bulk reference import or
            # reference-merge copies, which pass validate_on_insert=False. SGD is excluded
            # (it has its own data_novelty/display handling). Pipeline-generated tags are
            # excluded too: the companion is only for tags a human curator made, and
            # pipeline/script-created tags have created_by/updated_by users.id values that
            # are not the AGRKB: curies humans get (so the curator-only auto-creation does
            # not flood the DB with companion tags for machine-generated mixed tags).
            is_mixed = (topic_entity_tag_data.get("entity") is not None
                        and topic_entity_tag_data.get("entity_type") is not None
                        and topic_entity_tag_data.get("entity_type") != topic_entity_tag_data["topic"])
            is_positive = topic_entity_tag_data.get("negated") is False
            is_sgd = source.secondary_data_provider.abbreviation == "SGD"
            # Read the final persisted creator/updater off the committed row: when
            # updated_by is omitted on the request, the audited-model before_insert
            # event copies created_by into updated_by on new_db_obj (not on the dict
            # above), so new_db_obj holds the authoritative users.id values.
            created_by = new_db_obj.created_by or ""
            updated_by = new_db_obj.updated_by or ""
            is_human = created_by.startswith("AGRKB:") and updated_by.startswith("AGRKB:")
            if is_mixed and is_positive and not is_sgd and is_human:
                logger.info("Creating companion entity tag for mixed topic+entity tag")
                create_entity_tag_for_mixed_tag(db, topic_entity_tag_data, reference_id)
        logger.info("create_tag completed successfully")
        return (new_db_obj.topic_entity_tag_id, False)
    except (IntegrityError, HTTPException) as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"invalid request: {e}")


def create_entity_tag_for_mixed_tag(db: Session, mixed_tag_data: dict, reference_id: int):
    """SCRUM-6183: create the companion "pure entity" tag for a just-created positive
    mixed topic+entity tag.

    The companion has ``topic == entity_type`` and ``data_novelty`` set to the
    "existing data" term, reusing the originating tag's source and entity fields.

    Idempotent: if a pure entity tag (``topic == entity_type``) for this
    reference+entity already exists, nothing is created. The existence check is
    deliberately ``data_novelty``/source/creator-agnostic so we never create a second,
    semantically-redundant entity tag for the same entity, and it matches the
    back-fill's existence check.

    Non-fatal: any failure is logged and rolled back so it never disturbs the
    already-committed parent tag. The caller excludes SGD. The companion is itself
    ``topic == entity_type`` so it does not re-trigger this logic.
    """
    committed = False
    try:
        entity_type = mixed_tag_data["entity_type"]
        entity = mixed_tag_data["entity"]
        existing = db.query(TopicEntityTagModel).filter(
            TopicEntityTagModel.reference_id == reference_id,
            TopicEntityTagModel.entity == entity,
            TopicEntityTagModel.entity_type == entity_type,
            TopicEntityTagModel.topic == entity_type,
        ).first()
        if existing is not None:
            logger.info("Companion entity tag already exists; nothing to create")
            return
        entity_tag_data = copy.copy(mixed_tag_data)
        entity_tag_data["topic"] = entity_type
        entity_tag_data["data_novelty"] = EXISTING_DATA_NOVELTY_ATP
        entity_tag_data["negated"] = False
        # The remaining fields describe the topic-specific assertion, not the bare
        # entity, so they are reset on the companion tag.
        for field in ("note", "confidence_score", "confidence_level", "display_tag",
                      "ml_model_id", "validation_by_author",
                      "validation_by_professional_biocurator"):
            entity_tag_data[field] = None
        new_db_obj = TopicEntityTagModel(**entity_tag_data)
        db.add(new_db_obj)
        db.commit()
        committed = True
        db.refresh(new_db_obj, ['topic_entity_tag_source'])
        validate_tags(db=db, new_tag_obj=new_db_obj)
        logger.info(f"Created companion entity tag {new_db_obj.topic_entity_tag_id}")
    except Exception as e:
        # Non-fatal: the parent tag is already committed, so a companion failure must
        # never surface to the caller. Roll back only the companion's pending work.
        db.rollback()
        if committed:
            # The companion row was already written; only the post-insert validation
            # failed, so the row persists (validation can be recomputed later).
            logger.warning(f"Companion entity tag written but validation failed: {e}")
        else:
            logger.warning(f"Companion entity tag not created, skipping: {e}")


def set_indexing_status_for_no_tet_data(db: Session, mod_abbreviation, reference_curie, uid):

    reference_id = get_reference_id_from_curie_or_id(db, reference_curie)
    mod_id = get_mod_id_from_mod_abbreviation(db, mod_abbreviation)
    add_paper_to_mod_if_not_already(db, reference_curie, reference_id, mod_abbreviation, mod_id)
    update_manual_indexing_workflow_tag(db, mod_id, reference_id, "ATP:0000275")


def update_manual_indexing_workflow_tag(db: Session, mod_id, reference_id, index_wft):

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
        wft_obj = WorkflowTagModel(reference_id=reference_id,
                                   mod_id=mod_id,
                                   workflow_tag_id=index_wft)
        db.add(wft_obj)
        db.commit()
    elif wft.workflow_tag_id != index_wft:
        wft.workflow_tag_id = index_wft
        db.add(wft)
        db.commit()


def add_paper_to_mod_if_not_already(db: Session, reference_curie, reference_id, mod_abbreviation, mod_id):
    try:
        add_wft_141 = False

        mca_db_obj = (
            db.query(ModCorpusAssociationModel)
            .filter_by(mod_id=mod_id, reference_id=reference_id)
            .one_or_none()
        )

        if mca_db_obj is None:
            add_wft_141 = True
            new_mca = ModCorpusAssociationModel(
                reference_id=reference_id,
                mod_id=mod_id,
                corpus=True,
                mod_corpus_sort_source="manual_creation",
            )
            db.add(new_mca)

            logger.info(
                "Created ModCorpusAssociation (reference_id=%s, mod_id=%s)",
                reference_id,
                mod_id,
            )

        elif not mca_db_obj.corpus:
            add_wft_141 = True
            mca_db_obj.corpus = True
            mca_db_obj.mod_corpus_sort_source = "manual_creation"
            db.add(mca_db_obj)

            logger.info(
                "Updated ModCorpusAssociation to corpus=True "
                "(reference_id=%s, mod_id=%s)",
                reference_id,
                mod_id,
            )

        if add_wft_141:
            current_status = get_current_workflow_status(
                db,
                reference_curie,
                "ATP:0000140",
                mod_abbreviation,
            )

            if current_status is None:
                """
                transition_to_workflow_status(
                    db,
                    reference_curie,
                    mod_abbreviation,
                    "ATP:0000141",
                )
                """
                new_wft = WorkflowTagModel(reference_id=reference_id,
                                           mod_id=mod_id,
                                           workflow_tag_id='ATP:0000141')
                db.add(new_wft)
                logger.info(
                    "Transitioned workflow status to ATP:0000141 "
                    "(reference=%s, mod=%s)",
                    reference_curie,
                    mod_abbreviation,
                )

        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"An error: '{e}' occurred when adding {reference_curie} into corpus/adding file needed tag")


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
    if "created_by" in patch_data_dict and patch_data_dict["created_by"] is not None:
        patch_data_dict["created_by"] = map_to_user_id(patch_data_dict["created_by"], db)
    if "updated_by" in patch_data_dict and patch_data_dict["updated_by"] is not None:
        patch_data_dict["updated_by"] = map_to_user_id(patch_data_dict["updated_by"], db)
    add_audited_object_users_if_not_exist(db, patch_data_dict)
    for key, value in patch_data_dict.items():
        setattr(topic_entity_tag, key, value)
    db.commit()
    revalidate_all_tags(curie_or_reference_id=str(topic_entity_tag.reference_id))
    return {"message": "updated"}


def destroy_tag(db: Session, topic_entity_tag_id: int, mod_access: ModAccess):
    topic_entity_tag: TopicEntityTagModel = db.query(TopicEntityTagModel).filter(
        TopicEntityTagModel.topic_entity_tag_id == topic_entity_tag_id).one_or_none()
    if topic_entity_tag is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"topic_entityTag with the topic_entity_tag_id {topic_entity_tag_id} "
                                   f"is not available")

    """
    If a tag is created by a curator via the API or UI, then `created_by` is set to `user_id`.
    This allows us to set `created_by_mod` based on the mod to which `created_by` is associated,
    assuming person data is available in the database. However, if the tag is added by a script,
    `created_by` is set to `curator_id`. In this case, we set
    `created_by_mod` based on the mod in the `topic_entity_tag_source` table.
    Currently, `created_by_mod` always defaults to the mod in the `topic_entity_tag_source` table,
    as we lack the database data to map each user's id to a mod.
    """
    user_mod = MOD_ACCESS_ABBR[mod_access]
    created_by_mod = topic_entity_tag.topic_entity_tag_source.secondary_data_provider.abbreviation
    """
    fixed HTTP_403_Forbidden to HTTP_404_NOT_FOUND in following code since mypy complains
    about "HTTP_403_Forbidden" not found
    """
    if mod_access != ModAccess.ALL_ACCESS and user_mod != created_by_mod:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"You do not have permission to delete topic_entity_tag with the topic_entity_tag_id {topic_entity_tag_id} created by {created_by_mod}")

    reference_id = topic_entity_tag.reference_id
    db.delete(topic_entity_tag)
    db.commit()
    revalidate_all_tags(curie_or_reference_id=str(reference_id), delete_all_first=False, validation_values_only=False)


def atp_hierarchy_with_self(atp_id: Optional[str], ancestors: bool) -> Set[str]:
    """SCRUM-6188: set containing ``atp_id`` plus its ATP ancestors (``ancestors=True``)
    or descendants (``ancestors=False``).

    Returns an empty set when ``atp_id`` is None (``entity_type`` is nullable). Always
    includes ``atp_id`` itself, so an exact-equality match is preserved even when the
    node has no ancestors/descendants in the ontology graph.
    """
    if atp_id is None:
        return set()
    related = get_ancestors(onto_node=atp_id) if ancestors else get_descendants(onto_node=atp_id)
    result = set(related)
    result.add(atp_id)
    return result


def validate_tags_already_in_db_with_positive_tag(db, new_tag_obj: TopicEntityTagModel, related_tags_in_db,
                                                  calculate_validation_values: bool = True):
    # 1. new tag positive, existing tag positive = validate existing (right) if existing is more generic
    # 2. new tag positive, existing tag negative = validate existing (wrong) if existing is more generic
    more_generic_topics = set(get_ancestors(onto_node=new_tag_obj.topic))  # type: ignore
    more_generic_topics.add(new_tag_obj.topic)
    more_generic_novelty = set(get_ancestors(new_tag_obj.data_novelty))
    more_generic_novelty.add(new_tag_obj.data_novelty)
    # SCRUM-6188: entity_type matching is ATP-hierarchy-aware (a more specific new tag
    # validates a more generic existing tag), consistent with topic/data_novelty above.
    more_generic_entity_types = atp_hierarchy_with_self(new_tag_obj.entity_type, ancestors=True)
    tag_in_db: TopicEntityTagModel
    for tag_in_db in related_tags_in_db:
        if tag_in_db.topic in more_generic_topics:
            if tag_in_db.entity_type is None or (tag_in_db.entity_type in more_generic_entity_types
                                                 and tag_in_db.entity == new_tag_obj.entity):
                if tag_in_db.species is None or tag_in_db.species == new_tag_obj.species:
                    # Check data novelty
                    if tag_in_db.data_novelty in more_generic_novelty:
                        add_validation_to_db(db, tag_in_db, new_tag_obj,
                                             calculate_validation_values=calculate_validation_values)
    # validate pure entity-only tags if the new tag is a mixed topic + entity tag for the same entity
    if new_tag_obj.entity is not None and new_tag_obj.entity_type != new_tag_obj.topic:
        for tag_in_db in related_tags_in_db:
            if (tag_in_db.topic == tag_in_db.entity_type
                    and tag_in_db.entity_type in more_generic_entity_types
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
    # SCRUM-6188: entity_type matching is ATP-hierarchy-aware (a more generic negative
    # new tag validates a more specific existing tag), consistent with topic above.
    more_specific_entity_types = atp_hierarchy_with_self(new_tag_obj.entity_type, ancestors=False)
    tag_in_db: TopicEntityTagModel
    for tag_in_db in related_tags_in_db:
        if tag_in_db.topic in more_specific_topics:
            if new_tag_obj.entity_type is None or (tag_in_db.entity_type in more_specific_entity_types
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
                    and tag_in_db.entity_type in more_specific_entity_types
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
    # SCRUM-6188: entity_type matching is ATP-hierarchy-aware, following the same
    # generic/specific direction as the topic/data_novelty checks in each branch.
    more_specific_entity_types = atp_hierarchy_with_self(new_tag_obj.entity_type, ancestors=False)
    more_generic_entity_types = atp_hierarchy_with_self(new_tag_obj.entity_type, ancestors=True)
    tag_in_db: TopicEntityTagModel
    for tag_in_db in related_validating_tags_in_db:
        if (tag_in_db.negated is False and tag_in_db.topic in more_specific_topics
                and tag_in_db.data_novelty in more_specific_novelty):
            if new_tag_obj.entity_type is None or (tag_in_db.entity_type in more_specific_entity_types
                                                   and tag_in_db.entity == new_tag_obj.entity):
                if new_tag_obj.species is None or tag_in_db.species == new_tag_obj.species:
                    add_validation_to_db(db, new_tag_obj, tag_in_db,
                                         calculate_validation_values=calculate_validation_values)
        elif (tag_in_db.negated is True and tag_in_db.topic in more_generic_topics
              and tag_in_db.data_novelty in more_generic_novelty):
            if tag_in_db.entity_type is None or (tag_in_db.entity_type in more_generic_entity_types
                                                 and tag_in_db.entity == new_tag_obj.entity):
                if tag_in_db.species is None or tag_in_db.species == new_tag_obj.species:
                    add_validation_to_db(db, new_tag_obj, tag_in_db,
                                         calculate_validation_values=calculate_validation_values)
    # if the new tag is a pure entity-only tag and there are mixed topic + entity tags with the same entity
    # validate positive or negative new tag only if existing is positive
    if new_tag_obj.topic == new_tag_obj.entity_type:
        for tag_in_db in related_validating_tags_in_db:
            if (tag_in_db.entity_type != tag_in_db.topic and tag_in_db.entity_type in more_specific_entity_types
                    and new_tag_obj.entity == tag_in_db.entity and tag_in_db.negated is False):
                if tag_in_db.data_novelty in more_specific_novelty:
                    add_validation_to_db(db, new_tag_obj, tag_in_db,
                                         calculate_validation_values=calculate_validation_values)
    # if the new tag is a mixed topic + entity tag and there are pure entity-only tags with the same entity
    # validate only positive new tag if existing is negative
    if new_tag_obj.negated is False and new_tag_obj.entity is not None and new_tag_obj.entity_type != new_tag_obj.topic:
        for tag_in_db in related_validating_tags_in_db:
            if (tag_in_db.negated is True and tag_in_db.topic == tag_in_db.entity_type
                    and tag_in_db.entity_type in more_generic_entity_types
                    and new_tag_obj.entity == tag_in_db.entity and tag_in_db.data_novelty in more_generic_novelty):
                add_validation_to_db(db, new_tag_obj, tag_in_db,
                                     calculate_validation_values=calculate_validation_values)


def add_validation_to_db(db: Session, validated_tag: TopicEntityTagModel, validating_tag: TopicEntityTagModel,
                         calculate_validation_values: bool = True):
    logger.info(f"Adding validation: tag {validated_tag.topic_entity_tag_id} validated by tag {validating_tag.topic_entity_tag_id}")
    # topic_entity_tag_validation is a set-membership join table (composite PK, no other
    # columns). The overlapping validation rules can re-assert the same (validated,
    # validating) pair within a single pass -- e.g. a pure-entity companion tag matched by
    # the originating mixed tag under more than one rule -- so the insert must be idempotent.
    # A bare INSERT would raise UniqueViolation and abort the whole validation pass.
    result = db.execute(text("INSERT INTO topic_entity_tag_validation (validated_topic_entity_tag_id, "
                             "validating_topic_entity_tag_id) VALUES (:validated_id, :validating_id) "
                             "ON CONFLICT DO NOTHING"),
                        {"validated_id": validated_tag.topic_entity_tag_id,
                         "validating_id": validating_tag.topic_entity_tag_id})
    if result.rowcount == 0:
        # Pair already recorded; nothing changed, so there is nothing to recompute.
        return
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
    """
    Detect duplicate tags. Per SCRUM-5716 strict-REST design:
      - Branch 1 (exact duplicate, no new info)               -> raise 409 reason=duplicate
      - Branch 2 (exact duplicate, new note to append)        -> upsert in place, return existing tag id
      - Branch 3 (opposite_negation in abc_literature_system) -> raise 409 reason=opposite_negation
      - Branches 4/5 (different creator)                      -> raise 409 reason=different_creator
                                                                  (bypassable with force_insertion=True)
      - No duplicate                                          -> return None
    """
    logger.info("Starting duplicate tag check")
    new_tag_data = copy.copy(topic_entity_tag_data)
    new_tag_data.pop('validation_by_author', None)
    new_tag_data.pop('validation_by_professional_biocurator', None)
    new_tag_data.pop('date_created', None)
    date_updated: str = new_tag_data.pop('date_updated', '')
    note = new_tag_data.pop('note', None)
    created_by_user = get_default_user_value()

    # Use the same imputation the INSERT event applies, so the filter below
    # searches for the values a fresh row would actually be stored with.
    new_tag_data['created_by'], new_tag_data['updated_by'] = impute_audit_user_ids(
        new_tag_data.get('created_by'), new_tag_data.get('updated_by'), created_by_user
    )

    logger.info("Checking for exact duplicate with same creator")
    # Optimize: Build a single query with filters instead of filter_by for better performance
    query = db.query(TopicEntityTagModel)
    for key, value in new_tag_data.items():
        query = query.filter(getattr(TopicEntityTagModel, key) == value)
    existing_tag = query.first()
    if existing_tag:
        existing_date_updated = existing_tag.date_updated
        existing_note_list = existing_tag.note.split(" | ") if existing_tag.note else []
        if (note and note in existing_note_list) or note is None:
            # Branch 1: exact duplicate, request has no new note (or note already present).
            # get_tet_with_names is only called here (the 409-detail path), not in
            # branch 2 — the upsert response is built by the router via show_tag, so
            # there's no point enriching new_tag_data here.
            tag_data = get_tet_with_names(db, tet=new_tag_data, curie_or_reference_id=str(reference_id))
            if note:
                tag_data['note'] = note
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "reason": "duplicate",
                    "message": "The tag already exists in the database.",
                    "existing_tag_id": existing_tag.topic_entity_tag_id,
                    "existing_note": existing_tag.note,
                    "existing_tag": tag_data,
                }
            )
        else:
            # Branch 2: exact duplicate but request carries a new note -> idempotent upsert
            # (append to existing note). Status 200 returned by router.
            new_note = note if existing_tag.note is None else existing_tag.note + " | " + note
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
                return existing_tag.topic_entity_tag_id
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
            # Branch 3: opposite_negation conflict (not bypassable).
            tag_data = get_tet_with_names(db, tet=topic_entity_tag_data, curie_or_reference_id=str(reference_id))
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "reason": "opposite_negation",
                    "message": "One or more tags already exist in the database with the opposite "
                               "'negated' value.",
                    "tag_data": tag_data,
                    "conflicting_tag_ids": [tag.topic_entity_tag_id for tag in similar_tags],
                }
            )

    if force_insertion:
        return None
    new_tag_data_wo_creator = copy.copy(new_tag_data)
    new_tag_data_wo_creator.pop('created_by')
    new_tag_data_wo_creator.pop('updated_by')
    existing_tag = db.query(TopicEntityTagModel).filter_by(**new_tag_data_wo_creator).first()
    if existing_tag:
        # Branches 4 & 5: tag exists with a different creator. Same conflict reason for
        # both note-matches-or-empty and note-differs; differentiate via existing_note.
        tag_data = get_tet_with_names(db, tet=new_tag_data, curie_or_reference_id=str(reference_id))
        if note:
            tag_data['note'] = note
        tag_data['topic_entity_tag_id'] = existing_tag.topic_entity_tag_id
        if existing_tag.note == note or note is None:
            message = "The tag, created by another curator, already exists in the database."
        elif existing_tag.note:
            message = "The tag with a different note, created by another curator, already exists in the database."
        else:
            message = "The tag without a note, created by another curator, already exists in the database."
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "reason": "different_creator",
                "message": message,
                "existing_tag_id": existing_tag.topic_entity_tag_id,
                "existing_created_by": existing_tag.created_by,
                "existing_note": existing_tag.note,
                "existing_tag": tag_data,
            }
        )

    # if no duplicates found, return None
    return None


_orm_column_keys_cache: Dict[Any, List[str]] = {}


def _orm_column_keys(model) -> List[str]:
    """Return the mapped scalar-column attribute names for an ORM model, cached.

    Uses SQLAlchemy inspection so the projection in _serialize_reference_tag_rows
    automatically tracks column changes (and never includes relationships or
    SQLAlchemy internal state).
    """
    keys = _orm_column_keys_cache.get(model)
    if keys is None:
        keys = [attr.key for attr in sa_inspect(model).mapper.column_attrs]
        _orm_column_keys_cache[model] = keys
    return keys


def _project_orm_columns(obj, column_keys: List[str]) -> Dict[str, Any]:
    """Build a plain dict of an ORM object's scalar columns via getattr.

    Replaces jsonable_encoder(vars(obj)), which recursively encoded the whole
    object graph (eager-loaded relationships + SQLAlchemy _sa_instance_state) and
    dominated batch serialization. Reading only the mapped columns yields native
    Python values; the endpoint's response_model handles the final JSON encoding,
    so the serialized output is unchanged while the per-tag cost drops sharply.
    """
    return {key: getattr(obj, key) for key in column_keys}


def _serialize_reference_tag_rows(db: Session, rows: List[TopicEntityTagModel], curie_to_name: dict):
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
    tet_column_keys = _orm_column_keys(TopicEntityTagModel)
    source_column_keys = _orm_column_keys(TopicEntityTagSourceModel)
    all_tet = []
    for tet in rows:
        tet_data = _project_orm_columns(tet, tet_column_keys)
        source = tet.topic_entity_tag_source
        source_data = _project_orm_columns(source, source_column_keys) if source else None
        tet_data["topic_entity_tag_source"] = source_data
        # Replace top-level created_by/updated_by if we have a display name
        for k in ("created_by", "updated_by"):
            uid = tet_data.get(k)
            if uid and uid in id_to_display_name:
                tet_data[k] = id_to_display_name[uid]
        # Nested source object: replace created_by/updated_by if present
        if source_data:
            for k in ("created_by", "updated_by"):
                uid = source_data.get(k)
                if uid and uid in id_to_display_name:
                    source_data[k] = id_to_display_name[uid]
        add_list_of_users_who_validated_tag(tet, tet_data)
        add_list_of_validating_tag_ids(tet, tet_data)
        if source_data:
            source_data["secondary_data_provider_abbreviation"] = mod_id_to_mod[
                source.secondary_data_provider_id]
        # Add ML model version if associated with an ML model
        if tet.ml_model:
            tet_data["ml_model_version"] = tet.ml_model.version_num
        all_tet.append(tet_data)
    return [get_tet_with_names(db, tag, curie_to_name) for tag in all_tet]


def _resolve_reference_ids_for_batch(db: Session, curies_or_reference_ids: List[str]):
    ident_to_ref_id: Dict[str, Optional[int]] = {}
    agrkb_curies: Set[str] = set()
    cross_reference_curies: Set[str] = set()

    for ident in curies_or_reference_ids:
        if ident in ident_to_ref_id:
            continue
        if ident.isdigit():
            ident_to_ref_id[ident] = int(ident)
        else:
            ident_to_ref_id[ident] = None
            if ident.startswith("AGRKB:"):
                agrkb_curies.add(ident)
            else:
                cross_reference_curies.add(ident)

    if agrkb_curies:
        rows = db.query(ReferenceModel.curie, ReferenceModel.reference_id).filter(
            ReferenceModel.curie.in_(agrkb_curies)).all()
        for curie, reference_id in rows:
            ident_to_ref_id[curie] = reference_id

    if cross_reference_curies:
        rows = db.query(CrossReferenceModel.curie, CrossReferenceModel.reference_id).filter(
            CrossReferenceModel.curie.in_(cross_reference_curies),
            CrossReferenceModel.is_obsolete.is_(False)).all()
        for curie, reference_id in rows:
            if ident_to_ref_id[curie] is None:
                ident_to_ref_id[curie] = reference_id

    return ident_to_ref_id


def show_all_reference_tags(db: Session, curie_or_reference_id, page: int = 1, page_size: int = None, count_only: bool = False, sort_by: str = None, desc_sort: bool = False, column_only: str = None, column_filter: str = None, column_values: str = None, curie_to_name: dict = None):      # noqa: C901

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

    # Eager-load validated_by: add_list_of_users_who_validated_tag /
    # add_list_of_validating_tag_ids read it for every tag, so without this it
    # lazy-loads once per tag (N+1) -- the dominant server-side cost when the
    # batch endpoint runs this for many references. selectinload (one extra
    # query for the whole result set) is limit-safe, unlike a collection
    # joinedload.
    query = db.query(TopicEntityTagModel).options(
        joinedload(TopicEntityTagModel.topic_entity_tag_source),
        joinedload(TopicEntityTagModel.ml_model),
        selectinload(TopicEntityTagModel.validated_by)).filter(
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

        page_q = query.offset((page - 1) * page_size if page_size else None).limit(page_size)
        rows = page_q.all()
        # Callers (e.g. the batch endpoint) may supply a precomputed map built
        # once across many references, to avoid the per-reference external lookups.
        if curie_to_name is None:
            curie_to_name = get_curie_to_name_from_all_tets(db, curie_or_reference_id)
        return _serialize_reference_tag_rows(db, rows, curie_to_name)


def _ci_in(column, values):
    """Case-insensitive IN over a string column. Facet values arrive as exact
    keyword-aggregation strings, but topic/confidence-level curies can differ in
    case between the ES bucket keys, the DB rows and the UI (which uppercases),
    so compare on UPPER() to avoid silent mismatches."""
    upper_values = [str(v).upper() for v in values if v is not None]
    return func.upper(column).in_(upper_values)


def _apply_batch_tag_filters(query, filters: Optional[Dict[str, Any]]):
    """Restrict a TET query to the tags the initial search asked for.

    Mirrors the TET facet criteria the search UI sends (searchActions.js). The
    nested-TET facets -- topic / confidence-level / source-method / source-
    evidence / data-novelty -- are combined according to the search's
    ``apply_to_single_tag`` mode so the grid shows the same tags the search
    selected its references on:

      * single-tag mode (the default, and what an absent flag means): a tag must
        satisfy ALL of these positive criteria on its own. ES bundles them into
        ONE nested query (processCombinedTETFacets), so they are ANDed here too.
      * multi-tag mode: ES turns each facet into its OWN nested query
        (processSingleFacet), so a reference can match because tag A carries the
        topic while a different tag B carries the source-method. ANDing on one tag
        would then return zero tags for such a reference -- an empty grid row for
        a genuine search hit -- so the positive criteria are ORed instead (a tag
        is kept if it matches ANY selected facet), i.e. the union of tags the
        search asked for.

    Confidence-score range, entity-type/entity and every negated criterion are
    applied as per-tag refinements in BOTH modes: the score slider deliberately
    keeps unscored curator/author tags and drops only out-of-range scored ones
    (mirroring the grid's client-side filter), entity_type/entity are not
    nested-TET facets in the search (they ride the non-nested facets), and the
    negated source/SEA use whole-reference semantics the candidate references
    have already passed.

    Pushing this into SQL is the core fix for the slow grid load: instead of
    fetching every tag on every reference (all topics, all sources) and filtering
    client-side, we return only the matching tags -- a far smaller payload to
    serialize, transfer and render.
    """
    if not filters:
        return query

    # Absent flag => single-tag (AND), preserving the original behavior for any
    # caller (older UI / explicit single-facet use) that does not send the mode.
    single_tag = filters.get("apply_to_single_tag", True)

    # Positive nested-TET facets, combined per the search's single/multi-tag mode.
    positive_conditions = []
    topics = filters.get("topics")
    if topics:
        positive_conditions.append(_ci_in(TopicEntityTagModel.topic, topics))

    confidence_levels = filters.get("confidence_levels")
    if confidence_levels:
        positive_conditions.append(_ci_in(TopicEntityTagModel.confidence_level, confidence_levels))

    data_novelty = filters.get("data_novelty")
    if data_novelty:
        positive_conditions.append(_ci_in(TopicEntityTagModel.data_novelty, data_novelty))

    source_methods = filters.get("source_methods")
    if source_methods:
        positive_conditions.append(TopicEntityTagModel.topic_entity_tag_source.has(
            _ci_in(TopicEntityTagSourceModel.source_method, source_methods)))

    source_evidence_assertions = filters.get("source_evidence_assertions")
    if source_evidence_assertions:
        positive_conditions.append(TopicEntityTagModel.topic_entity_tag_source.has(
            _ci_in(TopicEntityTagSourceModel.source_evidence_assertion, source_evidence_assertions)))

    if positive_conditions:
        if single_tag:
            for cond in positive_conditions:
                query = query.filter(cond)
        else:
            query = query.filter(or_(*positive_conditions))

    # --- per-tag refinements, applied in both modes -------------------------- #
    entity_types = filters.get("entity_types")
    if entity_types:
        query = query.filter(_ci_in(TopicEntityTagModel.entity_type, entity_types))

    entities = filters.get("entities")
    if entities:
        query = query.filter(_ci_in(TopicEntityTagModel.entity, entities))

    negated_confidence_levels = filters.get("negated_confidence_levels")
    if negated_confidence_levels:
        # Drop tags whose confidence level is excluded (e.g. "Exclude NEG"), but
        # keep tags with no level set -- matching the search/grid behavior.
        upper_values = [str(v).upper() for v in negated_confidence_levels if v is not None]
        query = query.filter(or_(
            TopicEntityTagModel.confidence_level.is_(None),
            func.upper(TopicEntityTagModel.confidence_level).notin_(upper_values)
        ))

    negated_source_methods = filters.get("negated_source_methods")
    if negated_source_methods:
        query = query.filter(~TopicEntityTagModel.topic_entity_tag_source.has(
            _ci_in(TopicEntityTagSourceModel.source_method, negated_source_methods)))

    negated_source_evidence_assertions = filters.get("negated_source_evidence_assertions")
    if negated_source_evidence_assertions:
        query = query.filter(~TopicEntityTagModel.topic_entity_tag_source.has(
            _ci_in(TopicEntityTagSourceModel.source_evidence_assertion, negated_source_evidence_assertions)))

    score_min = filters.get("confidence_score_min")
    score_max = filters.get("confidence_score_max")
    if score_min is not None or score_max is not None:
        range_conds = []
        if score_min is not None:
            range_conds.append(TopicEntityTagModel.confidence_score >= score_min)
        if score_max is not None:
            range_conds.append(TopicEntityTagModel.confidence_score <= score_max)
        # Tags with no score are kept (mirrors the client-side score filter).
        query = query.filter(or_(
            TopicEntityTagModel.confidence_score.is_(None),
            and_(*range_conds)
        ))

    return query


def _source_label(source_data: Optional[Dict[str, Any]]) -> str:
    """Replicate the UI's sourceLabel (groupTets.js) so per-source counts key on
    the same label the grid renders."""
    if not source_data:
        return "unknown"
    method = source_data.get("source_method") or "unknown"
    sec = source_data.get("secondary_data_provider_abbreviation")
    dp = source_data.get("data_provider")
    if sec:
        return f"{method} / {sec}"
    if dp:
        return f"{method} / {dp}"
    return method


def _build_tag_counts(serialized_tags: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """Aggregate entity-tag counts per reference -> topic (and per source within
    a topic) so the grid gets authoritative totals from the API rather than
    recomputing them client-side. Topic keys are uppercased to match the UI's
    normalizeCurie.

    Curator-source tags are excluded -- identically to _build_tag_entries -- so
    these totals stay consistent with the displayed mini-rows: curator-submitted
    tags surface in the Validation column (computed client-side from the raw
    tags), not in the per-topic Sources/Tag/Conf/Note cells these counts back.
    Counting them here would let a topic show a count badge with no expandable
    rows (the mismatch is otherwise invisible because no consumer renders these
    counts yet)."""
    counts: Dict[int, Dict[str, Any]] = defaultdict(dict)
    for tag in serialized_tags:
        if _is_curator_source_tag(tag):
            continue
        ref_id = tag["reference_id"]
        topic = str(tag.get("topic") or "").upper()
        entity = tag.get("entity")
        negated = bool(tag.get("negated"))
        if not entity:
            kind = "topic_only"
        elif negated:
            kind = "entity_neg"
        else:
            kind = "entity_pos"

        topic_bucket = counts[ref_id].setdefault(
            topic,
            {"topic_only": 0, "entity_pos": 0, "entity_neg": 0, "total": 0, "by_source": {}}
        )
        topic_bucket[kind] += 1
        topic_bucket["total"] += 1

        label = _source_label(tag.get("topic_entity_tag_source"))
        src_bucket = topic_bucket["by_source"].setdefault(
            label, {"topic_only": 0, "entity_pos": 0, "entity_neg": 0}
        )
        src_bucket[kind] += 1
    return counts


def _is_curator_source_tag(tag: Dict[str, Any]) -> bool:
    source = tag.get("topic_entity_tag_source") or {}
    return source.get("validation_type") in ("professional_biocurator", "professional_curator")


def _entry_base(label: str, source_data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "aggregated": True,
        "sourceLabel": label,
        "source_label": label,
        "source_description": source_data.get("description"),
        "source_evidence_assertion": source_data.get("source_evidence_assertion"),
    }


def _entity_label(tag: Dict[str, Any]) -> str:
    return tag.get("entity_name") or tag.get("entity") or ""


def _score_summary(tags: List[Dict[str, Any]]) -> Dict[str, Any]:
    vals = [
        float(tag["confidence_score"])
        for tag in tags
        if tag.get("confidence_score") is not None
    ]
    if not vals:
        return {"confidence_score_min": None, "confidence_score_max": None, "confidence_score_count": 0}
    return {
        "confidence_score_min": min(vals),
        "confidence_score_max": max(vals),
        "confidence_score_count": len(vals),
    }


def _level_summary(tags: List[Dict[str, Any]]) -> Dict[str, Any]:
    levels = []
    seen = set()
    for tag in tags:
        level = tag.get("confidence_level")
        if level is None or level == "" or level in seen:
            continue
        seen.add(level)
        levels.append(level)
    return {"confidence_levels": levels}


def _note_summary(tags: List[Dict[str, Any]]) -> Dict[str, Any]:
    notes = [
        {"entity": _entity_label(tag), "note": tag.get("note")}
        for tag in tags
        if tag.get("note")
    ]
    return {"notes": notes}


def _build_tag_entries(serialized_tags: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """Build grid mini-rows per reference/topic/source in the API.

    The UI previously rebuilt these buckets for every cell render/filter/sort.
    Returning them with the batch endpoint keeps entity counts and per-source
    aggregation authoritative in the API while preserving raw tags separately
    for validation actions.
    """
    grouped: Dict[int, Dict[str, Dict[str, List[Dict[str, Any]]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )
    source_data_by_label: Dict[Tuple[int, str, str], Dict[str, Any]] = {}

    for tag in serialized_tags:
        if _is_curator_source_tag(tag):
            continue
        ref_id = tag["reference_id"]
        topic = str(tag.get("topic") or "").upper()
        source_data = tag.get("topic_entity_tag_source") or {}
        label = _source_label(source_data)
        grouped[ref_id][topic][label].append(tag)
        source_data_by_label[(ref_id, topic, label)] = source_data

    entries_by_ref_topic: Dict[int, Dict[str, Any]] = defaultdict(dict)
    for ref_id, by_topic in grouped.items():
        for topic, by_source in by_topic.items():
            entries = []
            for label, tags in by_source.items():
                source_data = source_data_by_label.get((ref_id, topic, label), {})
                topic_only = [tag for tag in tags if not tag.get("entity")]
                entity_positive = [tag for tag in tags if tag.get("entity") and not tag.get("negated")]
                entity_negative = [tag for tag in tags if tag.get("entity") and tag.get("negated")]

                for tag in topic_only:
                    entry = {
                        **_entry_base(label, source_data),
                        "key": f"t-{tag.get('topic_entity_tag_id')}",
                        "kind": "topic",
                        "topic_entity_tag_id": tag.get("topic_entity_tag_id"),
                        "negated": bool(tag.get("negated")),
                        "confidence_score": tag.get("confidence_score"),
                        "confidence_level": tag.get("confidence_level"),
                        "note": tag.get("note"),
                    }
                    entries.append(entry)

                for kind, tags_for_kind in (
                    ("entity-pos", entity_positive),
                    ("entity-neg", entity_negative),
                ):
                    if not tags_for_kind:
                        continue
                    entity_labels = [_entity_label(tag) for tag in tags_for_kind]
                    visible_entity_labels = [entity_label for entity_label in entity_labels[:20] if entity_label]
                    entities_text = ", ".join(visible_entity_labels)
                    if len(entity_labels) > 20:
                        entities_text += f", ... (+{len(entity_labels) - 20} more)"
                    entry = {
                        **_entry_base(label, source_data),
                        "key": f"{kind}-{label}",
                        "kind": kind,
                        "count": len(tags_for_kind),
                        "entities_text": entities_text,
                        **_score_summary(tags_for_kind),
                        **_level_summary(tags_for_kind),
                        **_note_summary(tags_for_kind),
                    }
                    entries.append(entry)
            entries_by_ref_topic[ref_id][topic] = entries
    return entries_by_ref_topic


def _build_validation_details(serialized_tags: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """Aggregate the curator validation of each reference -> topic so the grid's
    Validation column can sort, filter AND render without deriving any of it from
    raw tags client-side.

    Mirrors ValidationCell + validationState (UI): only *topic-level* tags (no
    entity) from a professional-biocurator/curator source count as a validation.
    Per (reference, topic) it returns::

        {"state": "positive" | "negative" | "conflict",
         "positives": <count>, "negatives": <count>,
         "by_curator": [{"name", "negated", "sources": [{"method", "label"}],
                         "species": [curie, ...]}, ...]}

    ``by_curator`` groups validations by (curator display name, polarity) -- the
    same grouping ValidationCell renders -- accumulating the distinct source
    methods (label = ``method [/ secondary_data_provider]``) and species per
    group, preserving first-seen order. ``name`` falls back to
    '(unknown curator)' so no validation is dropped. Topics with no curator
    validation are omitted (client treats an absent topic as 'unvalidated').
    Topic keys are uppercased to match the UI's normalizeCurie."""
    # ref_id -> topic -> {positives, negatives, by_curator: {(name, negated): grp}}
    acc: Dict[int, Dict[str, Dict[str, Any]]] = defaultdict(lambda: defaultdict(
        lambda: {"positives": 0, "negatives": 0, "by_curator": {}}))
    for tag in serialized_tags:
        if tag.get("entity") or not _is_curator_source_tag(tag):
            continue
        ref_id = tag["reference_id"]
        topic = str(tag.get("topic") or "").upper()
        bucket = acc[ref_id][topic]
        negated = bool(tag.get("negated"))
        if negated:
            bucket["negatives"] += 1
        else:
            bucket["positives"] += 1
        name = tag.get("created_by") or "(unknown curator)"
        group = bucket["by_curator"].get((name, negated))
        if group is None:
            group = {"name": name, "negated": negated, "sources": {}, "species": []}
            bucket["by_curator"][(name, negated)] = group
        source = tag.get("topic_entity_tag_source") or {}
        method = source.get("source_method")
        if method and method not in group["sources"]:
            sec = source.get("secondary_data_provider_abbreviation")
            group["sources"][method] = f"{method} / {sec}" if sec else method
        species = tag.get("species")
        if species and species not in group["species"]:
            group["species"].append(species)

    out: Dict[int, Dict[str, Any]] = defaultdict(dict)
    for ref_id, by_topic in acc.items():
        for topic, bucket in by_topic.items():
            pos, neg = bucket["positives"], bucket["negatives"]
            state = "conflict" if (pos and neg) else ("positive" if pos else "negative")
            out[ref_id][topic] = {
                "state": state,
                "positives": pos,
                "negatives": neg,
                "by_curator": [
                    {"name": g["name"], "negated": g["negated"],
                     "sources": [{"method": m, "label": lab} for m, lab in g["sources"].items()],
                     "species": g["species"]}
                    for g in bucket["by_curator"].values()
                ],
            }
    return out


def _build_filter_flags(serialized_tags: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """Per reference -> topic boolean flags backing the grid's per-topic cell
    filter (TopicCellFilter / cellPredicate) so it can filter without scanning
    raw tags. Computed over ALL tags of the (reference, topic) cell -- entity and
    topic-level, curator and non-curator -- matching cellPredicate's asTets set::

        has_any  -> the cell has >= 1 tag         ('has any tag'; 'empty' == not has_any)
        has_y    -> >= 1 tag with negated False    ('has Y')
        has_n    -> >= 1 tag with negated True      ('has N')
        has_note -> >= 1 tag with a non-empty note  ('has note')

    'my validation present' is intentionally omitted pending the curator-identity
    model (created_by is a display name in the serialized tag, the UI compares it
    to the Okta subject id). Topic keys are uppercased to match normalizeCurie."""
    flags: Dict[int, Dict[str, Any]] = defaultdict(lambda: defaultdict(
        lambda: {"has_any": False, "has_y": False, "has_n": False, "has_note": False}))
    for tag in serialized_tags:
        ref_id = tag["reference_id"]
        topic = str(tag.get("topic") or "").upper()
        cell = flags[ref_id][topic]
        cell["has_any"] = True
        if tag.get("negated") is True:
            cell["has_n"] = True
        elif tag.get("negated") is False:
            cell["has_y"] = True
        note = tag.get("note")
        if note is not None and note != "":
            cell["has_note"] = True
    return flags


def _build_discovery(serialized_tags: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Batch-global discovery aggregate backing the grid's column set and source
    filter so neither has to be derived from raw tags client-side. Unlike the
    other aggregates this is NOT keyed per reference -- it summarises the whole
    batch (post-filter) once.

    ``topics`` is the distinct set of topic columns present across the WHOLE batch
    -- over ALL tags (entity + topic-level, curator + non-curator) -- matching the
    column the grid renders whenever any tag exists for a topic. Each entry is
    ``{"curie": <UPPERCASED topic>, "name": <topic display name>}``.

    ``sources`` is the distinct set of source labels present, computed over
    NON-curator tags only -- identically to _build_tag_counts / _build_tag_entries
    -- because curator submissions surface in the Validation column, not in the
    per-source Sources cells the source filter targets. Each entry is
    ``{"label", "method", "secondary_data_provider", "data_provider"}`` so the UI
    can render and group without re-parsing the label.

    Both lists preserve first-seen order (deterministic: the batch query orders by
    reference_id then topic_entity_tag_id), consistent with the other aggregates;
    the client may re-sort for presentation. Topic keys are uppercased to match
    the UI's normalizeCurie."""
    topics: Dict[str, Dict[str, Any]] = {}
    sources: Dict[str, Dict[str, Any]] = {}
    for tag in serialized_tags:
        topic = str(tag.get("topic") or "").upper()
        if topic and topic not in topics:
            topics[topic] = {"curie": topic, "name": tag.get("topic_name") or topic}
        if _is_curator_source_tag(tag):
            continue
        source_data = tag.get("topic_entity_tag_source") or {}
        label = _source_label(source_data)
        if label not in sources:
            sources[label] = {
                "label": label,
                "method": source_data.get("source_method"),
                "secondary_data_provider": source_data.get("secondary_data_provider_abbreviation"),
                "data_provider": source_data.get("data_provider"),
            }
    return {"topics": list(topics.values()), "sources": list(sources.values())}


def show_all_reference_tags_for_references(db: Session, curies_or_reference_ids: List[str],
                                           filters: Optional[Dict[str, Any]] = None):
    """Batch variant of show_all_reference_tags.

    Returns ``{"tags": {ident: [...]}, "counts": {ident: {topic: {...}}},
    "entries": {ident: {topic: [...]}}}``.

    ``tags`` maps each input identifier -> its TET list (filtered to the optional
    ``filters`` -- the criteria from the initial search), reusing the
    single-reference logic so the per-tag enrichment (display names, validated_by,
    ml_model version, etc.) is identical. This lets the TET validation grid fetch
    every reference's tags for a search page in ONE request instead of firing one
    HTTP call per reference (the previous per-row fan-out was the grid's main
    bottleneck), and -- when the search specified an entity/topic -- only the
    matching tags rather than every tag on every reference.

    ``counts`` maps each identifier -> per-topic entity-tag counts (topic-only /
    positive-entity / negative-entity totals, plus a per-source breakdown), so the
    grid gets authoritative aggregates from the API. Unresolvable identifiers map
    to an empty list / empty counts, mirroring the per-row endpoint's behavior so a
    single bad id never fails the whole batch.

    The curie->name resolution (external A-team lookups) is done ONCE across the
    union of every reference's tags, rather than once per reference -- otherwise
    the batch would just serialize the same N x external-call fan-out into a
    single long request and end up slower than the old concurrent per-row calls.
    """
    total_start = perf_counter()
    resolve_start = perf_counter()
    ident_to_ref_id = _resolve_reference_ids_for_batch(db, curies_or_reference_ids)
    resolved_count = len([rid for rid in ident_to_ref_id.values() if rid is not None])
    resolve_ms = (perf_counter() - resolve_start) * 1000
    _log_tet_batch_timing(
        "TET batch refs resolved in %.1fms: inputs=%s unique=%s resolved=%s",
        resolve_ms,
        len(curies_or_reference_ids),
        len(ident_to_ref_id),
        resolved_count
    )

    ref_ids = [rid for rid in ident_to_ref_id.values() if rid is not None]
    if not ref_ids:
        _log_tet_batch_timing(
            "TET batch total in %.1fms: inputs=%s resolved=0 tags=0",
            (perf_counter() - total_start) * 1000,
            len(curies_or_reference_ids)
        )
        return {
            "tags": {ident: [] for ident in ident_to_ref_id},
            "counts": {ident: {} for ident in ident_to_ref_id},
            "entries": {ident: {} for ident in ident_to_ref_id},
            "validation": {ident: {} for ident in ident_to_ref_id},
            "filter_flags": {ident: {} for ident in ident_to_ref_id},
            "discovery": {"topics": [], "sources": []},
            "debug_timing": None
        }

    query_start = perf_counter()
    query = db.query(TopicEntityTagModel).options(
        joinedload(TopicEntityTagModel.topic_entity_tag_source),
        joinedload(TopicEntityTagModel.ml_model),
        selectinload(TopicEntityTagModel.validated_by)).filter(
        TopicEntityTagModel.reference_id.in_(ref_ids))
    # Restrict to the tags the initial search asked for (topic/confidence/source/
    # data-novelty/score). This is what keeps the grid load small and fast.
    query = _apply_batch_tag_filters(query, filters)
    rows = query.order_by(
        TopicEntityTagModel.reference_id,
        TopicEntityTagModel.topic_entity_tag_id).all()
    query_ms = (perf_counter() - query_start) * 1000
    _log_tet_batch_timing(
        "TET batch rows queried in %.1fms: refs=%s rows=%s",
        query_ms,
        len(ref_ids),
        len(rows)
    )

    # One name map for the union of all tags (the expensive external lookups).
    names_start = perf_counter()
    curie_to_name = build_curie_to_name_map(db, rows)
    names_ms = (perf_counter() - names_start) * 1000
    _log_tet_batch_timing(
        "TET batch names mapped in %.1fms: names=%s",
        names_ms,
        len(curie_to_name)
    )

    serialize_start = perf_counter()
    serialized_tags = _serialize_reference_tag_rows(db, rows, curie_to_name)
    tags_by_ref_id = defaultdict(list)
    for tag in serialized_tags:
        tags_by_ref_id[tag["reference_id"]].append(tag)
    counts_by_ref_id = _build_tag_counts(serialized_tags)
    entries_by_ref_id = _build_tag_entries(serialized_tags)
    validation_by_ref_id = _build_validation_details(serialized_tags)
    filter_flags_by_ref_id = _build_filter_flags(serialized_tags)
    discovery_result = _build_discovery(serialized_tags)
    serialize_ms = (perf_counter() - serialize_start) * 1000
    _log_tet_batch_timing(
        "TET batch serialized in %.1fms: tags=%s",
        serialize_ms,
        len(serialized_tags)
    )

    tags_result: Dict[str, Any] = {}
    counts_result: Dict[str, Any] = {}
    entries_result: Dict[str, Any] = {}
    validation_result: Dict[str, Any] = {}
    filter_flags_result: Dict[str, Any] = {}
    for ident, ref_id in ident_to_ref_id.items():
        if ref_id is None:
            tags_result[ident] = []
            counts_result[ident] = {}
            entries_result[ident] = {}
            validation_result[ident] = {}
            filter_flags_result[ident] = {}
        else:
            tags_result[ident] = tags_by_ref_id[ref_id]
            counts_result[ident] = counts_by_ref_id.get(ref_id, {})
            entries_result[ident] = entries_by_ref_id.get(ref_id, {})
            validation_result[ident] = validation_by_ref_id.get(ref_id, {})
            filter_flags_result[ident] = filter_flags_by_ref_id.get(ref_id, {})
    total_ms = (perf_counter() - total_start) * 1000
    _log_tet_batch_timing(
        "TET batch total in %.1fms: inputs=%s unique=%s resolved=%s tags=%s",
        total_ms,
        len(curies_or_reference_ids),
        len(ident_to_ref_id),
        resolved_count,
        len(serialized_tags)
    )
    debug_timing = None
    if _tet_batch_timing_enabled():
        debug_timing = {
            "resolve_ms": round(resolve_ms, 1),
            "query_ms": round(query_ms, 1),
            "names_ms": round(names_ms, 1),
            "serialize_ms": round(serialize_ms, 1),
            "total_ms": round(total_ms, 1),
            "refs_resolved": resolved_count,
            "rows": len(rows),
            "names": len(curie_to_name),
            "tags": len(serialized_tags),
        }
    return {
        "tags": tags_result,
        "counts": counts_result,
        "entries": entries_result,
        "validation": validation_result,
        "filter_flags": filter_flags_result,
        "discovery": discovery_result,
        "debug_timing": debug_timing
    }


def get_all_topic_entity_tags_by_mod(db: Session, mod_abbreviation: str, days_updated: int = 7):

    current_date = datetime.now()
    past_date = current_date - timedelta(days=int(days_updated))
    last_date_updated = past_date.strftime("%Y-%m-%d")

    rows = db.execute(text("SELECT cr.curie, tet.*, "
                           "get_most_current_email(u.person_id) AS email "
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
    return build_curie_to_name_map(db, ref_related_tets)


def get_curie_to_name_from_references(db: Session, reference_ids: List[int]):
    """Build ONE curie->name map for the union of all tags across many
    references. The external A-team name lookups (atpterm / ecoterm / species /
    entity) are the expensive part, so resolving the union once -- instead of
    once per reference -- is what makes the batch endpoint actually faster than
    the old per-reference fan-out."""
    if not reference_ids:
        return {}
    ref_related_tets = db.query(TopicEntityTagModel).options(
        joinedload(TopicEntityTagModel.topic_entity_tag_source)).filter(
        TopicEntityTagModel.reference_id.in_(reference_ids)).all()
    return build_curie_to_name_map(db, ref_related_tets)


def _get_cached_curie_names(curies, fetch_names):
    curie_list = list(dict.fromkeys([curie for curie in curies if curie]))
    curie_to_name = {}
    missing = []
    for curie in curie_list:
        cached_name = id_to_name_cache.get(curie)
        if cached_name is None:
            missing.append(curie)
        else:
            curie_to_name[curie] = cached_name

    if missing:
        fetched = fetch_names(missing) or {}
        curie_to_name.update(fetched)
        for curie, name in fetched.items():
            # Skip caching identity fallbacks (name == curie): map_curies_to_names
            # returns {curie: curie} when the A-team lookup fails or returns nothing.
            # Caching those would poison id_to_name_cache for the full TTL and keep the
            # grid showing raw curies even after A-team recovers. They are still returned
            # for this request (raw-curie display fallback); leaving them uncached makes
            # the next request re-fetch them.
            if name and name != curie:
                id_to_name_cache.set(curie, name)
    return curie_to_name


def build_curie_to_name_map(db: Session, ref_related_tets):
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
    entity_curie_to_name = _get_cached_curie_names(
        all_atp_terms,
        lambda missing: get_map_ateam_curies_to_names(category="atpterm", curies=missing)
    )
    entity_curie_to_name.update(_get_cached_curie_names(
        source_eco_codes,
        lambda missing: get_map_ateam_curies_to_names(category="ecoterm", curies=missing)
    ))
    entity_curie_to_name.update(_get_cached_curie_names(
        tag_species,
        lambda missing: get_map_ateam_curies_to_names(category="species", curies=missing)
    ))
    for entity_id_validation, entity_type_curies_dict in entity_id_validation_entity_type_entities.items():
        for entity_type, curies in entity_type_curies_dict.items():
            entity_type_name = entity_curie_to_name[entity_type]
            entity_curie_to_name.update(_get_cached_curie_names(
                curies,
                lambda missing: get_map_entity_curies_to_names(
                    db, entity_id_validation=entity_id_validation,
                    curies_category=entity_type_name,
                    curies=missing)
            ))
    for curie_without_name in (all_entity_curies | all_atp_terms) - set(entity_curie_to_name.keys()):
        entity_curie_to_name[curie_without_name] = curie_without_name
    return entity_curie_to_name


def get_tet_with_names(db: Session, tet, curie_to_name_mapping: Dict = None, curie_or_reference_id: str = None):
    if curie_to_name_mapping is None:
        curie_to_name_mapping = get_curie_to_name_from_all_tets(db, str(curie_or_reference_id))
    # Shallow-copy only the two levels we add keys to (the top-level tag dict and
    # its nested topic_entity_tag_source dict) instead of copy.deepcopy(tet). The
    # previous deepcopy recursed into every nested value of every tag and
    # dominated batch serialization (~443ms for 1614 tags); since we only ever
    # add "<field>_name" keys at these two levels, a two-level shallow copy is
    # equivalent and far cheaper while still leaving the input dict untouched.
    new_tet = dict(tet)
    new_source = None
    source = new_tet.get("topic_entity_tag_source")
    if source:
        new_source = dict(source)
        new_tet["topic_entity_tag_source"] = new_source
    for tet_field_name, tet_field_value in tet.items():
        if tet_field_name == "topic_entity_tag_source":
            if new_source is not None:
                for source_field_name, source_field_value in tet_field_value.items():
                    if source_field_name in TET_SOURCE_CURIE_FIELDS:
                        new_field = f"{source_field_name}_name"
                        new_source[new_field] = curie_to_name_mapping.get(source_field_value, source_field_value)
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
