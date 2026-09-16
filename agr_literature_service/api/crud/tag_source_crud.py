"""
tag_source_crud.py
==================

CRUD for the shared tag_source table. Split out of topic_entity_tag_crud.py by
SCRUM-6518: the source is no longer TET-specific, and curation_status needs the
ABC-source helper without importing the whole TET module.

This module must not import topic_entity_tag_crud - the dependency runs the
other way. Its shared helpers live in topic_entity_tag_utils, which depends on
neither.
"""
from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import and_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.topic_entity_tag_utils import (
    add_audited_object_users_if_not_exist,
    add_source_obj_to_db_session,
    get_source_from_db,
)
from agr_literature_service.api.models import ModModel, TagSourceModel
from agr_literature_service.api.schemas.tag_source_schemas import (
    TagSourceSchemaCreate,
    TagSourceSchemaUpdate,
)

# The ABC source: the per-MOD "professional biocurator using the ABC data entry
# form" row. Shared by the TET validation write path, the curation_status
# attribution path and the SCRUM-6518 backfill.
CURATOR_VALIDATION_SOURCE_EVIDENCE_ASSERTION = "ATP:0000036"
CURATOR_VALIDATION_SOURCE_METHOD = "abc_literature_system"
CURATOR_VALIDATION_TYPE = "professional_curator"
CURATOR_VALIDATION_SOURCE_DESCRIPTION = (
    "Trained professional biocurator specializing in curation of model organism "
    "data using the ABC data entry form.")


def create_source(db: Session, source: TagSourceSchemaCreate):
    source_data = {key: value for key, value in jsonable_encoder(source).items() if value is not None}
    source_obj = add_source_obj_to_db_session(db, source_data)
    try:
        db.commit()
    except (IntegrityError, HTTPException) as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"invalid request: {e}")
    return source_obj.tag_source_id


def destroy_source(db: Session, tag_source_id: int):
    source = get_source_from_db(db, tag_source_id)
    db.delete(source)
    db.commit()


def patch_source(db: Session, tag_source_id: int, source_patch: TagSourceSchemaUpdate):
    source = get_source_from_db(db, tag_source_id)
    source_patch_data = source_patch.model_dump(exclude_unset=True)
    add_audited_object_users_if_not_exist(db, source_patch_data)
    for key, value in source_patch_data.items():
        setattr(source, key, value)
    db.commit()
    return {"message": "updated"}


def show_source(db: Session, tag_source_id: int):
    source = get_source_from_db(db, tag_source_id)
    source_data = jsonable_encoder(source)
    del source_data["secondary_data_provider_id"]
    source_data["secondary_data_provider_abbreviation"] = source.secondary_data_provider.abbreviation
    return source_data


def show_source_by_name(db: Session, source_evidence_assertion: str, source_method: str,
                        data_provider: str, secondary_data_provider_abbreviation: str):
    secondary_data_provider = db.query(ModModel.mod_id).filter(
        ModModel.abbreviation == secondary_data_provider_abbreviation).one_or_none()
    if secondary_data_provider is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Cannot find the specified secondary data provider")
    source = db.query(TagSourceModel).filter(
        and_(
            TagSourceModel.source_evidence_assertion == source_evidence_assertion,
            TagSourceModel.source_method == source_method,
            TagSourceModel.data_provider == data_provider,
            TagSourceModel.secondary_data_provider_id == secondary_data_provider.mod_id
        )
    ).one_or_none()
    if source is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cannot find the specified Source")
    source_data = jsonable_encoder(source)
    del source_data["secondary_data_provider_id"]
    source_data["secondary_data_provider_abbreviation"] = secondary_data_provider_abbreviation
    return source_data


def show_all_source(db: Session):
    return [jsonable_encoder(source) for source in db.query(TagSourceModel).all()]


def get_or_create_abc_source(db: Session, mod_abbreviation: str) -> TagSourceModel:
    """Resolve the per-MOD ABC source used for grid validations and for curation
    status attribution (SCRUM-6518), creating
    it if absent. Server-side equivalent of the UI's getCuratorSourceId (GET the
    source by name, POST to create on 404) so the validate write path no longer
    needs the client to resolve a source id first.

    validation_type is set to CURATOR_VALIDATION_TYPE ('professional_curator') to
    match exactly what getCuratorSourceId POSTs. NOTE the source unique key
    (source_evidence_assertion, source_method, data_provider,
    secondary_data_provider) excludes validation_type, so when a source already
    exists for the MOD it is reused verbatim -- the same row the UI write path
    uses. That means the resolved source's validation_type is NOT guaranteed to be
    'professional_curator': a pre-existing curator source may be
    'professional_biocurator' for some MODs, and the opposite-negation guard in
    check_for_duplicate_tags (Branch 3) fires only for that value. validate_topic
    does not rely on the validation_type either way -- it deletes the curator's
    prior validation before inserting the new one, so a flipped re-validation
    never trips Branch 3 regardless of the source's validation_type."""
    mod = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).one_or_none()
    if mod is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Cannot find the MOD '{mod_abbreviation}'")

    def _lookup():
        return db.query(TagSourceModel).filter(
            TagSourceModel.source_evidence_assertion == CURATOR_VALIDATION_SOURCE_EVIDENCE_ASSERTION,
            TagSourceModel.source_method == CURATOR_VALIDATION_SOURCE_METHOD,
            TagSourceModel.data_provider == mod_abbreviation,
            TagSourceModel.secondary_data_provider_id == mod.mod_id,
        ).first()

    source = _lookup()
    if source is not None:
        return source
    try:
        new_source_id = create_source(db, TagSourceSchemaCreate(
            source_evidence_assertion=CURATOR_VALIDATION_SOURCE_EVIDENCE_ASSERTION,
            source_method=CURATOR_VALIDATION_SOURCE_METHOD,
            validation_type=CURATOR_VALIDATION_TYPE,
            description=CURATOR_VALIDATION_SOURCE_DESCRIPTION,
            data_provider=mod_abbreviation,
            secondary_data_provider_abbreviation=mod_abbreviation,
        ))
    except HTTPException:
        # Lost a create race with a concurrent first-time validation for the same
        # MOD: create_source rolls back and raises 422 on the unique-key
        # violation. The row exists now -- re-fetch and use it rather than
        # failing the request.
        db.rollback()
        source = _lookup()
        if source is None:
            raise
        return source
    return get_source_from_db(db, new_source_id)
