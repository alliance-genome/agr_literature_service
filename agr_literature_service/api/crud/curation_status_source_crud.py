"""
curation_status_source_crud.py
==============================

SCRUM-6518. Source attributions for curation status.

Every function here is independent of the curation_status row: none of them
read or write its values. Sources may disagree with the base row and with each
other, and nothing is reconciled.
"""
from typing import Any, Dict, List

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from agr_literature_service.api.models import (
    CurationStatusModel,
    CurationStatusSourceAssociationModel,
    TagSourceModel,
)

# The columns a source reports. Anything outside this set is never mirrored
# from a curation_status write onto an association.
VALUE_FIELDS = ("curation_status", "curation_tag", "note")


def _get_curation_status_or_404(db: Session, curation_status_id: int) -> CurationStatusModel:
    curation_status = db.query(CurationStatusModel).filter(
        CurationStatusModel.curation_status_id == curation_status_id).one_or_none()
    if curation_status is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"CurationStatus with curation_status_id {curation_status_id} not found")
    return curation_status


def _get_association_or_404(db: Session,
                            curation_status_source_association_id: int
                            ) -> CurationStatusSourceAssociationModel:
    association = db.query(CurationStatusSourceAssociationModel).filter(
        CurationStatusSourceAssociationModel.curation_status_source_association_id
        == curation_status_source_association_id).one_or_none()
    if association is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(f"CurationStatusSourceAssociation with curation_status_source_association_id "
                    f"{curation_status_source_association_id} not found"))
    return association


def get_tag_source_or_404(db: Session, tag_source_id: int) -> TagSourceModel:
    source = db.query(TagSourceModel).filter(
        TagSourceModel.tag_source_id == tag_source_id).one_or_none()
    if source is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"TagSource with tag_source_id {tag_source_id} not found")
    return source


def stage_association(db: Session, curation_status_id: int, tag_source_id: int,
                      values: Dict[str, Any],
                      curation_status_mod_id: int) -> CurationStatusSourceAssociationModel:
    """Upsert an association WITHOUT committing; the caller owns the transaction.

    curation_status_crud uses this to write the base row and its attribution in
    a single transaction: validating the source only after the base row is
    committed would 404 while leaving an orphaned curation_status behind, and
    the caller's retry would then trip the (topic, reference_id, mod_id) unique
    constraint (found in review).

    ``values`` carries only the fields the caller is asserting; fields absent
    from it are left as they are on an existing association, so a PATCH that
    only changes the note does not blank this source's reported status. An
    empty ``values`` is allowed and records that this source touched the row
    without asserting any value.

    date_created is deliberately NOT set here - AuditedModel.before_insert
    stamps it (tz-aware UTC) for every audited table, and pre-setting it both
    duplicates that and makes this table's timestamps naive while every other
    table's are aware.
    """
    source = get_tag_source_or_404(db, tag_source_id)
    # A source may DISAGREE with the base row, but it may not belong to another
    # MOD: a WB curation_status attributed to an SGD source would make per-MOD
    # rollups count a foreign MOD against the paper. The owning MOD is the
    # source's secondary_data_provider, not its data_provider.
    if source.secondary_data_provider_id != curation_status_mod_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(f"TagSource {tag_source_id} belongs to a different MOD than "
                    f"curation_status {curation_status_id}; a curation status may only be "
                    f"attributed to a source of its own MOD"))
    association = db.query(CurationStatusSourceAssociationModel).filter(
        CurationStatusSourceAssociationModel.curation_status_id == curation_status_id,
        CurationStatusSourceAssociationModel.tag_source_id == tag_source_id,
    ).one_or_none()
    if association is None:
        association = CurationStatusSourceAssociationModel(
            curation_status_id=curation_status_id,
            tag_source_id=tag_source_id,
        )
        db.add(association)
    for field in VALUE_FIELDS:
        if field in values:
            setattr(association, field, values[field])
    return association


def upsert_association_and_show(db: Session, curation_status_id: int, tag_source_id: int,
                                values: Dict[str, Any]) -> Dict[str, Any]:
    """upsert_association, returning the flattened read shape without re-querying."""
    curation_status = _get_curation_status_or_404(db, curation_status_id)
    source = get_tag_source_or_404(db, tag_source_id)
    association = stage_association(db, curation_status_id, tag_source_id, values,
                                    curation_status.mod_id)
    db.commit()
    db.refresh(association)
    return _as_dict(association, source)


def patch_association_and_show(db: Session, curation_status_source_association_id: int,
                               association_update) -> Dict[str, Any]:
    """patch_association, returning the flattened read shape without re-querying."""
    association = patch_association(db, curation_status_source_association_id,
                                    association_update)
    source = get_tag_source_or_404(db, association.tag_source_id)
    return _as_dict(association, source)


def patch_association(db: Session, curation_status_source_association_id: int,
                      association_update) -> CurationStatusSourceAssociationModel:
    association = _get_association_or_404(db, curation_status_source_association_id)
    for field, value in association_update.model_dump(exclude_unset=True).items():
        setattr(association, field, value)
    db.commit()
    db.refresh(association)
    return association


def destroy_association(db: Session, curation_status_source_association_id: int) -> None:
    association = _get_association_or_404(db, curation_status_source_association_id)
    db.delete(association)
    db.commit()


def destroy_associations_for_source(db: Session, tag_source_id: int) -> int:
    """Wipe one source's attributions ahead of a reload. The curation_status
    rows themselves are untouched.

    Deletes row by row rather than with Query.delete(): sqlalchemy-continuum
    does not track bulk deletes, and this is the endpoint the loader workflow
    actually uses, so a bulk wipe would leave the version history claiming the
    old attributions are still live (found in review). 404s on an unknown
    tag_source_id, like every other endpoint in this router.
    """
    get_tag_source_or_404(db, tag_source_id)
    associations = db.query(CurationStatusSourceAssociationModel).filter(
        CurationStatusSourceAssociationModel.tag_source_id == tag_source_id).all()
    for association in associations:
        db.delete(association)
    db.commit()
    return len(associations)


def show_associations(db: Session, curation_status_id: int) -> List[Dict[str, Any]]:
    """Every source's report for this curation_status row."""
    _get_curation_status_or_404(db, curation_status_id)
    rows = (
        db.query(CurationStatusSourceAssociationModel, TagSourceModel)
        .join(TagSourceModel,
              CurationStatusSourceAssociationModel.tag_source_id == TagSourceModel.tag_source_id)
        .filter(CurationStatusSourceAssociationModel.curation_status_id == curation_status_id)
        .order_by(CurationStatusSourceAssociationModel.curation_status_source_association_id)
        .all()
    )
    return [_as_dict(association, source) for association, source in rows]


def _as_dict(association: CurationStatusSourceAssociationModel,
             source: TagSourceModel) -> Dict[str, Any]:
    return {
        "curation_status_source_association_id": association.curation_status_source_association_id,
        "curation_status_id": association.curation_status_id,
        "tag_source_id": association.tag_source_id,
        "curation_status": association.curation_status,
        "curation_tag": association.curation_tag,
        "note": association.note,
        "date_created": association.date_created,
        "date_updated": association.date_updated,
        "created_by": association.created_by,
        "updated_by": association.updated_by,
        "source_evidence_assertion": source.source_evidence_assertion,
        "source_method": source.source_method,
        "data_provider": source.data_provider,
        # A source's owning MOD is its secondary_data_provider, NOT data_provider
        # (see test_show_all_reference_tags_batch_mod_filter). Consumers grouping
        # attributions per MOD need this one.
        "secondary_data_provider_abbreviation": (
            source.secondary_data_provider.abbreviation if source.secondary_data_provider else None),
        "validation_type": source.validation_type,
    }
