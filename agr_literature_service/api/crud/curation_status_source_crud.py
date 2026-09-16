"""
curation_status_source_crud.py
==============================

SCRUM-6518. Source attributions for curation status.

Every function here is independent of the curation_status row: none of them
read or write its values. Sources may disagree with the base row and with each
other, and nothing is reconciled.
"""
from datetime import datetime
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


def upsert_association(db: Session, curation_status_id: int, tag_source_id: int,
                       values: Dict[str, Any]) -> CurationStatusSourceAssociationModel:
    """Record (or re-record) what one source reports for this curation_status.

    ``values`` carries only the fields the caller is asserting; fields absent
    from it are left as they are on an existing association, so a PATCH that
    only changes the note does not blank this source's reported status.

    Requires an existing curation_status row - it never auto-creates the anchor.
    """
    _get_curation_status_or_404(db, curation_status_id)
    if db.query(TagSourceModel).filter(
            TagSourceModel.tag_source_id == tag_source_id).one_or_none() is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"TagSource with tag_source_id {tag_source_id} not found")

    association = db.query(CurationStatusSourceAssociationModel).filter(
        CurationStatusSourceAssociationModel.curation_status_id == curation_status_id,
        CurationStatusSourceAssociationModel.tag_source_id == tag_source_id,
    ).one_or_none()
    if association is None:
        association = CurationStatusSourceAssociationModel(
            curation_status_id=curation_status_id,
            tag_source_id=tag_source_id,
            date_created=datetime.utcnow(),
        )
        db.add(association)
    for field in VALUE_FIELDS:
        if field in values:
            setattr(association, field, values[field])
    db.commit()
    db.refresh(association)
    return association


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
    rows themselves are untouched."""
    deleted = db.query(CurationStatusSourceAssociationModel).filter(
        CurationStatusSourceAssociationModel.tag_source_id == tag_source_id).delete(
            synchronize_session=False)
    db.commit()
    return deleted


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


def show_association(db: Session,
                     curation_status_source_association_id: int) -> Dict[str, Any]:
    """One association, in the same shape show_associations returns."""
    association = _get_association_or_404(db, curation_status_source_association_id)
    source = db.query(TagSourceModel).filter(
        TagSourceModel.tag_source_id == association.tag_source_id).one()
    return _as_dict(association, source)


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
        "validation_type": source.validation_type,
    }
