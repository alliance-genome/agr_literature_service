import logging
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from agr_literature_service.api.models import PersonLineageSubmissionModel, PersonModel
from agr_literature_service.api.crud import person_lineage_crud
from agr_literature_service.api.crud.user_utils import map_to_user_id

logger = logging.getLogger(__name__)

_SCALAR_FIELDS = {
    "person_one_name", "person_two_name", "relationship", "who_sent_this",
    "person_one_id", "person_two_id", "start_date", "end_date", "status",
}
_NOT_NULL = {"person_one_name", "person_two_name", "relationship", "who_sent_this", "status"}


def _validate_person_link(db: Session, person_id: Optional[int], label: str) -> None:
    if person_id is None:
        return
    exists = db.query(PersonModel.person_id).filter(PersonModel.person_id == person_id).first()
    if not exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"{label} references person_id {person_id}, which was not found",
        )


def create(db: Session, payload: Dict[str, Any]) -> PersonLineageSubmissionModel:
    data = jsonable_encoder(payload)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    _validate_person_link(db, data.get("person_one_id"), "person_one_id")
    _validate_person_link(db, data.get("person_two_id"), "person_two_id")

    obj = PersonLineageSubmissionModel(
        person_one_name=data["person_one_name"],
        person_two_name=data["person_two_name"],
        relationship=data["relationship"],
        who_sent_this=data["who_sent_this"],
        person_one_id=data.get("person_one_id"),
        person_two_id=data.get("person_two_id"),
        start_date=data.get("start_date"),
        end_date=data.get("end_date"),
    )
    db.add(obj)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Database constraint violation; please verify input and retry.",
        )
    db.refresh(obj)
    return obj


def show(db: Session, person_lineage_submission_id: int) -> PersonLineageSubmissionModel:
    obj = (
        db.query(PersonLineageSubmissionModel)
        .filter(PersonLineageSubmissionModel.person_lineage_submission_id == person_lineage_submission_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PersonLineageSubmission with id {person_lineage_submission_id} not found",
        )
    return obj


def list_all(db: Session) -> List[PersonLineageSubmissionModel]:
    return (
        db.query(PersonLineageSubmissionModel)
        .order_by(PersonLineageSubmissionModel.person_lineage_submission_id.asc())
        .all()
    )


def patch(db: Session, person_lineage_submission_id: int, patch_dict: Dict[str, Any]) -> Dict[str, Any]:
    obj: Optional[PersonLineageSubmissionModel] = (
        db.query(PersonLineageSubmissionModel)
        .filter(PersonLineageSubmissionModel.person_lineage_submission_id == person_lineage_submission_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PersonLineageSubmission with id {person_lineage_submission_id} not found",
        )

    data = jsonable_encoder(patch_dict)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    if "person_one_id" in data:
        _validate_person_link(db, data["person_one_id"], "person_one_id")
    if "person_two_id" in data:
        _validate_person_link(db, data["person_two_id"], "person_two_id")

    for field, value in data.items():
        if field not in _SCALAR_FIELDS:
            continue
        if field in _NOT_NULL and value is None:
            continue
        setattr(obj, field, value)

    db.commit()
    return {"message": "updated"}


def destroy(db: Session, person_lineage_submission_id: int) -> None:
    obj = (
        db.query(PersonLineageSubmissionModel)
        .filter(PersonLineageSubmissionModel.person_lineage_submission_id == person_lineage_submission_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PersonLineageSubmission with id {person_lineage_submission_id} not found",
        )
    db.delete(obj)
    db.commit()


def validate(db: Session, person_lineage_submission_id: int) -> PersonLineageSubmissionModel:
    """Promote a fully-resolved submission to a canonical person_lineage.

    Requires both person ids to be resolved. Finds or creates the canonical PPR
    for (person_one_id, person_two_id, relationship), links the submission to it,
    and sets status to 'validated' (new canonical) or 'duplicate' (already existed).
    """
    obj = show(db, person_lineage_submission_id)

    # A rejected submission must be deliberately un-rejected (its status reset)
    # before it can be validated — validate() won't silently reverse a rejection
    # (which could otherwise create or link a canonical row).
    if obj.status == "rejected":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="A rejected submission cannot be validated; reset its status first.",
        )

    # Idempotent: if the submission is already linked to a canonical PPR,
    # re-validating is a no-op — return it unchanged (don't re-link or flip
    # 'validated' to 'duplicate'). This also makes "reset status, validate again"
    # harmless without resurrecting a stale state.
    if obj.person_lineage_id is not None:
        return obj

    if obj.person_one_id is None or obj.person_two_id is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Both person_one_id and person_two_id must be resolved before validating.",
        )

    canonical, created = person_lineage_crud.find_or_create(
        db,
        person_one_id=obj.person_one_id,
        person_two_id=obj.person_two_id,
        relationship=obj.relationship,
        start_date=obj.start_date,
        end_date=obj.end_date,
    )

    obj.person_lineage_id = canonical.person_lineage_id
    obj.status = "validated" if created else "duplicate"

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Database constraint violation; please verify input and retry.",
        )
    db.refresh(obj)
    return obj
