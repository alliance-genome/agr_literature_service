import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from agr_literature_service.api.models import PersonLineageModel, PersonModel
from agr_literature_service.api.schemas import SYMMETRIC_RELATIONSHIPS, PersonPersonRole
from agr_literature_service.api.crud.user_utils import map_to_user_id

logger = logging.getLogger(__name__)

_SCALAR_FIELDS = {"relationship", "start_date", "end_date"}


def _normalize_pair(person_one_id: int, person_two_id: int, relationship: Any) -> Tuple[int, int]:
    """For non-directional relationships, return the pair in ascending id order so
    (A, B) and (B, A) collapse to the same canonical row. Directional relationships
    keep the submitted order.
    """
    rel = relationship.value if isinstance(relationship, PersonPersonRole) else relationship
    if rel in SYMMETRIC_RELATIONSHIPS and person_one_id > person_two_id:
        return person_two_id, person_one_id
    return person_one_id, person_two_id


def _reject_self_pair(person_one_id: int, person_two_id: int) -> None:
    if person_one_id == person_two_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="person_one_id and person_two_id must be different people",
        )


def _validate_person(db: Session, person_id: int, label: str) -> None:
    exists = db.query(PersonModel.person_id).filter(PersonModel.person_id == person_id).first()
    if not exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"{label} references person_id {person_id}, which was not found",
        )


def create(db: Session, payload: Dict[str, Any]) -> PersonLineageModel:
    data = jsonable_encoder(payload)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    _reject_self_pair(data["person_one_id"], data["person_two_id"])
    _validate_person(db, data["person_one_id"], "person_one_id")
    _validate_person(db, data["person_two_id"], "person_two_id")

    one_id, two_id = _normalize_pair(
        data["person_one_id"], data["person_two_id"], data["relationship"]
    )
    obj = PersonLineageModel(
        person_one_id=one_id,
        person_two_id=two_id,
        relationship=data["relationship"],
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
            detail=(
                "A person_lineage with this person_one_id, person_two_id and "
                "relationship already exists."
            ),
        )
    db.refresh(obj)
    return obj


def find_or_create(
    db: Session,
    person_one_id: int,
    person_two_id: int,
    relationship: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> Tuple[PersonLineageModel, bool]:
    """Return (canonical, created). Looks up an existing canonical PPR for the
    (person_one_id, person_two_id, relationship) triple; creates one if absent.
    For non-directional relationships the pair is normalized to ascending id
    order first, so a reversed submission matches the existing row.
    """
    _reject_self_pair(person_one_id, person_two_id)
    person_one_id, person_two_id = _normalize_pair(person_one_id, person_two_id, relationship)
    existing = (
        db.query(PersonLineageModel)
        .filter(
            PersonLineageModel.person_one_id == person_one_id,
            PersonLineageModel.person_two_id == person_two_id,
            PersonLineageModel.relationship == relationship,
        )
        .first()
    )
    if existing:
        return existing, False

    obj = PersonLineageModel(
        person_one_id=person_one_id,
        person_two_id=person_two_id,
        relationship=relationship,
        start_date=start_date,
        end_date=end_date,
    )
    db.add(obj)
    db.flush()
    return obj, True


def show(db: Session, person_lineage_id: int) -> PersonLineageModel:
    obj = (
        db.query(PersonLineageModel)
        .filter(PersonLineageModel.person_lineage_id == person_lineage_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PersonLineage with id {person_lineage_id} not found",
        )
    return obj


def list_all(db: Session) -> List[PersonLineageModel]:
    return (
        db.query(PersonLineageModel)
        .order_by(PersonLineageModel.person_lineage_id.asc())
        .all()
    )


def patch(db: Session, person_lineage_id: int, patch_dict: Dict[str, Any]) -> Dict[str, Any]:
    obj: Optional[PersonLineageModel] = (
        db.query(PersonLineageModel)
        .filter(PersonLineageModel.person_lineage_id == person_lineage_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PersonLineage with id {person_lineage_id} not found",
        )

    data = jsonable_encoder(patch_dict)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    for field, value in data.items():
        if field not in _SCALAR_FIELDS:
            continue
        if field == "relationship" and value is None:
            continue
        setattr(obj, field, value)

    # If the (possibly updated) relationship is non-directional, re-normalize the
    # id order so a row patched into collaborator_of can't become a reversed
    # duplicate of an existing collaborator_of for the same pair.
    obj.person_one_id, obj.person_two_id = _normalize_pair(
        obj.person_one_id, obj.person_two_id, obj.relationship
    )

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Database constraint violation; please verify input and retry.",
        )
    return {"message": "updated"}


def destroy(db: Session, person_lineage_id: int) -> None:
    obj = (
        db.query(PersonLineageModel)
        .filter(PersonLineageModel.person_lineage_id == person_lineage_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PersonLineage with id {person_lineage_id} not found",
        )
    db.delete(obj)
    db.commit()
