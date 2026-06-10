import logging
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from agr_literature_service.api.models import PersonLineageModel, PersonModel
from agr_literature_service.api.crud.user_utils import map_to_user_id

logger = logging.getLogger(__name__)

_SCALAR_FIELDS = {
    "person_one_name", "person_two_name", "relationship", "who_sent_this",
    "person_one", "person_two", "start_date", "end_date",
}
_NOT_NULL = {"person_one_name", "person_two_name", "relationship", "who_sent_this"}


def _validate_person_link(db: Session, person_id: Optional[int], label: str) -> None:
    if person_id is None:
        return
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

    _validate_person_link(db, data.get("person_one"), "person_one")
    _validate_person_link(db, data.get("person_two"), "person_two")

    obj = PersonLineageModel(
        person_one_name=data["person_one_name"],
        person_two_name=data["person_two_name"],
        relationship=data["relationship"],
        who_sent_this=data["who_sent_this"],
        person_one=data.get("person_one"),
        person_two=data.get("person_two"),
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
            status_code=404,
            detail=f"PersonLineage with id {person_lineage_id} not found",
        )

    data = jsonable_encoder(patch_dict)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    if "person_one" in data:
        _validate_person_link(db, data["person_one"], "person_one")
    if "person_two" in data:
        _validate_person_link(db, data["person_two"], "person_two")

    for field, value in data.items():
        if field not in _SCALAR_FIELDS:
            continue
        if field in _NOT_NULL and value is None:
            continue
        setattr(obj, field, value)

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
