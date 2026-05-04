"""
person_name_crud.py
"""
import logging
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.models import PersonModel, PersonNameModel
from agr_literature_service.api.crud.user_utils import map_to_user_id

logger = logging.getLogger(__name__)


def create_for_person(db: Session, person_id: int, payload: Dict[str, Any]) -> PersonNameModel:
    """
    Create a PersonName row that belongs to a Person (person_id).

    Semantics:
      - last_name is required.
      - primary:
          * If explicitly True: demote any existing primary for this person.
          * If not provided and this is the first name for the person: auto-set True.
          * Otherwise: leave as None/False.
    """
    person = db.query(PersonModel).filter(PersonModel.person_id == person_id).first()
    if not person:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person with person_id {person_id} not found",
        )

    data = jsonable_encoder(payload)
    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    if "last_name" not in data or not data["last_name"]:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="last_name is required",
        )

    # Determine primary flag
    requested_primary: Optional[bool] = data.get("primary")
    has_existing = (
        db.query(PersonNameModel.person_name_id)
        .filter(PersonNameModel.person_id == person_id)
        .first()
        is not None
    )
    if requested_primary is True:
        # Demote existing primary for this person
        db.query(PersonNameModel).filter(
            PersonNameModel.person_id == person_id,
            PersonNameModel.primary.is_(True),
        ).update({"primary": False}, synchronize_session=False)
        primary_value: Optional[bool] = True
    elif requested_primary is None and not has_existing:
        # First name for this person — auto-set primary
        primary_value = True
    else:
        primary_value = requested_primary

    obj = PersonNameModel(
        person_id=person_id,
        first_name=data.get("first_name"),
        middle_name=data.get("middle_name"),
        last_name=data["last_name"],
        primary=primary_value,
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


def list_for_person(db: Session, person_id: int) -> List[PersonNameModel]:
    """
    List all names for a person, ordering primary names first.
    """
    person_exists = db.query(PersonModel.person_id).filter(PersonModel.person_id == person_id).first()
    if not person_exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person with person_id {person_id} not found",
        )
    return (
        db.query(PersonNameModel)
        .filter(PersonNameModel.person_id == person_id)
        .order_by(PersonNameModel.primary.desc().nulls_last(), PersonNameModel.person_name_id.asc())
        .all()
    )


def show(db: Session, person_name_id: int) -> PersonNameModel:
    obj = db.query(PersonNameModel).filter(PersonNameModel.person_name_id == person_name_id).first()
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PersonName with person_name_id {person_name_id} not found",
        )
    return obj


def patch(db: Session, person_name_id: int, patch_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Patch a PersonName row.

    Supports:
      - first_name, middle_name, last_name
      - primary: if set to True, demotes the old primary for the same person.
    """
    obj: Optional[PersonNameModel] = (
        db.query(PersonNameModel).filter(PersonNameModel.person_name_id == person_name_id).first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PersonName with person_name_id {person_name_id} not found",
        )

    data = jsonable_encoder(patch_dict)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    if "first_name" in data:
        obj.first_name = data["first_name"]
    if "middle_name" in data:
        obj.middle_name = data["middle_name"]
    if "last_name" in data:
        if not data["last_name"]:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="last_name cannot be empty",
            )
        obj.last_name = data["last_name"]

    if "primary" in data:
        new_primary = data["primary"]
        if new_primary is True:
            # Demote other primaries for this person
            db.query(PersonNameModel).filter(
                PersonNameModel.person_id == obj.person_id,
                PersonNameModel.person_name_id != person_name_id,
                PersonNameModel.primary.is_(True),
            ).update({"primary": False}, synchronize_session=False)
        obj.primary = new_primary

    db.commit()
    return {"message": "updated"}


def destroy(db: Session, person_name_id: int) -> None:
    obj = db.query(PersonNameModel).filter(PersonNameModel.person_name_id == person_name_id).first()
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PersonName with person_name_id {person_name_id} not found",
        )
    db.delete(obj)
    db.commit()
