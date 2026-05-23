"""
person_email_crud.py
"""
import logging
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import func
from sqlalchemy.orm import Session

from agr_literature_service.api.models import PersonEmailModel, PersonModel
from agr_literature_service.api.crud.person_crud import normalize_email
from agr_literature_service.api.crud.user_utils import map_to_user_id

logger = logging.getLogger(__name__)


def create_for_person(
    db: Session, person_id: int, payload: Dict[str, Any]
) -> PersonEmailModel:
    """
    Create a person_email row that belongs to a Person.

    Semantics:
      - email_address is normalized (trimmed, case preserved).
      - (person_id, lower(email_address)) must be unique.
    """
    person = db.query(PersonModel).filter(
        PersonModel.person_id == person_id
    ).first()
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

    if "email_address" not in data or not data["email_address"]:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="email_address is required",
        )

    email_addr = normalize_email(data["email_address"])

    dup = (
        db.query(PersonEmailModel.person_email_id)
        .filter(
            PersonEmailModel.person_id == person_id,
            func.lower(PersonEmailModel.email_address) == email_addr.lower(),
        )
        .first()
    )
    if dup:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Email '{email_addr}' already exists for person_id {person_id}"
            ),
        )

    obj = PersonEmailModel(
        person_id=person_id,
        email_address=email_addr,
        date_made_old_email=data.get("date_made_old_email"),
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


def list_for_person(db: Session, person_id: int) -> List[PersonEmailModel]:
    """List all emails for a person, most-recently-touched first."""
    person_exists = (
        db.query(PersonModel.person_id)
        .filter(PersonModel.person_id == person_id)
        .first()
    )
    if not person_exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person with person_id {person_id} not found",
        )
    return (
        db.query(PersonEmailModel)
        .filter(PersonEmailModel.person_id == person_id)
        .order_by(
            PersonEmailModel.date_updated.desc().nulls_last(),
            PersonEmailModel.date_created.desc().nulls_last(),
            PersonEmailModel.person_email_id.desc(),
        )
        .all()
    )


def show(db: Session, person_email_id: int) -> PersonEmailModel:
    obj = (
        db.query(PersonEmailModel)
        .filter(PersonEmailModel.person_email_id == person_email_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"person_email with person_email_id {person_email_id} not found",
        )
    return obj


def patch(
    db: Session, person_email_id: int, patch_dict: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Patch a person_email row.

    Supports:
      - email_address (normalized, uniqueness per person)
      - date_made_old_email
    """
    obj: Optional[PersonEmailModel] = (
        db.query(PersonEmailModel)
        .filter(PersonEmailModel.person_email_id == person_email_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"person_email with person_email_id {person_email_id} not found",
        )

    data = jsonable_encoder(patch_dict)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    if "email_address" in data and data["email_address"] is not None:
        new_addr = normalize_email(data["email_address"])
        dup = (
            db.query(PersonEmailModel.person_email_id)
            .filter(
                PersonEmailModel.person_id == obj.person_id,
                func.lower(PersonEmailModel.email_address) == new_addr.lower(),
                PersonEmailModel.person_email_id != person_email_id,
            )
            .first()
        )
        if dup:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"Email '{new_addr}' already exists for person_id "
                    f"{obj.person_id}"
                ),
            )
        obj.email_address = new_addr

    if "date_made_old_email" in data:
        obj.date_made_old_email = data["date_made_old_email"]

    db.commit()
    return {"message": "updated"}


def destroy(db: Session, person_email_id: int) -> None:
    obj = (
        db.query(PersonEmailModel)
        .filter(PersonEmailModel.person_email_id == person_email_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"person_email with person_email_id {person_email_id} not found",
        )

    db.delete(obj)
    db.commit()
