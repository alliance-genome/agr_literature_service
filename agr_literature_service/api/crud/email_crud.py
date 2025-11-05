"""
email_crud.py
"""
import logging
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.models import EmailModel, PersonModel
from agr_literature_service.api.crud.person_crud import normalize_email
from agr_literature_service.api.crud.user_utils import map_to_user_id

logger = logging.getLogger(__name__)


def create_for_person(db: Session, person_id: int, payload: Dict[str, Any]) -> EmailModel:
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

    if "email_address" not in data or not data["email_address"]:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="email_address is required",
        )

    email_addr = normalize_email(data["email_address"])

    # Avoid duplicate per unique constraint (person_id, email_address)
    dup = (
        db.query(EmailModel.email_id)
        .filter(EmailModel.person_id == person_id, EmailModel.email_address == email_addr)
        .first()
    )
    if dup:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Email '{email_addr}' already exists for person_id {person_id}",
        )

    obj = EmailModel(
        person_id=person_id,
        email_address=email_addr,
        date_invalidated=data.get("date_invalidated"),
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


def list_for_person(db: Session, person_id: int) -> List[EmailModel]:
    person_exists = db.query(PersonModel.person_id).filter(PersonModel.person_id == person_id).first()
    if not person_exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person with person_id {person_id} not found",
        )
    return (
        db.query(EmailModel)
        .filter(EmailModel.person_id == person_id)
        .order_by(EmailModel.email_id.asc())
        .all()
    )


def show(db: Session, email_id: int) -> EmailModel:
    obj = db.query(EmailModel).filter(EmailModel.email_id == email_id).first()
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Email with email_id {email_id} not found",
        )
    return obj


def patch(db: Session, email_id: int, patch_dict: Dict[str, Any]) -> Dict[str, Any]:
    obj: Optional[EmailModel] = db.query(EmailModel).filter(EmailModel.email_id == email_id).first()
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Email with email_id {email_id} not found",
        )

    data = jsonable_encoder(patch_dict)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    if "email_address" in data and data["email_address"] is not None:
        new_addr = normalize_email(data["email_address"])
        # check uniqueness for this person
        dup = (
            db.query(EmailModel.email_id)
            .filter(
                EmailModel.person_id == obj.person_id,
                EmailModel.email_address == new_addr,
                EmailModel.email_id != email_id,
            )
            .first()
        )
        if dup:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Email '{new_addr}' already exists for person_id {obj.person_id}",
            )
        obj.email_address = new_addr

    if "date_invalidated" in data:
        obj.date_invalidated = data["date_invalidated"]

    db.commit()
    return {"message": "updated"}


def destroy(db: Session, email_id: int) -> None:
    obj = db.query(EmailModel).filter(EmailModel.email_id == email_id).first()
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Email with email_id {email_id} not found",
        )
    db.delete(obj)
    db.commit()
