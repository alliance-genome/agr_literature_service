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
    """
    Create an Email row that belongs to a Person (person_id).

    Semantics:
      - email_address is normalized (lowercased, trimmed).
      - (person_id, email_address) must be unique.
      - primary:
          * If explicitly provided in payload:
                - If True: set all other emails for this person to primary=False.
                - If False: just add as non-primary.
          * If not provided:
                - If this is the first email for that person -> primary=True.
                - Otherwise -> primary=False.
      - Because of ck_email_person_primary_nulls_together, for person emails we must
        set primary to True/False (not None).
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

    # Determine primary flag
    requested_primary: Optional[bool] = data.get("primary")
    has_existing = (
        db.query(EmailModel.email_id)
        .filter(EmailModel.person_id == person_id)
        .first()
        is not None
    )
    if requested_primary is None:
        # Default: first email -> primary=True, else primary=False
        primary_value: bool = not has_existing
    else:
        primary_value = bool(requested_primary)

    # If this email will be primary, demote other primaries for this person
    if primary_value:
        db.query(EmailModel).filter(
            EmailModel.person_id == person_id,
            EmailModel.primary.is_(True),
        ).update({"primary": False}, synchronize_session=False)

    obj = EmailModel(
        person_id=person_id,
        email_address=email_addr,
        date_invalidated=data.get("date_invalidated"),
        primary=primary_value,
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


def list_for_person(db: Session, person_id: int) -> List[EmailModel]:
    """
    List all emails for a person, ordering primary emails first.
    """
    person_exists = db.query(PersonModel.person_id).filter(PersonModel.person_id == person_id).first()
    if not person_exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person with person_id {person_id} not found",
        )
    return (
        db.query(EmailModel)
        .filter(EmailModel.person_id == person_id)
        .order_by(EmailModel.primary.desc().nulls_last(), EmailModel.email_id.asc())
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
    """
    Patch an Email row.

    Supports:
      - email_address (normalized, uniqueness per person)
      - date_invalidated
      - primary (for person emails, must be True/False, not None)
        * If set to True, demotes other primaries for the same person.
    """
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

    # email_address change
    if "email_address" in data and data["email_address"] is not None:
        new_addr = normalize_email(data["email_address"])
        # check uniqueness for this person
        if obj.person_id is not None:
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

    # primary change
    if "primary" in data:
        new_primary = data["primary"]
        # For person emails, primary cannot be None (due to check constraint)
        if obj.person_id is not None and new_primary is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="primary must be true or false for person emails",
            )
        obj.primary = new_primary
        # If set to True, demote others
        if obj.person_id is not None and new_primary:
            db.query(EmailModel).filter(
                EmailModel.person_id == obj.person_id,
                EmailModel.email_id != email_id,
                EmailModel.primary.is_(True),
            ).update({"primary": False}, synchronize_session=False)

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
