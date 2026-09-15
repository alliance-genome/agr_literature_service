"""
person_institution_crud.py
"""
import logging
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session, selectinload

from agr_literature_service.api.models import PersonInstitutionModel, PersonModel
from agr_literature_service.api.crud.user_utils import map_to_user_id

logger = logging.getLogger(__name__)


def normalize_institution(s: str) -> str:
    """Strip surrounding whitespace; preserve case for storage.

    Institutions are uncontrolled free text (SCRUM-5910 settled that they are
    strings, not ROR objects), so the only normalization is trimming. Raises
    ValueError on a blank value so callers can convert it into a 422.
    """
    v = (s or "").strip()
    if not v:
        raise ValueError("institution must not be blank")
    return v


def create_for_person(
    db: Session, person_id: int, payload: Dict[str, Any]
) -> PersonInstitutionModel:
    """
    Create a person_institution row that belongs to a Person.

    Semantics:
      - institution is normalized (trimmed, case preserved).
      - There is deliberately NO uniqueness check, unlike person_email: people
        return to institutions, so an old row and a new active row may carry the
        same string.
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

    try:
        institution = normalize_institution(data.get("institution"))
    except ValueError as err:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(err),
        )

    obj = PersonInstitutionModel(
        person_id=person_id,
        institution=institution,
        date_made_old_institution=data.get("date_made_old_institution"),
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


def list_for_person(db: Session, person_id: int) -> List[PersonInstitutionModel]:
    """List all institutions for a person, most-recently-touched first."""
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
        db.query(PersonInstitutionModel)
        .options(selectinload(PersonInstitutionModel.person))
        .filter(PersonInstitutionModel.person_id == person_id)
        .order_by(
            PersonInstitutionModel.date_updated.desc().nulls_last(),
            PersonInstitutionModel.date_created.desc().nulls_last(),
            PersonInstitutionModel.person_institution_id.desc(),
        )
        .all()
    )


def show(db: Session, person_institution_id: int) -> PersonInstitutionModel:
    obj = (
        db.query(PersonInstitutionModel)
        .options(selectinload(PersonInstitutionModel.person))
        .filter(
            PersonInstitutionModel.person_institution_id == person_institution_id
        )
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"person_institution with person_institution_id "
                f"{person_institution_id} not found"
            ),
        )
    return obj


def patch(
    db: Session, person_institution_id: int, patch_dict: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Patch a person_institution row.

    Supports:
      - institution (normalized)
      - date_made_old_institution
    """
    obj: Optional[PersonInstitutionModel] = (
        db.query(PersonInstitutionModel)
        .filter(
            PersonInstitutionModel.person_institution_id == person_institution_id
        )
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"person_institution with person_institution_id "
                f"{person_institution_id} not found"
            ),
        )

    data = jsonable_encoder(patch_dict)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    if "institution" in data and data["institution"] is not None:
        try:
            obj.institution = normalize_institution(data["institution"])
        except ValueError as err:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=str(err),
            )

    if "date_made_old_institution" in data:
        obj.date_made_old_institution = data["date_made_old_institution"]

    db.commit()
    return {"message": "updated"}


def destroy(db: Session, person_institution_id: int) -> None:
    obj = (
        db.query(PersonInstitutionModel)
        .filter(
            PersonInstitutionModel.person_institution_id == person_institution_id
        )
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"person_institution with person_institution_id "
                f"{person_institution_id} not found"
            ),
        )

    db.delete(obj)
    db.commit()
