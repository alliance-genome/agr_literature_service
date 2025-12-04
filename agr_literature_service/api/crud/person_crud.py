import logging
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from agr_literature_service.api.models import PersonModel, EmailModel, PersonCrossReferenceModel
from agr_literature_service.api.schemas import PersonSchemaCreate
from agr_literature_service.api.crud.user_utils import map_to_user_id

logger = logging.getLogger(__name__)


def normalize_email(s: str) -> str:
    return s.strip().lower()


def create(db: Session, payload: PersonSchemaCreate) -> PersonModel:
    data: Dict[str, Any] = jsonable_encoder(payload)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    # Create the Person row first
    emails_data = data.pop("emails", None)
    xrefs_data = data.pop("cross_references", None)

    obj = PersonModel(**data)
    db.add(obj)
    db.flush()  # get person_id for children

    # Create child emails
    if emails_data:
        for e in emails_data:
            email_addr = normalize_email(e["email_address"])
            # skip duplicates for this person
            dup = (
                db.query(EmailModel.email_id)
                .filter(EmailModel.person_id == obj.person_id)
                .filter(EmailModel.email_address == email_addr)
                .first()
            )
            if dup:
                continue
            db.add(
                EmailModel(
                    person_id=obj.person_id,
                    email_address=email_addr,
                    date_invalidated=e.get("date_invalidated"),
                )
            )

    def curie_prefix_from(curie: str) -> str:
        curie = curie.strip()
        if curie.count(":") != 1:
            raise ValueError(f"Invalid CURIE '{curie}': expected exactly one colon")
        return curie.split(":", 1)[0]

    # Create child cross-references
    if xrefs_data:
        for xr in xrefs_data:
            curie = xr["curie"]
            curie = curie.strip()
            curie_prefix = curie_prefix_from(curie)
            db.add(
                PersonCrossReferenceModel(
                    person_id=obj.person_id,
                    curie=curie,
                    curie_prefix=curie_prefix,
                    pages=xr.get("pages"),
                    is_obsolete=bool(xr.get("is_obsolete", False)),
                )
            )

    db.commit()
    db.refresh(obj)
    return obj


def destroy(db: Session, person_id: int) -> None:
    obj: Optional[PersonModel] = (
        db.query(PersonModel).filter(PersonModel.person_id == person_id).first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person with person_id {person_id} not found",
        )
    db.delete(obj)
    db.commit()


def patch(db: Session, person_id: int, patch_dict: Dict[str, Any]) -> Dict[str, Any]:
    obj: Optional[PersonModel] = (
        db.query(PersonModel).filter(PersonModel.person_id == person_id).first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person with person_id {person_id} not found",
        )

    data = jsonable_encoder(patch_dict)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    # update only scalar/column fields; skip relationship fields
    RELATIONSHIP_FIELDS = {"emails", "cross_references"}
    for field, _value in list(data.items()):
        if field in RELATIONSHIP_FIELDS:
            data.pop(field, None)

    ALLOWED = {"display_name", "curie", "mod_roles"}
    for field, value in data.items():
        if field in ALLOWED:
            setattr(obj, field, value)

    db.commit()
    return {"message": "updated"}


def show(db: Session, person_id: int) -> PersonModel:
    obj: Optional[PersonModel] = (
        db.query(PersonModel)
        .options(
            joinedload(PersonModel.emails),
            joinedload(PersonModel.cross_references),
        )
        .filter(PersonModel.person_id == person_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person with person_id {person_id} not found",
        )
    return obj


def get_by_email(db: Session, email: str) -> Optional[PersonModel]:
    if not email:
        return None
    email_norm = normalize_email(email)
    return (
        db.query(PersonModel)
        .join(EmailModel, EmailModel.person_id == PersonModel.person_id)
        .options(
            joinedload(PersonModel.emails),
            joinedload(PersonModel.cross_references),
        )
        .filter(func.lower(EmailModel.email_address) == email_norm)
        .first()
    )


def find_by_name(db: Session, name: str) -> List[PersonModel]:
    """
    Case-insensitive partial match on display_name.
    """
    if not name:
        return []
    pattern = f"%{name.strip()}%"
    return (
        db.query(PersonModel)
        .options(
            joinedload(PersonModel.emails),
            joinedload(PersonModel.cross_references),
        )
        .filter(PersonModel.display_name.ilike(pattern))
        .order_by(PersonModel.display_name.asc())
        .all()
    )
