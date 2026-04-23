import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import func
from sqlalchemy.orm import Session, selectinload

from agr_literature_service.api.models import (
    PersonModel,
    EmailModel,
    PersonCrossReferenceModel,
    PersonNameModel,
    PersonNoteModel,
)
from agr_literature_service.api.schemas import PersonSchemaCreate
from agr_literature_service.api.crud.user_utils import map_to_user_id
from agr_literature_service.global_utils import get_next_person_curie

logger = logging.getLogger(__name__)

ADDRESS_FIELDS = {"city", "state", "postal_code", "country", "street_address"}


def normalize_email(s: str) -> str:
    return s.strip().lower()


def create(db: Session, payload: PersonSchemaCreate) -> PersonModel:  # noqa: C901
    data: Dict[str, Any] = jsonable_encoder(payload)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    # Create the Person row first
    emails_data = data.pop("emails", None)
    xrefs_data = data.pop("cross_references", None)
    names_data = data.pop("names", None)
    notes_data = data.pop("notes", None)

    data["curie"] = get_next_person_curie(db)

    # Set address_last_updated if any address field is provided
    has_address = bool(ADDRESS_FIELDS & payload.model_fields_set)

    obj = PersonModel(**data)
    if has_address:
        obj.address_last_updated = datetime.utcnow()
    db.add(obj)
    db.flush()  # get person_id for children

    # Create child emails
    if emails_data:
        # Check if any email in the batch explicitly marks itself primary
        has_primary_set = any(e.get("primary") is True for e in emails_data)
        for idx, e in enumerate(emails_data):
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
            # Determine primary flag — EmailModel requires primary to be
            # non-NULL when person_id is non-NULL (ck_email_person_primary_nulls_together).
            requested_primary = e.get("primary")
            if requested_primary is None:
                primary_value = (idx == 0 and not has_primary_set)
            else:
                primary_value = bool(requested_primary)
            db.add(
                EmailModel(
                    person_id=obj.person_id,
                    email_address=email_addr,
                    date_invalidated=e.get("date_invalidated"),
                    primary=primary_value,
                )
            )

    # Create child names
    if names_data:
        # Find which name should be primary
        primary_idx = None
        for idx, n in enumerate(names_data):
            if n.get("primary") is True:
                primary_idx = idx
                break
        # Default to first name if none marked
        if primary_idx is None:
            primary_idx = 0

        for idx, n in enumerate(names_data):
            db.add(
                PersonNameModel(
                    person_id=obj.person_id,
                    first_name=n.get("first_name"),
                    middle_name=n.get("middle_name"),
                    last_name=n["last_name"],
                    primary=True if idx == primary_idx else None,
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

    # Create child notes
    if notes_data:
        for n in notes_data:
            db.add(
                PersonNoteModel(
                    person_id=obj.person_id,
                    note=n["note"],
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
    RELATIONSHIP_FIELDS = {"emails", "cross_references", "names", "notes"}
    for field, _value in list(data.items()):
        if field in RELATIONSHIP_FIELDS:
            data.pop(field, None)

    ALLOWED = {
        "display_name", "mod_roles",
        "webpage", "active_status",
        "city", "state", "postal_code", "country", "street_address",
        "biography_research_interest",
    }
    for field, value in data.items():
        if field in ALLOWED:
            setattr(obj, field, value)

    # Update address_last_updated if any address field was patched
    if ADDRESS_FIELDS & data.keys():
        obj.address_last_updated = datetime.utcnow()

    db.commit()
    return {"message": "updated"}


def show(db: Session, person_id: int) -> PersonModel:
    obj: Optional[PersonModel] = (
        db.query(PersonModel)
        .options(
            selectinload(PersonModel.emails),
            selectinload(PersonModel.cross_references),
            selectinload(PersonModel.names),
            selectinload(PersonModel.notes),
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
            selectinload(PersonModel.emails),
            selectinload(PersonModel.cross_references),
            selectinload(PersonModel.names),
            selectinload(PersonModel.notes),
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
            selectinload(PersonModel.emails),
            selectinload(PersonModel.cross_references),
            selectinload(PersonModel.names),
            selectinload(PersonModel.notes),
        )
        .filter(PersonModel.display_name.ilike(pattern))
        .order_by(PersonModel.display_name.asc())
        .all()
    )
