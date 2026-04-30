import logging
from typing import Any, Dict, List

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from agr_literature_service.api.models import PersonModel, PersonCrossReferenceModel
from agr_literature_service.api.crud.user_utils import map_to_user_id

logger = logging.getLogger(__name__)


def _curie_prefix(curie: str) -> str:
    curie = curie.strip()
    if curie.count(":") != 1:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid CURIE '{curie}': expected exactly one colon",
        )
    return curie.split(":", 1)[0]


def _clean_pages(pages):
    if not pages:
        return None
    cleaned = [p.strip() for p in pages if isinstance(p, str) and p.strip()]
    return cleaned or None


def create_for_person(db: Session, person_id: int, payload: Dict[str, Any]) -> PersonCrossReferenceModel:
    person = db.query(PersonModel).filter(PersonModel.person_id == person_id).first()
    if not person:
        raise HTTPException(status_code=404, detail=f"Person with person_id {person_id} not found")

    data = jsonable_encoder(payload)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    curie = (data.get("curie") or "").strip()
    if not curie:
        raise HTTPException(status_code=422, detail="curie is required")

    curie_prefix = _curie_prefix(curie)

    # curie is globally unique on person_cross_reference (uq_person_xref_curie).
    dup = (
        db.query(PersonCrossReferenceModel.person_cross_reference_id)
        .filter(PersonCrossReferenceModel.curie == curie)
        .first()
    )
    if dup:
        raise HTTPException(
            status_code=422,
            detail=f"Cross-reference '{curie}' already exists",
        )

    # (person_id, curie_prefix) is unique per-person (uq_person_xref_person_prefix).
    prefix_dup = (
        db.query(PersonCrossReferenceModel.person_cross_reference_id)
        .filter(
            PersonCrossReferenceModel.person_id == person_id,
            PersonCrossReferenceModel.curie_prefix == curie_prefix,
        )
        .first()
    )
    if prefix_dup:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Another cross-reference with prefix '{curie_prefix}' "
                f"already exists for this person."
            ),
        )

    obj = PersonCrossReferenceModel(
        person_id=person_id,
        curie=curie,
        curie_prefix=curie_prefix,
        pages=_clean_pages(data.get("pages")),
        is_obsolete=bool(data.get("is_obsolete", False)),
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


def list_for_person(db: Session, person_id: int) -> List[PersonCrossReferenceModel]:
    person_exists = db.query(PersonModel.person_id).filter(PersonModel.person_id == person_id).first()
    if not person_exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person with person_id {person_id} not found",
        )
    return (
        db.query(PersonCrossReferenceModel)
        .filter(PersonCrossReferenceModel.person_id == person_id)
        .order_by(PersonCrossReferenceModel.person_cross_reference_id.asc())
        .all()
    )


def get_by_curie_or_id(db: Session, curie_or_id: str) -> PersonCrossReferenceModel:
    pcr_id = int(curie_or_id) if curie_or_id.isdigit() else None
    obj = (
        db.query(PersonCrossReferenceModel)
        .filter(
            or_(
                PersonCrossReferenceModel.curie == curie_or_id,
                PersonCrossReferenceModel.person_cross_reference_id == pcr_id,
            )
        )
        .order_by(PersonCrossReferenceModel.is_obsolete)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PersonCrossReference with curie or id {curie_or_id} not found",
        )
    return obj


def show(db: Session, person_cross_reference_id: int) -> PersonCrossReferenceModel:
    obj = (
        db.query(PersonCrossReferenceModel)
        .filter(PersonCrossReferenceModel.person_cross_reference_id == person_cross_reference_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PersonCrossReference with id {person_cross_reference_id} not found",
        )
    return obj


def patch(db: Session, person_cross_reference_id: int, patch_dict: Dict[str, Any]) -> Dict[str, Any]:
    obj = (
        db.query(PersonCrossReferenceModel)
        .filter(PersonCrossReferenceModel.person_cross_reference_id == person_cross_reference_id)
        .first()
    )
    if not obj:
        raise HTTPException(status_code=404, detail=f"PersonCrossReference with id {person_cross_reference_id} not found")

    data = jsonable_encoder(patch_dict)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    if "curie" in data and data["curie"] is not None:
        new_curie = data["curie"].strip()
        new_prefix = _curie_prefix(new_curie)

        # curie is globally unique on person_cross_reference (uq_person_xref_curie).
        dup = (
            db.query(PersonCrossReferenceModel.person_cross_reference_id)
            .filter(
                PersonCrossReferenceModel.curie == new_curie,
                PersonCrossReferenceModel.person_cross_reference_id != person_cross_reference_id,
            )
            .first()
        )
        if dup:
            raise HTTPException(
                status_code=422,
                detail=f"Cross-reference '{new_curie}' already exists",
            )

        # (person_id, curie_prefix) is unique per-person (uq_person_xref_person_prefix).
        # NULL person_ids are exempt — Postgres treats NULLs in unique constraints
        # as distinct, so the constraint only fires when person_id is non-null.
        if obj.person_id is not None:
            prefix_dup = (
                db.query(PersonCrossReferenceModel.person_cross_reference_id)
                .filter(
                    PersonCrossReferenceModel.person_id == obj.person_id,
                    PersonCrossReferenceModel.curie_prefix == new_prefix,
                    PersonCrossReferenceModel.person_cross_reference_id != person_cross_reference_id,
                )
                .first()
            )
            if prefix_dup:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"Another cross-reference with prefix '{new_prefix}' "
                        f"already exists for this person."
                    ),
                )

        obj.curie = new_curie
        obj.curie_prefix = new_prefix

    if "pages" in data:
        obj.pages = _clean_pages(data["pages"])

    if "is_obsolete" in data:
        obj.is_obsolete = bool(data["is_obsolete"])

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Database constraint violation; please verify input and retry.",
        )
    return {"message": "updated"}


def destroy(db: Session, person_cross_reference_id: int) -> None:
    obj = (
        db.query(PersonCrossReferenceModel)
        .filter(PersonCrossReferenceModel.person_cross_reference_id == person_cross_reference_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PersonCrossReference with id {person_cross_reference_id} not found",
        )
    db.delete(obj)
    db.commit()
