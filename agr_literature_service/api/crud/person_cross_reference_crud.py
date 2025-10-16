import logging
from typing import Any, Dict, List

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.models import PersonModel, PersonCrossReferenceModel

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
    curie = (data.get("curie") or "").strip()
    if not curie:
        raise HTTPException(status_code=422, detail="curie is required")

    # prevent duplicate for the same person
    dup = (
        db.query(PersonCrossReferenceModel.person_cross_reference_id)
        .filter(
            PersonCrossReferenceModel.person_id == person_id,
            PersonCrossReferenceModel.curie == curie,
        )
        .first()
    )
    if dup:
        raise HTTPException(
            status_code=422,
            detail=f"Cross-reference '{curie}' already exists for person_id {person_id}",
        )

    obj = PersonCrossReferenceModel(
        person_id=person_id,
        curie=curie,
        curie_prefix=_curie_prefix(curie),
        pages=_clean_pages(data.get("pages")),
        is_obsolete=bool(data.get("is_obsolete", False)),
    )
    db.add(obj)
    db.commit()
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

    if "curie" in data and data["curie"] is not None:
        new_curie = data["curie"].strip()
        # duplicate check for same person
        dup = (
            db.query(PersonCrossReferenceModel.person_cross_reference_id)
            .filter(
                PersonCrossReferenceModel.person_id == obj.person_id,
                PersonCrossReferenceModel.curie == new_curie,
                PersonCrossReferenceModel.person_cross_reference_id != person_cross_reference_id,
            )
            .first()
        )
        if dup:
            raise HTTPException(
                status_code=422,
                detail=f"Cross-reference '{new_curie}' already exists for person_id {obj.person_id}",
            )
        obj.curie = new_curie
        obj.curie_prefix = _curie_prefix(new_curie)

    if "pages" in data:
        obj.pages = _clean_pages(data["pages"])

    if "is_obsolete" in data:
        obj.is_obsolete = bool(data["is_obsolete"])

    db.commit()
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
