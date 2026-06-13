import logging
from typing import Any, Dict, List

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from agr_literature_service.api.models import LaboratoryModel, LaboratoryCrossReferenceModel
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


def _check_xref_unique(db, laboratory_id, curie, curie_prefix, exclude_id=None):
    """Enforce the two uniqueness rules at the application layer (defends the DB
    constraints): curie is globally unique (uq_laboratory_xref_curie), and
    (laboratory_id, curie_prefix) is unique per-laboratory
    (uq_laboratory_xref_laboratory_prefix). exclude_id skips the row being patched.
    """
    curie_q = db.query(LaboratoryCrossReferenceModel.laboratory_cross_reference_id).filter(
        LaboratoryCrossReferenceModel.curie == curie
    )
    if exclude_id is not None:
        curie_q = curie_q.filter(LaboratoryCrossReferenceModel.laboratory_cross_reference_id != exclude_id)
    if curie_q.first():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Cross-reference '{curie}' already exists",
        )

    # NULL laboratory_id rows are exempt — Postgres treats NULLs as distinct.
    if laboratory_id is not None:
        prefix_q = db.query(LaboratoryCrossReferenceModel.laboratory_cross_reference_id).filter(
            LaboratoryCrossReferenceModel.laboratory_id == laboratory_id,
            LaboratoryCrossReferenceModel.curie_prefix == curie_prefix,
        )
        if exclude_id is not None:
            prefix_q = prefix_q.filter(
                LaboratoryCrossReferenceModel.laboratory_cross_reference_id != exclude_id
            )
        if prefix_q.first():
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"Another cross-reference with prefix '{curie_prefix}' "
                    f"already exists for this laboratory."
                ),
            )


def create_for_laboratory(db: Session, laboratory_id: int, payload: Dict[str, Any]) -> LaboratoryCrossReferenceModel:
    lab = db.query(LaboratoryModel).filter(LaboratoryModel.laboratory_id == laboratory_id).first()
    if not lab:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Laboratory with laboratory_id {laboratory_id} not found",
        )

    data = jsonable_encoder(payload)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    curie = (data.get("curie") or "").strip()
    if not curie:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="curie is required")

    curie_prefix = _curie_prefix(curie)

    _check_xref_unique(db, laboratory_id, curie, curie_prefix)

    obj = LaboratoryCrossReferenceModel(
        laboratory_id=laboratory_id,
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


def list_for_laboratory(db: Session, laboratory_id: int) -> List[LaboratoryCrossReferenceModel]:
    lab_exists = db.query(LaboratoryModel.laboratory_id).filter(LaboratoryModel.laboratory_id == laboratory_id).first()
    if not lab_exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Laboratory with laboratory_id {laboratory_id} not found",
        )
    return (
        db.query(LaboratoryCrossReferenceModel)
        .options(selectinload(LaboratoryCrossReferenceModel.laboratory))
        .filter(LaboratoryCrossReferenceModel.laboratory_id == laboratory_id)
        .order_by(LaboratoryCrossReferenceModel.laboratory_cross_reference_id.asc())
        .all()
    )


def get_by_curie_or_id(db: Session, curie_or_id: str) -> LaboratoryCrossReferenceModel:
    lcr_id = int(curie_or_id) if curie_or_id.isdigit() else None
    obj = (
        db.query(LaboratoryCrossReferenceModel)
        .filter(
            or_(
                LaboratoryCrossReferenceModel.curie == curie_or_id,
                LaboratoryCrossReferenceModel.laboratory_cross_reference_id == lcr_id,
            )
        )
        .order_by(LaboratoryCrossReferenceModel.is_obsolete)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"LaboratoryCrossReference with curie or id {curie_or_id} not found",
        )
    return obj


def show(db: Session, laboratory_cross_reference_id: int) -> LaboratoryCrossReferenceModel:
    obj = (
        db.query(LaboratoryCrossReferenceModel)
        .options(selectinload(LaboratoryCrossReferenceModel.laboratory))
        .filter(LaboratoryCrossReferenceModel.laboratory_cross_reference_id == laboratory_cross_reference_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"LaboratoryCrossReference with id {laboratory_cross_reference_id} not found",
        )
    return obj


def patch(db: Session, laboratory_cross_reference_id: int, patch_dict: Dict[str, Any]) -> Dict[str, Any]:
    obj = (
        db.query(LaboratoryCrossReferenceModel)
        .filter(LaboratoryCrossReferenceModel.laboratory_cross_reference_id == laboratory_cross_reference_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"LaboratoryCrossReference with id {laboratory_cross_reference_id} not found",
        )

    data = jsonable_encoder(patch_dict)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    if "curie" in data and data["curie"] is not None:
        new_curie = data["curie"].strip()
        new_prefix = _curie_prefix(new_curie)

        _check_xref_unique(
            db, obj.laboratory_id, new_curie, new_prefix,
            exclude_id=laboratory_cross_reference_id,
        )

        obj.curie = new_curie
        obj.curie_prefix = new_prefix

    if "pages" in data:
        obj.pages = _clean_pages(data["pages"])

    if "is_obsolete" in data and data["is_obsolete"] is not None:
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


def destroy(db: Session, laboratory_cross_reference_id: int) -> None:
    obj = (
        db.query(LaboratoryCrossReferenceModel)
        .filter(LaboratoryCrossReferenceModel.laboratory_cross_reference_id == laboratory_cross_reference_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"LaboratoryCrossReference with id {laboratory_cross_reference_id} not found",
        )
    db.delete(obj)
    db.commit()
