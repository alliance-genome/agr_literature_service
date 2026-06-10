import logging
from typing import Any, Dict, Optional

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from agr_literature_service.api.models import (
    LaboratoryModel,
    LaboratoryCrossReferenceModel,
)
from agr_literature_service.api.schemas import LaboratorySchemaCreate
from agr_literature_service.api.crud.user_utils import map_to_user_id

logger = logging.getLogger(__name__)

# AGRKB curie derived from the laboratory_id until MATI Laboratory support exists.
# e.g. laboratory_id=1 -> "AGRKB:705000000000001".
CURIE_PREFIX = "AGRKB:705"


def laboratory_curie_from_id(laboratory_id: int) -> str:
    return f"{CURIE_PREFIX}{laboratory_id:012d}"


def resolve_laboratory_id(db: Session, curie_or_laboratory_id: str) -> int:
    laboratory_id = int(curie_or_laboratory_id) if curie_or_laboratory_id.isdigit() else None
    lab = (
        db.query(LaboratoryModel.laboratory_id)
        .filter(
            or_(
                LaboratoryModel.curie == curie_or_laboratory_id,
                LaboratoryModel.laboratory_id == laboratory_id,
            )
        )
        .one_or_none()
    )
    if not lab:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Laboratory with curie or laboratory_id {curie_or_laboratory_id} not found",
        )
    return lab[0]


def _curie_prefix_from(curie: str) -> str:
    curie = curie.strip()
    if curie.count(":") != 1:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid CURIE '{curie}': expected exactly one colon",
        )
    return curie.split(":", 1)[0]


def create(db: Session, payload: LaboratorySchemaCreate) -> LaboratoryModel:
    data: Dict[str, Any] = jsonable_encoder(payload)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    xrefs_data = data.pop("cross_references", None)

    # Validate cross-references against the two unique constraints on
    # laboratory_cross_reference: curie is globally unique, and
    # (laboratory_id, curie_prefix) is unique per-laboratory.
    if xrefs_data:
        seen_curies: set = set()
        seen_prefixes: set = set()
        for xr in xrefs_data:
            curie = xr["curie"].strip()
            curie_prefix = _curie_prefix_from(curie)

            if curie in seen_curies:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"Cross-reference '{curie}' is duplicated in the request",
                )
            seen_curies.add(curie)

            if curie_prefix in seen_prefixes:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(
                        f"Multiple cross-references with prefix '{curie_prefix}' "
                        "in the request; at most one per prefix is allowed."
                    ),
                )
            seen_prefixes.add(curie_prefix)

            existing = (
                db.query(LaboratoryCrossReferenceModel.laboratory_cross_reference_id)
                .filter(LaboratoryCrossReferenceModel.curie == curie)
                .first()
            )
            if existing:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"Cross-reference '{curie}' already exists",
                )

    obj = LaboratoryModel(**data)
    db.add(obj)
    db.flush()  # get laboratory_id

    # Derive the curie from the laboratory_id.
    obj.curie = laboratory_curie_from_id(obj.laboratory_id)

    if xrefs_data:
        for xr in xrefs_data:
            curie = xr["curie"].strip()
            curie_prefix = _curie_prefix_from(curie)
            db.add(
                LaboratoryCrossReferenceModel(
                    laboratory_id=obj.laboratory_id,
                    curie=curie,
                    curie_prefix=curie_prefix,
                    pages=xr.get("pages"),
                    is_obsolete=bool(xr.get("is_obsolete", False)),
                )
            )

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


def destroy(db: Session, curie_or_laboratory_id: str) -> None:
    laboratory_id = resolve_laboratory_id(db, curie_or_laboratory_id)
    obj: Optional[LaboratoryModel] = (
        db.query(LaboratoryModel).filter(LaboratoryModel.laboratory_id == laboratory_id).first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Laboratory with curie or laboratory_id {curie_or_laboratory_id} not found",
        )
    db.delete(obj)
    db.commit()


def patch(db: Session, curie_or_laboratory_id: str, patch_dict: Dict[str, Any]) -> Dict[str, Any]:
    laboratory_id = resolve_laboratory_id(db, curie_or_laboratory_id)
    obj: Optional[LaboratoryModel] = (
        db.query(LaboratoryModel).filter(LaboratoryModel.laboratory_id == laboratory_id).first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Laboratory with curie or laboratory_id {curie_or_laboratory_id} not found",
        )

    data = jsonable_encoder(patch_dict)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    ALLOWED = {
        "name", "strain_designation", "institution", "webpage",
        "city", "state", "postal_code", "country", "street_address",
        "email", "email_visibility", "lab_is_open", "status",
        "research_area", "short_research_description",
        "additional_information", "private_note",
    }
    # NOT NULL columns must not be set to null via PATCH.
    NOT_NULL = {"lab_is_open", "status"}
    for field, value in data.items():
        if field not in ALLOWED:
            continue
        if field in NOT_NULL and value is None:
            continue
        setattr(obj, field, value)

    db.commit()
    return {"message": "updated"}


def show(db: Session, curie_or_laboratory_id: str) -> LaboratoryModel:
    laboratory_id = resolve_laboratory_id(db, curie_or_laboratory_id)
    obj: Optional[LaboratoryModel] = (
        db.query(LaboratoryModel)
        .options(selectinload(LaboratoryModel.cross_references))
        .filter(LaboratoryModel.laboratory_id == laboratory_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Laboratory with curie or laboratory_id {curie_or_laboratory_id} not found",
        )
    return obj
