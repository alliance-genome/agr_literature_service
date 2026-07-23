import logging
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from agr_literature_service.api.models import (
    LaboratoryModel,
    LaboratoryAlleleDesignationModel,
    ModModel,
)
from agr_literature_service.api.crud.user_utils import map_to_user_id

logger = logging.getLogger(__name__)


def _resolve_mod_id(db: Session, mod_abbreviation: str) -> int:
    row = (
        db.query(ModModel.mod_id)
        .filter(ModModel.abbreviation == mod_abbreviation)
        .one_or_none()
    )
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"MOD with abbreviation '{mod_abbreviation}' not found",
        )
    return row[0]


def create_for_laboratory(db: Session, laboratory_id: int, payload: Dict[str, Any]) -> LaboratoryAlleleDesignationModel:
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

    mod_id = _resolve_mod_id(db, data["mod_abbreviation"])

    obj = LaboratoryAlleleDesignationModel(
        laboratory_id=laboratory_id,
        mod_id=mod_id,
        allele_designation=data["allele_designation"],
        is_obsolete=bool(data.get("is_obsolete", False)),
    )
    db.add(obj)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "An active allele designation for this laboratory and MOD already exists."
            ),
        )
    db.refresh(obj)
    return obj


def list_for_laboratory(db: Session, laboratory_id: int) -> List[LaboratoryAlleleDesignationModel]:
    lab_exists = db.query(LaboratoryModel.laboratory_id).filter(LaboratoryModel.laboratory_id == laboratory_id).first()
    if not lab_exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Laboratory with laboratory_id {laboratory_id} not found",
        )
    return (
        db.query(LaboratoryAlleleDesignationModel)
        .options(
            selectinload(LaboratoryAlleleDesignationModel.laboratory),
            selectinload(LaboratoryAlleleDesignationModel.mod),
        )
        .filter(LaboratoryAlleleDesignationModel.laboratory_id == laboratory_id)
        .order_by(LaboratoryAlleleDesignationModel.laboratory_allele_designation_id.asc())
        .all()
    )


def show(db: Session, laboratory_allele_designation_id: int) -> LaboratoryAlleleDesignationModel:
    obj = (
        db.query(LaboratoryAlleleDesignationModel)
        .options(
            selectinload(LaboratoryAlleleDesignationModel.laboratory),
            selectinload(LaboratoryAlleleDesignationModel.mod),
        )
        .filter(
            LaboratoryAlleleDesignationModel.laboratory_allele_designation_id
            == laboratory_allele_designation_id
        )
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"LaboratoryAlleleDesignation with id {laboratory_allele_designation_id} not found",
        )
    return obj


def patch(db: Session, laboratory_allele_designation_id: int, patch_dict: Dict[str, Any]) -> Dict[str, Any]:
    obj: Optional[LaboratoryAlleleDesignationModel] = (
        db.query(LaboratoryAlleleDesignationModel)
        .filter(
            LaboratoryAlleleDesignationModel.laboratory_allele_designation_id
            == laboratory_allele_designation_id
        )
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"LaboratoryAlleleDesignation with id {laboratory_allele_designation_id} not found",
        )

    data = jsonable_encoder(patch_dict)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    if "mod_abbreviation" in data and data["mod_abbreviation"] is not None:
        obj.mod_id = _resolve_mod_id(db, data["mod_abbreviation"])

    if "allele_designation" in data and data["allele_designation"] is not None:
        obj.allele_designation = data["allele_designation"]

    if "is_obsolete" in data and data["is_obsolete"] is not None:
        obj.is_obsolete = bool(data["is_obsolete"])

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "An active allele designation for this laboratory and MOD already exists."
            ),
        )
    return {"message": "updated"}


def destroy(db: Session, laboratory_allele_designation_id: int) -> None:
    obj = (
        db.query(LaboratoryAlleleDesignationModel)
        .filter(
            LaboratoryAlleleDesignationModel.laboratory_allele_designation_id
            == laboratory_allele_designation_id
        )
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"LaboratoryAlleleDesignation with id {laboratory_allele_designation_id} not found",
        )
    db.delete(obj)
    db.commit()
