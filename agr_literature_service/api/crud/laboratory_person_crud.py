import logging
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from agr_literature_service.api.models import (
    LaboratoryModel,
    LaboratoryPersonModel,
    PersonModel,
)
from agr_literature_service.api.crud.user_utils import map_to_user_id

logger = logging.getLogger(__name__)

_SCALAR_FIELDS = {
    "is_pi", "former_pi", "alum",
    "is_lab_contact", "can_edit_lab", "lab_position",
}
_NOT_NULL = {"is_lab_contact", "can_edit_lab"}


def create_for_laboratory(db: Session, laboratory_id: int, payload: Dict[str, Any]) -> LaboratoryPersonModel:
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

    person_id = data.get("person_id")
    person = db.query(PersonModel.person_id).filter(PersonModel.person_id == person_id).first()
    if not person:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person with person_id {person_id} not found",
        )

    obj = LaboratoryPersonModel(
        laboratory_id=laboratory_id,
        person_id=person_id,
        is_pi=data.get("is_pi"),
        former_pi=data.get("former_pi"),
        alum=data.get("alum"),
        is_lab_contact=bool(data.get("is_lab_contact", False)),
        can_edit_lab=bool(data.get("can_edit_lab", False)),
        lab_position=data.get("lab_position"),
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


def list_for_laboratory(db: Session, laboratory_id: int) -> List[LaboratoryPersonModel]:
    lab_exists = db.query(LaboratoryModel.laboratory_id).filter(LaboratoryModel.laboratory_id == laboratory_id).first()
    if not lab_exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Laboratory with laboratory_id {laboratory_id} not found",
        )
    return (
        db.query(LaboratoryPersonModel)
        .options(
            selectinload(LaboratoryPersonModel.person),
            selectinload(LaboratoryPersonModel.laboratory),
        )
        .filter(LaboratoryPersonModel.laboratory_id == laboratory_id)
        .order_by(LaboratoryPersonModel.laboratory_person_id.asc())
        .all()
    )


def list_for_person(db: Session, person_id: int) -> List[LaboratoryPersonModel]:
    person_exists = db.query(PersonModel.person_id).filter(PersonModel.person_id == person_id).first()
    if not person_exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person with person_id {person_id} not found",
        )
    return (
        db.query(LaboratoryPersonModel)
        .options(
            selectinload(LaboratoryPersonModel.person),
            selectinload(LaboratoryPersonModel.laboratory),
        )
        .filter(LaboratoryPersonModel.person_id == person_id)
        .order_by(LaboratoryPersonModel.laboratory_person_id.asc())
        .all()
    )


def show(db: Session, laboratory_person_id: int) -> LaboratoryPersonModel:
    obj = (
        db.query(LaboratoryPersonModel)
        .options(
            selectinload(LaboratoryPersonModel.laboratory),
            selectinload(LaboratoryPersonModel.person),
        )
        .filter(LaboratoryPersonModel.laboratory_person_id == laboratory_person_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"LaboratoryPerson with id {laboratory_person_id} not found",
        )
    return obj


def patch(db: Session, laboratory_person_id: int, patch_dict: Dict[str, Any]) -> Dict[str, Any]:
    obj: Optional[LaboratoryPersonModel] = (
        db.query(LaboratoryPersonModel)
        .filter(LaboratoryPersonModel.laboratory_person_id == laboratory_person_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"LaboratoryPerson with id {laboratory_person_id} not found",
        )

    data = jsonable_encoder(patch_dict)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    for field, value in data.items():
        if field not in _SCALAR_FIELDS:
            continue
        if field in _NOT_NULL and value is None:
            continue
        setattr(obj, field, value)

    db.commit()
    return {"message": "updated"}


def destroy(db: Session, laboratory_person_id: int) -> None:
    obj = (
        db.query(LaboratoryPersonModel)
        .filter(LaboratoryPersonModel.laboratory_person_id == laboratory_person_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"LaboratoryPerson with id {laboratory_person_id} not found",
        )
    db.delete(obj)
    db.commit()
