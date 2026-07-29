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
from agr_literature_service.api.crud import vocabulary_crud
from agr_literature_service.api.crud.vocabulary_crud import VocabularyTermRefSchema
from agr_literature_service.api.crud.vocabulary_seed_data import LAB_POSITION_VOCAB

logger = logging.getLogger(__name__)

_SCALAR_FIELDS = {
    "is_pi", "former_pi", "alum",
    "is_lab_contact", "can_edit_lab",
}
_NOT_NULL = {"is_lab_contact", "can_edit_lab"}


def _to_show_dict(db: Session, obj: LaboratoryPersonModel) -> Dict[str, Any]:
    return {
        "laboratory_person_id": obj.laboratory_person_id,
        "laboratory_id": obj.laboratory_id,
        "laboratory_curie": obj.laboratory_curie,
        "laboratory_name": obj.laboratory_name,
        "laboratory_strain_designation": obj.laboratory_strain_designation,
        "person_id": obj.person_id,
        "person_curie": obj.person_curie,
        "person_display_name": obj.person_display_name,
        "is_pi": obj.is_pi, "former_pi": obj.former_pi, "alum": obj.alum,
        "is_lab_contact": obj.is_lab_contact, "can_edit_lab": obj.can_edit_lab,
        "lab_position": vocabulary_crud.serialize_term_ref(
            db, obj.lab_position_vocabulary_term_abc_id),
        "date_created": obj.date_created, "date_updated": obj.date_updated,
        "created_by": obj.created_by, "updated_by": obj.updated_by,
    }


def create_for_laboratory(db: Session, laboratory_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
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

    term_id = data.get("lab_position")
    if term_id is not None:
        vocabulary_crud.validate_term_id(db, LAB_POSITION_VOCAB, term_id)
    obj = LaboratoryPersonModel(
        laboratory_id=laboratory_id,
        person_id=person_id,
        is_pi=data.get("is_pi"),
        former_pi=data.get("former_pi"),
        alum=data.get("alum"),
        is_lab_contact=bool(data.get("is_lab_contact", False)),
        can_edit_lab=bool(data.get("can_edit_lab", False)),
        lab_position_vocabulary_term_abc_id=term_id,
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
    return _to_show_dict(db, obj)


def list_for_laboratory(db: Session, laboratory_id: int) -> List[Dict[str, Any]]:
    lab_exists = db.query(LaboratoryModel.laboratory_id).filter(LaboratoryModel.laboratory_id == laboratory_id).first()
    if not lab_exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Laboratory with laboratory_id {laboratory_id} not found",
        )
    rows = (
        db.query(LaboratoryPersonModel)
        .options(
            selectinload(LaboratoryPersonModel.person),
            selectinload(LaboratoryPersonModel.laboratory),
        )
        .filter(LaboratoryPersonModel.laboratory_id == laboratory_id)
        .order_by(LaboratoryPersonModel.laboratory_person_id.asc())
        .all()
    )
    return [_to_show_dict(db, o) for o in rows]


def list_for_person(db: Session, person_id: int) -> List[Dict[str, Any]]:
    person_exists = db.query(PersonModel.person_id).filter(PersonModel.person_id == person_id).first()
    if not person_exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person with person_id {person_id} not found",
        )
    rows = (
        db.query(LaboratoryPersonModel)
        .options(
            selectinload(LaboratoryPersonModel.person),
            selectinload(LaboratoryPersonModel.laboratory),
        )
        .filter(LaboratoryPersonModel.person_id == person_id)
        .order_by(LaboratoryPersonModel.laboratory_person_id.asc())
        .all()
    )
    return [_to_show_dict(db, o) for o in rows]


def show(db: Session, laboratory_person_id: int) -> Dict[str, Any]:
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
    return _to_show_dict(db, obj)


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

    if "lab_position" in data:
        term_id = data["lab_position"]
        if term_id is not None:
            vocabulary_crud.validate_term_id(db, LAB_POSITION_VOCAB, term_id)
        obj.lab_position_vocabulary_term_abc_id = term_id

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


# --- Forward-reference resolution -------------------------------------------------
# laboratory_person_schemas declares its lab_position field type
# (VocabularyTermRefSchema) under TYPE_CHECKING to avoid a schemas<->crud import
# cycle. VocabularyTermRefSchema is a name in this module's globals (imported above),
# so calling model_rebuild() here completes those read schemas. The two parent
# schemas that embed LaboratoryPersonSchemaRelated must be rebuilt afterwards so
# their nested reference picks up the now-complete model.
_ = VocabularyTermRefSchema  # keep the name bound for model_rebuild's namespace
from agr_literature_service.api.schemas.laboratory_person_schemas import (  # noqa: E402
    LaboratoryPersonSchemaShow,
    LaboratoryPersonSchemaRelated,
)
from agr_literature_service.api.schemas.person_schemas import PersonSchemaShow  # noqa: E402
from agr_literature_service.api.schemas.laboratory_schemas import LaboratorySchemaShow  # noqa: E402

LaboratoryPersonSchemaShow.model_rebuild()
LaboratoryPersonSchemaRelated.model_rebuild()
PersonSchemaShow.model_rebuild()
LaboratorySchemaShow.model_rebuild()
