"""
person_note_crud.py
"""
import logging
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.models import PersonModel, PersonNoteModel
from agr_literature_service.api.crud.user_utils import map_to_user_id

logger = logging.getLogger(__name__)


def create_for_person(db: Session, person_id: int, payload: Dict[str, Any]) -> PersonNoteModel:
    """
    Create a PersonNote row that belongs to a Person (person_id).

    Semantics:
      - note is required.
      - A person can have multiple notes.
    """
    person = db.query(PersonModel).filter(PersonModel.person_id == person_id).first()
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

    if "note" not in data or not data["note"]:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="note is required",
        )

    obj = PersonNoteModel(
        person_id=person_id,
        note=data["note"],
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


def list_for_person(db: Session, person_id: int) -> List[PersonNoteModel]:
    """
    List all notes for a person.
    """
    person_exists = db.query(PersonModel.person_id).filter(PersonModel.person_id == person_id).first()
    if not person_exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person with person_id {person_id} not found",
        )
    return (
        db.query(PersonNoteModel)
        .filter(PersonNoteModel.person_id == person_id)
        .order_by(PersonNoteModel.person_note_id.asc())
        .all()
    )


def show(db: Session, person_note_id: int) -> PersonNoteModel:
    obj = db.query(PersonNoteModel).filter(PersonNoteModel.person_note_id == person_note_id).first()
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PersonNote with person_note_id {person_note_id} not found",
        )
    return obj


def patch(db: Session, person_note_id: int, patch_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Patch a PersonNote row. Supports:
      - note (string)
    """
    obj: Optional[PersonNoteModel] = (
        db.query(PersonNoteModel).filter(PersonNoteModel.person_note_id == person_note_id).first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PersonNote with person_note_id {person_note_id} not found",
        )

    data = jsonable_encoder(patch_dict)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    if "note" in data:
        if not data["note"]:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="note cannot be empty",
            )
        obj.note = data["note"]

    db.commit()
    return {"message": "updated"}


def destroy(db: Session, person_note_id: int) -> None:
    obj = db.query(PersonNoteModel).filter(PersonNoteModel.person_note_id == person_note_id).first()
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"PersonNote with person_note_id {person_note_id} not found",
        )
    db.delete(obj)
    db.commit()
