import logging
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import and_, func
from sqlalchemy.orm import Session, joinedload

from agr_literature_service.api.models.person_model import PersonModel
from agr_literature_service.api.models.email_model import EmailModel
from agr_literature_service.api.models.person_setting_model import PersonSettingModel
from agr_literature_service.api.crud.user_utils import map_to_user_id

logger = logging.getLogger(__name__)


def normalize_email(s: str) -> str:
    return s.strip().lower()


def _non_empty_or_422(field: str, value: Optional[str]) -> str:
    v = (value or "").strip()
    if not v:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"{field} must be a non-empty string",
        )
    return v


def _assert_person_exists(db: Session, person_id: int) -> None:
    exists = db.query(PersonModel.person_id).filter(PersonModel.person_id == person_id).first()
    if not exists:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"person_id {person_id} does not exist",
        )


def _assert_default_unique(
    db: Session,
    person_id: int,
    component_name: str,
    exclude_person_setting_id: Optional[int] = None,
) -> None:
    """
    Enforce at-most-one default per (person_id, component_name).
    This mirrors the DB partial unique index at the application layer
    to give a friendlier error before hitting the constraint.
    """
    q = (
        db.query(PersonSettingModel.person_setting_id)
        .filter(PersonSettingModel.person_id == person_id)
        .filter(PersonSettingModel.component_name == component_name)
        .filter(PersonSettingModel.default_setting.is_(True))
    )
    if exclude_person_setting_id is not None:
        q = q.filter(PersonSettingModel.person_setting_id != exclude_person_setting_id)

    existing = q.first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "A default setting already exists for this (person_id, component_name). "
                "Unset the existing default or set this row to default_setting = false."
            ),
        )


def create(db: Session, payload) -> PersonSettingModel:
    """
    Create a PersonSetting row.
    Enforces:
      - person exists
      - non-empty component_name / setting_name
      - only one default per (person_id, component_name)
    """
    data: Dict[str, Any] = jsonable_encoder(payload)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    person_id = data.get("person_id")
    if person_id is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="person_id is required",
        )
    _assert_person_exists(db, int(person_id))

    component_name = _non_empty_or_422("component_name", data.get("component_name"))
    setting_name = _non_empty_or_422("setting_name", data.get("setting_name"))

    is_default = bool(data.get("default_setting", False))
    if is_default:
        _assert_default_unique(db, person_id, component_name)

    obj = PersonSettingModel(
        person_id=person_id,
        component_name=component_name,
        setting_name=setting_name,
        default_setting=is_default,
        json_settings=data.get("json_settings") or {},
        created_by=data.get("created_by"),
        updated_by=data.get("updated_by"),
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


def destroy(db: Session, person_setting_id: int) -> None:
    obj: Optional[PersonSettingModel] = (
        db.query(PersonSettingModel)
        .filter(PersonSettingModel.person_setting_id == person_setting_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"person_setting_id {person_setting_id} not found",
        )
    db.delete(obj)
    db.commit()


def patch(db: Session, person_setting_id: int, patch_dict: Dict[str, Any]) -> Dict[str, Any]:
    obj: Optional[PersonSettingModel] = (
        db.query(PersonSettingModel)
        .filter(PersonSettingModel.person_setting_id == person_setting_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"person_setting_id {person_setting_id} not found",
        )

    data = jsonable_encoder(patch_dict)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    # Prepare new target values for uniqueness check
    new_person_id = int(data.get("person_id", obj.person_id))
    new_component_name = data.get("component_name", obj.component_name)
    new_default_setting = data.get("default_setting", obj.default_setting)

    if "person_id" in data and data["person_id"] is not None:
        _assert_person_exists(db, new_person_id)

    if "component_name" in data and data["component_name"] is not None:
        new_component_name = _non_empty_or_422("component_name", new_component_name)

    if "setting_name" in data and data["setting_name"] is not None:
        data["setting_name"] = _non_empty_or_422("setting_name", data["setting_name"])

    # If this row is (or will become) the default, ensure no other default conflicts
    if bool(new_default_setting):
        _assert_default_unique(
            db,
            person_id=new_person_id,
            component_name=new_component_name,
            exclude_person_setting_id=person_setting_id,
        )

    # Only update scalar fields defined on the table
    ALLOWED = {"person_id", "component_name", "setting_name", "default_setting", "json_settings"}
    for field, value in data.items():
        if field in ALLOWED:
            setattr(obj, field, value)

    db.commit()
    return {"message": "updated"}


def show(db: Session, person_setting_id: int) -> PersonSettingModel:
    obj: Optional[PersonSettingModel] = (
        db.query(PersonSettingModel)
        .options(joinedload(PersonSettingModel.person))
        .filter(PersonSettingModel.person_setting_id == person_setting_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"person_setting_id {person_setting_id} not found",
        )
    return obj


# ---------- Lookup helpers used by router ----------
def get_by_email(db: Session, email: str) -> List[PersonSettingModel]:
    if not email:
        return []
    email_norm = normalize_email(email)
    return (
        db.query(PersonSettingModel)
        .join(PersonModel, PersonModel.person_id == PersonSettingModel.person_id)
        .join(EmailModel, and_(EmailModel.person_id == PersonModel.person_id))
        .options(joinedload(PersonSettingModel.person))
        .filter(func.lower(EmailModel.email_address) == email_norm)
        .order_by(PersonSettingModel.component_name.asc(), PersonSettingModel.setting_name.asc())
        .all()
    )


def find_by_name(db: Session, name: str) -> List[PersonSettingModel]:
    """
    Case-insensitive partial match on Person.display_name.
    """
    if not name:
        return []
    pattern = f"%{name.strip()}%"
    return (
        db.query(PersonSettingModel)
        .join(PersonModel, PersonModel.person_id == PersonSettingModel.person_id)
        .options(joinedload(PersonSettingModel.person))
        .filter(PersonModel.display_name.ilike(pattern))
        .order_by(PersonModel.display_name.asc(), PersonSettingModel.component_name.asc(), PersonSettingModel.setting_name.asc())
        .all()
    )
