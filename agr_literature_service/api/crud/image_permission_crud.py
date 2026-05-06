"""
image_permission_crud.py
========================
"""

from typing import Dict, List, Optional

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload

from agr_literature_service.api.models import (
    ImagePermissionModel,
    ResourceImagePermissionModel,
    ResourceModel,
)
from agr_literature_service.api.schemas import (
    ImagePermissionSchemaPost,
    ImagePermissionSchemaUpdate,
    ResourceImagePermissionSchemaPost,
    ResourceImagePermissionSchemaUpdate,
)


def _validate_year_range(start_year: Optional[int], end_year: Optional[int]) -> None:
    if start_year is not None and end_year is not None and end_year < start_year:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="end_year must be greater than or equal to start_year",
        )


def _get_image_permission(db: Session, image_permission_id: int) -> ImagePermissionModel:
    image_permission = db.query(ImagePermissionModel).filter(
        ImagePermissionModel.image_permission_id == image_permission_id
    ).one_or_none()
    if image_permission is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"ImagePermission with image_permission_id {image_permission_id} not found",
        )
    return image_permission


def _get_resource(db: Session, resource_curie: str) -> ResourceModel:
    resource = db.query(ResourceModel).filter(ResourceModel.curie == resource_curie).one_or_none()
    if resource is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Resource with curie {resource_curie} not found",
        )
    return resource


def _get_resource_image_permission(
    db: Session,
    resource_image_permission_id: int,
) -> ResourceImagePermissionModel:
    resource_image_permission = db.query(ResourceImagePermissionModel).filter(
        ResourceImagePermissionModel.resource_image_permission_id == resource_image_permission_id
    ).one_or_none()
    if resource_image_permission is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                "ResourceImagePermission with resource_image_permission_id "
                f"{resource_image_permission_id} not found"
            ),
        )
    return resource_image_permission


def _serialize_resource_image_permission(obj: ResourceImagePermissionModel) -> Dict:
    image_permission = None
    if obj.image_permission:
        image_permission = {
            "image_permission_id": obj.image_permission.image_permission_id,
            "name": obj.image_permission.name,
            "permission_text": obj.image_permission.permission_text,
            "permission_url": obj.image_permission.permission_url,
            "can_display_images": obj.image_permission.can_display_images,
            "date_created": obj.image_permission.date_created,
            "date_updated": obj.image_permission.date_updated,
            "created_by": obj.image_permission.created_by,
            "updated_by": obj.image_permission.updated_by,
        }
    data = {
        "resource_image_permission_id": obj.resource_image_permission_id,
        "resource_id": obj.resource_id,
        "image_permission_id": obj.image_permission_id,
        "start_year": obj.start_year,
        "end_year": obj.end_year,
        "notes": obj.notes,
        "date_created": obj.date_created,
        "date_updated": obj.date_updated,
        "created_by": obj.created_by,
        "updated_by": obj.updated_by,
        "image_permission": image_permission,
    }
    if obj.resource:
        data["resource_curie"] = obj.resource.curie
        data["resource_title"] = obj.resource.title
    return jsonable_encoder(data)


def create_image_permission(db: Session, image_permission: ImagePermissionSchemaPost) -> int:
    image_permission_data = jsonable_encoder(image_permission)
    db_obj = ImagePermissionModel(**image_permission_data)
    db.add(db_obj)
    try:
        db.commit()
    except IntegrityError as err:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Error creating image_permission: {err}",
        )
    db.refresh(db_obj)
    return db_obj.image_permission_id


def patch_image_permission(
    db: Session,
    image_permission_id: int,
    image_permission_update: ImagePermissionSchemaUpdate,
) -> Dict[str, str]:
    db_obj = _get_image_permission(db, image_permission_id)
    update_data = image_permission_update.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(db_obj, field, value)
    try:
        db.commit()
    except IntegrityError as err:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Error updating image_permission: {err}",
        )
    return {"message": "updated"}


def destroy_image_permission(db: Session, image_permission_id: int) -> None:
    db_obj = _get_image_permission(db, image_permission_id)
    db.delete(db_obj)
    db.commit()


def show_image_permission(db: Session, image_permission_id: int) -> ImagePermissionModel:
    return _get_image_permission(db, image_permission_id)


def show_all_image_permissions(db: Session) -> List[ImagePermissionModel]:
    return db.query(ImagePermissionModel).order_by(ImagePermissionModel.name).all()


def create_resource_image_permission(
    db: Session,
    resource_image_permission: ResourceImagePermissionSchemaPost,
) -> int:
    data = jsonable_encoder(resource_image_permission)
    resource_curie = data.pop("resource_curie")
    _validate_year_range(data.get("start_year"), data.get("end_year"))
    resource = _get_resource(db, resource_curie)
    _get_image_permission(db, data["image_permission_id"])
    data["resource_id"] = resource.resource_id
    db_obj = ResourceImagePermissionModel(**data)
    db.add(db_obj)
    try:
        db.commit()
    except IntegrityError as err:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Error creating resource_image_permission: {err}",
        )
    db.refresh(db_obj)
    return db_obj.resource_image_permission_id


def patch_resource_image_permission(
    db: Session,
    resource_image_permission_id: int,
    resource_image_permission_update: ResourceImagePermissionSchemaUpdate,
) -> Dict[str, str]:
    db_obj = _get_resource_image_permission(db, resource_image_permission_id)
    update_data = resource_image_permission_update.model_dump(exclude_unset=True)
    start_year = update_data.get("start_year", db_obj.start_year)
    end_year = update_data.get("end_year", db_obj.end_year)
    _validate_year_range(start_year, end_year)
    if "image_permission_id" in update_data:
        _get_image_permission(db, update_data["image_permission_id"])
    for field, value in update_data.items():
        setattr(db_obj, field, value)
    try:
        db.commit()
    except IntegrityError as err:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Error updating resource_image_permission: {err}",
        )
    return {"message": "updated"}


def destroy_resource_image_permission(db: Session, resource_image_permission_id: int) -> None:
    db_obj = _get_resource_image_permission(db, resource_image_permission_id)
    db.delete(db_obj)
    db.commit()


def show_resource_image_permission(db: Session, resource_image_permission_id: int) -> Dict:
    db_obj = db.query(ResourceImagePermissionModel).options(
        joinedload(ResourceImagePermissionModel.resource),
        joinedload(ResourceImagePermissionModel.image_permission),
    ).filter(
        ResourceImagePermissionModel.resource_image_permission_id == resource_image_permission_id
    ).one_or_none()
    if db_obj is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                "ResourceImagePermission with resource_image_permission_id "
                f"{resource_image_permission_id} not found"
            ),
        )
    return _serialize_resource_image_permission(db_obj)


def show_resource_image_permissions_for_resource(db: Session, resource_curie: str) -> List[Dict]:
    resource = _get_resource(db, resource_curie)
    rows = db.query(ResourceImagePermissionModel).options(
        joinedload(ResourceImagePermissionModel.resource),
        joinedload(ResourceImagePermissionModel.image_permission),
    ).filter(
        ResourceImagePermissionModel.resource_id == resource.resource_id
    ).order_by(
        ResourceImagePermissionModel.start_year.asc().nullsfirst(),
        ResourceImagePermissionModel.end_year.asc().nullsfirst(),
        ResourceImagePermissionModel.resource_image_permission_id.asc(),
    ).all()
    return [_serialize_resource_image_permission(row) for row in rows]


def show_all_resource_image_permissions(db: Session) -> List[Dict]:
    rows = db.query(ResourceImagePermissionModel).options(
        joinedload(ResourceImagePermissionModel.resource),
        joinedload(ResourceImagePermissionModel.image_permission),
    ).order_by(ResourceImagePermissionModel.resource_image_permission_id).all()
    return [_serialize_resource_image_permission(row) for row in rows]
