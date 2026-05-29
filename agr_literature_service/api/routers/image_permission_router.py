from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Response, Security, status
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.auth import get_authenticated_user
from agr_literature_service.api.crud import image_permission_crud
from agr_literature_service.api.schemas import (
    ImagePermissionSchemaPost,
    ImagePermissionSchemaShow,
    ImagePermissionSchemaUpdate,
    ResourceImagePermissionSchemaPost,
    ResourceImagePermissionSchemaShow,
    ResourceImagePermissionSchemaUpdate,
)
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.util.resource_urls import (
    image_permission_url,
    resource_image_permission_url,
)

router = APIRouter(
    prefix="/image_permission",
    tags=["Image Permission"]
)

get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post("/",
             status_code=status.HTTP_201_CREATED,
             response_model=ImagePermissionSchemaShow)
def create_image_permission(
    request: ImagePermissionSchemaPost,
    response: Response,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    new_id = image_permission_crud.create_image_permission(db, request)
    response.headers["Location"] = image_permission_url(new_id)
    return image_permission_crud.show_image_permission(db, new_id)


@router.get("/all",
            status_code=200,
            response_model=List[ImagePermissionSchemaShow])
def show_all_image_permissions(
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    return image_permission_crud.show_all_image_permissions(db)


@router.post("/resource",
             status_code=status.HTTP_201_CREATED,
             response_model=ResourceImagePermissionSchemaShow)
def create_resource_image_permission(
    request: ResourceImagePermissionSchemaPost,
    response: Response,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    new_id = image_permission_crud.create_resource_image_permission(db, request)
    response.headers["Location"] = resource_image_permission_url(new_id)
    return image_permission_crud.show_resource_image_permission(db, new_id)


@router.get("/resource/all",
            status_code=200,
            response_model=List[ResourceImagePermissionSchemaShow])
def show_all_resource_image_permissions(
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    return image_permission_crud.show_all_resource_image_permissions(db)


@router.get("/resource/{resource_curie}",
            status_code=200,
            response_model=List[ResourceImagePermissionSchemaShow])
def show_resource_image_permissions_for_resource(
    resource_curie: str,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    return image_permission_crud.show_resource_image_permissions_for_resource(db, resource_curie)


@router.get("/resource_link/{resource_image_permission_id}",
            status_code=200,
            response_model=ResourceImagePermissionSchemaShow)
def show_resource_image_permission(
    resource_image_permission_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    return image_permission_crud.show_resource_image_permission(db, resource_image_permission_id)


@router.patch("/resource_link/{resource_image_permission_id}",
              status_code=status.HTTP_200_OK,
              response_model=ResourceImagePermissionSchemaShow)
def patch_resource_image_permission(
    resource_image_permission_id: int,
    request: ResourceImagePermissionSchemaUpdate,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    image_permission_crud.patch_resource_image_permission(
        db, resource_image_permission_id, request,
    )
    return image_permission_crud.show_resource_image_permission(db, resource_image_permission_id)


@router.delete("/resource_link/{resource_image_permission_id}",
               status_code=status.HTTP_204_NO_CONTENT)
def destroy_resource_image_permission(
    resource_image_permission_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    image_permission_crud.destroy_resource_image_permission(db, resource_image_permission_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/{image_permission_id}",
            status_code=200,
            response_model=ImagePermissionSchemaShow)
def show_image_permission(
    image_permission_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    return image_permission_crud.show_image_permission(db, image_permission_id)


@router.patch("/{image_permission_id}",
              status_code=status.HTTP_200_OK,
              response_model=ImagePermissionSchemaShow)
def patch_image_permission(
    image_permission_id: int,
    request: ImagePermissionSchemaUpdate,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    image_permission_crud.patch_image_permission(db, image_permission_id, request)
    return image_permission_crud.show_image_permission(db, image_permission_id)


@router.delete("/{image_permission_id}",
               status_code=status.HTTP_204_NO_CONTENT)
def destroy_image_permission(
    image_permission_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    image_permission_crud.destroy_image_permission(db, image_permission_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
