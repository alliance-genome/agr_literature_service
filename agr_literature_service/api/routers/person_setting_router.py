from typing import List, Dict, Any
from fastapi import APIRouter, Depends, Response, Security, status

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import person_setting_crud
from agr_literature_service.api.schemas import (
    PersonSettingSchemaCreate,
    PersonSettingSchemaUpdate,
    PersonSettingSchemaShow,
    ResponseMessageSchema,
)
from agr_literature_service.api.user import set_global_user_from_cognito

from agr_cognito_auth import get_cognito_user_swagger

router = APIRouter(prefix="/person_setting", tags=["PersonSetting"])

get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post("/", status_code=status.HTTP_201_CREATED, response_model=PersonSettingSchemaShow)
def create(
    request: PersonSettingSchemaCreate,
    user: Dict[str, Any] = Security(get_cognito_user_swagger),
    db: Session = db_session,
):
    """
    Create a person_setting record.
    """
    set_global_user_from_cognito(db, user)
    return person_setting_crud.create(db, request)


@router.delete("/{person_setting_id}", status_code=status.HTTP_204_NO_CONTENT)
def destroy(
    person_setting_id: int,
    user: Dict[str, Any] = Security(get_cognito_user_swagger),
    db: Session = db_session,
):
    """
    Delete a person_setting row by internal ID.
    """
    set_global_user_from_cognito(db, user)
    person_setting_crud.destroy(db, person_setting_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch(
    "/{person_setting_id}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ResponseMessageSchema,
)
def patch(
    person_setting_id: int,
    request: PersonSettingSchemaUpdate,
    user: Dict[str, Any] = Security(get_cognito_user_swagger),
    db: Session = db_session,
):
    """
    Patch a person_setting row by internal ID.
    """
    set_global_user_from_cognito(db, user)
    patch_data = request.model_dump(exclude_unset=True)
    return person_setting_crud.patch(db, person_setting_id, patch_data)


@router.get(
    "/{person_setting_id}",
    response_model=PersonSettingSchemaShow,
    status_code=status.HTTP_200_OK,
)
def show(
    person_setting_id: int,
    db: Session = db_session,
):
    """
    Get a person_setting row by internal ID.
    """
    return person_setting_crud.show(db, person_setting_id)


@router.get(
    "/by/okta/{okta_id}",
    response_model=List[PersonSettingSchemaShow],
    status_code=status.HTTP_200_OK,
)
def get_by_okta_id(
    okta_id: str,
    db: Session = db_session,
):
    """
    Get person_setting rows by Okta user ID.
    Returns 200 with a list (possibly multiple rows) or 204 if none.
    """
    person_setting_list = person_setting_crud.get_by_okta_id(db, okta_id)
    if not person_setting_list:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return person_setting_list


@router.get(
    "/by/email/{email}",
    response_model=List[PersonSettingSchemaShow],
    status_code=status.HTTP_200_OK,
)
def get_by_email(
    email: str,
    db: Session = db_session,
):
    """
    Get person_setting rows by email (exact match).
    Returns 200 with a list (possibly multiple rows) or 204 if none.
    """
    person_setting_list = person_setting_crud.get_by_email(db, email)
    if not person_setting_list:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return person_setting_list


@router.get(
    "/by/name",
    response_model=List[PersonSettingSchemaShow],
    status_code=status.HTTP_200_OK,
)
def get_by_name(
    name: str,
    db: Session = db_session,
):
    """
    Find person_setting rows by person display name.
    Matching strategy (exact/ILIKE) is implemented inside person_setting_crud.
    """
    return person_setting_crud.find_by_name(db, name)
