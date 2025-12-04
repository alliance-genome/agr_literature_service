from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends, Response, Security, status

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import person_crud
from agr_literature_service.api.schemas import (
    PersonSchemaCreate,
    PersonSchemaUpdate,
    PersonSchemaShow,
    ResponseMessageSchema,
)
from agr_literature_service.api.user import set_global_user_from_cognito

from agr_cognito_py import get_cognito_user_swagger

router = APIRouter(prefix="/person", tags=["Person"])

get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post("/", status_code=status.HTTP_201_CREATED, response_model=PersonSchemaShow)
def create(
    request: PersonSchemaCreate,
    user: Dict[str, Any] = Security(get_cognito_user_swagger),
    db: Session = db_session,
):
    """
    Create a person.
    """
    set_global_user_from_cognito(db, user)
    return person_crud.create(db, request)


@router.delete("/{person_id}", status_code=status.HTTP_204_NO_CONTENT)
def destroy(
    person_id: int,
    user: Dict[str, Any] = Security(get_cognito_user_swagger),
    db: Session = db_session,
):
    """
    Delete a person by internal ID.
    """
    set_global_user_from_cognito(db, user)
    person_crud.destroy(db, person_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch(
    "/{person_id}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ResponseMessageSchema,
)
def patch(
    person_id: int,
    request: PersonSchemaUpdate,
    user: Dict[str, Any] = Security(get_cognito_user_swagger),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    patch_data = request.model_dump(exclude_unset=True)
    return person_crud.patch(db, person_id, patch_data)


@router.get('/whoami')
def get_user_info_from_cognito(
    user: Dict[str, Any] = Security(get_cognito_user_swagger)
):
    """Get information about the currently authenticated user."""
    return {
        "user_id": user["sub"],
        "email": user["email"],
        "name": user["name"],
        "groups": user["cognito:groups"]
    }


@router.get(
    "/{person_id}",
    response_model=PersonSchemaShow,
    status_code=status.HTTP_200_OK,
)
def show(
    person_id: int,
    db: Session = db_session,
):
    """
    Get a person by internal ID.
    """
    return person_crud.show(db, person_id)


@router.get(
    "/by/email/{email}",
    response_model=Optional[PersonSchemaShow],
    status_code=status.HTTP_200_OK,
)
def get_by_email(
    email: str,
    db: Session = db_session,
):
    """
    Get a single person by email (exact match).
    Returns 200 with the person if found; 204 (no content) if not found.
    """
    person = person_crud.get_by_email(db, email)
    if not person:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return person


@router.get(
    "/by/name",
    response_model=List[PersonSchemaShow],
    status_code=status.HTTP_200_OK,
)
def get_by_name(
    name: str,
    db: Session = db_session,
):
    """
    Find people by name. Returns a (possibly empty) list.
    Implement the matching strategy (exact/ILIKE) inside person_crud.
    """
    return person_crud.find_by_name(db, name)
