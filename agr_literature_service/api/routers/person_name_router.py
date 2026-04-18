from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends, Response, Security, status

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import person_name_crud
from agr_literature_service.api.schemas import (
    PersonNameSchemaCreate,
    PersonNameSchemaShow,
    PersonNameSchemaRelated,
    PersonNameSchemaUpdate,
    ResponseMessageSchema,
)
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user

router = APIRouter(prefix="/person_name", tags=["Person Name"])

get_db = database.get_db
db_session: Session = Depends(get_db)


# Create a name for a person (person_id from path)
@router.post(
    "/person/{person_id}",
    status_code=status.HTTP_201_CREATED,
    response_model=PersonNameSchemaShow,
)
def create_for_person(
    person_id: int,
    request: PersonNameSchemaCreate,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    return person_name_crud.create_for_person(db, person_id, request)


# List all names for a person
@router.get(
    "/person/{person_id}",
    status_code=status.HTTP_200_OK,
    response_model=List[PersonNameSchemaRelated],
)
def list_for_person(
    person_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    return person_name_crud.list_for_person(db, person_id)


# Get one name by ID
@router.get(
    "/{person_name_id}",
    status_code=status.HTTP_200_OK,
    response_model=PersonNameSchemaShow,
)
def show(
    person_name_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    return person_name_crud.show(db, person_name_id)


# Patch one name by ID
@router.patch(
    "/{person_name_id}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ResponseMessageSchema,
)
def patch(
    person_name_id: int,
    request: PersonNameSchemaUpdate,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    patch_data = request.model_dump(exclude_unset=True)
    return person_name_crud.patch(db, person_name_id, patch_data)


# Delete one name by ID
@router.delete("/{person_name_id}", status_code=status.HTTP_204_NO_CONTENT)
def destroy(
    person_name_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    person_name_crud.destroy(db, person_name_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
