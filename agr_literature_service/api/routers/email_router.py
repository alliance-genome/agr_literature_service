from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends, Response, Security, status

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import email_crud
from agr_literature_service.api.schemas import (
    EmailSchemaCreate,
    EmailSchemaShow,
    EmailSchemaRelated,
    EmailSchemaUpdate,
    ResponseMessageSchema,
)
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user

router = APIRouter(prefix="/email", tags=["Email"])

get_db = database.get_db
db_session: Session = Depends(get_db)


# Create an email for a person (person_id from path)
@router.post(
    "/person/{person_id}",
    status_code=status.HTTP_201_CREATED,
    response_model=EmailSchemaShow,
)
def create_for_person(
    person_id: int,
    request: EmailSchemaCreate,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    return email_crud.create_for_person(db, person_id, request)


# List all emails for a person
@router.get(
    "/person/{person_id}",
    status_code=status.HTTP_200_OK,
    response_model=List[EmailSchemaRelated],
)
def list_for_person(
    person_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    return email_crud.list_for_person(db, person_id)


# Get one email by ID
@router.get(
    "/{email_id}",
    status_code=status.HTTP_200_OK,
    response_model=EmailSchemaShow,
)
def show(
    email_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    return email_crud.show(db, email_id)


# Patch one email by ID
@router.patch(
    "/{email_id}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ResponseMessageSchema,
)
def patch(
    email_id: int,
    request: EmailSchemaUpdate,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    patch_data = request.model_dump(exclude_unset=True)
    return email_crud.patch(db, email_id, patch_data)


# Delete one email by ID
@router.delete("/{email_id}", status_code=status.HTTP_204_NO_CONTENT)
def destroy(
    email_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    email_crud.destroy(db, email_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
