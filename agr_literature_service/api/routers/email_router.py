from typing import List

from fastapi import APIRouter, Depends, Response, Security, status
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import email_crud
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas import (
    EmailSchemaCreate,
    EmailSchemaShow,
    EmailSchemaRelated,
    EmailSchemaUpdate,
    ResponseMessageSchema,
)
from agr_literature_service.api.user import set_global_user_from_okta

router = APIRouter(prefix="/email", tags=["Email"])

get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


# Create an email for a person (person_id from path)
@router.post(
    "/person/{person_id}",
    status_code=status.HTTP_201_CREATED,
    response_model=EmailSchemaShow,
)
def create_for_person(
    person_id: int,
    request: EmailSchemaCreate,
    user: OktaUser = db_user,
    db: Session = db_session,
):
    set_global_user_from_okta(db, user)
    return email_crud.create_for_person(db, person_id, request)


# List all emails for a person
@router.get(
    "/person/{person_id}",
    status_code=status.HTTP_200_OK,
    response_model=List[EmailSchemaRelated],
)
def list_for_person(
    person_id: int,
    db: Session = db_session,
):
    return email_crud.list_for_person(db, person_id)


# Get one email by ID
@router.get(
    "/{email_id}",
    status_code=status.HTTP_200_OK,
    response_model=EmailSchemaShow,
)
def show(
    email_id: int,
    db: Session = db_session,
):
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
    user: OktaUser = db_user,
    db: Session = db_session,
):
    set_global_user_from_okta(db, user)
    patch_data = request.model_dump(exclude_unset=True)
    return email_crud.patch(db, email_id, patch_data)


# Delete one email by ID
@router.delete("/{email_id}", status_code=status.HTTP_204_NO_CONTENT)
def destroy(
    email_id: int,
    user: OktaUser = db_user,
    db: Session = db_session,
):
    set_global_user_from_okta(db, user)
    email_crud.destroy(db, email_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
