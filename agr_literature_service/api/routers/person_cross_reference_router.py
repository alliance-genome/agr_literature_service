from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends, Response, Security, status

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import person_cross_reference_crud
from agr_literature_service.api.schemas import (
    PersonCrossReferenceSchemaCreate,
    PersonCrossReferenceSchemaShow,
    PersonCrossReferenceSchemaRelated,
    ResponseMessageSchema,
)
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user

router = APIRouter(prefix="/person_cross_reference", tags=["Person CrossReference"])

get_db = database.get_db
db_session: Session = Depends(get_db)


# Create a cross-reference for a person (person_id from path)
@router.post(
    "/person/{person_id}",
    status_code=status.HTTP_201_CREATED,
    response_model=PersonCrossReferenceSchemaShow,
)
def create_for_person(
    person_id: int,
    request: PersonCrossReferenceSchemaCreate,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    return person_cross_reference_crud.create_for_person(db, person_id, request)


# List cross-references for a person
@router.get(
    "/person/{person_id}",
    status_code=status.HTTP_200_OK,
    response_model=List[PersonCrossReferenceSchemaRelated],
)
def list_for_person(
    person_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    return person_cross_reference_crud.list_for_person(db, person_id)


# Get one cross-reference by ID
@router.get(
    "/{person_cross_reference_id}",
    status_code=status.HTTP_200_OK,
    response_model=PersonCrossReferenceSchemaShow,
)
def show(
    person_cross_reference_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    return person_cross_reference_crud.show(db, person_cross_reference_id)


# Patch one cross-reference by ID
@router.patch(
    "/{person_cross_reference_id}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ResponseMessageSchema,
)
def patch(
    person_cross_reference_id: int,
    request: PersonCrossReferenceSchemaCreate,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    patch_data = request.model_dump(exclude_unset=True)
    return person_cross_reference_crud.patch(db, person_cross_reference_id, patch_data)


# Delete one cross-reference by ID
@router.delete(
    "/{person_cross_reference_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def destroy(
    person_cross_reference_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    person_cross_reference_crud.destroy(db, person_cross_reference_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
