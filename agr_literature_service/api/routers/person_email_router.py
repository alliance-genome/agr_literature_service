from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends, Response, Security, status

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import person_email_crud, person_crud
from agr_literature_service.api.schemas import (
    PersonEmailSchemaCreate,
    PersonEmailSchemaShow,
    PersonEmailSchemaRelated,
    PersonEmailSchemaUpdate,
)
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user

router = APIRouter(prefix="/person_email", tags=["PersonEmail"])

get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post(
    "/person/{curie_or_person_id}",
    status_code=status.HTTP_201_CREATED,
    response_model=PersonEmailSchemaShow,
)
def create_for_person(
    curie_or_person_id: str,
    request: PersonEmailSchemaCreate,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    person_id = person_crud.resolve_person_id(db, curie_or_person_id)
    return person_email_crud.create_for_person(db, person_id, request)


@router.get(
    "/person/{curie_or_person_id}",
    status_code=status.HTTP_200_OK,
    response_model=List[PersonEmailSchemaRelated],
)
def list_for_person(
    curie_or_person_id: str,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    person_id = person_crud.resolve_person_id(db, curie_or_person_id)
    return person_email_crud.list_for_person(db, person_id)


@router.get(
    "/{person_email_id}",
    status_code=status.HTTP_200_OK,
    response_model=PersonEmailSchemaShow,
)
def show(
    person_email_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    return person_email_crud.show(db, person_email_id)


@router.patch(
    "/{person_email_id}",
    status_code=status.HTTP_200_OK,
    response_model=PersonEmailSchemaShow,
)
def patch(
    person_email_id: int,
    request: PersonEmailSchemaUpdate,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    patch_data = request.model_dump(exclude_unset=True)
    person_email_crud.patch(db, person_email_id, patch_data)
    return person_email_crud.show(db, person_email_id)


@router.delete("/{person_email_id}", status_code=status.HTTP_204_NO_CONTENT)
def destroy(
    person_email_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    person_email_crud.destroy(db, person_email_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
