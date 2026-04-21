from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends, Response, Security, status

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import person_note_crud
from agr_literature_service.api.schemas import (
    PersonNoteSchemaCreate,
    PersonNoteSchemaShow,
    PersonNoteSchemaRelated,
    PersonNoteSchemaUpdate,
    ResponseMessageSchema,
)
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user

router = APIRouter(prefix="/person_note", tags=["Person Note"])

get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post(
    "/person/{person_id}",
    status_code=status.HTTP_201_CREATED,
    response_model=PersonNoteSchemaShow,
)
def create_for_person(
    person_id: int,
    request: PersonNoteSchemaCreate,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    return person_note_crud.create_for_person(db, person_id, request)


@router.get(
    "/person/{person_id}",
    status_code=status.HTTP_200_OK,
    response_model=List[PersonNoteSchemaRelated],
)
def list_for_person(
    person_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    return person_note_crud.list_for_person(db, person_id)


@router.get(
    "/{person_note_id}",
    status_code=status.HTTP_200_OK,
    response_model=PersonNoteSchemaShow,
)
def show(
    person_note_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    return person_note_crud.show(db, person_note_id)


@router.patch(
    "/{person_note_id}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ResponseMessageSchema,
)
def patch(
    person_note_id: int,
    request: PersonNoteSchemaUpdate,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    patch_data = request.model_dump(exclude_unset=True)
    return person_note_crud.patch(db, person_note_id, patch_data)


@router.delete("/{person_note_id}", status_code=status.HTTP_204_NO_CONTENT)
def destroy(
    person_note_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    person_note_crud.destroy(db, person_note_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
