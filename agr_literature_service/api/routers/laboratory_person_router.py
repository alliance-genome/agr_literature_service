from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends, Response, Security, status

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import laboratory_crud, laboratory_person_crud, person_crud
from agr_literature_service.api.schemas import (
    LaboratoryPersonSchemaPost,
    LaboratoryPersonSchemaUpdate,
    LaboratoryPersonSchemaShow,
    LaboratoryPersonSchemaRelated,
)
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user

router = APIRouter(prefix="/laboratory_person", tags=["Laboratory"])

get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    response_model=LaboratoryPersonSchemaShow,
)
def create(
    request: LaboratoryPersonSchemaPost,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    """Link a person to a laboratory; both are named by curie (or id) in the body."""
    set_global_user_from_cognito(db, user)
    laboratory_id = laboratory_crud.resolve_laboratory_id(db, request.laboratory_curie)
    person_id = person_crud.resolve_person_id(db, request.person_curie)
    data = request.model_dump(exclude={"laboratory_curie", "person_curie"})
    data["person_id"] = person_id
    return laboratory_person_crud.create_for_laboratory(db, laboratory_id, data)


@router.get(
    "/laboratory/{curie_or_laboratory_id}",
    status_code=status.HTTP_200_OK,
    response_model=List[LaboratoryPersonSchemaRelated],
)
def list_for_laboratory(
    curie_or_laboratory_id: str,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    laboratory_id = laboratory_crud.resolve_laboratory_id(db, curie_or_laboratory_id)
    return laboratory_person_crud.list_for_laboratory(db, laboratory_id)


@router.get(
    "/person/{curie_or_person_id}",
    status_code=status.HTTP_200_OK,
    response_model=List[LaboratoryPersonSchemaRelated],
)
def list_for_person(
    curie_or_person_id: str,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    person_id = person_crud.resolve_person_id(db, curie_or_person_id)
    return laboratory_person_crud.list_for_person(db, person_id)


@router.get(
    "/{laboratory_person_id}",
    status_code=status.HTTP_200_OK,
    response_model=LaboratoryPersonSchemaShow,
)
def show(
    laboratory_person_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    return laboratory_person_crud.show(db, laboratory_person_id)


@router.patch(
    "/{laboratory_person_id}",
    status_code=status.HTTP_200_OK,
    response_model=LaboratoryPersonSchemaShow,
)
def patch(
    laboratory_person_id: int,
    request: LaboratoryPersonSchemaUpdate,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    patch_data = request.model_dump(exclude_unset=True)
    laboratory_person_crud.patch(db, laboratory_person_id, patch_data)
    return laboratory_person_crud.show(db, laboratory_person_id)


@router.delete(
    "/{laboratory_person_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def destroy(
    laboratory_person_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    laboratory_person_crud.destroy(db, laboratory_person_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
