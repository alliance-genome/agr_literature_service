from typing import Optional, Dict, Any

from fastapi import APIRouter, Depends, Response, Security, status

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import laboratory_crud
from agr_literature_service.api.schemas import (
    LaboratorySchemaCreate,
    LaboratorySchemaUpdate,
    LaboratorySchemaShow,
)
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user
from agr_literature_service.api.util.resource_urls import laboratory_url

router = APIRouter(prefix="/laboratory", tags=["Laboratory"])

get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post("/", status_code=status.HTTP_201_CREATED, response_model=LaboratorySchemaShow)
def create(
    request: LaboratorySchemaCreate,
    response: Response,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    """Create a laboratory."""
    set_global_user_from_cognito(db, user)
    laboratory = laboratory_crud.create(db, request)
    response.headers["Location"] = laboratory_url(laboratory.curie)
    return laboratory


@router.delete("/{curie_or_laboratory_id}", status_code=status.HTTP_204_NO_CONTENT)
def destroy(
    curie_or_laboratory_id: str,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    """Delete a laboratory by curie or internal ID."""
    set_global_user_from_cognito(db, user)
    laboratory_crud.destroy(db, curie_or_laboratory_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch(
    "/{curie_or_laboratory_id}",
    status_code=status.HTTP_200_OK,
    response_model=LaboratorySchemaShow,
)
def patch(
    curie_or_laboratory_id: str,
    request: LaboratorySchemaUpdate,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    patch_data = request.model_dump(exclude_unset=True)
    laboratory_crud.patch(db, curie_or_laboratory_id, patch_data)
    return laboratory_crud.show(db, curie_or_laboratory_id)


@router.get(
    "/{curie_or_laboratory_id}",
    response_model=LaboratorySchemaShow,
    status_code=status.HTTP_200_OK,
)
def show(
    curie_or_laboratory_id: str,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    """Get a laboratory by curie or internal ID."""
    return laboratory_crud.show(db, curie_or_laboratory_id)
