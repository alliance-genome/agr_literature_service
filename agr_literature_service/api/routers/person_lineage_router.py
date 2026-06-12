from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends, Response, Security, status

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import person_lineage_crud, person_crud
from agr_literature_service.api.schemas import (
    PersonLineageSchemaCreate,
    PersonLineageSchemaUpdate,
    PersonLineageSchemaShow,
)
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user
from agr_literature_service.api.util.resource_urls import person_lineage_url

router = APIRouter(prefix="/person_lineage", tags=["PersonLineage"])

get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post("/", status_code=status.HTTP_201_CREATED, response_model=PersonLineageSchemaShow)
def create(
    request: PersonLineageSchemaCreate,
    response: Response,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    """Create a validated (canonical) person-to-person relationship."""
    set_global_user_from_cognito(db, user)
    lineage = person_lineage_crud.create(db, request.model_dump())
    response.headers["Location"] = person_lineage_url(lineage.person_lineage_id)
    return lineage


# Canonical PPRs for a person, on either side. Declared before the /{id} catch-all.
@router.get(
    "/person/{curie_or_person_id}",
    status_code=status.HTTP_200_OK,
    response_model=List[PersonLineageSchemaShow],
)
def list_for_person(
    curie_or_person_id: str,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    person_id = person_crud.resolve_person_id(db, curie_or_person_id)
    return person_lineage_crud.list_for_person(db, person_id)


@router.delete("/{person_lineage_id}", status_code=status.HTTP_204_NO_CONTENT)
def destroy(
    person_lineage_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    person_lineage_crud.destroy(db, person_lineage_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch(
    "/{person_lineage_id}",
    status_code=status.HTTP_200_OK,
    response_model=PersonLineageSchemaShow,
)
def patch(
    person_lineage_id: int,
    request: PersonLineageSchemaUpdate,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    patch_data = request.model_dump(exclude_unset=True)
    person_lineage_crud.patch(db, person_lineage_id, patch_data)
    return person_lineage_crud.show(db, person_lineage_id)


@router.get(
    "/{person_lineage_id}",
    response_model=PersonLineageSchemaShow,
    status_code=status.HTTP_200_OK,
)
def show(
    person_lineage_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    return person_lineage_crud.show(db, person_lineage_id)
