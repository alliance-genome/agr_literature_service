from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends, Response, Security, status

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import person_lineage_submission_crud, person_crud
from agr_literature_service.api.schemas import (
    PersonLineageSubmissionSchemaCreate,
    PersonLineageSubmissionSchemaUpdate,
    PersonLineageSubmissionSchemaShow,
)
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user
from agr_literature_service.api.util.resource_urls import person_lineage_submission_url

router = APIRouter(prefix="/person_lineage_submission", tags=["PersonLineageSubmission"])

get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post("/", status_code=status.HTTP_201_CREATED, response_model=PersonLineageSubmissionSchemaShow)
def create(
    request: PersonLineageSubmissionSchemaCreate,
    response: Response,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    """Submit a person-to-person relationship claim."""
    set_global_user_from_cognito(db, user)
    submission = person_lineage_submission_crud.create(db, request.model_dump())
    response.headers["Location"] = person_lineage_submission_url(submission.person_lineage_submission_id)
    return submission


@router.get(
    "/",
    status_code=status.HTTP_200_OK,
    response_model=List[PersonLineageSubmissionSchemaShow],
)
def list_all(
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    return person_lineage_submission_crud.list_all(db)


# Promote/validate a resolved submission to a canonical person_lineage.
# Declared before the catch-all /{id} routes.
@router.post(
    "/{person_lineage_submission_id}/validate",
    status_code=status.HTTP_200_OK,
    response_model=PersonLineageSubmissionSchemaShow,
)
def validate(
    person_lineage_submission_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    return person_lineage_submission_crud.validate(db, person_lineage_submission_id)


# Submissions in which a person is resolved, on either side.
# Declared before the catch-all /{id} routes.
@router.get(
    "/person/{curie_or_person_id}",
    status_code=status.HTTP_200_OK,
    response_model=List[PersonLineageSubmissionSchemaShow],
)
def list_for_person(
    curie_or_person_id: str,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    person_id = person_crud.resolve_person_id(db, curie_or_person_id)
    return person_lineage_submission_crud.list_for_person(db, person_id)


@router.delete("/{person_lineage_submission_id}", status_code=status.HTTP_204_NO_CONTENT)
def destroy(
    person_lineage_submission_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    person_lineage_submission_crud.destroy(db, person_lineage_submission_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch(
    "/{person_lineage_submission_id}",
    status_code=status.HTTP_200_OK,
    response_model=PersonLineageSubmissionSchemaShow,
)
def patch(
    person_lineage_submission_id: int,
    request: PersonLineageSubmissionSchemaUpdate,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    patch_data = request.model_dump(exclude_unset=True)
    person_lineage_submission_crud.patch(db, person_lineage_submission_id, patch_data)
    return person_lineage_submission_crud.show(db, person_lineage_submission_id)


@router.get(
    "/{person_lineage_submission_id}",
    response_model=PersonLineageSubmissionSchemaShow,
    status_code=status.HTTP_200_OK,
)
def show(
    person_lineage_submission_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    return person_lineage_submission_crud.show(db, person_lineage_submission_id)
