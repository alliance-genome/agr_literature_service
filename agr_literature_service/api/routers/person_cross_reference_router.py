from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends, Response, Security, status

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import person_crud, person_cross_reference_crud
from agr_literature_service.api.crud.utils import patterns_check
from agr_literature_service.api.schemas import (
    PersonCrossReferenceSchemaPost,
    PersonCrossReferenceSchemaShow,
    PersonCrossReferenceSchemaRelated,
    PersonCrossReferenceSchemaUpdate,
)
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user

router = APIRouter(prefix="/person_cross_reference", tags=["Person"])

get_db = database.get_db
db_session: Session = Depends(get_db)


# Create a cross-reference; the owning person is named by curie (or id) in the body.
@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    response_model=PersonCrossReferenceSchemaShow,
)
def create(
    request: PersonCrossReferenceSchemaPost,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    person_id = person_crud.resolve_person_id(db, request.person_curie)
    return person_cross_reference_crud.create_for_person(db, person_id, request)


# List cross-references for a person
@router.get(
    "/person/{curie_or_person_id}",
    status_code=status.HTTP_200_OK,
    response_model=List[PersonCrossReferenceSchemaRelated],
)
def list_for_person(
    curie_or_person_id: str,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    person_id = person_crud.resolve_person_id(db, curie_or_person_id)
    return person_cross_reference_crud.list_for_person(db, person_id)


# Person xref curie-pattern checks. These live on the Person router (not the
# generic /cross_reference/check/{datatype} route) so they group under the
# "Person" Swagger tag and are discoverable next to the other person-xref
# endpoints -- recommended over sharing the cross_reference route, whose tag would
# bury them. They reuse the same patterns_check yml mechanism (person.yml) that
# backs the reference/resource checks. Declared before /{...id} (int) to be safe.
@router.get("/check/patterns", status_code=status.HTTP_200_OK)
def show_patterns(
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
):
    return patterns_check.get_patterns()["person"]


@router.get("/check/curie/{curie:path}", status_code=status.HTTP_200_OK)
def check_curie(
    curie: str,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
):
    ret = patterns_check.check_pattern("person", curie)
    if ret is None:
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    return ret


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
    return person_cross_reference_crud.show(db, person_cross_reference_id)


# Patch one cross-reference by ID
@router.patch(
    "/{person_cross_reference_id}",
    status_code=status.HTTP_200_OK,
    response_model=PersonCrossReferenceSchemaShow,
)
def patch(
    person_cross_reference_id: int,
    request: PersonCrossReferenceSchemaUpdate,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    patch_data = request.model_dump(exclude_unset=True)
    person_cross_reference_crud.patch(db, person_cross_reference_id, patch_data)
    return person_cross_reference_crud.show(db, person_cross_reference_id)


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
