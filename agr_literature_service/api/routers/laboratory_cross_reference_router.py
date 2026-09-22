from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends, Response, Security, status

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import laboratory_crud, laboratory_cross_reference_crud
from agr_literature_service.api.crud.utils import patterns_check
from agr_literature_service.api.schemas import (
    LaboratoryCrossReferenceSchemaPost,
    LaboratoryCrossReferenceSchemaUpdate,
    LaboratoryCrossReferenceSchemaShow,
    LaboratoryCrossReferenceSchemaRelated,
)
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user

router = APIRouter(prefix="/laboratory_cross_reference", tags=["Laboratory"])

get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    response_model=LaboratoryCrossReferenceSchemaShow,
)
def create(
    request: LaboratoryCrossReferenceSchemaPost,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    """Create a cross-reference; the owning laboratory is named by curie (or id) in the body."""
    set_global_user_from_cognito(db, user)
    laboratory_id = laboratory_crud.resolve_laboratory_id(db, request.laboratory_curie)
    return laboratory_cross_reference_crud.create_for_laboratory(db, laboratory_id, request)


@router.get(
    "/laboratory/{curie_or_laboratory_id}",
    status_code=status.HTTP_200_OK,
    response_model=List[LaboratoryCrossReferenceSchemaRelated],
)
def list_for_laboratory(
    curie_or_laboratory_id: str,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    laboratory_id = laboratory_crud.resolve_laboratory_id(db, curie_or_laboratory_id)
    return laboratory_cross_reference_crud.list_for_laboratory(db, laboratory_id)


# Laboratory xref curie-pattern checks. These live on the Laboratory router (not
# the generic /cross_reference/check/{datatype} route) so they group under the
# "Laboratory" Swagger tag and are discoverable next to the other laboratory-xref
# endpoints -- recommended over sharing the cross_reference route, whose tag would
# bury them. They reuse the same patterns_check yml mechanism (laboratory.yml) that
# backs the reference/resource checks. Declared before /{...id} (int) to be safe.
@router.get("/check/patterns", status_code=status.HTTP_200_OK)
def show_patterns(
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
):
    return patterns_check.get_patterns()["laboratory"]


@router.get("/check/curie/{curie:path}", status_code=status.HTTP_200_OK)
def check_curie(
    curie: str,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
):
    ret = patterns_check.check_pattern("laboratory", curie)
    if ret is None:
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    return ret


@router.get(
    "/{laboratory_cross_reference_id}",
    status_code=status.HTTP_200_OK,
    response_model=LaboratoryCrossReferenceSchemaShow,
)
def show(
    laboratory_cross_reference_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    return laboratory_cross_reference_crud.show(db, laboratory_cross_reference_id)


@router.patch(
    "/{laboratory_cross_reference_id}",
    status_code=status.HTTP_200_OK,
    response_model=LaboratoryCrossReferenceSchemaShow,
)
def patch(
    laboratory_cross_reference_id: int,
    request: LaboratoryCrossReferenceSchemaUpdate,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    patch_data = request.model_dump(exclude_unset=True)
    laboratory_cross_reference_crud.patch(db, laboratory_cross_reference_id, patch_data)
    return laboratory_cross_reference_crud.show(db, laboratory_cross_reference_id)


@router.delete(
    "/{laboratory_cross_reference_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def destroy(
    laboratory_cross_reference_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    laboratory_cross_reference_crud.destroy(db, laboratory_cross_reference_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
