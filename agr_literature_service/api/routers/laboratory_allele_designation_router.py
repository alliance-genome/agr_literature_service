from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends, Response, Security, status

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import laboratory_crud, laboratory_allele_designation_crud
from agr_literature_service.api.schemas import (
    LaboratoryAlleleDesignationSchemaPost,
    LaboratoryAlleleDesignationSchemaUpdate,
    LaboratoryAlleleDesignationSchemaShow,
    LaboratoryAlleleDesignationSchemaRelated,
)
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user

router = APIRouter(prefix="/laboratory_allele_designation", tags=["Laboratory"])

get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    response_model=LaboratoryAlleleDesignationSchemaShow,
)
def create(
    request: LaboratoryAlleleDesignationSchemaPost,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    """Create an allele designation; the owning laboratory is named by curie (or id) in the body."""
    set_global_user_from_cognito(db, user)
    laboratory_id = laboratory_crud.resolve_laboratory_id(db, request.laboratory_curie)
    return laboratory_allele_designation_crud.create_for_laboratory(db, laboratory_id, request)


@router.get(
    "/laboratory/{curie_or_laboratory_id}",
    status_code=status.HTTP_200_OK,
    response_model=List[LaboratoryAlleleDesignationSchemaRelated],
)
def list_for_laboratory(
    curie_or_laboratory_id: str,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    laboratory_id = laboratory_crud.resolve_laboratory_id(db, curie_or_laboratory_id)
    return laboratory_allele_designation_crud.list_for_laboratory(db, laboratory_id)


@router.get(
    "/{laboratory_allele_designation_id}",
    status_code=status.HTTP_200_OK,
    response_model=LaboratoryAlleleDesignationSchemaShow,
)
def show(
    laboratory_allele_designation_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    return laboratory_allele_designation_crud.show(db, laboratory_allele_designation_id)


@router.patch(
    "/{laboratory_allele_designation_id}",
    status_code=status.HTTP_200_OK,
    response_model=LaboratoryAlleleDesignationSchemaShow,
)
def patch(
    laboratory_allele_designation_id: int,
    request: LaboratoryAlleleDesignationSchemaUpdate,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    patch_data = request.model_dump(exclude_unset=True)
    laboratory_allele_designation_crud.patch(db, laboratory_allele_designation_id, patch_data)
    return laboratory_allele_designation_crud.show(db, laboratory_allele_designation_id)


@router.delete(
    "/{laboratory_allele_designation_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def destroy(
    laboratory_allele_designation_id: int,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    laboratory_allele_designation_crud.destroy(db, laboratory_allele_designation_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
