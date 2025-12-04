from fastapi import APIRouter, Depends, Response, Security, status
from typing import Dict, Any

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import reference_relation_crud
from agr_literature_service.api.schemas import (
    ReferenceRelationSchemaPost,
    ReferenceRelationSchemaShow,
    ReferenceRelationSchemaPatch,
)
from agr_literature_service.api.user import set_global_user_from_cognito

from agr_cognito_py import get_cognito_user_swagger

router = APIRouter(
    prefix="/reference_relation",
    tags=["Reference Relation"],
)

get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    response_model=int,
)
def create(
    request: ReferenceRelationSchemaPost,
    user: Dict[str, Any] = Security(get_cognito_user_swagger),
    db: Session = db_session,
) -> int:
    set_global_user_from_cognito(db, user)
    new_id = reference_relation_crud.create(db, request)
    return new_id


@router.delete(
    "/{reference_relation_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def destroy(
    reference_relation_id: int,
    user: Dict[str, Any] = Security(get_cognito_user_swagger),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    reference_relation_crud.destroy(db, reference_relation_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch(
    "/{reference_relation_id}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=int,
)
def patch(
    reference_relation_id: int,
    request: ReferenceRelationSchemaPatch,
    user: Dict[str, Any] = Security(get_cognito_user_swagger),
    db: Session = db_session,
) -> int:
    set_global_user_from_cognito(db, user)
    update_data = request.model_dump(exclude_unset=True)
    return reference_relation_crud.patch(db, reference_relation_id, update_data)


@router.get(
    "/{reference_relation_id}",
    response_model=ReferenceRelationSchemaShow,
    status_code=status.HTTP_200_OK,
)
def show(
    reference_relation_id: int,
    db: Session = db_session,
) -> ReferenceRelationSchemaShow:
    return reference_relation_crud.show(db, reference_relation_id)


@router.get(
    "/{reference_relation_id}/versions",
    status_code=status.HTTP_200_OK,
)
def show_versions(
    reference_relation_id: int,
    db: Session = db_session,
):
    return reference_relation_crud.show_changesets(db, reference_relation_id)
