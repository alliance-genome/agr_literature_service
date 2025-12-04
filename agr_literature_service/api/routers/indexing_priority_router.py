from fastapi import APIRouter, Depends, Response, Security, status

from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional, Dict, Any

from agr_literature_service.api import database
from agr_literature_service.api.crud import indexing_priority_crud
from agr_literature_service.api.schemas.indexing_priority_schemas import (
    IndexingPrioritySchemaShow,
    IndexingPrioritySchemaUpdate,
    IndexingPrioritySchemaPost,
)
from agr_literature_service.api.user import set_global_user_from_cognito

from agr_cognito_py import get_cognito_user_swagger

router = APIRouter(
    prefix="/indexing_priority",
    tags=["Indexing Priority"],
)

get_db = database.get_db
db_session: Session = Depends(get_db)


class SetPriorityBody(BaseModel):
    reference_curie: str
    mod_abbreviation: str
    indexing_priority: str
    confidence_score: float


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    response_model=int,
)
def create(
    request: IndexingPrioritySchemaPost,
    user: Dict[str, Any] = Security(get_cognito_user_swagger),
    db: Session = db_session,
) -> int:
    set_global_user_from_cognito(db, user)
    new_id = indexing_priority_crud.create(db, request)
    return new_id


@router.delete(
    "/{indexing_priority_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
def destroy(
    indexing_priority_id: int,
    user: Dict[str, Any] = Security(get_cognito_user_swagger),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    indexing_priority_crud.destroy(db, indexing_priority_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch(
    "/{indexing_priority_id}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=int,
)
async def patch(
    indexing_priority_id: int,
    request: IndexingPrioritySchemaUpdate,
    user: Dict[str, Any] = Security(get_cognito_user_swagger),
    db: Session = db_session,
) -> int:
    set_global_user_from_cognito(db, user)
    updates = request.model_dump(exclude_unset=True)
    indexing_priority_crud.patch(db, indexing_priority_id, updates)
    return indexing_priority_id


@router.get(
    "/{indexing_priority_id}",
    response_model=IndexingPrioritySchemaShow,
    status_code=status.HTTP_200_OK,
)
def show(
    indexing_priority_id: int,
    db: Session = db_session,
) -> IndexingPrioritySchemaShow:
    data = indexing_priority_crud.show(db, indexing_priority_id)
    return IndexingPrioritySchemaShow.model_validate(data)


@router.get(
    "/get_priority_tag/{reference_curie}/{mod_abbreviation}",
    status_code=status.HTTP_200_OK,
)
def get_indexing_priority_tag(
    reference_curie: str,
    mod_abbreviation: Optional[str] = None,
    db: Session = db_session,
):
    if mod_abbreviation != 'ZFIN':
        return {}
    return indexing_priority_crud.get_indexing_priority_tag(
        db, reference_curie
    )


@router.post(
    "/set_priority",
    status_code=status.HTTP_200_OK,
)
def set_priority(
    body: SetPriorityBody,
    user: Dict[str, Any] = Security(get_cognito_user_swagger),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    return indexing_priority_crud.set_priority(
        db,
        body.reference_curie,
        body.mod_abbreviation,
        body.indexing_priority,
        body.confidence_score,
    )
