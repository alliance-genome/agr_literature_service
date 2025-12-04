from fastapi import APIRouter, Depends, Response, Security, status
from typing import Dict, Any

from sqlalchemy.orm import Session
from pydantic import BaseModel

from agr_literature_service.api import database
from agr_literature_service.api.crud import manual_indexing_tag_crud
from agr_literature_service.api.schemas.manual_indexing_tag_schemas import (
    ManualIndexingTagSchemaShow,
    ManualIndexingTagSchemaUpdate,
    ManualIndexingTagSchemaPost,
)
from agr_literature_service.api.user import set_global_user_from_cognito

from agr_cognito_py import get_cognito_user_swagger

router = APIRouter(
    prefix="/manual_indexing_tag",
    tags=["Manual Indexing Tag"],
)

get_db = database.get_db
db_session: Session = Depends(get_db)


class SetManualIndexingTagBody(BaseModel):
    reference_curie: str
    mod_abbreviation: str
    curation_tag: str
    confidence_score: float


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    response_model=int,
)
def create(
    request: ManualIndexingTagSchemaPost,
    user: Dict[str, Any] = Security(get_cognito_user_swagger),
    db: Session = db_session,
) -> int:
    set_global_user_from_cognito(db, user)
    new_id = manual_indexing_tag_crud.create(db, request)
    return new_id


@router.delete(
    "/{manual_indexing_tag_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
def destroy(
    manual_indexing_tag_id: int,
    user: Dict[str, Any] = Security(get_cognito_user_swagger),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    manual_indexing_tag_crud.destroy(db, manual_indexing_tag_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch(
    "/{manual_indexing_tag_id}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=int,
)
async def patch(
    manual_indexing_tag_id: int,
    request: ManualIndexingTagSchemaUpdate,
    user: Dict[str, Any] = Security(get_cognito_user_swagger),
    db: Session = db_session,
) -> int:
    set_global_user_from_cognito(db, user)
    updates = request.model_dump(exclude_unset=True)
    manual_indexing_tag_crud.patch(db, manual_indexing_tag_id, updates)
    return manual_indexing_tag_id


@router.get(
    "/{manual_indexing_tag_id}",
    response_model=ManualIndexingTagSchemaShow,
    status_code=status.HTTP_200_OK,
)
def show(
    manual_indexing_tag_id: int,
    db: Session = db_session,
) -> ManualIndexingTagSchemaShow:
    data = manual_indexing_tag_crud.show(db, manual_indexing_tag_id)
    return ManualIndexingTagSchemaShow.model_validate(data)


@router.get(
    "/get_manual_indexing_tag/{reference_curie}/{mod_abbreviation}",
    status_code=status.HTTP_200_OK,
)
def get_manual_indexing_tag(
    reference_curie: str,
    mod_abbreviation: str,
    db: Session = db_session,
):
    return manual_indexing_tag_crud.get_manual_indexing_tag(
        db, reference_curie, mod_abbreviation
    )


@router.post(
    "/set_manual_indexing_tag",
    status_code=status.HTTP_200_OK,
)
def set_manual_indexing_tag(
    body: SetManualIndexingTagBody,
    user: Dict[str, Any] = Security(get_cognito_user_swagger),
    db: Session = db_session,
):
    set_global_user_from_cognito(db, user)
    return manual_indexing_tag_crud.set_manual_indexing_tag(
        db,
        body.reference_curie,
        body.mod_abbreviation,
        body.curation_tag,
        body.confidence_score,
    )
