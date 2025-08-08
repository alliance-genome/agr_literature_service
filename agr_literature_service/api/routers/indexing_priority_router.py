from fastapi import APIRouter, Depends, Response, Security, status
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import indexing_priority_crud
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.schemas import (
    IndexingPrioritySchemaShow,
    IndexingPrioritySchemaUpdate,
    IndexingPrioritySchemaPost,
)
from agr_literature_service.api.user import set_global_user_from_okta

router = APIRouter(
    prefix="/indexing_priority",
    tags=["Indexing Priority"],
)

get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    response_model=int,
)
def create(
    request: IndexingPrioritySchemaPost,
    user: OktaUser = db_user,
    db: Session = db_session,
) -> int:
    set_global_user_from_okta(db, user)
    new_id = indexing_priority_crud.create(db, request)
    return new_id


@router.delete(
    "/{indexing_priority_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def destroy(
    priority_id: int,
    user: OktaUser = db_user,
    db: Session = db_session,
):
    set_global_user_from_okta(db, user)
    indexing_priority_crud.destroy(db, priority_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch(
    "/{indexing_priority_id}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=int,
)
async def patch(
    indexing_priority_id: int,
    request: IndexingPrioritySchemaUpdate,
    user: OktaUser = db_user,
    db: Session = db_session,
) -> int:
    set_global_user_from_okta(db, user)
    updates = request.dict(exclude_unset=True)
    # perform the update (this should return the same ID)
    indexing_priority_crud.patch(db, indexing_priority_id, updates)
    # return the integer id so FastAPI can validate it
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
    return indexing_priority_crud.show(db, indexing_priority_id)


@router.post(
    "/set_priority/{reference_curie}/{mod_abbreviation}/{priority}/{confidence_score}",
    status_code=status.HTTP_200_OK,
)
def set_priority(
    reference_curie: str,
    mod_abbreviation: str,
    priority: str,
    confidence_score: float,
    db: Session = db_session,
):
    return indexing_priority_crud.set_priority(db, reference_curie, mod_abbreviation,
                                               priority, confidence_score)
