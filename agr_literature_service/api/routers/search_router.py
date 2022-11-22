from typing import List

from fastapi import APIRouter, Security, Depends

from agr_literature_service.api import database
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.crud import search_crud
from sqlalchemy.orm import Session
from agr_literature_service.api.schemas import ReferenceSchemaNeedReviewShow, FacetsOptionsSchema


router = APIRouter(
    prefix="/search",
    tags=["Search"])


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.post("/references/",
             status_code=200)
def search(body: FacetsOptionsSchema):
    return search_crud.search_references(query=body.query, facets_values=body.facets_values,
                                         facets_limits=body.facets_limits,
                                         size_result_count=body.size_result_count,
                                         page=body.page,
                                         return_facets_only=body.return_facets_only,
                                         author_filter=body.author_filter,
                                         published_filter=body.published_filter)


@router.get('/need_review',
            status_code=200,
            response_model=List[ReferenceSchemaNeedReviewShow])
def show_need_review(mod_abbreviation: str, count: int = None, db: Session = db_session):
    return search_crud.show_need_review(mod_abbreviation, count, db)
