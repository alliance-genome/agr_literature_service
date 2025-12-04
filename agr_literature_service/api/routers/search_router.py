from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends

from agr_literature_service.api import database
from agr_literature_service.api.crud import search_crud
from agr_literature_service.api.schemas import FacetsOptionsSchema


router = APIRouter(
    prefix="/search",
    tags=["Search"])


get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post("/references/",
             status_code=200)
def search(body: FacetsOptionsSchema):
    try:
        result = search_crud.search_references(query=body.query, facets_values=body.facets_values,
                                               negated_facets_values=body.negated_facets_values,
                                               facets_limits=body.facets_limits,
                                               size_result_count=body.size_result_count,
                                               sort_by_published_date_order=body.sort_by_published_date_order,
                                               page=body.page,
                                               return_facets_only=body.return_facets_only,
                                               author_filter=body.author_filter,
                                               date_pubmed_modified=body.date_pubmed_modified,
                                               date_pubmed_arrive=body.date_pubmed_arrive,
                                               date_published=body.date_published,
                                               date_created=body.date_created,
                                               query_fields=body.query_fields,
                                               partial_match=body.partial_match,
                                               tet_nested_facets_values=body.tet_nested_facets_values,
                                               sort=body.sort)
        return result
    except Exception as e:
        return {"error": str(e)}
