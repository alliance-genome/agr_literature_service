from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, Security

from agr_literature_service.api import database
from agr_literature_service.api.auth import get_authenticated_user
from agr_literature_service.api.crud import sort_crud
from agr_literature_service.api.schemas import ReferenceSchemaNeedReviewShow, ReferenceSchemaNeedReviewResponse


router = APIRouter(
    prefix="/sort",
    tags=["Sort"])


get_db = database.get_db
db_session: Session = Depends(get_db)


@router.get('/need_review/sort_sources',
            status_code=200,
            response_model=List[str])
def get_need_review_sort_sources(
    mod_abbreviation: str,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session
):
    """
    Get distinct mod_corpus_sort_source values for needs_review papers.
    Only returns sources that have at least one paper with corpus=NULL for the given MOD.
    """
    return sort_crud.get_need_review_sort_sources(mod_abbreviation, db)


@router.get('/need_review',
            status_code=200,
            response_model=ReferenceSchemaNeedReviewResponse)
def show_need_review(
    mod_abbreviation: str,
    count: int = 100,
    search_query: Optional[str] = None,
    sort_source: Optional[str] = None,
    sort_by: Optional[str] = "curie",
    sort_order: Optional[str] = "desc",
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session
):
    """
    Get references needing review with optional search, filter, and sort.

    Args:
        mod_abbreviation: The MOD abbreviation (e.g., 'WB', 'SGD')
        count: Maximum number of results to return (default 100)
        search_query: Optional keyword to search in title, journal, author
        sort_source: Optional mod_corpus_sort_source value to filter by
        sort_by: Field to sort by ('curie' or 'date_published')
        sort_order: Sort order ('asc' or 'desc')

    Returns:
        Response with total_count and list of references
    """
    return sort_crud.show_need_review(
        mod_abbreviation, count, db,
        search_query=search_query,
        sort_source=sort_source,
        sort_by=sort_by,
        sort_order=sort_order
    )


@router.get('/prepublication_pipeline',
            status_code=200,
            response_model=List[ReferenceSchemaNeedReviewShow])
def show_prepublication_pipeline(mod_abbreviation: str,
                                 count: int = None,
                                 user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                                 db: Session = db_session):
    return sort_crud.show_prepublication_pipeline(mod_abbreviation, count, db)


@router.get('/recently_sorted',
            status_code=200)
def show_recently_sorted(mod_abbreviation: str,
                         count: int = None,
                         user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                         db: Session = db_session,
                         curator: str = None,
                         day: int = 7):
    return sort_crud.show_recently_sorted(db, mod_abbreviation, count, curator, day)
