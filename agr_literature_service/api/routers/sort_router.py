from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends
from typing import List

from agr_literature_service.api import database
from agr_literature_service.api.crud import sort_crud
from agr_literature_service.api.schemas import ReferenceSchemaNeedReviewShow


router = APIRouter(
    prefix="/sort",
    tags=["Sort"])


get_db = database.get_db
db_session: Session = Depends(get_db)


@router.get('/need_review',
            status_code=200,
            response_model=List[ReferenceSchemaNeedReviewShow])
def show_need_review(mod_abbreviation: str, count: int = None, db: Session = db_session):
    return sort_crud.show_need_review(mod_abbreviation, count, db)


@router.get('/need_prioritization',
            status_code=200,
            response_model=List[ReferenceSchemaNeedReviewShow])
def show_need_prioritization(mod_abbreviation: str, count: int = None, db: Session = db_session):
    return sort_crud.show_need_prioritization(mod_abbreviation, count, db)


@router.get('/prepublication_pipeline',
            status_code=200,
            response_model=List[ReferenceSchemaNeedReviewShow])
def show_prepublication_pipeline(mod_abbreviation: str, count: int = None, db: Session = db_session):
    return sort_crud.show_prepublication_pipeline(mod_abbreviation, count, db)


@router.get('/recently_sorted',
            status_code=200)
def show_recently_sorted(mod_abbreviation: str, count: int = None, db: Session = db_session, curator: str = None, day: int = 7):
    return sort_crud.show_recently_sorted(db, mod_abbreviation, count, curator, day)
