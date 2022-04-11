from typing import List

from fastapi import APIRouter, Security, Depends

from literature import database
from literature.routers.authentication import auth
from literature.crud import search_crud
from sqlalchemy.orm import Session
from literature.schemas import ReferenceSchemaNeedReviewShow


router = APIRouter(
    prefix="/search",
    tags=["Search"])


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)


@router.get("/references/{query}",
            status_code=200)
def search(query: str):
    return search_crud.search_references(query=query)


@router.get('/need_review',
            status_code=200,
            response_model=List[ReferenceSchemaNeedReviewShow])
def show_need_review(mod_abbreviation: str, db: Session = db_session):
    return search_crud.show_need_review(mod_abbreviation, db)
