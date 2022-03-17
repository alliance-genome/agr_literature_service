import subprocess

from fastapi import APIRouter
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from literature.crud import search_crud

router = APIRouter(
    prefix="/search",
    tags=['Search'])


@router.get('/references/{query}',
            status_code=200)
def search(query: str):
    return search_crud.search_references(query=query)
