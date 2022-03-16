from fastapi import APIRouter, Security

from literature.crud import search_crud
from literature.routers.authentication import auth


router = APIRouter(
    prefix="/search",
    tags=['Search']
)


db_user = Security(auth.get_user)


@router.get('',
            status_code=200)
def show(q: str):
    return search_crud.show(query=q)
