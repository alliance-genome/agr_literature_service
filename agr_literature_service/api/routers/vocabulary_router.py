from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Security
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import vocabulary_crud
from agr_literature_service.api.auth import get_authenticated_user

router = APIRouter(
    prefix="/vocabulary",
    tags=['Vocabulary']
)

get_db = database.get_db
db_session: Session = Depends(get_db)


@router.get('/',
            status_code=200)
def list_all(user: Optional[Dict[str, Any]] = Security(get_authenticated_user)) -> List[str]:
    return vocabulary_crud.list_vocabularies()


@router.get('/{name}',
            status_code=200)
def show(name: str,
         user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
         db: Session = db_session) -> List[Dict[str, Any]]:
    return vocabulary_crud.get_vocabulary(db, name)


@router.get('/{name}/autocomplete',
            status_code=200)
def autocomplete(name: str,
                 q: str = "",
                 user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                 db: Session = db_session) -> List[Dict[str, Any]]:
    return vocabulary_crud.search_vocabulary(db, name, q)
