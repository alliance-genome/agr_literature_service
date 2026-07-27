from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Security

from agr_literature_service.api.crud import vocabulary_crud
from agr_literature_service.api.auth import get_authenticated_user

router = APIRouter(
    prefix="/vocabulary",
    tags=['Vocabulary']
)


@router.get('/',
            status_code=200)
def list_all(user: Optional[Dict[str, Any]] = Security(get_authenticated_user)) -> List[str]:
    return vocabulary_crud.list_vocabularies()


@router.get('/{name}',
            status_code=200)
def show(name: str,
         user: Optional[Dict[str, Any]] = Security(get_authenticated_user)) -> List[Dict[str, Any]]:
    return vocabulary_crud.get_vocabulary(name)
