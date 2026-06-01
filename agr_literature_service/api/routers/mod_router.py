from fastapi import APIRouter, Depends, Response, Security, status
from typing import Dict, Any, Optional

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import mod_crud
from agr_literature_service.api.schemas import (ModSchemaPost, ModSchemaShow, ModSchemaUpdate)
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user
from agr_literature_service.api.util.resource_urls import mod_url

router = APIRouter(
    prefix="/mod",
    tags=['Mod']
)


get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=ModSchemaShow)
def create(request: ModSchemaPost,
           response: Response,
           user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
           db: Session = db_session):
    set_global_user_from_cognito(db, user)
    mod = mod_crud.create(db, request)
    response.headers["Location"] = mod_url(mod.abbreviation)
    return mod


@router.patch('/{mod_id}',
              status_code=status.HTTP_200_OK,
              response_model=ModSchemaShow)
async def patch(mod_id: int,
                request: ModSchemaUpdate,
                user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                db: Session = db_session):
    set_global_user_from_cognito(db, user)
    patch = request.model_dump(exclude_unset=True)
    return mod_crud.patch(db, mod_id, patch)


@router.get('/{abbreviation}',
            response_model=ModSchemaShow,
            status_code=200)
def show(abbreviation: str,
         user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
         db: Session = db_session):
    return mod_crud.show(db, abbreviation)


@router.get('/taxons/{type}',
            status_code=200)
def taxons(type: str,
           user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
           db: Session = db_session):
    return mod_crud.taxons(db, type)


@router.get('/{mod_id}/versions',
            status_code=200)
def show_versions(mod_id: int,
                  user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                  db: Session = db_session):
    return mod_crud.show_changesets(db, mod_id)
