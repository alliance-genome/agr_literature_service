from fastapi import APIRouter, Depends, Response, Security, status
from typing import Dict, Any

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import mod_corpus_association_crud
from agr_literature_service.api.schemas import (ModCorpusAssociationSchemaPost,
                                                ModCorpusAssociationSchemaShow,
                                                ModCorpusAssociationSchemaUpdate,
                                                ResponseMessageSchema)
from agr_literature_service.api.user import set_global_user_from_cognito

from agr_cognito_py import get_cognito_user_swagger
import logging
import logging.config
router = APIRouter(
    prefix="/reference/mod_corpus_association",
    tags=['Reference']
)

logging.basicConfig(level=logging.INFO)
get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=int)
def create(request: ModCorpusAssociationSchemaPost,
           user: Dict[str, Any] = Security(get_cognito_user_swagger),
           db: Session = db_session) -> int:
    set_global_user_from_cognito(db, user)
    return mod_corpus_association_crud.create(db, request)


@router.delete('/{mod_corpus_association_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def destroy(mod_corpus_association_id: int,
            user: Dict[str, Any] = Security(get_cognito_user_swagger),
            db: Session = db_session):
    set_global_user_from_cognito(db, user)
    mod_corpus_association_crud.destroy(db, mod_corpus_association_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/{mod_corpus_association_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
async def patch(mod_corpus_association_id: int,
                request: ModCorpusAssociationSchemaUpdate,
                user: Dict[str, Any] = Security(get_cognito_user_swagger),
                db: Session = db_session) -> int:
    set_global_user_from_cognito(db, user)
    patch = request.model_dump(exclude_unset=True)
    return mod_corpus_association_crud.patch(db, mod_corpus_association_id, patch)


@router.get('/{mod_corpus_association_id}',
            response_model=ModCorpusAssociationSchemaShow,
            status_code=200)
def show(mod_corpus_association_id: int,
         db: Session = db_session):
    return mod_corpus_association_crud.show(db, mod_corpus_association_id)


@router.get('/reference/{curie}/mod_abbreviation/{mod_abbreviation}',
            response_model=int,
            status_code=200)
def show_id(curie: str, mod_abbreviation: str,
            db: Session = db_session):
    return mod_corpus_association_crud.show_by_reference_mod_abbreviation(db, curie, mod_abbreviation)


@router.get('/{mod_corpus_association_id}/versions',
            status_code=200)
def show_versions(mod_corpus_association_id: int,
                  db: Session = db_session):
    return mod_corpus_association_crud.show_changesets(db, mod_corpus_association_id)
