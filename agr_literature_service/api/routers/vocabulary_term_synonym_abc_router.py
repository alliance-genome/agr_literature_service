from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Response, Security, status
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import vocabulary_term_synonym_abc_crud
from agr_literature_service.api.schemas import (
    VocabularyTermSynonymAbcSchemaPost, VocabularyTermSynonymAbcSchemaShow, VocabularyTermSynonymAbcSchemaUpdate)
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user

router = APIRouter(prefix="/vocabulary_term_synonym_abc", tags=["Vocabulary ABC Term Synonym"])
get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post('/', status_code=status.HTTP_201_CREATED)
def create(request: VocabularyTermSynonymAbcSchemaPost, response: Response,
           user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
           db: Session = db_session):
    set_global_user_from_cognito(db, user)
    return vocabulary_term_synonym_abc_crud.create(db, request)


@router.get('/{vocabulary_term_synonym_abc_id}', status_code=200,
            response_model=VocabularyTermSynonymAbcSchemaShow)
def show(vocabulary_term_synonym_abc_id: int,
         user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
         db: Session = db_session):
    return vocabulary_term_synonym_abc_crud.show(db, vocabulary_term_synonym_abc_id)


@router.patch('/{vocabulary_term_synonym_abc_id}', status_code=status.HTTP_200_OK)
def patch(vocabulary_term_synonym_abc_id: int, request: VocabularyTermSynonymAbcSchemaUpdate,
          user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
          db: Session = db_session):
    set_global_user_from_cognito(db, user)
    return vocabulary_term_synonym_abc_crud.patch(
        db, vocabulary_term_synonym_abc_id, request.model_dump(exclude_unset=True))


@router.delete('/{vocabulary_term_synonym_abc_id}', status_code=status.HTTP_204_NO_CONTENT)
def destroy(vocabulary_term_synonym_abc_id: int,
            user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
            db: Session = db_session):
    set_global_user_from_cognito(db, user)
    vocabulary_term_synonym_abc_crud.destroy(db, vocabulary_term_synonym_abc_id)
