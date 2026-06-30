"""HTTP endpoints for the embedding_file catalog (SCRUM-6141).

Embeddings are discovered/downloaded through the existing referencefile APIs
(``show_all`` always lists embedding rows, and ``download_file/{referencefile_id}``
fetches the parquet); this router adds the write/lookup surface for the catalog
itself. Deletion is
intentionally NOT exposed here: an embedding is removed by deleting its parquet
referencefile (``DELETE /reference/referencefile/{id}``), which cascades the
catalog row away and cleans up S3 — deleting only the catalog row would orphan
the parquet.
"""
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, File, Security, UploadFile, status
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import embedding_file_crud
from agr_literature_service.api.schemas.embedding_file_schemas import (
    EmbeddingFileSchemaCreate,
    EmbeddingFileSchemaShow,
)
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user

router = APIRouter(
    prefix="/reference/embedding_file",
    tags=['Reference'])

get_db = database.get_db
db_session: Session = Depends(get_db)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=EmbeddingFileSchemaShow)
def create(reference_curie: str,
           profile_name: str,
           version: int,
           model_name: Optional[str] = None,
           source_referencefile_id: Optional[int] = None,
           mod_abbreviation: Optional[str] = None,
           file: UploadFile = File(...),  # noqa: B008
           user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
           db: Session = db_session):
    """Register an embedding: upload the parquet and upsert its catalog row.

    The parquet is stored as an ``embedding`` referencefile (md5/S3/dedup, no
    file-upload workflow side effects); the catalog row is keyed on
    ``(reference, profile_name, version, source_referencefile_id)`` and upserted
    -- idempotent, so re-posting re-points the existing row at the new parquet
    instead of creating a duplicate.

    Metadata is passed as query params; the parquet as the multipart ``file``.

        curl -X POST 'http://localhost:8080/reference/embedding_file/?reference_curie=AGRKB:101000000000001&profile_name=openai-3-small-fulltext-doc&version=1&model_name=openai:text-embedding-3-small@v1&mod_abbreviation=WB' \\
          -H 'Authorization: Bearer <token>' \\
          -F 'file=@embedding.parquet'
    """
    set_global_user_from_cognito(db, user)
    request = EmbeddingFileSchemaCreate(
        reference_curie=reference_curie,
        profile_name=profile_name,
        version=version,
        model_name=model_name,
        source_referencefile_id=source_referencefile_id,
        mod_abbreviation=mod_abbreviation,
    )
    return embedding_file_crud.create_or_update(db, request, file)


@router.get('/{embedding_file_id}',
            status_code=status.HTTP_200_OK,
            response_model=EmbeddingFileSchemaShow)
def show(embedding_file_id: int,
         user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
         db: Session = db_session):
    return embedding_file_crud.get(db, embedding_file_id)
