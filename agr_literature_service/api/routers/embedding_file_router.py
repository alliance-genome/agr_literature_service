"""HTTP endpoints for the embedding_file catalog (SCRUM-6141).

Read-only lookup surface. Embeddings are discovered/downloaded through the
existing referencefile APIs (``show_all`` always lists embedding rows, and
``download_file/{referencefile_id}`` fetches the parquet — MOD access on the
parquet is inherited from its source file).

Creation is intentionally NOT exposed over HTTP: embeddings are generated and
registered ABC-internally only (``embedding_file_crud.create_or_update``, used
by the conversion pipeline — SCRUM-6142), which also enforces that the parquet
inherits the source referencefile's access. Deletion is likewise not exposed:
an embedding is removed by deleting its parquet referencefile
(``DELETE /reference/referencefile/{id}``), which cascades the catalog row away
and cleans up S3 — deleting only the catalog row would orphan the parquet.
"""
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Security, status
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import embedding_file_crud
from agr_literature_service.api.schemas.embedding_file_schemas import EmbeddingFileSchemaShow
from agr_literature_service.api.auth import get_authenticated_user

router = APIRouter(
    prefix="/reference/embedding_file",
    tags=['Reference'])

get_db = database.get_db
db_session: Session = Depends(get_db)


@router.get('/{embedding_file_id}',
            status_code=status.HTTP_200_OK,
            response_model=EmbeddingFileSchemaShow)
def show(embedding_file_id: int,
         user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
         db: Session = db_session):
    return embedding_file_crud.get(db, embedding_file_id)
