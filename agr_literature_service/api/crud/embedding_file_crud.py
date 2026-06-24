from typing import List

from fastapi import HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.referencefile_crud import file_upload_single
from agr_literature_service.api.crud.reference_utils import get_reference
from agr_literature_service.api.models import EmbeddingFileModel
from agr_literature_service.api.schemas.embedding_file_schemas import EmbeddingFileSchemaCreate


def create_or_update(db: Session, request: EmbeddingFileSchemaCreate,
                     file: UploadFile) -> EmbeddingFileModel:
    """Store the parquet as an `embedding` referencefile and upsert the
    catalog row on the unique key (reference, profile_name, version,
    source_referencefile_id). Idempotent: re-running re-points the existing
    row at the (possibly new) parquet instead of duplicating.
    """
    reference = get_reference(db=db, curie_or_reference_id=request.reference_curie)

    # 1. Store the parquet via the existing single-file uploader (md5/S3/dedup,
    #    no main-PDF workflow-tag side effects).
    metadata = {
        "reference_curie": request.reference_curie,
        "mod_abbreviation": request.mod_abbreviation,
        "display_name": f"embedding_{request.profile_name}_v{request.version}",
        "file_class": "embedding",
        "file_publication_status": "final",
        "file_extension": "parquet",
        "pdf_type": None,
        "is_annotation": False,
    }
    parquet_rf = file_upload_single(db, metadata, file)

    # 2. Upsert the catalog row on the unique key.
    row = db.query(EmbeddingFileModel).filter(
        EmbeddingFileModel.reference_id == reference.reference_id,
        EmbeddingFileModel.profile_name == request.profile_name,
        EmbeddingFileModel.version == request.version,
        EmbeddingFileModel.source_referencefile_id == request.source_referencefile_id,
    ).one_or_none()
    if row is None:
        row = EmbeddingFileModel(
            reference_id=reference.reference_id,
            profile_name=request.profile_name,
            version=request.version,
            model_name=request.model_name,
            source_referencefile_id=request.source_referencefile_id,
            parquet_referencefile_id=parquet_rf.referencefile_id,
        )
        db.add(row)
    else:
        row.model_name = request.model_name
        row.parquet_referencefile_id = parquet_rf.referencefile_id
    db.commit()
    db.refresh(row)
    return row


def get(db: Session, embedding_file_id: int) -> EmbeddingFileModel:
    row = db.query(EmbeddingFileModel).filter(
        EmbeddingFileModel.embedding_file_id == embedding_file_id).one_or_none()
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"embedding_file {embedding_file_id} not found")
    return row


def destroy(db: Session, embedding_file_id: int) -> None:
    row = get(db, embedding_file_id)
    db.delete(row)
    db.commit()


def get_embeddings_for_source(db: Session,
                              source_referencefile_id: int) -> List[EmbeddingFileModel]:
    return db.query(EmbeddingFileModel).filter(
        EmbeddingFileModel.source_referencefile_id == source_referencefile_id).all()
