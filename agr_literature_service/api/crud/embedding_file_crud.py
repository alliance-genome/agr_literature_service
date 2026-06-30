from typing import List

from fastapi import HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.referencefile_crud import file_upload_single
from agr_literature_service.api.crud.referencefile_utils import remove_from_s3_and_db
from agr_literature_service.api.crud.reference_utils import get_reference
from agr_literature_service.api.models import EmbeddingFileModel, ReferencefileModel
from agr_literature_service.api.schemas.embedding_file_schemas import EmbeddingFileSchemaCreate


def create_or_update(db: Session, request: EmbeddingFileSchemaCreate,
                     file: UploadFile) -> EmbeddingFileModel:
    """Store the parquet as an `embedding` referencefile and upsert the catalog
    row on the unique key (reference, profile_name, version,
    source_referencefile_id). Idempotent: re-running re-points the existing row
    at the (possibly new) parquet instead of duplicating, and when the parquet
    actually changes the superseded one is deleted so it does not leak in DB/S3
    or surface as a bare embedding in show_all.
    """
    reference = get_reference(db=db, curie_or_reference_id=request.reference_curie)

    # Validate source_referencefile_id BEFORE uploading the parquet. The FK is
    # only enforced at commit, so an invalid id would otherwise upload + commit
    # the parquet and then fail the catalog insert (500), orphaning the parquet.
    if request.source_referencefile_id is not None:
        source = db.query(ReferencefileModel).filter(
            ReferencefileModel.referencefile_id == request.source_referencefile_id
        ).one_or_none()
        if source is None or source.reference_id != reference.reference_id:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"source_referencefile_id {request.source_referencefile_id} does not "
                       f"exist or does not belong to {request.reference_curie}")

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
        db.commit()
        db.refresh(row)
        return row

    # Existing row: re-point at the (possibly new) parquet, then drop the
    # previous parquet if the content changed and nothing else references it.
    old_parquet_id = row.parquet_referencefile_id
    row.model_name = request.model_name
    row.parquet_referencefile_id = parquet_rf.referencefile_id
    db.commit()
    db.refresh(row)
    if old_parquet_id != parquet_rf.referencefile_id:
        _delete_parquet_if_orphaned(db, old_parquet_id)
    return row


def _delete_parquet_if_orphaned(db: Session, parquet_referencefile_id: int) -> None:
    """Delete a superseded parquet `embedding` referencefile (DB + S3) once no
    embedding_file row points at it. Called after a catalog row is re-pointed to
    a new parquet so the old one does not leak. No-op if any row still references
    it (md5 dedup can make rows share a parquet)."""
    still_referenced = db.query(EmbeddingFileModel).filter(
        EmbeddingFileModel.parquet_referencefile_id == parquet_referencefile_id).first()
    if still_referenced is not None:
        return
    old_rf = db.query(ReferencefileModel).filter(
        ReferencefileModel.referencefile_id == parquet_referencefile_id).one_or_none()
    if old_rf is not None:
        remove_from_s3_and_db(db, old_rf)


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
