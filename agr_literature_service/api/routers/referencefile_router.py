import json
import logging
import asyncio
import tempfile
import io
import os
from json import JSONDecodeError
from typing import Union, List
from datetime import datetime

from fastapi import APIRouter, Depends, Security, status, File, UploadFile, HTTPException
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import referencefile_crud
from agr_literature_service.api.deps import s3_auth
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.routers.okta_utils import get_okta_mod_access
from agr_literature_service.api.schemas import ResponseMessageSchema
from agr_literature_service.api.schemas.referencefile_schemas import ReferencefileSchemaShow, ReferencefileSchemaUpdate, \
    ReferencefileSchemaRelated, ReferenceFileAllMainPDFIdsSchemaPost
from agr_literature_service.api.user import set_global_user_from_okta

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/reference/referencefile",
    tags=['Reference'])


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)
s3_session = Depends(s3_auth)


@router.post('/file_upload/',
             status_code=status.HTTP_201_CREATED,
             response_model=str
             )
def file_upload(reference_curie: str = None,
                display_name: str = None,
                file_class: str = None,
                file_publication_status: str = None,
                file_extension: str = "",
                pdf_type: str = None,
                is_annotation: bool = None,
                mod_abbreviation: str = None,
                upload_if_already_converted: bool = False,
                file: UploadFile = File(...),  # noqa: B008
                metadata_file: Union[UploadFile, None] = File(default=None),  # noqa: B008
                user: OktaUser = db_user,
                db: Session = db_session):
    """

    Sample usage with curl

    - metadata provided as file

        metadata file json format:

            {
                "reference_curie": "AGRKB:101000000000001",
                "display_name": "test",
                "file_class": "main",
                "file_publication_status": "final",
                "file_extension": "txt",
                "pdf_type": null,
                "is_annotation": "false",
                "mod_abbreviation": "WB"
            }

        request:

            curl -X 'POST' 'http://localhost:8080/reference/referencefile/file_upload/' \\
             -H 'accept: application/json' \\
             -H 'Authorization: Bearer <okta_token>' \\
             -H 'Content-Type: multipart/form-data' \\
             -F 'file=@test2.txt;type=text/plain' \\
             -F 'metadata_file=@metadata_file.txt;type=text/plain'

    - metadata as url parameters

        request:

            curl -X 'POST' 'http://localhost:8080/reference/referencefile/file_upload/?reference_curie=AGRKB:101000000000001&display_name=test&file_class=main&file_publication_status=final&file_extension=txt&pdf_type=null&is_annotation=false' \\
             -H 'accept: application/json' \\
             -H 'Authorization: Bearer <okta_token>' \\
             -H 'Content-Type: multipart/form-data' \\
             -F 'file=@test2.txt;type=text/plain' \\
             -F 'metadata_file='

    """
    if is_annotation is None:
        is_annotation = False
    set_global_user_from_okta(db, user)
    metadata = None
    if reference_curie and display_name and file_class and file_publication_status:
        metadata = {
            "reference_curie": reference_curie,
            "display_name": display_name,
            "file_class": file_class,
            "file_publication_status": file_publication_status,
            "file_extension": file_extension,
            "pdf_type": pdf_type,
            "is_annotation": is_annotation,
            "mod_abbreviation": mod_abbreviation
        }
    elif metadata_file is not None:
        try:
            metadata = json.load(metadata_file.file)
        except JSONDecodeError:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail="The provided metadata file is not a valid json file")
    if not metadata or not metadata["reference_curie"] or not metadata["display_name"] or not \
            metadata["file_class"] or not metadata["file_publication_status"]:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="The provided metadata is not valid")
    referencefile_crud.file_upload(db, metadata, file, upload_if_already_converted)
    return 'success'


@router.get('/download_file/{referencefile_id}',
            status_code=status.HTTP_200_OK)
def download_file(referencefile_id: int,
                  user: OktaUser = db_user,
                  db: Session = db_session):
    set_global_user_from_okta(db, user)
    return referencefile_crud.download_file(db, referencefile_id, get_okta_mod_access(user))


@router.get('/additional_files_tarball/{reference_id}',
            status_code=status.HTTP_200_OK)
def download_additional_files_tarball(reference_id: int,
                                      user: OktaUser = db_user,
                                      db: Session = db_session):
    set_global_user_from_okta(db, user)
    return referencefile_crud.download_additional_files_tarball(db, reference_id, get_okta_mod_access(user))


@router.delete('/{referencefile_id}',
               response_model=str)
def delete(referencefile_id: int,
           user: OktaUser = db_user,
           db: Session = db_session):
    set_global_user_from_okta(db, user)
    referencefile_crud.destroy(db, referencefile_id, get_okta_mod_access(user))
    return 'success'


@router.get('/{referencefile_id}',
            status_code=status.HTTP_200_OK,
            response_model=ReferencefileSchemaShow)
def show(referencefile_id: int,
         db: Session = db_session):
    return referencefile_crud.show(db, referencefile_id)


@router.get('/show_all/{curie_or_reference_id}',
            status_code=status.HTTP_200_OK,
            response_model=List[ReferencefileSchemaRelated])
def show_all(curie_or_reference_id: str,
             db: Session = db_session):
    return referencefile_crud.show_all(db, curie_or_reference_id)


@router.post('/show_main_pdf_ids_for_curies',
             status_code=status.HTTP_200_OK,
             response_model=dict)
def show_main_pdf_ids_for_curies(data: ReferenceFileAllMainPDFIdsSchemaPost,
                                 db: Session = db_session):
    return referencefile_crud.get_main_pdf_referencefile_ids_for_ref_curies_list(
        db=db, curies=data.curies, mod_abbreviation=data.mod_abbreviation)


@router.patch('/{referencefile_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
def patch(referencefile_id: int,
          request: ReferencefileSchemaUpdate,
          user: OktaUser = db_user,
          db: Session = db_session):
    set_global_user_from_okta(db, user)
    return referencefile_crud.patch(db, referencefile_id, request.dict(exclude_unset=True))


@router.post('/merge/{curie_or_reference_id}/{losing_referencefile_id}/{winning_referencefile_id}',
             status_code=201)
def merge_referencefiles(curie_or_reference_id: str,
                         losing_referencefile_id: int,
                         winning_referencefile_id: int,
                         user: OktaUser = db_user,
                         db: Session = db_session):
    set_global_user_from_okta(db, user)
    return referencefile_crud.merge_referencefiles(db, curie_or_reference_id,
                                                   losing_referencefile_id,
                                                   winning_referencefile_id)


# Bulk Upload Endpoints

@router.post('/bulk_upload_archive/',
             status_code=status.HTTP_202_ACCEPTED,
             response_model=dict)
async def bulk_upload_archive(
    mod_abbreviation: str,
    archive: UploadFile = File(...),  # noqa: B008
    user: OktaUser = db_user,
    db: Session = db_session
):
    """
    Start bulk upload job for MOD-specific reference files archive.

    Archive structure:
    - Root files = main files (PDFs, etc.)
    - Subdirectory files = supplement files (directory name = reference ID)

    MOD-specific filename parsing:
    - WB: {wbpaper_id}_{author_year}[_{options}].{ext} -> WB:WBPaper{id}
    - FB: {pmid}_{author_year}[_{options}].{ext} -> PMID:{pmid}
    - Others: {id}_{author_year}[_{options}].{ext} -> AGRKB:{id} (if 15 digits)

    Returns job ID for tracking progress via /bulk_upload_status/{job_id}
    """
    from agr_literature_service.api.utils.bulk_upload_manager import upload_manager
    from agr_literature_service.api.utils.bulk_upload_utils import validate_archive_structure

    set_global_user_from_okta(db, user)

    # Validate archive before processing
    validation = validate_archive_structure(archive.file)
    if not validation["valid"]:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid archive format: {validation['error']}"
        )

    if validation["total_files"] == 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Archive contains no files"
        )

    # Create job
    job_id = upload_manager.create_job(
        user_id=user.cid,
        mod_abbreviation=mod_abbreviation,
        filename=archive.filename or "unknown.archive"
    )

    # Start background processing
    asyncio.create_task(process_bulk_upload_async(job_id, archive, mod_abbreviation, db))

    return {
        "job_id": job_id,
        "status": "started",
        "message": f"Bulk upload job started for {mod_abbreviation}",
        "total_files": validation["total_files"],
        "main_files": validation["main_files"],
        "supplement_files": validation["supplement_files"],
        "status_url": f"/reference/referencefile/bulk_upload_status/{job_id}"
    }


@router.get('/bulk_upload_status/{job_id}',
            status_code=status.HTTP_200_OK,
            response_model=dict)
def get_bulk_upload_status(
    job_id: str,
    user: OktaUser = db_user,
    db: Session = db_session
):
    """Get status and progress of specific bulk upload job."""
    from agr_literature_service.api.utils.bulk_upload_manager import upload_manager

    set_global_user_from_okta(db, user)

    job = upload_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Check if user owns this job (could add admin check here if needed)
    if job.user_id != user.cid:
        raise HTTPException(status_code=403, detail="Access denied")

    return job.to_dict()


@router.get('/bulk_upload_active/',
            status_code=status.HTTP_200_OK,
            response_model=List[dict])
def get_active_bulk_uploads(
    mod_abbreviation: str = None,
    user: OktaUser = db_user,
    db: Session = db_session
):
    """Get all currently active bulk upload jobs."""
    from agr_literature_service.api.utils.bulk_upload_manager import upload_manager

    set_global_user_from_okta(db, user)

    # Show only user's jobs unless they're admin
    user_id = user.cid

    active_jobs = upload_manager.get_active_jobs(
        user_id=user_id,
        mod_abbreviation=mod_abbreviation
    )

    return [job.to_dict() for job in active_jobs]


@router.get('/bulk_upload_history/',
            status_code=status.HTTP_200_OK,
            response_model=List[dict])
def get_bulk_upload_history(
    limit: int = 10,
    user: OktaUser = db_user,
    db: Session = db_session
):
    """Get recent bulk upload jobs for current user."""
    from agr_literature_service.api.utils.bulk_upload_manager import upload_manager

    set_global_user_from_okta(db, user)

    recent_jobs = upload_manager.get_recent_jobs(
        user_id=user.cid,
        limit=limit
    )

    return [job.to_dict() for job in recent_jobs]


@router.post('/bulk_upload_validate/',
             status_code=status.HTTP_200_OK,
             response_model=dict)
def validate_bulk_upload_archive(
    archive: UploadFile = File(...),  # noqa: B008
    user: OktaUser = db_user,
    db: Session = db_session
):
    """Validate archive structure without uploading."""
    from agr_literature_service.api.utils.bulk_upload_utils import validate_archive_structure

    set_global_user_from_okta(db, user)

    validation = validate_archive_structure(archive.file)
    return validation


# Background processing function
async def process_bulk_upload_async(job_id: str, archive: UploadFile,
                                    mod_abbreviation: str, db: Session):
    """Background task to process bulk upload."""
    from agr_literature_service.api.utils.bulk_upload_manager import upload_manager
    from agr_literature_service.api.utils.bulk_upload_utils import (
        extract_and_classify_files, classify_and_parse_file, process_single_file
    )

    try:
        # Read archive content into memory for processing
        archive_content = await archive.read()

        with tempfile.TemporaryDirectory() as temp_dir:
            # Extract files
            with io.BytesIO(archive_content) as archive_file:
                file_info_list = extract_and_classify_files(archive_file, temp_dir)

            # Update job with total file count
            upload_manager.update_job(job_id, total_files=len(file_info_list))

            # Sort to process main files first, then supplements
            file_info_list.sort(key=lambda x: (not x[1], x[0]))  # Main files first

            # Process files
            for i, (file_path, _) in enumerate(file_info_list):
                try:
                    filename = os.path.basename(file_path)

                    # Update current file being processed
                    job = upload_manager.get_job(job_id)
                    if job:
                        job.current_file = filename
                        job.last_update = datetime.utcnow()

                    # Process file
                    metadata = classify_and_parse_file(file_path, temp_dir, mod_abbreviation)
                    result = process_single_file(file_path, metadata, db)

                    # Update progress
                    success = result.get("status") == "success"
                    error = result.get("error", "") if not success else ""

                    if job:
                        job.update_progress(
                            processed=i + 1,
                            current_file=filename,
                            success=success,
                            error=error
                        )

                except Exception as e:
                    # Handle individual file error
                    logger.error(f"Error processing file {filename}: {str(e)}")
                    job = upload_manager.get_job(job_id)
                    if job:
                        job.update_progress(
                            processed=i + 1,
                            current_file=filename,
                            success=False,
                            error=str(e)
                        )

            # Mark job as completed
            upload_manager.complete_job(job_id, success=True)

    except Exception as e:
        # Mark job as failed
        logger.error(f"Bulk upload job {job_id} failed: {str(e)}")
        upload_manager.complete_job(job_id, success=False, error=str(e))
