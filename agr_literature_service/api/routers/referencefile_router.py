import json
import io
import logging
from json import JSONDecodeError
from typing import Union, List, Any, Dict, Optional

from fastapi import APIRouter, Depends, Security, status, File, UploadFile, \
    BackgroundTasks, HTTPException
from fastapi.responses import PlainTextResponse

from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import referencefile_crud
from agr_literature_service.api.deps import s3_auth
from agr_cognito_py import get_mod_access
from agr_literature_service.api.schemas import ResponseMessageSchema
from agr_literature_service.api.schemas.referencefile_schemas import ReferencefileSchemaShow, \
    ReferencefileSchemaUpdate, ReferencefileSchemaRelated, ReferenceFileAllMainPDFIdsSchemaPost
from agr_literature_service.api.utils.bulk_upload_utils import validate_archive_structure
from agr_literature_service.api.utils.bulk_upload_manager import upload_manager
from agr_literature_service.api.utils.bulk_upload_processor import process_bulk_upload_async
from agr_literature_service.api.user import set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user, read_auth_bypass

logger = logging.getLogger(__name__)

_ArchiveParam = File(...)

router = APIRouter(
    prefix="/reference/referencefile",
    tags=['Reference'])


get_db = database.get_db
db_session: Session = Depends(get_db)
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
                user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
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
             -H 'Authorization: Bearer <auth_token>' \\
             -H 'Content-Type: multipart/form-data' \\
             -F 'file=@test2.txt;type=text/plain' \\
             -F 'metadata_file=@metadata_file.txt;type=text/plain'

    - metadata as url parameters

        request:

            curl -X 'POST' 'http://localhost:8080/reference/referencefile/file_upload/?reference_curie=AGRKB:101000000000001&display_name=test&file_class=main&file_publication_status=final&file_extension=txt&pdf_type=null&is_annotation=false' \\
             -H 'accept: application/json' \\
             -H 'Authorization: Bearer <auth_token>' \\
             -H 'Content-Type: multipart/form-data' \\
             -F 'file=@test2.txt;type=text/plain' \\
             -F 'metadata_file='

    """
    if is_annotation is None:
        is_annotation = False
    set_global_user_from_cognito(db, user)
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
                  user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                  db: Session = db_session):
    set_global_user_from_cognito(db, user)
    return referencefile_crud.download_file(db, referencefile_id, get_mod_access(user) if user else [])


@router.get('/additional_files_tarball/{reference_id}',
            status_code=status.HTTP_200_OK)
def download_additional_files_tarball(reference_id: int,
                                      user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                                      db: Session = db_session):
    set_global_user_from_cognito(db, user)
    return referencefile_crud.download_additional_files_tarball(db, reference_id, get_mod_access(user) if user else [])


@router.delete('/{referencefile_id}',
               response_model=str)
def delete(referencefile_id: int,
           user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
           db: Session = db_session):
    set_global_user_from_cognito(db, user)
    referencefile_crud.destroy(db, referencefile_id, get_mod_access(user) if user else [])
    return 'success'


@router.get('/{referencefile_id}',
            status_code=status.HTTP_200_OK,
            response_model=ReferencefileSchemaShow)
def show(referencefile_id: int,
         user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
         db: Session = db_session):
    set_global_user_from_cognito(db, user)
    return referencefile_crud.show(db, referencefile_id)


@router.get('/show_all/{curie_or_reference_id}',
            status_code=status.HTTP_200_OK,
            response_model=List[ReferencefileSchemaRelated])
def show_all(curie_or_reference_id: str,
             user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
             db: Session = db_session):
    set_global_user_from_cognito(db, user)
    return referencefile_crud.show_all(db, curie_or_reference_id)


@router.post('/show_main_pdf_ids_for_curies',
             status_code=status.HTTP_200_OK,
             response_model=dict)
@read_auth_bypass
def show_main_pdf_ids_for_curies(data: ReferenceFileAllMainPDFIdsSchemaPost,
                                 user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                                 db: Session = db_session):
    set_global_user_from_cognito(db, user)
    return referencefile_crud.get_main_pdf_referencefile_ids_for_ref_curies_list(
        db=db, curies=data.curies, mod_abbreviation=data.mod_abbreviation)


@router.patch('/{referencefile_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
def patch(referencefile_id: int,
          request: ReferencefileSchemaUpdate,
          user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
          db: Session = db_session):
    set_global_user_from_cognito(db, user)
    return referencefile_crud.patch(db, referencefile_id, request.model_dump(exclude_unset=True))


@router.post('/merge/{curie_or_reference_id}/{losing_referencefile_id}/{winning_referencefile_id}',
             status_code=201)
def merge_referencefiles(curie_or_reference_id: str,
                         losing_referencefile_id: int,
                         winning_referencefile_id: int,
                         user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                         db: Session = db_session):
    set_global_user_from_cognito(db, user)
    return referencefile_crud.merge_referencefiles(db, curie_or_reference_id, losing_referencefile_id, winning_referencefile_id)


# Bulk Upload Endpoints
@router.post(
    "/bulk_upload_archive/",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=Dict[str, Any],
)
async def bulk_upload_archive(
    *,
    mod_abbreviation: str,
    background_tasks: BackgroundTasks,
    archive: UploadFile = _ArchiveParam,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session
):
    """
    Start a bulk-upload of a ZIP/TAR archive of reference files for a single MOD.

    Archive structure:
    - Root files = main files (PDFs, etc.)
    - Subdirectory files = supplement files (directory name = reference ID)

    MOD-specific filename parsing:
    - WB: {wbpaper_id}_{author_year}[_{options}].{ext} -> WB:WBPaper{id}
    - FB: {pmid}_{author_year}[_{options}].{ext} -> PMID:{pmid}
    - Others: {id}_{author_year}[_{options}].{ext} -> AGRKB:{id} (if 15 digits)

    Returns job ID for tracking progress via /bulk_upload_status/{job_id}
    """

    logger.info(f"bulk_upload_archive called with mod_abbreviation={mod_abbreviation}")

    # 1. authenticate
    set_global_user_from_cognito(db, user)

    # 2. validate archive structure
    try:
        validation = validate_archive_structure(archive.file)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Archive validation failed: {e}",
        )
    if not validation.get("valid", False):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid archive format: {validation.get('error')}",
        )
    if validation.get("total_files", 0) == 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Archive contains no files",
        )

    # rewind the upload so the background task can read it again
    try:
        archive.file.seek(0)
    except Exception:
        # if this fails, wrap in a new UploadFile on BytesIO
        data = await archive.read()
        archive = UploadFile(filename=archive.filename, file=io.BytesIO(data))

    # 3. create a new bulk‐upload job
    user_id: str = "unknown"
    if user:
        user_id = str(user.get("cognito:username") or user.get("sub", "unknown"))
    job_id = upload_manager.create_job(
        user_id=user_id,
        mod_abbreviation=mod_abbreviation,
        filename=archive.filename or "archive.unknown",
    )

    # 4. schedule the background task
    background_tasks.add_task(
        process_bulk_upload_async,
        job_id,
        archive,
        mod_abbreviation,
        db,
    )

    # 5. respond immediately
    return {
        "job_id": job_id,
        "status": "started",
        "message": f"Bulk upload job started for {mod_abbreviation}",
        "total_files": validation["total_files"],
        "main_files": validation["main_files"],
        "supplement_files": validation["supplement_files"],
        "status_url": f"/reference/referencefile/bulk_upload_status/{job_id}",
    }


@router.get(
    "/bulk_upload_status/{job_id}",
    status_code=status.HTTP_200_OK,
    response_class=PlainTextResponse,
    responses={200: {"content": {"application/json": {}}}},
)
def get_bulk_upload_status(
    job_id: str,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session
):
    job = upload_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # pretty-print with 2-space indent
    payload = json.dumps(job.to_dict(), indent=2)

    return PlainTextResponse(
        content=payload,
        media_type="application/json",
        status_code=status.HTTP_200_OK
    )


"""
@router.get('/bulk_upload_active/',
            status_code=status.HTTP_200_OK,
            response_model=List[dict])
def get_active_bulk_uploads(
    mod_abbreviation: str = None,
    db: Session = db_session
):
    # Get all currently active bulk upload jobs.
    active_jobs = upload_manager.get_active_jobs(
        user_id=None,
        mod_abbreviation=mod_abbreviation
    )

    return [job.to_dict() for job in active_jobs]


@router.get('/bulk_upload_history/',
            status_code=status.HTTP_200_OK,
            response_model=List[dict])
def get_bulk_upload_history(
    limit: int = 10,
    db: Session = db_session
):
    # Get recent bulk upload jobs for current user.

    recent_jobs = upload_manager.get_recent_jobs(
        user_id=None,
        limit=limit
    )

    return [job.to_dict() for job in recent_jobs]


@router.post('/bulk_upload_validate/',
             status_code=status.HTTP_200_OK,
             response_model=dict)
def validate_bulk_upload_archive(
    archive: UploadFile = File(...),
    user: Dict[str, Any] = Security(get_cognito_user_swagger),
    db: Session = db_session
):
    # Validate archive structure without uploading.
    try:
        # Read file content into memory
        content = archive.file.read()
        archive_bytes = io.BytesIO(content)
        archive_bytes.seek(0)

        # Check for PDF
        if is_pdf_file(archive_bytes):
            return {
                'valid': True,
                'total_files': 1,
                'main_files': 1,
                'supplement_files': 0,
                'main_file_list': ['PDF file'],
                'supplement_file_list': []
            }

        # Validate other formats
        return validate_compressed_archive(archive_bytes)

    except Exception as e:
        logger.error(f"Validation error: {str(e)}")
        return {
            'valid': False,
            'error': f'Validation error: {str(e)}',
            'total_files': 0,
            'main_files': 0,
            'supplement_files': 0,
            'main_file_list': [],
            'supplement_file_list': []
        }
    finally:
        archive.file.seek(0)
"""
