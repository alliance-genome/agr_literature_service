import json
import io
import logging
from json import JSONDecodeError
from typing import Optional, List, Dict, Any

from fastapi import (
    APIRouter,
    HTTPException,
    status,
    Depends,
    File,
    UploadFile,
    BackgroundTasks
)
from fastapi.responses import PlainTextResponse
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import referencefile_crud
from agr_literature_service.api.deps import s3_auth
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.routers.okta_utils import get_okta_mod_access
from agr_literature_service.api.schemas import ResponseMessageSchema
from agr_literature_service.api.schemas.referencefile_schemas import (
    ReferencefileSchemaShow,
    ReferencefileSchemaUpdate,
    ReferencefileSchemaRelated,
    ReferenceFileAllMainPDFIdsSchemaPost
)
from agr_literature_service.api.utils.bulk_upload_utils import validate_archive_structure
from agr_literature_service.api.utils.bulk_upload_manager import upload_manager
from agr_literature_service.api.utils.bulk_upload_processor import process_bulk_upload_async
from agr_literature_service.api.user import set_global_user_from_okta

logger = logging.getLogger(__name__)


# Dependency defaults to avoid B008 linting errors
FILE_PARAM = File(...)
METADATA_FILE_PARAM = File(None)
ARCHIVE_PARAM = File(...)
USER_SECURITY = Depends(auth.get_user)
DB_DEP = Depends(database.get_db)
S3_SESSION_DEP = Depends(s3_auth)


router = APIRouter(prefix="/reference/referencefile", tags=["Reference"])


@router.post(
    "/file_upload/",
    status_code=status.HTTP_201_CREATED,
    response_model=str
)
async def file_upload(
    file: UploadFile = FILE_PARAM,
    metadata_file: Optional[UploadFile] = METADATA_FILE_PARAM,
    reference_curie: Optional[str] = None,
    display_name: Optional[str] = None,
    file_class: Optional[str] = None,
    file_publication_status: Optional[str] = None,
    file_extension: str = "",
    pdf_type: Optional[str] = None,
    is_annotation: bool = False,
    mod_abbreviation: Optional[str] = None,
    upload_if_already_converted: bool = False,
    user: OktaUser = USER_SECURITY,
    db: Session = DB_DEP,
    s3_session=S3_SESSION_DEP
) -> str:
    """
    Upload a single reference file. Metadata via query params or JSON file.
    """
    set_global_user_from_okta(db, user)

    if metadata_file:
        try:
            metadata = json.load(metadata_file.file)
        except JSONDecodeError:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="The provided metadata file is not a valid JSON"
            )
    else:
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

    required = [
        "reference_curie",
        "display_name",
        "file_class",
        "file_publication_status"
    ]
    missing = [f for f in required if not metadata.get(f)]
    if missing:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Missing metadata fields: {', '.join(missing)}"
        )

    referencefile_crud.file_upload(db, metadata, file, upload_if_already_converted)
    return "success"


@router.get(
    "/download_file/{referencefile_id}",
    status_code=status.HTTP_200_OK
)
def download_file(
    referencefile_id: int,
    user: OktaUser = USER_SECURITY,
    db: Session = DB_DEP
) -> Any:
    set_global_user_from_okta(db, user)
    return referencefile_crud.download_file(
        db,
        referencefile_id,
        get_okta_mod_access(user)
    )


@router.get(
    "/additional_files_tarball/{reference_id}",
    status_code=status.HTTP_200_OK
)
def download_additional_files_tarball(
    reference_id: int,
    user: OktaUser = USER_SECURITY,
    db: Session = DB_DEP
) -> Any:
    set_global_user_from_okta(db, user)
    return referencefile_crud.download_additional_files_tarball(
        db,
        reference_id,
        get_okta_mod_access(user)
    )


@router.delete(
    "/{referencefile_id}",
    response_model=str
)
def delete(
    referencefile_id: int,
    user: OktaUser = USER_SECURITY,
    db: Session = DB_DEP
) -> str:
    set_global_user_from_okta(db, user)
    referencefile_crud.destroy(
        db,
        referencefile_id,
        get_okta_mod_access(user)
    )
    return "success"


@router.get(
    "/{referencefile_id}",
    status_code=status.HTTP_200_OK,
    response_model=ReferencefileSchemaShow
)
def show(
    referencefile_id: int,
    db: Session = DB_DEP
) -> Dict[str, Any]:
    return referencefile_crud.show(db, referencefile_id)


@router.get(
    "/show_all/{curie_or_reference_id}",
    status_code=status.HTTP_200_OK,
    response_model=List[ReferencefileSchemaRelated]
)
def show_all(
    curie_or_reference_id: str,
    db: Session = DB_DEP
) -> List[Dict[str, Any]]:
    return referencefile_crud.show_all(db, curie_or_reference_id)


@router.post(
    "/show_main_pdf_ids_for_curies",
    status_code=status.HTTP_200_OK,
    response_model=Dict[str, Any]
)
def show_main_pdf_ids_for_curies(
    data: ReferenceFileAllMainPDFIdsSchemaPost,
    db: Session = DB_DEP
) -> Dict[str, int]:
    return referencefile_crud.get_main_pdf_referencefile_ids_for_ref_curies_list(
        db=db,
        curies=data.curies,
        mod_abbreviation=data.mod_abbreviation
    )


@router.patch(
    "/{referencefile_id}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ResponseMessageSchema
)
def patch(
    referencefile_id: int,
    request: ReferencefileSchemaUpdate,
    user: OktaUser = USER_SECURITY,
    db: Session = DB_DEP
) -> Dict[str, str]:
    set_global_user_from_okta(db, user)
    return referencefile_crud.patch(
        db,
        referencefile_id,
        request.dict(exclude_unset=True)
    )


@router.post(
    "/merge/{curie_or_reference_id}/{losing_referencefile_id}/{winning_referencefile_id}",
    status_code=status.HTTP_201_CREATED
)
def merge_referencefiles(
    curie_or_reference_id: str,
    losing_referencefile_id: int,
    winning_referencefile_id: int,
    user: OktaUser = USER_SECURITY,
    db: Session = DB_DEP
) -> Any:
    set_global_user_from_okta(db, user)
    return referencefile_crud.merge_referencefiles(
        db,
        curie_or_reference_id,
        losing_referencefile_id,
        winning_referencefile_id
    )


@router.post(
    "/bulk_upload_archive/",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=Dict[str, Any]
)
async def bulk_upload_archive(
    background_tasks: BackgroundTasks,
    mod_abbreviation: str,
    archive: UploadFile = ARCHIVE_PARAM,
    user: OktaUser = USER_SECURITY,
    db: Session = DB_DEP
) -> Dict[str, Any]:
    """
    Start a bulk-upload of a ZIP/TAR archive of reference files for a single MOD.
    """
    logger.info(f"bulk_upload_archive called with mod_abbreviation={mod_abbreviation}")
    set_global_user_from_okta(db, user)

    try:
        validation = validate_archive_structure(archive.file)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Archive validation failed: {e}"
        )
    if not validation.get("valid", False):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid archive format: {validation.get('error')}"
        )
    if validation.get("total_files", 0) == 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Archive contains no files"
        )

    try:
        archive.file.seek(0)
    except Exception:
        data = await archive.read()
        archive = UploadFile(
            filename=archive.filename,
            file=io.BytesIO(data)
        )

    job_id = upload_manager.create_job(
        user_id=user.cid,
        mod_abbreviation=mod_abbreviation,
        filename=archive.filename or "archive.unknown"
    )
    background_tasks.add_task(
        process_bulk_upload_async,
        job_id,
        archive,
        mod_abbreviation,
        db
    )

    return {
        "job_id": job_id,
        "status": "started",
        "message": f"Bulk upload job started for {mod_abbreviation}",
        "total_files": validation["total_files"],
        "main_files": validation["main_files"],
        "supplement_files": validation["supplement_files"],
        "status_url": f"/reference/referencefile/bulk_upload_status/{job_id}"
    }


@router.get(
    "/bulk_upload_status/{job_id}",
    status_code=status.HTTP_200_OK,
    response_class=PlainTextResponse,
    responses={200: {"content": {"application/json": {}}}}
)
def get_bulk_upload_status(
    job_id: str,
    db: Session = DB_DEP
) -> Any:
    job = upload_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    payload = json.dumps(job.to_dict(), indent=2)
    return PlainTextResponse(content=payload, media_type="application/json", status_code=status.HTTP_200_OK)
