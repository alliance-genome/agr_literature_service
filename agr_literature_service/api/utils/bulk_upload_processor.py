import asyncio
import tempfile
import shutil
import os
from agr_literature_service.api.utils.bulk_upload_utils import (
    extract_and_classify_files,
    classify_and_parse_file,
    process_single_file
)
from agr_literature_service.api.utils.bulk_upload_manager import upload_manager
from fastapi import UploadFile
from sqlalchemy.orm import Session


async def process_bulk_upload_async(
    job_id: str,
    archive: UploadFile,
    mod_abbreviation: str,
    db: Session
):
    """
    Background task to process a bulk upload:
     - read the entire UploadFile into memory
     - extract to a fresh temp subdir
     - parse & upload each file
     - update progress
     - clean up
    """
    temp_dir = tempfile.mkdtemp(prefix=job_id + '_', dir=base_dir if (base_dir := os.environ.get('LOG_PATH')) else None)

    try:
        # 1) Save archive to temp file instead of reading into memory
        archive_path = os.path.join(temp_dir, archive.filename or "archive")
        with open(archive_path, "wb") as f:
            shutil.copyfileobj(archive.file, f)

        # 2) Process using the saved file
        with open(archive_path, "rb") as f:
            files = extract_and_classify_files(f, temp_dir, archive.filename)
        upload_manager.update_job(job_id, total_files=len(files))

        # 3) Process each file
        for idx, (path, _) in enumerate(files, start=1):
            metadata = classify_and_parse_file(path, temp_dir, mod_abbreviation)
            result = process_single_file(path, metadata, db)

            # 4) Update progress
            success = (result.get('status') == 'success')
            error = '' if success else result.get('error', '')
            upload_manager.update_progress(
                job_id=job_id,
                processed=idx,
                current_file=os.path.basename(path),
                success=success,
                error=error
            )

            # Let the loop occasionally yield
            await asyncio.sleep(0)

        # 5) Mark complete
        upload_manager.complete_job(job_id, success=True)

    except Exception as e:
        upload_manager.complete_job(job_id, success=False, error=str(e))

    finally:
        # 7) Clean up only the directory we created
        shutil.rmtree(temp_dir, ignore_errors=True)
