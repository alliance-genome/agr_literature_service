import asyncio
import tempfile
import shutil
from agr_literature_service.api.utils.bulk_upload_utils import (
    extract_and_classify_files,
    classify_and_parse_file,
    process_single_file
)
from agr_literature_service.api.utils.bulk_upload_manager import upload_manager


async def process_bulk_upload_async(job_id: str, archive_file, mod_abbreviation: str, db):
    """Background task to process bulk upload jobs asynchronously."""
    temp_dir = tempfile.mkdtemp()
    try:
        # 1) Extract and count files
        files = extract_and_classify_files(archive_file, temp_dir)
        upload_manager.update_job(job_id, total_files=len(files))

        # 2) Process each file sequentially
        for idx, (path, _) in enumerate(files, start=1):
            metadata = classify_and_parse_file(path, temp_dir, mod_abbreviation)
            result = process_single_file(path, metadata, db)

            # 3) Update progress
            success = (result.get('status') == 'success')
            error = result.get('error', '') if not success else ''
            upload_manager.update_progress(
                job_id,
                processed=idx,
                current_file=path,
                success=success,
                error=error
            )

            # Yield control to event loop
            await asyncio.sleep(0)

        # 4) Mark the job as completed successfully
        upload_manager.complete_job(job_id, success=True)

    except Exception as e:
        # On any exception, mark job as failed
        upload_manager.complete_job(job_id, success=False, error=str(e))

    finally:
        # Always clean up temporary directory
        shutil.rmtree(temp_dir, ignore_errors=True)
