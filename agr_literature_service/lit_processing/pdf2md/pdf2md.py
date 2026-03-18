"""
PDF to Markdown conversion using PDFX service.

This script converts PDF files to Markdown using the PDFX service at
https://pdfx.alliancegenome.org. It supports multiple extraction methods
(grobid, docling, marker, merged) and stores each output as a separate file.
"""
import logging
import os
import time
from datetime import datetime
from io import BytesIO
from typing import Optional, Dict, List, Tuple

import requests
from fastapi import UploadFile
from sqlalchemy import create_engine, desc, and_, not_, exists, select
from sqlalchemy.orm import sessionmaker, Session

from agr_literature_service.api.crud.referencefile_crud import (
    get_main_pdf_referencefile_id, download_file, file_upload
)
from agr_literature_service.api.crud.workflow_tag_crud import get_jobs, job_change_atp_code
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.models import (
    ModModel, ReferencefileModel, ReferenceModel, CrossReferenceModel
)
from agr_cognito_py import ModAccess
from agr_literature_service.lit_processing.utils.report_utils import send_report


logger = logging.getLogger(__name__)

# Extraction methods and their corresponding file classes
EXTRACTION_METHODS = {
    "grobid": "converted_grobid_main",
    "docling": "converted_docling_main",
    "marker": "converted_marker_main",
    "merged": "converted_merged_main",
}

# Token cache
_token_cache: Dict[str, any] = {
    "token": None,
    "expires_at": 0
}


def get_pdfx_token() -> str:
    """
    Obtain PDFX bearer token using Cognito client_credentials grant.
    Token is cached and refreshed when expired.

    Returns:
        str: The access token for PDFX API authentication.

    Raises:
        ValueError: If required environment variables are not set.
        requests.RequestException: If token request fails.
    """
    current_time = time.time()

    # Return cached token if still valid (with 60 second buffer)
    if _token_cache["token"] and _token_cache["expires_at"] > current_time + 60:
        return _token_cache["token"]

    client_id = os.environ.get("PDFX_CLIENT_ID")
    client_secret = os.environ.get("PDFX_CLIENT_SECRET")
    token_url = os.environ.get("PDFX_TOKEN_URL", "https://auth.alliancegenome.org/oauth2/token")
    scope = os.environ.get("PDFX_SCOPE", "pdfx-api/extract")

    if not client_id or not client_secret:
        raise ValueError("PDFX_CLIENT_ID and PDFX_CLIENT_SECRET environment variables must be set")

    response = requests.post(
        token_url,
        auth=(client_id, client_secret),
        data={"grant_type": "client_credentials", "scope": scope},
        timeout=30
    )
    response.raise_for_status()

    token_data = response.json()
    _token_cache["token"] = token_data["access_token"]
    # Cache expiry time (typically 3600 seconds)
    _token_cache["expires_at"] = current_time + token_data.get("expires_in", 3600)

    return _token_cache["token"]


def submit_pdf_to_pdfx(
    file_content: bytes,
    token: str,
    methods: str = "grobid,docling,marker",
    merge: bool = True,
    reference_curie: str = None,
    mod_abbreviation: str = None,
    max_retries: int = 3,
    retry_delay: int = 5
) -> str:
    """
    Submit a PDF to the PDFX service for processing.

    Args:
        file_content: The PDF file content as bytes.
        token: The bearer token for authentication.
        methods: Comma-separated extraction methods to use.
        merge: Whether to generate merged output.
        reference_curie: Optional reference identifier for logging.
        mod_abbreviation: Optional MOD abbreviation for logging.
        max_retries: Maximum number of retry attempts for transient failures.
        retry_delay: Delay between retries in seconds.

    Returns:
        str: The process_id for tracking the extraction job.

    Raises:
        requests.RequestException: If submission fails after all retries.
    """
    pdfx_api_url = os.environ.get("PDFX_API_URL", "https://pdfx.alliancegenome.org")
    url = f"{pdfx_api_url}/api/v1/extract"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    # Prepare multipart form data
    data = {
        "methods": methods,
        "merge": str(merge).lower()
    }

    logger.info(f"Submitting PDF to PDFX for {reference_curie or 'unknown reference'}")

    last_exception = None
    for attempt in range(max_retries):
        try:
            # Recreate files dict for each attempt (file pointer may be exhausted)
            files = {
                "file": ("document.pdf", file_content, "application/pdf")
            }
            response = requests.post(url, headers=headers, files=files, data=data, timeout=120)
            response.raise_for_status()

            result = response.json()
            process_id = result.get("process_id")

            if not process_id:
                raise ValueError(f"No process_id returned from PDFX: {result}")

            logger.info(f"PDFX submission successful. Process ID: {process_id}")
            return process_id

        except (requests.exceptions.SSLError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            last_exception = e
            if attempt < max_retries - 1:
                logger.warning(f"PDFX submission attempt {attempt + 1} failed: {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logger.error(f"PDFX submission failed after {max_retries} attempts")
                raise

    raise last_exception


def poll_pdfx_status(
    process_id: str,
    token: str,
    timeout: int = 900,
    poll_interval: int = 10
) -> Dict:
    """
    Poll the PDFX service for job completion.

    Args:
        process_id: The process ID to check.
        token: The bearer token for authentication.
        timeout: Maximum time to wait in seconds (default 15 minutes).
        poll_interval: Time between polls in seconds.

    Returns:
        dict: The status response containing job state and results.

    Raises:
        TimeoutError: If job doesn't complete within timeout.
        requests.RequestException: If polling request fails.
    """
    pdfx_api_url = os.environ.get("PDFX_API_URL", "https://pdfx.alliancegenome.org")
    url = f"{pdfx_api_url}/api/v1/extract/{process_id}"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    start_time = time.time()
    last_status = None

    while time.time() - start_time < timeout:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        status_data = response.json()
        current_status = status_data.get("status")

        if current_status != last_status:
            logger.info(f"PDFX job {process_id} status: {current_status}")
            last_status = current_status

        if current_status in ["completed", "complete"]:
            return status_data
        elif current_status == "failed":
            error_msg = status_data.get("error", "Unknown error")
            raise RuntimeError(f"PDFX job failed: {error_msg}")
        elif current_status in ["queued", "warming", "processing", "progress", "pending", "started"]:
            time.sleep(poll_interval)
        else:
            logger.warning(f"Unknown PDFX status: {current_status}")
            time.sleep(poll_interval)

    raise TimeoutError(f"PDFX job {process_id} timed out after {timeout} seconds")


def download_pdfx_result(process_id: str, method: str, token: str) -> bytes:
    """
    Download the markdown result for a specific extraction method.

    Args:
        process_id: The process ID of the completed job.
        method: The extraction method (grobid, docling, marker, merged).
        token: The bearer token for authentication.

    Returns:
        bytes: The markdown content.

    Raises:
        requests.RequestException: If download fails.
    """
    pdfx_api_url = os.environ.get("PDFX_API_URL", "https://pdfx.alliancegenome.org")
    url = f"{pdfx_api_url}/api/v1/extract/{process_id}/download/{method}"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.get(url, headers=headers, timeout=60)
    response.raise_for_status()

    return response.content


def get_newest_main_pdfs(db: Session, limit: int = 50) -> List[Dict]:
    """
    Get the newest main PDF files from the database.

    Args:
        db: Database session.
        limit: Maximum number of PDFs to return.

    Returns:
        List of dicts with referencefile info.
    """
    results = []

    # Query for main PDFs ordered by date_created descending
    query = (
        db.query(ReferencefileModel)
        .filter(
            ReferencefileModel.file_class == "main",
            ReferencefileModel.file_publication_status == "final",
            ReferencefileModel.pdf_type == "pdf"
        )
        .order_by(desc(ReferencefileModel.date_created))
        .limit(limit)
    )

    for ref_file in query.all():
        reference = db.query(ReferenceModel).filter(
            ReferenceModel.reference_id == ref_file.reference_id
        ).one_or_none()

        if not reference:
            continue

        # Get MOD abbreviation from referencefile_mods
        mod_abbreviation = None
        for ref_file_mod in ref_file.referencefile_mods:
            if ref_file_mod.mod:
                mod_abbreviation = ref_file_mod.mod.abbreviation
                break

        results.append({
            "referencefile_id": ref_file.referencefile_id,
            "reference_id": reference.reference_id,
            "reference_curie": reference.curie,
            "display_name": ref_file.display_name,
            "file_extension": ref_file.file_extension,
            "mod_abbreviation": mod_abbreviation
        })

    return results


def get_unprocessed_pdfs_since_year(db: Session, since_year: int) -> List[Dict]:
    """
    Get main PDF files created since a given year that haven't been converted to markdown.

    A PDF is considered "unprocessed" if it has no associated markdown files
    (none of the 4 extraction methods: grobid, docling, marker, merged).

    Args:
        db: Database session.
        since_year: Year to filter from (e.g., 2025 means Jan 1, 2025 onwards).

    Returns:
        List of dicts with referencefile info for PDFs needing processing.
    """
    results = []

    # Start date is January 1st of the given year
    start_date = datetime(since_year, 1, 1)

    # File classes for markdown outputs
    md_file_classes = list(EXTRACTION_METHODS.values())

    logger.info(f"Querying for unprocessed PDFs since {start_date.strftime('%Y-%m-%d')}...")

    # Subquery to find reference_ids that already have markdown files
    md_exists_subquery = (
        select(ReferencefileModel.reference_id)
        .where(ReferencefileModel.file_class.in_(md_file_classes))
        .distinct()
    )

    # Query for main PDFs since the given year that don't have markdown files
    query = (
        db.query(ReferencefileModel)
        .filter(
            ReferencefileModel.file_class == "main",
            ReferencefileModel.file_publication_status == "final",
            ReferencefileModel.pdf_type == "pdf",
            ReferencefileModel.date_created >= start_date,
            ~ReferencefileModel.reference_id.in_(md_exists_subquery)
        )
        .order_by(desc(ReferencefileModel.date_created))
    )

    all_pdfs = query.all()
    logger.info(f"Found {len(all_pdfs)} unprocessed PDFs since {since_year}")

    for ref_file in all_pdfs:
        reference = db.query(ReferenceModel).filter(
            ReferenceModel.reference_id == ref_file.reference_id
        ).one_or_none()

        if not reference:
            continue

        # Get MOD abbreviation from referencefile_mods
        mod_abbreviation = None
        for ref_file_mod in ref_file.referencefile_mods:
            if ref_file_mod.mod:
                mod_abbreviation = ref_file_mod.mod.abbreviation
                break

        results.append({
            "referencefile_id": ref_file.referencefile_id,
            "reference_id": reference.reference_id,
            "reference_curie": reference.curie,
            "display_name": ref_file.display_name,
            "file_extension": ref_file.file_extension,
            "mod_abbreviation": mod_abbreviation,
            "date_created": ref_file.date_created
        })

    return results


def process_pdfs_since_year(since_year: int):
    """
    Process all unprocessed PDFs since a given year.

    Args:
        since_year: Year to filter from (e.g., 2025 means Jan 1, 2025 onwards).
    """
    start_time = time.time()

    engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
    new_session = sessionmaker(bind=engine, autoflush=True)
    db = new_session()

    logger.info(f"Starting PDF to Markdown conversion for PDFs since {since_year}")

    # Get token
    try:
        token = get_pdfx_token()
    except Exception as e:
        logger.error(f"Failed to obtain PDFX token: {e}")
        return

    # Get unprocessed PDFs
    pdf_list = get_unprocessed_pdfs_since_year(db, since_year=since_year)
    logger.info(f"Found {len(pdf_list)} unprocessed PDFs to convert")

    if not pdf_list:
        logger.info("No unprocessed PDFs found. Exiting.")
        db.close()
        return {"total": 0, "success": 0, "failed": 0}

    # Processing statistics
    total_count = len(pdf_list)
    success_count = 0
    failure_count = 0
    objects_with_errors = []
    pdf_times = []

    for idx, ref_file_info in enumerate(pdf_list, 1):
        pdf_start_time = time.time()
        reference_curie = ref_file_info["reference_curie"]

        logger.info(f"Processing {idx}/{total_count}: {reference_curie}")

        # Refresh token if needed
        try:
            token = get_pdfx_token()
        except Exception as e:
            logger.error(f"Failed to refresh token: {e}")
            continue

        success, error_msg = process_single_pdf(db, ref_file_info, token)

        pdf_elapsed = time.time() - pdf_start_time
        pdf_times.append(pdf_elapsed)

        if success:
            success_count += 1
            logger.info(f"Completed {reference_curie} in {pdf_elapsed:.2f}s")
        else:
            failure_count += 1
            objects_with_errors.append({
                "reference_curie": reference_curie,
                "display_name": ref_file_info["display_name"],
                "file_extension": ref_file_info["file_extension"],
                "mod_abbreviation": ref_file_info.get("mod_abbreviation", "N/A"),
                "error": error_msg
            })
            logger.error(f"Failed {reference_curie} after {pdf_elapsed:.2f}s: {error_msg}")

    # Calculate timing statistics
    total_elapsed = time.time() - start_time
    avg_time = sum(pdf_times) / len(pdf_times) if pdf_times else 0
    min_time = min(pdf_times) if pdf_times else 0
    max_time = max(pdf_times) if pdf_times else 0

    # Log summary
    logger.info("=" * 60)
    logger.info(f"PDF to Markdown Conversion Summary (Since {since_year})")
    logger.info("=" * 60)
    logger.info(f"Total PDFs processed: {total_count}")
    logger.info(f"Successful: {success_count}")
    logger.info(f"Failed: {failure_count}")
    logger.info("-" * 60)
    logger.info("Timing Statistics:")
    logger.info(f"  Total time: {total_elapsed:.2f}s ({total_elapsed/60:.2f} minutes)")
    logger.info(f"  Average per PDF: {avg_time:.2f}s")
    logger.info(f"  Min time: {min_time:.2f}s")
    logger.info(f"  Max time: {max_time:.2f}s")
    logger.info("=" * 60)

    # Send error report if there were failures
    if objects_with_errors:
        error_message = f"PDF to Markdown Conversion Errors (Since {since_year})\n\n"
        error_message += f"Total: {total_count}, Success: {success_count}, Failed: {failure_count}\n\n"
        error_message += f"Total time: {total_elapsed:.2f}s, Avg per PDF: {avg_time:.2f}s\n\n"
        error_message += "Failed conversions:\n"
        for error_obj in objects_with_errors:
            error_message += f"{error_obj['mod_abbreviation']}\t{error_obj['reference_curie']}\t"
            error_message += f"{error_obj['display_name']}.{error_obj['file_extension']}\t{error_obj['error']}\n"

        subject = f"pdf2md conversion errors (since {since_year})"
        send_report(subject, error_message)

    db.close()

    return {
        "total": total_count,
        "success": success_count,
        "failed": failure_count,
        "total_time_seconds": total_elapsed,
        "avg_time_per_pdf": avg_time,
        "min_time": min_time,
        "max_time": max_time
    }


def process_single_pdf(
    db: Session,
    ref_file_info: Dict,
    token: str,
    methods_to_extract: List[str] = None
) -> Tuple[bool, Optional[str]]:
    """
    Process a single PDF file through PDFX.

    Args:
        db: Database session.
        ref_file_info: Dict with referencefile info.
        token: PDFX bearer token.
        methods_to_extract: List of methods to extract (default: all 4).

    Returns:
        Tuple of (success: bool, error_message: Optional[str]).
    """
    if methods_to_extract is None:
        methods_to_extract = list(EXTRACTION_METHODS.keys())

    referencefile_id = ref_file_info["referencefile_id"]
    reference_curie = ref_file_info["reference_curie"]
    display_name = ref_file_info["display_name"]
    mod_abbreviation = ref_file_info.get("mod_abbreviation")

    try:
        # Download the PDF content
        file_content = download_file(
            db=db,
            referencefile_id=referencefile_id,
            mod_access=ModAccess.ALL_ACCESS,
            use_in_api=False
        )

        # Determine which methods to request (merged requires merge=True)
        request_methods = [m for m in methods_to_extract if m != "merged"]
        include_merge = "merged" in methods_to_extract

        # Submit to PDFX
        process_id = submit_pdf_to_pdfx(
            file_content=file_content,
            token=token,
            methods=",".join(request_methods) if request_methods else "grobid,docling,marker",
            merge=include_merge,
            reference_curie=reference_curie,
            mod_abbreviation=mod_abbreviation
        )

        # Poll for completion
        status_data = poll_pdfx_status(process_id, token)

        # Download and store each method's output
        successful_methods = []
        for method in methods_to_extract:
            try:
                markdown_content = download_pdfx_result(process_id, method, token)

                if not markdown_content or len(markdown_content) < 10:
                    logger.warning(f"Empty or minimal content for {method} on {reference_curie}")
                    continue

                file_class = EXTRACTION_METHODS[method]
                output_display_name = f"{display_name}_{method}"

                metadata = {
                    "reference_curie": reference_curie,
                    "display_name": output_display_name,
                    "file_class": file_class,
                    "file_publication_status": "final",
                    "file_extension": "md",
                    "pdf_type": None,
                    "is_annotation": None,
                    "mod_abbreviation": mod_abbreviation
                }

                file_upload(
                    db=db,
                    metadata=metadata,
                    file=UploadFile(
                        file=BytesIO(markdown_content),
                        filename=f"{output_display_name}.md"
                    ),
                    upload_if_already_converted=True
                )

                successful_methods.append(method)
                logger.info(f"Uploaded {method} markdown for {reference_curie}")

            except Exception as e:
                logger.error(f"Failed to download/upload {method} for {reference_curie}: {e}")

        if successful_methods:
            logger.info(f"Successfully processed {reference_curie} with methods: {successful_methods}")
            return True, None
        else:
            return False, "No methods successfully extracted"

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Failed to process {reference_curie}: {error_msg}")
        return False, error_msg


def process_newest_pdfs(limit: int = 50):
    """
    Process the newest main PDFs and convert them to markdown.

    Args:
        limit: Number of newest PDFs to process.
    """
    start_time = time.time()

    engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
    new_session = sessionmaker(bind=engine, autoflush=True)
    db = new_session()

    logger.info(f"Starting PDF to Markdown conversion for {limit} newest PDFs")

    # Get token
    try:
        token = get_pdfx_token()
    except Exception as e:
        logger.error(f"Failed to obtain PDFX token: {e}")
        return

    # Get newest PDFs
    pdf_list = get_newest_main_pdfs(db, limit=limit)
    logger.info(f"Found {len(pdf_list)} PDFs to process")

    # Processing statistics
    total_count = len(pdf_list)
    success_count = 0
    failure_count = 0
    objects_with_errors = []
    pdf_times = []

    for idx, ref_file_info in enumerate(pdf_list, 1):
        pdf_start_time = time.time()
        reference_curie = ref_file_info["reference_curie"]

        logger.info(f"Processing {idx}/{total_count}: {reference_curie}")

        # Refresh token if needed
        try:
            token = get_pdfx_token()
        except Exception as e:
            logger.error(f"Failed to refresh token: {e}")
            continue

        success, error_msg = process_single_pdf(db, ref_file_info, token)

        pdf_elapsed = time.time() - pdf_start_time
        pdf_times.append(pdf_elapsed)

        if success:
            success_count += 1
            logger.info(f"Completed {reference_curie} in {pdf_elapsed:.2f}s")
        else:
            failure_count += 1
            objects_with_errors.append({
                "reference_curie": reference_curie,
                "display_name": ref_file_info["display_name"],
                "file_extension": ref_file_info["file_extension"],
                "mod_abbreviation": ref_file_info.get("mod_abbreviation", "N/A"),
                "error": error_msg
            })
            logger.error(f"Failed {reference_curie} after {pdf_elapsed:.2f}s: {error_msg}")

    # Calculate timing statistics
    total_elapsed = time.time() - start_time
    avg_time = sum(pdf_times) / len(pdf_times) if pdf_times else 0
    min_time = min(pdf_times) if pdf_times else 0
    max_time = max(pdf_times) if pdf_times else 0

    # Log summary
    logger.info("=" * 60)
    logger.info("PDF to Markdown Conversion Summary")
    logger.info("=" * 60)
    logger.info(f"Total PDFs processed: {total_count}")
    logger.info(f"Successful: {success_count}")
    logger.info(f"Failed: {failure_count}")
    logger.info("-" * 60)
    logger.info("Timing Statistics:")
    logger.info(f"  Total time: {total_elapsed:.2f}s ({total_elapsed/60:.2f} minutes)")
    logger.info(f"  Average per PDF: {avg_time:.2f}s")
    logger.info(f"  Min time: {min_time:.2f}s")
    logger.info(f"  Max time: {max_time:.2f}s")
    logger.info("=" * 60)

    # Send error report if there were failures
    if objects_with_errors:
        error_message = "PDF to Markdown Conversion Errors\n\n"
        error_message += f"Total: {total_count}, Success: {success_count}, Failed: {failure_count}\n\n"
        error_message += f"Total time: {total_elapsed:.2f}s, Avg per PDF: {avg_time:.2f}s\n\n"
        error_message += "Failed conversions:\n"
        for error_obj in objects_with_errors:
            error_message += f"{error_obj['mod_abbreviation']}\t{error_obj['reference_curie']}\t"
            error_message += f"{error_obj['display_name']}.{error_obj['file_extension']}\t{error_obj['error']}\n"

        subject = "pdf2md conversion errors"
        send_report(subject, error_message)

    db.close()

    return {
        "total": total_count,
        "success": success_count,
        "failed": failure_count,
        "total_time_seconds": total_elapsed,
        "avg_time_per_pdf": avg_time,
        "min_time": min_time,
        "max_time": max_time
    }


def main():
    """
    Main entry point for workflow-based PDF to Markdown conversion.

    This processes PDFs based on workflow tags (text_convert_job).
    """
    start_time = time.time()

    engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
    new_session = sessionmaker(bind=engine, autoflush=True)
    db = new_session()

    limit = 1000
    offset = 0
    all_jobs = []

    logger.info("Started loading all text conversion jobs.")
    seen_wf_tag_ids = set()

    while jobs := get_jobs(db, "text_convert_job", limit, offset):
        for job in jobs:
            if job["reference_workflow_tag_id"] in seen_wf_tag_ids:
                logger.warning("Duplicate job found. Skipping.")
            else:
                all_jobs.append(job)
                seen_wf_tag_ids.add(job["reference_workflow_tag_id"])
        offset += limit
        logger.info(f"Loaded batch of {len(jobs)} jobs. Total jobs loaded: {len(all_jobs)}")

    logger.info("Finished loading all text conversion jobs.")

    # Get token
    try:
        token = get_pdfx_token()
    except Exception as e:
        logger.error(f"Failed to obtain PDFX token: {e}")
        return

    mod_abbreviation_from_mod_id = {}
    objects_with_errors = []
    total_count = len(all_jobs)
    success_count = 0
    failure_count = 0
    pdf_times = []

    for idx, job in enumerate(all_jobs, 1):
        pdf_start_time = time.time()
        add_to_error_list = False

        ref_id = job['reference_id']
        reference_workflow_tag_id = job['reference_workflow_tag_id']
        mod_id = job['mod_id']
        reference_curie = job['reference_curie']

        logger.info(f"Processing {idx}/{total_count}: {reference_curie}")

        if mod_id not in mod_abbreviation_from_mod_id:
            mod_abbreviation = db.query(ModModel.abbreviation).filter(
                ModModel.mod_id == mod_id
            ).one().abbreviation
            mod_abbreviation_from_mod_id[mod_id] = mod_abbreviation
        else:
            mod_abbreviation = mod_abbreviation_from_mod_id[mod_id]

        ref_file_id_to_convert = get_main_pdf_referencefile_id(
            db=db,
            curie_or_reference_id=ref_id,
            mod_abbreviation=mod_abbreviation
        )

        if ref_file_id_to_convert:
            ref_file_obj: ReferencefileModel = db.query(ReferencefileModel).filter(
                ReferencefileModel.referencefile_id == ref_file_id_to_convert
            ).one()

            ref_file_info = {
                "referencefile_id": ref_file_id_to_convert,
                "reference_id": ref_id,
                "reference_curie": reference_curie,
                "display_name": ref_file_obj.display_name,
                "file_extension": ref_file_obj.file_extension,
                "mod_abbreviation": mod_abbreviation
            }

            # Refresh token if needed
            try:
                token = get_pdfx_token()
            except Exception as e:
                logger.error(f"Failed to refresh token: {e}")
                add_to_error_list = True
                job_change_atp_code(db, reference_workflow_tag_id, "on_failed")
                continue

            success, error_msg = process_single_pdf(db, ref_file_info, token)

            pdf_elapsed = time.time() - pdf_start_time
            pdf_times.append(pdf_elapsed)

            if success:
                success_count += 1
                job_change_atp_code(db, reference_workflow_tag_id, "on_success")
                logger.info(f"Completed {reference_curie} in {pdf_elapsed:.2f}s")
            else:
                failure_count += 1
                add_to_error_list = True
                job_change_atp_code(db, reference_workflow_tag_id, "on_failed")
                logger.error(f"Failed {reference_curie} after {pdf_elapsed:.2f}s")

            if add_to_error_list:
                mod_cross_ref = db.query(CrossReferenceModel).join(
                    ReferenceModel, CrossReferenceModel.reference_id == ReferenceModel.reference_id
                ).filter(
                    ReferenceModel.curie == reference_curie,
                    CrossReferenceModel.curie_prefix == mod_abbreviation
                ).one_or_none()

                objects_with_errors.append({
                    "reference_curie": reference_curie,
                    "display_name": ref_file_obj.display_name,
                    "file_extension": ref_file_obj.file_extension,
                    "mod_abbreviation": mod_abbreviation,
                    "mod_cross_ref": mod_cross_ref.curie if mod_cross_ref else "N/A",
                    "error": error_msg
                })

    # Calculate timing statistics
    total_elapsed = time.time() - start_time
    avg_time = sum(pdf_times) / len(pdf_times) if pdf_times else 0
    min_time = min(pdf_times) if pdf_times else 0
    max_time = max(pdf_times) if pdf_times else 0

    # Log summary
    logger.info("=" * 60)
    logger.info("PDF to Markdown Conversion Summary (Workflow Mode)")
    logger.info("=" * 60)
    logger.info(f"Total jobs processed: {total_count}")
    logger.info(f"Successful: {success_count}")
    logger.info(f"Failed: {failure_count}")
    logger.info("-" * 60)
    logger.info("Timing Statistics:")
    logger.info(f"  Total time: {total_elapsed:.2f}s ({total_elapsed/60:.2f} minutes)")
    logger.info(f"  Average per PDF: {avg_time:.2f}s")
    logger.info(f"  Min time: {min_time:.2f}s")
    logger.info(f"  Max time: {max_time:.2f}s")
    logger.info("=" * 60)

    # Send error report
    if objects_with_errors:
        error_message = "PDF to Markdown Conversion Errors (Workflow Mode)\n\n"
        error_message += f"Total: {total_count}, Success: {success_count}, Failed: {failure_count}\n\n"
        error_message += f"Total time: {total_elapsed:.2f}s, Avg per PDF: {avg_time:.2f}s\n\n"
        error_message += "Failed conversions:\n"
        for error_obj in objects_with_errors:
            error_message += f"{error_obj['mod_abbreviation']}\t{error_obj['mod_cross_ref']}\t"
            error_message += f"{error_obj['reference_curie']}\t{error_obj['display_name']}."
            error_message += f"{error_obj['file_extension']}\t{error_obj.get('error', 'Unknown')}\n"

        subject = "pdf2md conversion errors"
        send_report(subject, error_message)

    db.close()


if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    import argparse

    parser = argparse.ArgumentParser(
        description="Convert PDF files to Markdown using PDFX service.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process PDFs via workflow jobs (default)
  python -m agr_literature_service.lit_processing.pdf2md.pdf2md

  # Process N newest PDFs
  python -m agr_literature_service.lit_processing.pdf2md.pdf2md --newest 50

  # Process all unprocessed PDFs since 2025
  python -m agr_literature_service.lit_processing.pdf2md.pdf2md --since 2025
        """
    )
    parser.add_argument(
        "--newest",
        type=int,
        metavar="N",
        help="Process N newest main PDFs (regardless of processing status)"
    )
    parser.add_argument(
        "--since",
        type=int,
        metavar="YEAR",
        help="Process all unprocessed PDFs since YEAR (e.g., 2025)"
    )

    args = parser.parse_args()

    if args.newest and args.since:
        print("Error: Cannot use --newest and --since together. Choose one.")
        exit(1)

    if args.newest:
        print(f"Processing {args.newest} newest main PDFs...")
        result = process_newest_pdfs(limit=args.newest)
        print(f"\nResults: {result}")
    elif args.since:
        print(f"Processing all unprocessed PDFs since {args.since}...")
        result = process_pdfs_since_year(since_year=args.since)
        print(f"\nResults: {result}")
    else:
        # Workflow mode (default)
        main()
