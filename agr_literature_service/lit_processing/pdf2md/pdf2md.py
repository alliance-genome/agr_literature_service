"""
PDF to Markdown conversion using PDFX service.

This script converts PDF files to Markdown using the PDFX service at
https://pdfx.alliancegenome.org. It supports multiple extraction methods
(grobid, docling, marker, merged) and stores each output as a separate file.
"""
import logging
import time
from datetime import datetime
from io import BytesIO
from typing import Dict, List, Optional, Tuple

from fastapi import UploadFile
from sqlalchemy import create_engine, desc, select
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

# Import PDFX utilities from pdf2md_utils
try:
    from .pdf2md_utils import (
        EXTRACTION_METHODS,
        get_pdfx_token,
        submit_pdf_to_pdfx,
        poll_pdfx_status,
        download_pdfx_result,
    )
except ImportError:
    # Fallback for direct script execution
    import pdf2md_utils as _pdf2md_utils  # type: ignore[import-not-found]
    EXTRACTION_METHODS = _pdf2md_utils.EXTRACTION_METHODS
    get_pdfx_token = _pdf2md_utils.get_pdfx_token
    submit_pdf_to_pdfx = _pdf2md_utils.submit_pdf_to_pdfx
    poll_pdfx_status = _pdf2md_utils.poll_pdfx_status
    download_pdfx_result = _pdf2md_utils.download_pdfx_result


logger = logging.getLogger(__name__)


def get_newest_main_pdfs(db: Session, limit: int = 50, skip_xml: bool = False) -> List[Dict]:  # pragma: no cover
    """
    Get the newest main PDF files from the database.

    Args:
        db: Database session.
        limit: Maximum number of PDFs to return.
        skip_xml: If True, skip papers that already have XML/nXML files.

    Returns:
        List of dicts with referencefile info.
    """
    results = []

    # Base query for main PDFs ordered by date_created descending
    query = (
        db.query(ReferencefileModel)
        .filter(
            ReferencefileModel.file_class == "main",
            ReferencefileModel.file_publication_status == "final",
            ReferencefileModel.pdf_type == "pdf"
        )
    )

    # Optionally exclude references that already have XML files
    if skip_xml:
        xml_exists_subquery = (
            select(ReferencefileModel.reference_id)
            .where(ReferencefileModel.file_extension.in_(["xml", "nxml"]))
            .distinct()
        )
        query = query.filter(~ReferencefileModel.reference_id.in_(xml_exists_subquery))

    query = query.order_by(desc(ReferencefileModel.date_created)).limit(limit)

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


def get_unprocessed_pdfs_since_year(db: Session, since_year: int, skip_xml: bool = False) -> List[Dict]:  # pragma: no cover
    """
    Get main PDF files created since a given year that haven't been converted to markdown.

    A PDF is considered "unprocessed" if it has no associated markdown files
    (none of the 4 extraction methods: grobid, docling, marker, merged).

    Args:
        db: Database session.
        since_year: Year to filter from (e.g., 2025 means Jan 1, 2025 onwards).
        skip_xml: If True, skip papers that already have XML/nXML files.

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
    )

    # Optionally exclude references that already have XML files
    if skip_xml:
        xml_exists_subquery = (
            select(ReferencefileModel.reference_id)
            .where(ReferencefileModel.file_extension.in_(["xml", "nxml"]))
            .distinct()
        )
        query = query.filter(~ReferencefileModel.reference_id.in_(xml_exists_subquery))

    query = query.order_by(desc(ReferencefileModel.date_created))

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


def process_pdfs_since_year(since_year: int, skip_xml: bool = False):  # pragma: no cover
    """
    Process all unprocessed PDFs since a given year.

    Args:
        since_year: Year to filter from (e.g., 2025 means Jan 1, 2025 onwards).
        skip_xml: If True, skip papers that already have XML/nXML files.
    """
    start_time = time.time()

    engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
    new_session = sessionmaker(bind=engine, autoflush=True)
    db = new_session()

    logger.info(f"Starting PDF to Markdown conversion for PDFs since {since_year}")
    if skip_xml:
        logger.info("Skipping papers that already have XML/nXML files")

    # Get token
    try:
        token = get_pdfx_token()
    except Exception as e:
        logger.error(f"Failed to obtain PDFX token: {e}")
        return

    # Get unprocessed PDFs
    pdf_list = get_unprocessed_pdfs_since_year(db, since_year=since_year, skip_xml=skip_xml)
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
    logger.info(f"  Total time: {total_elapsed:.2f}s ({total_elapsed / 60:.2f} minutes)")
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


def process_single_pdf(  # pragma: no cover
    db: Session,
    ref_file_info: Dict,
    token: str,
    methods_to_extract: Optional[List[str]] = None
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
        poll_pdfx_status(process_id, token)

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


def process_newest_pdfs(limit: int = 50, skip_xml: bool = False):  # pragma: no cover
    """
    Process the newest main PDFs and convert them to markdown.

    Args:
        limit: Number of newest PDFs to process.
        skip_xml: If True, skip papers that already have XML/nXML files.
    """
    start_time = time.time()

    engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
    new_session = sessionmaker(bind=engine, autoflush=True)
    db = new_session()

    logger.info(f"Starting PDF to Markdown conversion for {limit} newest PDFs")
    if skip_xml:
        logger.info("Skipping papers that already have XML/nXML files")

    # Get token
    try:
        token = get_pdfx_token()
    except Exception as e:
        logger.error(f"Failed to obtain PDFX token: {e}")
        return

    # Get newest PDFs
    pdf_list = get_newest_main_pdfs(db, limit=limit, skip_xml=skip_xml)
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
    logger.info(f"  Total time: {total_elapsed:.2f}s ({total_elapsed / 60:.2f} minutes)")
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


def main():  # pragma: no cover
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
    logger.info(f"  Total time: {total_elapsed:.2f}s ({total_elapsed / 60:.2f} minutes)")
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

  # Process newest PDFs, skipping those with XML files
  python -m agr_literature_service.lit_processing.pdf2md.pdf2md --newest 50 --skip-xml
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
    parser.add_argument(
        "--skip-xml",
        action="store_true",
        help="Skip papers that already have XML/nXML files"
    )

    args = parser.parse_args()

    if args.newest and args.since:
        print("Error: Cannot use --newest and --since together. Choose one.")
        exit(1)

    if args.skip_xml and not (args.newest or args.since):
        print("Error: --skip-xml requires --newest or --since mode.")
        exit(1)

    if args.newest:
        print(f"Processing {args.newest} newest main PDFs...")
        if args.skip_xml:
            print("Skipping papers with existing XML/nXML files.")
        result = process_newest_pdfs(limit=args.newest, skip_xml=args.skip_xml)
        print(f"\nResults: {result}")
    elif args.since:
        print(f"Processing all unprocessed PDFs since {args.since}...")
        if args.skip_xml:
            print("Skipping papers with existing XML/nXML files.")
        result = process_pdfs_since_year(since_year=args.since, skip_xml=args.skip_xml)
        print(f"\nResults: {result}")
    else:
        # Workflow mode (default)
        main()
