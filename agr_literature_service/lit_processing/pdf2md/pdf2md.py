"""
Reference to Markdown conversion.

For each reference processed, this script prefers converting a publisher-provided
nXML/JATS file to Markdown (via agr_abc_document_parsers) when available, and
falls back to extracting the main PDF via the PDFX service
(https://pdfx.alliancegenome.org) otherwise. All supplemental PDFs for the
reference are also converted via PDFX.

PDFX supports multiple extraction methods (grobid, docling, marker, merged) and
stores each output as a separate file; nXML conversion produces a single
merged-style markdown output.
"""
import logging
import time
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
from agr_cognito_py import ModAccess, get_admin_token
from agr_literature_service.lit_processing.utils.report_utils import send_report

from agr_literature_service.lit_processing.pdf2md.pdf2md_utils import (
    EXTRACTION_METHODS,
    submit_pdf_to_pdfx,
    poll_pdfx_status,
    download_pdfx_result,
    get_nxml_referencefile,
    process_nxml_to_markdown,
    process_supplemental_pdfs,
)


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


def get_unprocessed_pdfs_since_year(
    db: Session,
    since_year: int,
    skip_xml: bool = False,
    limit: Optional[int] = None
) -> List[Dict]:  # pragma: no cover
    """
    Get main PDF files for papers published since a given year that haven't been converted to markdown.

    A PDF is considered "unprocessed" if it has no associated markdown files
    (none of the 4 extraction methods: grobid, docling, marker, merged).

    Args:
        db: Database session.
        since_year: Year to filter from (e.g., 2025 means papers published in 2025 or later).
        skip_xml: If True, skip papers that already have XML/nXML files.
        limit: Optional limit on number of PDFs to return. If None, return all.

    Returns:
        List of dicts with referencefile info for PDFs needing processing.
    """
    results = []

    # date_published is a String column with formats like '2025-01-01' or '2025'
    # String comparison with year works correctly for ISO-like formats
    since_year_str = str(since_year)

    # File classes for markdown outputs
    md_file_classes = list(EXTRACTION_METHODS.values())

    logger.info(f"Querying for unprocessed PDFs for papers published since {since_year}...")

    # Subquery to find reference_ids that already have markdown files
    md_exists_subquery = (
        select(ReferencefileModel.reference_id)
        .where(ReferencefileModel.file_class.in_(md_file_classes))
        .distinct()
    )

    # Query for main PDFs for papers published since the given year
    query = (
        db.query(ReferencefileModel)
        .join(ReferenceModel, ReferencefileModel.reference_id == ReferenceModel.reference_id)
        .filter(
            ReferencefileModel.file_class == "main",
            ReferencefileModel.file_publication_status == "final",
            ReferencefileModel.pdf_type == "pdf",
            ReferenceModel.date_published >= since_year_str,
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

    query = query.order_by(desc(ReferenceModel.date_published))

    if limit:
        query = query.limit(limit)

    all_pdfs = query.all()
    logger.info(f"Found {len(all_pdfs)} unprocessed PDFs for papers published since {since_year}")

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
            "date_published": reference.date_published
        })

    return results


def _process_reference_list(  # pragma: no cover
    db: Session,
    reference_list: List[Dict],
    token: str,
    start_time: float,
    summary_suffix: str = "",
    report_subject: str = "pdf2md conversion errors",
    prefer_nxml: bool = True,
    process_supplements: bool = True
) -> Dict:
    """
    Common processing loop for reference-to-Markdown conversion.

    Used by process_references_since_year and process_newest_references. Each
    reference is converted via nXML when available (unless prefer_nxml is
    False), or via PDFX otherwise; supplemental PDFs are also processed when
    process_supplements is True.

    Args:
        db: Database session.
        reference_list: List of reference info dicts to process.
        token: Initial PDFX bearer token.
        start_time: Start time for timing statistics.
        summary_suffix: Suffix for log summary title (e.g., " (Since 2025)").
        report_subject: Subject line for error report email.
        prefer_nxml: Prefer nXML over PDFX for main-file conversion.
        process_supplements: Also convert supplemental PDFs.

    Returns:
        Dict with processing statistics.
    """
    total_count = len(reference_list)
    success_count = 0
    failure_count = 0
    objects_with_errors: List[Dict] = []
    ref_times: List[float] = []

    for idx, ref_file_info in enumerate(reference_list, 1):
        ref_start_time = time.time()
        reference_curie = ref_file_info["reference_curie"]

        logger.info(f"Processing {idx}/{total_count}: {reference_curie}")

        # Refresh token if needed
        try:
            token = get_admin_token()
        except Exception as e:
            logger.error(f"Failed to refresh token: {e}")
            failure_count += 1
            objects_with_errors.append({
                "reference_curie": reference_curie,
                "display_name": ref_file_info["display_name"],
                "file_extension": ref_file_info["file_extension"],
                "mod_abbreviation": ref_file_info.get("mod_abbreviation", "N/A"),
                "error": f"Failed to refresh token: {e}"
            })
            continue

        success, error_msg = process_single_reference(
            db, ref_file_info, token,
            prefer_nxml=prefer_nxml,
            process_supplements=process_supplements
        )

        ref_elapsed = time.time() - ref_start_time
        ref_times.append(ref_elapsed)

        if success:
            success_count += 1
            logger.info(f"Completed {reference_curie} in {ref_elapsed:.2f}s")
        else:
            failure_count += 1
            objects_with_errors.append({
                "reference_curie": reference_curie,
                "display_name": ref_file_info["display_name"],
                "file_extension": ref_file_info["file_extension"],
                "mod_abbreviation": ref_file_info.get("mod_abbreviation", "N/A"),
                "error": error_msg
            })
            logger.error(f"Failed {reference_curie} after {ref_elapsed:.2f}s: {error_msg}")

    # Calculate timing statistics
    total_elapsed = time.time() - start_time
    avg_time = sum(ref_times) / len(ref_times) if ref_times else 0
    min_time = min(ref_times) if ref_times else 0
    max_time = max(ref_times) if ref_times else 0

    # Log summary
    logger.info("=" * 60)
    logger.info(f"Reference to Markdown Conversion Summary{summary_suffix}")
    logger.info("=" * 60)
    logger.info(f"Total references processed: {total_count}")
    logger.info(f"Successful: {success_count}")
    logger.info(f"Failed: {failure_count}")
    logger.info("-" * 60)
    logger.info("Timing Statistics:")
    logger.info(f"  Total time: {total_elapsed:.2f}s ({total_elapsed / 60:.2f} minutes)")
    logger.info(f"  Average per reference: {avg_time:.2f}s")
    logger.info(f"  Min time: {min_time:.2f}s")
    logger.info(f"  Max time: {max_time:.2f}s")
    logger.info("=" * 60)

    # Send error report if there were failures
    if objects_with_errors:
        error_message = f"Reference to Markdown Conversion Errors{summary_suffix}\n\n"
        error_message += f"Total: {total_count}, Success: {success_count}, Failed: {failure_count}\n\n"
        error_message += f"Total time: {total_elapsed:.2f}s, Avg per reference: {avg_time:.2f}s\n\n"
        error_message += "Failed conversions:\n"
        for error_obj in objects_with_errors:
            error_message += f"{error_obj['mod_abbreviation']}\t{error_obj['reference_curie']}\t"
            error_message += f"{error_obj['display_name']}.{error_obj['file_extension']}\t{error_obj['error']}\n"

        send_report(report_subject, error_message)

    return {
        "total": total_count,
        "success": success_count,
        "failed": failure_count,
        "total_time_seconds": total_elapsed,
        "avg_time_per_reference": avg_time,
        "min_time": min_time,
        "max_time": max_time
    }


def process_references_since_year(  # pragma: no cover
    since_year: int,
    skip_xml: bool = False,
    limit: Optional[int] = None,
    prefer_nxml: bool = True,
    process_supplements: bool = True
):
    """
    Process all unconverted references whose main PDF is from a given year onwards.

    Enumeration is keyed on main-PDF presence (see get_unprocessed_pdfs_since_year);
    per-reference processing prefers nXML (unless prefer_nxml=False) and also
    handles supplemental PDFs (unless process_supplements=False).

    Args:
        since_year: Year to filter from (e.g., 2025 means Jan 1, 2025 onwards).
        skip_xml: If True, skip papers that already have XML/nXML files.
        limit: Optional limit on number of references to process. If None, process all.
        prefer_nxml: Prefer nXML over PDFX for main-file conversion.
        process_supplements: Also convert supplemental PDFs.
    """
    start_time = time.time()

    engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
    new_session = sessionmaker(bind=engine, autoflush=True)
    db = new_session()

    try:
        logger.info(f"Starting reference-to-Markdown conversion for papers since {since_year}")
        if skip_xml:
            logger.info("Skipping papers that already have XML/nXML files")
        if limit:
            logger.info(f"Limiting to {limit} references")
        if not prefer_nxml:
            logger.info("Forcing PDFX for main file (nXML preference disabled)")
        if not process_supplements:
            logger.info("Skipping supplemental PDFs")

        # Get token
        try:
            token = get_admin_token()
        except Exception as e:
            logger.error(f"Failed to obtain PDFX token: {e}")
            return None

        # Get unprocessed references
        reference_list = get_unprocessed_pdfs_since_year(
            db, since_year=since_year, skip_xml=skip_xml, limit=limit
        )
        logger.info(f"Found {len(reference_list)} unprocessed references to convert")

        if not reference_list:
            logger.info("No unprocessed references found. Exiting.")
            return {"total": 0, "success": 0, "failed": 0}

        return _process_reference_list(
            db=db,
            reference_list=reference_list,
            token=token,
            start_time=start_time,
            summary_suffix=f" (Since {since_year})",
            report_subject=f"pdf2md conversion errors (since {since_year})",
            prefer_nxml=prefer_nxml,
            process_supplements=process_supplements
        )
    finally:
        db.close()


def _convert_main_pdf_via_pdfx(  # pragma: no cover
    db: Session,
    referencefile_id: int,
    reference_curie: str,
    display_name: str,
    mod_abbreviation: Optional[str],
    token: str,
    methods_to_extract: List[str]
) -> Tuple[bool, Optional[str]]:
    """
    Convert a main PDF via PDFX and upload the resulting markdown files.

    Returns:
        Tuple of (success: bool, error_message: Optional[str]).
    """
    try:
        file_content = download_file(
            db=db,
            referencefile_id=referencefile_id,
            mod_access=ModAccess.ALL_ACCESS,
            use_in_api=False
        )

        # Determine which methods to request (merged requires merge=True)
        request_methods = [m for m in methods_to_extract if m != "merged"]
        include_merge = "merged" in methods_to_extract

        process_id = submit_pdf_to_pdfx(
            file_content=file_content,
            token=token,
            methods=",".join(request_methods) if request_methods else "grobid,docling,marker",
            merge=include_merge,
            reference_curie=reference_curie,
            mod_abbreviation=mod_abbreviation
        )

        poll_pdfx_status(process_id, token)

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
            logger.info(
                f"Successfully processed main PDF for {reference_curie} "
                f"with methods: {successful_methods}"
            )
            return True, None
        return False, "No methods successfully extracted"

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Failed to process main PDF for {reference_curie}: {error_msg}")
        return False, error_msg


def process_single_reference(  # pragma: no cover
    db: Session,
    ref_file_info: Dict,
    token: str,
    methods_to_extract: Optional[List[str]] = None,
    prefer_nxml: bool = True,
    process_supplements: bool = True
) -> Tuple[bool, Optional[str]]:
    """
    Convert the main content of a single reference to Markdown, plus any
    supplemental PDFs.

    Flow:
      1. If prefer_nxml is True and the reference has an nXML file, convert
         it to markdown via agr_abc_document_parsers. On nXML failure, fall
         back to PDFX.
      2. Otherwise, run the main PDF through PDFX (grobid, docling, marker, merged).
      3. If process_supplements is True, process every supplemental PDF for
         the reference via PDFX.

    Supplement failures are logged/reported but do not fail the reference; the
    reference succeeds if its main conversion produced any markdown output.

    Args:
        db: Database session.
        ref_file_info: Dict with reference/file info. Must include
            reference_id, reference_curie, display_name, and (for PDF fallback)
            referencefile_id of the main PDF plus file_extension. May include
            mod_abbreviation.
        token: PDFX bearer token.
        methods_to_extract: List of methods to extract (default: all 4).
        prefer_nxml: When True (default), prefer nXML conversion over PDFX for
            the main file when an nXML file is available. When False, always
            use PDFX on the main PDF.
        process_supplements: When True (default), also convert supplemental
            PDFs via PDFX. When False, skip supplement processing.

    Returns:
        Tuple of (success: bool, error_message: Optional[str]).
    """
    if methods_to_extract is None:
        methods_to_extract = list(EXTRACTION_METHODS.keys())

    reference_id = ref_file_info["reference_id"]
    reference_curie = ref_file_info["reference_curie"]
    display_name = ref_file_info["display_name"]
    mod_abbreviation = ref_file_info.get("mod_abbreviation")

    main_success = False
    main_error: Optional[str] = None

    # 1. Prefer nXML when available (unless forced to PDFX)
    if prefer_nxml:
        nxml_ref_file = get_nxml_referencefile(db, reference_id)
        if nxml_ref_file is not None:
            main_success, main_error = process_nxml_to_markdown(
                db=db,
                nxml_ref_file=nxml_ref_file,
                reference_curie=reference_curie,
                mod_abbreviation=mod_abbreviation
            )
            if not main_success:
                logger.warning(
                    f"nXML conversion failed for {reference_curie} ({main_error}); "
                    f"falling back to PDFX on main PDF"
                )

    # 2. Fall back to (or use directly) PDFX on the main PDF
    if not main_success:
        referencefile_id = ref_file_info.get("referencefile_id")
        if referencefile_id is None:
            combined = "No nXML conversion succeeded and no main PDF available"
            if main_error:
                combined = f"{combined} (nXML error: {main_error})"
            return False, combined

        pdfx_success, pdfx_error = _convert_main_pdf_via_pdfx(
            db=db,
            referencefile_id=referencefile_id,
            reference_curie=reference_curie,
            display_name=display_name,
            mod_abbreviation=mod_abbreviation,
            token=token,
            methods_to_extract=methods_to_extract
        )
        main_success = pdfx_success
        if not pdfx_success:
            if main_error:
                main_error = f"nXML error: {main_error}; PDFX error: {pdfx_error}"
            else:
                main_error = pdfx_error

    # 3. Optionally convert supplemental PDFs regardless of main path used
    if process_supplements:
        try:
            sup_succeeded, sup_failed, sup_errors = process_supplemental_pdfs(
                db=db,
                reference_id=reference_id,
                reference_curie=reference_curie,
                token=token,
                methods_to_extract=methods_to_extract
            )
            if sup_succeeded or sup_failed:
                logger.info(
                    f"Supplemental PDFs for {reference_curie}: "
                    f"{sup_succeeded} succeeded, {sup_failed} failed"
                )
            for err in sup_errors:
                logger.error(f"Supplemental PDF error for {reference_curie}: {err}")
        except Exception as e:
            logger.error(
                f"Unexpected error while processing supplemental PDFs for "
                f"{reference_curie}: {e}"
            )

    if main_success:
        return True, None
    return False, main_error or "Main reference conversion failed"


def process_newest_references(  # pragma: no cover
    limit: int = 50,
    skip_xml: bool = False,
    prefer_nxml: bool = True,
    process_supplements: bool = True
):
    """
    Process the newest references (keyed on main-PDF enumeration) and convert
    them to markdown (nXML-preferred unless disabled) plus their supplemental
    PDFs (unless disabled).

    Args:
        limit: Number of newest references to process.
        skip_xml: If True, skip papers that already have XML/nXML files.
        prefer_nxml: Prefer nXML over PDFX for main-file conversion.
        process_supplements: Also convert supplemental PDFs.
    """
    start_time = time.time()

    engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
    new_session = sessionmaker(bind=engine, autoflush=True)
    db = new_session()

    try:
        logger.info(
            f"Starting reference-to-Markdown conversion for {limit} newest references"
        )
        if skip_xml:
            logger.info("Skipping papers that already have XML/nXML files")
        if not prefer_nxml:
            logger.info("Forcing PDFX for main file (nXML preference disabled)")
        if not process_supplements:
            logger.info("Skipping supplemental PDFs")

        # Get token
        try:
            token = get_admin_token()
        except Exception as e:
            logger.error(f"Failed to obtain PDFX token: {e}")
            return None

        # Enumerate via newest main PDFs
        reference_list = get_newest_main_pdfs(db, limit=limit, skip_xml=skip_xml)
        logger.info(f"Found {len(reference_list)} references to process")

        if not reference_list:
            logger.info("No references found to process. Exiting.")
            return {"total": 0, "success": 0, "failed": 0}

        return _process_reference_list(
            db=db,
            reference_list=reference_list,
            token=token,
            start_time=start_time,
            summary_suffix="",
            report_subject="pdf2md conversion errors",
            prefer_nxml=prefer_nxml,
            process_supplements=process_supplements
        )
    finally:
        db.close()


def _resolve_workflow_ref_file_info(  # pragma: no cover
    db: Session,
    ref_id: int,
    reference_curie: str,
    mod_abbreviation: str,
    prefer_nxml: bool
) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Look up the main source file (nXML preferred when enabled, else main PDF)
    for a workflow job and build the ref_file_info dict.

    Returns:
        (ref_file_info, None) when a source was found; (None, error_msg) when
        neither an nXML nor a main PDF is available for the reference.
    """
    nxml_ref_file = (
        get_nxml_referencefile(db, ref_id) if prefer_nxml else None
    )
    ref_file_id = get_main_pdf_referencefile_id(
        db=db,
        curie_or_reference_id=str(ref_id),
        mod_abbreviation=mod_abbreviation
    )

    if nxml_ref_file is None and not ref_file_id:
        if prefer_nxml:
            return None, "No nXML or main PDF found for reference"
        return None, "No main PDF found for reference"

    if nxml_ref_file is not None:
        display_name = nxml_ref_file.display_name
        file_extension = nxml_ref_file.file_extension
    else:
        pdf_ref_obj = db.query(ReferencefileModel).filter(
            ReferencefileModel.referencefile_id == ref_file_id
        ).one()
        display_name = pdf_ref_obj.display_name
        file_extension = pdf_ref_obj.file_extension

    return {
        "referencefile_id": ref_file_id,
        "reference_id": ref_id,
        "reference_curie": reference_curie,
        "display_name": display_name,
        "file_extension": file_extension,
        "mod_abbreviation": mod_abbreviation
    }, None


def _build_workflow_error_record(  # pragma: no cover
    db: Session,
    reference_curie: str,
    display_name: str,
    file_extension: str,
    mod_abbreviation: str,
    error_msg: str
) -> Dict:
    """Build a failure record for workflow-mode error reporting."""
    mod_cross_ref = db.query(CrossReferenceModel).join(
        ReferenceModel, CrossReferenceModel.reference_id == ReferenceModel.reference_id
    ).filter(
        ReferenceModel.curie == reference_curie,
        CrossReferenceModel.curie_prefix == mod_abbreviation
    ).one_or_none()

    return {
        "reference_curie": reference_curie,
        "display_name": display_name,
        "file_extension": file_extension,
        "mod_abbreviation": mod_abbreviation,
        "mod_cross_ref": mod_cross_ref.curie if mod_cross_ref else "N/A",
        "error": error_msg
    }


def main(  # pragma: no cover
    prefer_nxml: bool = True,
    process_supplements: bool = True
):
    """
    Main entry point for workflow-based reference-to-Markdown conversion.

    Processes references based on workflow tags (text_convert_job). For each
    reference: prefers nXML over PDF for the main content conversion (unless
    prefer_nxml=False) and also converts supplemental PDFs (unless
    process_supplements=False). When prefer_nxml is True, jobs are marked
    failed only if neither an nXML nor a main PDF is available; when
    prefer_nxml is False, a main PDF is required.

    Args:
        prefer_nxml: Prefer nXML over PDFX for main-file conversion.
        process_supplements: Also convert supplemental PDFs.
    """
    start_time = time.time()

    engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
    new_session = sessionmaker(bind=engine, autoflush=True)
    db = new_session()

    try:
        limit = 1000
        offset = 0
        all_jobs = []

        if not prefer_nxml:
            logger.info("Forcing PDFX for main file (nXML preference disabled)")
        if not process_supplements:
            logger.info("Skipping supplemental PDFs")

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

        mod_abbreviation_from_mod_id: Dict[int, str] = {}
        objects_with_errors = []
        total_count = len(all_jobs)
        success_count = 0
        failure_count = 0
        skipped_count = 0
        ref_times = []

        for idx, job in enumerate(all_jobs, 1):
            ref_start_time = time.time()
            error_msg: Optional[str] = None

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

            ref_file_info, resolve_error = _resolve_workflow_ref_file_info(
                db=db,
                ref_id=ref_id,
                reference_curie=reference_curie,
                mod_abbreviation=mod_abbreviation,
                prefer_nxml=prefer_nxml
            )

            if ref_file_info is None:
                skipped_count += 1
                error_msg = resolve_error or "Could not resolve reference source file"
                logger.warning(f"{error_msg} for {reference_curie}; marking job as failed")
                job_change_atp_code(db, reference_workflow_tag_id, "on_failed")
                objects_with_errors.append(_build_workflow_error_record(
                    db, reference_curie, "N/A", "N/A", mod_abbreviation, error_msg
                ))
                continue

            display_name = ref_file_info["display_name"]
            file_extension = ref_file_info["file_extension"]

            # Refresh token if needed
            token = get_admin_token()

            success, error_msg = process_single_reference(
                db, ref_file_info, token,
                prefer_nxml=prefer_nxml,
                process_supplements=process_supplements
            )

            ref_elapsed = time.time() - ref_start_time
            ref_times.append(ref_elapsed)

            if success:
                success_count += 1
                job_change_atp_code(db, reference_workflow_tag_id, "on_success")
                logger.info(f"Completed {reference_curie} in {ref_elapsed:.2f}s")
            else:
                failure_count += 1
                job_change_atp_code(db, reference_workflow_tag_id, "on_failed")
                logger.error(f"Failed {reference_curie} after {ref_elapsed:.2f}s")
                objects_with_errors.append(_build_workflow_error_record(
                    db, reference_curie, display_name, file_extension,
                    mod_abbreviation, error_msg or "Unknown"
                ))

        # Calculate timing statistics
        total_elapsed = time.time() - start_time
        avg_time = sum(ref_times) / len(ref_times) if ref_times else 0
        min_time = min(ref_times) if ref_times else 0
        max_time = max(ref_times) if ref_times else 0

        # Log summary
        logger.info("=" * 60)
        logger.info("Reference to Markdown Conversion Summary (Workflow Mode)")
        logger.info("=" * 60)
        logger.info(f"Total jobs processed: {total_count}")
        logger.info(f"Successful: {success_count}")
        logger.info(f"Failed: {failure_count}")
        logger.info(f"Skipped (no nXML or main PDF): {skipped_count}")
        logger.info("-" * 60)
        logger.info("Timing Statistics:")
        logger.info(f"  Total time: {total_elapsed:.2f}s ({total_elapsed / 60:.2f} minutes)")
        logger.info(f"  Average per reference: {avg_time:.2f}s")
        logger.info(f"  Min time: {min_time:.2f}s")
        logger.info(f"  Max time: {max_time:.2f}s")
        logger.info("=" * 60)

        # Send error report
        if objects_with_errors:
            error_message = "Reference to Markdown Conversion Errors (Workflow Mode)\n\n"
            error_message += f"Total: {total_count}, Success: {success_count}, "
            error_message += f"Failed: {failure_count}, Skipped: {skipped_count}\n\n"
            error_message += f"Total time: {total_elapsed:.2f}s, Avg per reference: {avg_time:.2f}s\n\n"
            error_message += "Failed conversions:\n"
            for error_obj in objects_with_errors:
                error_message += f"{error_obj['mod_abbreviation']}\t{error_obj['mod_cross_ref']}\t"
                error_message += f"{error_obj['reference_curie']}\t{error_obj['display_name']}."
                error_message += f"{error_obj['file_extension']}\t{error_obj.get('error', 'Unknown')}\n"

            subject = "pdf2md conversion errors"
            send_report(subject, error_message)
    finally:
        db.close()


if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Convert references to Markdown. Prefers nXML over PDF for the main "
            "file and always processes supplemental PDFs via the PDFX service."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process references via workflow jobs (default)
  python -m agr_literature_service.lit_processing.pdf2md.pdf2md

  # Process N newest references (enumerated by newest main PDFs)
  python -m agr_literature_service.lit_processing.pdf2md.pdf2md --newest 50

  # Process all unconverted references for papers published since 2025
  python -m agr_literature_service.lit_processing.pdf2md.pdf2md --since 2025

  # Process newest references, skipping those with XML files
  python -m agr_literature_service.lit_processing.pdf2md.pdf2md --newest 50 --skip-xml
        """
    )
    parser.add_argument(
        "--newest",
        type=int,
        metavar="N",
        help="Process N newest references (enumerated by newest main PDFs)"
    )
    parser.add_argument(
        "--since",
        type=int,
        metavar="YEAR",
        help="Process unconverted references for papers published since YEAR (e.g., 2025)"
    )
    parser.add_argument(
        "--skip-xml",
        action="store_true",
        help="Skip papers that already have XML/nXML files"
    )
    parser.add_argument(
        "--limit",
        type=int,
        metavar="N",
        help="Limit number of references to process (only applies to --since mode)"
    )
    parser.add_argument(
        "--no-xml",
        dest="prefer_nxml",
        action="store_false",
        help=(
            "Always use PDFX for the main file, even when an nXML file is "
            "available (default: prefer nXML when present)"
        )
    )
    parser.add_argument(
        "--no-supplements",
        dest="process_supplements",
        action="store_false",
        help="Skip supplemental PDFs (default: process all supplemental PDFs)"
    )

    args = parser.parse_args()

    if args.newest and args.since:
        print("Error: Cannot use --newest and --since together. Choose one.")
        exit(1)

    if args.skip_xml and not (args.newest or args.since):
        print("Error: --skip-xml requires --newest or --since mode.")
        exit(1)

    if args.limit and not args.since:
        print("Error: --limit only applies to --since mode. Use --newest N instead.")
        exit(1)

    if args.newest:
        print(f"Processing {args.newest} newest references...")
        if args.skip_xml:
            print("Skipping papers with existing XML/nXML files.")
        if not args.prefer_nxml:
            print("Forcing PDFX for main file (--no-xml).")
        if not args.process_supplements:
            print("Skipping supplemental PDFs (--no-supplements).")
        result = process_newest_references(
            limit=args.newest,
            skip_xml=args.skip_xml,
            prefer_nxml=args.prefer_nxml,
            process_supplements=args.process_supplements
        )
        print(f"\nResults: {result}")
    elif args.since:
        print(f"Processing unconverted references since {args.since}...")
        if args.limit:
            print(f"Limiting to {args.limit} references.")
        if args.skip_xml:
            print("Skipping papers with existing XML/nXML files.")
        if not args.prefer_nxml:
            print("Forcing PDFX for main file (--no-xml).")
        if not args.process_supplements:
            print("Skipping supplemental PDFs (--no-supplements).")
        result = process_references_since_year(
            since_year=args.since,
            skip_xml=args.skip_xml,
            limit=args.limit,
            prefer_nxml=args.prefer_nxml,
            process_supplements=args.process_supplements
        )
        print(f"\nResults: {result}")
    else:
        # Workflow mode (default)
        main(
            prefer_nxml=args.prefer_nxml,
            process_supplements=args.process_supplements
        )
