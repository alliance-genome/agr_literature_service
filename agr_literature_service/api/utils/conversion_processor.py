"""
Background task for on-demand file conversion.

Dispatches to the existing batch conversion primitives in
``agr_literature_service.lit_processing.pdf2md.pdf2md_utils`` rather than
re-implementing any conversion logic here. Per-source dedup (SCRUM-6041):
the assessment's ``pending_main`` / ``pending_supplements`` lists drive
conversion, so already-converted source files are never re-processed —
even when a later workflow tag (added by a different MOD or after new
files were uploaded) re-triggers the job for the same reference.
"""
import logging
from typing import Any, Dict, List, Optional, Tuple

from agr_literature_service.api.database.main import SessionLocal
from agr_literature_service.api.utils.conversion_job_manager import conversion_manager

logger = logging.getLogger(__name__)


def _all_extraction_methods() -> List[str]:
    from agr_literature_service.lit_processing.pdf2md.pdf2md_utils import (
        EXTRACTION_METHODS,
    )
    return list(EXTRACTION_METHODS.keys())


def _record_single_pdf(
    db: Any,
    job_id: str,
    reference_id: int,
    pdf_file: Any,
    success: bool,
    methods_uploaded: Any,
    error: Optional[str],
    output_file_class: str,
) -> None:
    """Record one per-source progress entry for a PDF that was just processed.

    The converted side points at the merged output row when the merged method
    succeeded; otherwise it's left null so the on-demand response surfaces the
    failure cleanly.
    """
    from agr_literature_service.api.crud.file_conversion_crud import (
        find_converted_referencefile_id,
    )
    merged_uploaded = "merged" in (methods_uploaded or [])
    source_display_name = pdf_file.display_name
    converted_display_name = (
        f"{source_display_name}_merged" if merged_uploaded else None
    )
    converted_rf_id: Optional[int] = None
    if merged_uploaded and converted_display_name is not None:
        converted_rf_id = find_converted_referencefile_id(
            db, reference_id, converted_display_name, output_file_class,
        )
    conversion_manager.record_file_progress(
        job_id=job_id,
        source_display_name=source_display_name,
        source_file_class=pdf_file.file_class,
        source_referencefile_id=int(pdf_file.referencefile_id),
        converted_display_name=converted_display_name,
        converted_file_class=output_file_class if merged_uploaded else None,
        converted_referencefile_id=converted_rf_id,
        success=success,
        error=error,
    )


def _convert_pending_nxml(
    db: Any, job_id: str, reference_id: int, reference_curie: str,
    nxml_ref_file: Any, mod_abbreviation: Optional[str],
) -> Tuple[bool, Optional[str]]:
    """Run nXML→MD conversion for a single pending nXML source."""
    from agr_literature_service.api.crud.file_conversion_crud import (
        find_converted_referencefile_id,
    )
    from agr_literature_service.lit_processing.pdf2md.pdf2md_utils import (
        process_nxml_to_markdown,
    )
    success, error = process_nxml_to_markdown(
        db=db,
        nxml_ref_file=nxml_ref_file,
        reference_curie=reference_curie,
        mod_abbreviation=mod_abbreviation,
    )
    converted_display_name = (
        f"{nxml_ref_file.display_name}_nxml" if success else None
    )
    converted_rf_id: Optional[int] = None
    if success and converted_display_name is not None:
        converted_rf_id = find_converted_referencefile_id(
            db, reference_id, converted_display_name, "converted_merged_main",
        )
    conversion_manager.record_file_progress(
        job_id=job_id,
        source_display_name=nxml_ref_file.display_name,
        source_file_class="nXML",
        source_referencefile_id=int(nxml_ref_file.referencefile_id),
        converted_display_name=converted_display_name,
        converted_file_class=(
            "converted_merged_main" if success else None
        ),
        converted_referencefile_id=converted_rf_id,
        success=success,
        error=error,
    )
    return success, error


def _convert_pending_pdf(
    db: Any, job_id: str, reference_id: int, reference_curie: str,
    pdf_file: Any, output_file_class: str, token: str,
) -> Tuple[bool, Optional[str]]:
    """Run PDFX→MD conversion for a single pending main or supplement PDF."""
    from agr_literature_service.lit_processing.pdf2md.pdf2md_utils import (
        _process_single_pdf_file,
    )
    try:
        success, methods_uploaded, error = _process_single_pdf_file(
            db=db,
            pdf_file=pdf_file,
            reference_curie=reference_curie,
            token=token,
            methods_to_extract=_all_extraction_methods(),
        )
    except Exception as exc:
        success, methods_uploaded, error = False, [], str(exc)
    _record_single_pdf(
        db, job_id, reference_id, pdf_file,
        success, methods_uploaded, error,
        output_file_class,
    )
    return success, error


def _convert_pending_main(
    db: Any, job_id: str, reference_id: int, reference_curie: str,
    assessment: Dict[str, Any], get_token,
) -> Tuple[bool, List[str]]:
    """Convert each entry in assessment['pending_main']. Returns
    (any_failure, failure_messages)."""
    any_failure = False
    failure_messages: List[str] = []
    for entry in assessment.get("pending_main") or []:
        ref_file = entry["ref_file"]
        if entry["kind"] == "nxml":
            success, error = _convert_pending_nxml(
                db, job_id, reference_id, reference_curie,
                ref_file, assessment["mod_abbreviation"],
            )
            tag = "nXML main"
        else:
            success, error = _convert_pending_pdf(
                db, job_id, reference_id, reference_curie,
                ref_file, "converted_merged_main", get_token(),
            )
            tag = f"PDF main '{ref_file.display_name}'"
        if not success:
            any_failure = True
            if error:
                failure_messages.append(f"{tag}: {error}")
    return any_failure, failure_messages


def _convert_pending_supplements(
    db: Any, job_id: str, reference_id: int, reference_curie: str,
    assessment: Dict[str, Any], get_token,
) -> Tuple[bool, List[str]]:
    """Convert each entry in assessment['pending_supplements']. Returns
    (any_failure, failure_messages)."""
    any_failure = False
    failure_messages: List[str] = []
    for ref_file in assessment.get("pending_supplements") or []:
        success, error = _convert_pending_pdf(
            db, job_id, reference_id, reference_curie,
            ref_file, "converted_merged_supplement", get_token(),
        )
        if not success:
            any_failure = True
            if error:
                failure_messages.append(
                    f"PDF supplement '{ref_file.display_name}': {error}"
                )
    return any_failure, failure_messages


def run_conversion_job(job_id: str, reference_id: int, reference_curie: str,
                       overwrite_tei_md: bool = False) -> None:
    """
    Execute a pending conversion job.

    Opens its own SessionLocal (the request's DB session has closed by the time
    the BackgroundTasks runner invokes this). The assessment's
    ``pending_main`` / ``pending_supplements`` lists tell us exactly which
    source files still need conversion; we run NXML or PDFX once per pending
    source, skip everything that's already been converted, and report
    progress to ``conversion_manager`` before marking the job completed
    or failed.

    ``overwrite_tei_md`` — when True, TEI-derived converted Markdown rows
    (display_name ending in ``_tei``) are not counted as cached, so the job
    will re-run the conversion. On success, the legacy TEI-derived rows are
    deleted so the new higher-quality output replaces them.

    Never raises: any exception is captured on the job.
    """
    from agr_literature_service.api.crud.file_conversion_crud import (
        _assess_reference,
        delete_tei_derived_md_rows,
    )
    from agr_literature_service.api.crud.reference_utils import get_reference

    db = SessionLocal()
    try:
        reference = get_reference(db, str(reference_id), load_referencefiles=True)
        assessment = _assess_reference(db, reference, overwrite_tei_md=overwrite_tei_md)

        # Lazily fetch the PDFX token: we only need it when there's at
        # least one pending PDF source (main or supplement). nXML-only
        # conversions skip this.
        cached_token: Dict[str, str] = {}

        def get_token() -> str:
            if "token" not in cached_token:
                from agr_cognito_py import get_admin_token
                cached_token["token"] = get_admin_token()
            return cached_token["token"]

        main_failure, main_errors = _convert_pending_main(
            db, job_id, reference_id, reference_curie, assessment, get_token,
        )
        supp_failure, supp_errors = _convert_pending_supplements(
            db, job_id, reference_id, reference_curie, assessment, get_token,
        )

        any_failure = main_failure or supp_failure
        failure_messages = main_errors + supp_errors

        overall_success = not any_failure
        if overwrite_tei_md and overall_success:
            # Re-query the reference so the freshly-uploaded converted rows
            # are visible, then drop any legacy TEI-derived MD rows.
            db.expire_all()
            reference = get_reference(db, str(reference_id), load_referencefiles=True)
            delete_tei_derived_md_rows(
                db, reference,
                ["converted_merged_main", "converted_merged_supplement"],
            )
        conversion_manager.complete_job(
            job_id=job_id,
            success=overall_success,
            error="; ".join(failure_messages) if failure_messages else "",
        )

    except Exception as exc:
        logger.exception(f"Conversion job {job_id} failed with unexpected error")
        conversion_manager.complete_job(job_id=job_id, success=False, error=str(exc))
    finally:
        db.close()
