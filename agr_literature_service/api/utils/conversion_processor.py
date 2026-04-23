"""
Background task for on-demand file conversion.

Dispatches to the existing batch conversion primitives in
``agr_literature_service.lit_processing.pdf2md.pdf2md_utils`` rather than
re-implementing any conversion logic here.
"""
import logging
from typing import Any, Dict, List, Optional

from agr_literature_service.api.database.main import SessionLocal
from agr_literature_service.api.utils.conversion_job_manager import conversion_manager

logger = logging.getLogger(__name__)


def _record_pdf_details(db: Any, job_id: str, reference_id: int,
                        details: List[Any], output_file_class: str) -> None:
    """Translate process_pdf_for_reference per-PDF details into job progress entries.
    Each source PDF produces one entry; if the 'merged' method succeeded, the
    converted side points at the merged output row for that PDF."""
    from agr_literature_service.api.crud.file_conversion_crud import find_converted_referencefile_id
    for detail in details:
        merged_uploaded = "merged" in (detail.get("methods_uploaded") or [])
        source_display_name = detail["display_name"]
        converted_display_name = f"{source_display_name}_merged" if merged_uploaded else None
        converted_rf_id: Optional[int] = None
        if merged_uploaded and converted_display_name is not None:
            converted_rf_id = find_converted_referencefile_id(
                db, reference_id, converted_display_name, output_file_class,
            )
        conversion_manager.record_file_progress(
            job_id=job_id,
            source_display_name=source_display_name,
            source_file_class=detail["file_class"],
            source_referencefile_id=detail.get("referencefile_id"),
            converted_display_name=converted_display_name,
            converted_file_class=output_file_class if merged_uploaded else None,
            converted_referencefile_id=converted_rf_id,
            success=detail["success"],
            error=detail.get("error"),
        )


def run_conversion_job(job_id: str, reference_id: int, reference_curie: str,
                       overwrite_tei_md: bool = False) -> None:
    """
    Execute a pending conversion job.

    Opens its own SessionLocal (the request's DB session has closed by the time
    the BackgroundTasks runner invokes this). Figures out what's missing for
    the reference and dispatches NXML or PDFX conversions to the batch
    primitives. Reports progress to ``conversion_manager`` and marks the job
    completed or failed before returning.

    ``overwrite_tei_md`` — when True, TEI-derived converted Markdown rows
    (display_name ending in ``_tei``) are not counted as cached, so the job
    will re-run the conversion. On success, the legacy TEI-derived rows are
    deleted so the new higher-quality output replaces them.

    Never raises: any exception is captured on the job.
    """
    from agr_literature_service.api.crud.file_conversion_crud import (
        _assess_reference,
        delete_tei_derived_md_rows,
        find_converted_referencefile_id,
    )
    from agr_literature_service.api.crud.reference_utils import get_reference
    from agr_literature_service.lit_processing.pdf2md.pdf2md_utils import (
        process_nxml_to_markdown,
        process_pdf_for_reference,
    )

    db = SessionLocal()
    try:
        reference = get_reference(db, str(reference_id), load_referencefiles=True)
        assessment = _assess_reference(db, reference, overwrite_tei_md=overwrite_tei_md)

        any_failure = False
        failure_messages = []

        # Main: prefer NXML (sync, fast) if available; else fall back to PDFX.
        if assessment["main_missing"]:
            if assessment["nxml_source"] is not None:
                nxml_source = assessment["nxml_source"]
                success, error = process_nxml_to_markdown(
                    db=db,
                    nxml_ref_file=nxml_source,
                    reference_curie=reference_curie,
                    mod_abbreviation=assessment["mod_abbreviation"],
                )
                converted_display_name = f"{nxml_source.display_name}_nxml" if success else None
                converted_rf_id: Optional[int] = None
                if success and converted_display_name is not None:
                    converted_rf_id = find_converted_referencefile_id(
                        db, reference_id, converted_display_name, "converted_merged_main",
                    )
                conversion_manager.record_file_progress(
                    job_id=job_id,
                    source_display_name=nxml_source.display_name,
                    source_file_class="nXML",
                    source_referencefile_id=nxml_source.referencefile_id,
                    converted_display_name=converted_display_name,
                    converted_file_class="converted_merged_main" if success else None,
                    converted_referencefile_id=converted_rf_id,
                    success=success,
                    error=error,
                )
                if not success:
                    any_failure = True
                    if error:
                        failure_messages.append(f"nXML main: {error}")
            elif assessment["main_pdf_available"]:
                result = process_pdf_for_reference(
                    db=db,
                    curie=reference_curie,
                    pdf_type="main",
                )
                _record_pdf_details(db, job_id, reference_id,
                                    result.get("details", []), "converted_merged_main")
                if not result.get("success"):
                    any_failure = True
                    err = result.get("error")
                    if err:
                        failure_messages.append(f"PDF main: {err}")

        # Supplements: always PDFX when any supplement PDFs exist.
        if assessment["supp_missing"] and assessment["supp_pdf_available"]:
            result = process_pdf_for_reference(
                db=db,
                curie=reference_curie,
                pdf_type="supplement",
            )
            _record_pdf_details(db, job_id, reference_id,
                                result.get("details", []), "converted_merged_supplement")
            if not result.get("success"):
                any_failure = True
                err = result.get("error")
                if err:
                    failure_messages.append(f"PDF supplements: {err}")

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
