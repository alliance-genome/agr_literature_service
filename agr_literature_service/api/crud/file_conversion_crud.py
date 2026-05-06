"""
Orchestrator for the on-demand file-conversion endpoint.

Decides, for a given reference, whether a converted Markdown file already
exists and, if not, triggers a conversion. NXML sources convert
synchronously (fast). PDFs go to PDFX in a background task unless the caller
sets ``wait=true``.

The endpoint reports ONLY conversion status (plus per-file progress from the
most recent job). Callers that want the resulting file listing should call the
existing ``GET /reference/referencefile/show_all/`` endpoint once the status
flips to ``converted``.

All actual conversion work is delegated to the batch primitives in
``agr_literature_service.lit_processing.pdf2md.pdf2md_utils``.
"""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import BackgroundTasks, HTTPException, status
from sqlalchemy.orm import Session

from agr_cognito_py import MOD_ACCESS_ABBR, ModAccess
from agr_literature_service.api.crud.reference_utils import get_reference
from agr_literature_service.api.crud.referencefile_utils import remove_from_s3_and_db
from agr_literature_service.api.models import ReferenceModel, ReferencefileModel
from agr_literature_service.api.utils.conversion_job_manager import ConversionJob, conversion_manager
from agr_literature_service.api.utils.conversion_processor import run_conversion_job
from agr_literature_service.lit_processing.pdf2md.pdf2md_utils import (
    PendingMainSource,
    get_nxml_referencefile,
    get_pdf_files_for_reference,
    is_eligible_for_supplement_conversion,
    pending_main_sources,
    pending_supplement_sources,
    process_nxml_to_markdown,
    sync_converted_file_mods_to_sources,
)

logger = logging.getLogger(__name__)

STATUS_CONVERTED = "converted"
STATUS_RUNNING = "running"
STATUS_FAILED = "failed"
STATUS_NO_SOURCES = "no_sources"


def _check_permission(reference: ReferenceModel, mod_access: Any) -> None:
    """
    Raise 403 if the caller cannot read this reference's files.

    Mirrors the permission logic in ``referencefile_crud.download_file``:
    open_access references are readable by anyone; otherwise the caller
    needs ALL_ACCESS or a MOD match against one of the referencefile mods.
    ``mod_access`` may be an empty list (unauthenticated), in which case
    only open_access references are readable.
    """
    if reference.copyright_license and reference.copyright_license.open_access:
        return
    if not isinstance(mod_access, ModAccess):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Reference is not open access and caller has no MOD access.",
        )
    if mod_access == ModAccess.ALL_ACCESS:
        return
    if mod_access == ModAccess.NO_ACCESS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Reference is not open access and caller has no MOD access.",
        )
    caller_mod = MOD_ACCESS_ABBR.get(mod_access)
    for ref_file in reference.referencefiles or []:
        for ref_file_mod in ref_file.referencefile_mods:
            if ref_file_mod.mod is None:
                return
            if caller_mod and ref_file_mod.mod.abbreviation == caller_mod:
                return
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Reference is not open access and caller's MOD does not match any referencefile.",
    )


def _assess_reference(db: Session, reference: ReferenceModel,
                      overwrite_tei_md: bool = False) -> Dict[str, Any]:
    """
    Inspect a reference's files and sources. Return a dict with:
        - main_cached: bool — at least one converted_merged_main row exists
        - supp_cached: bool — at least one converted_merged_supplement row exists
        - nxml_source: ReferencefileModel | None
        - main_pdfs: list[ReferencefileModel] of all main PDFs
        - supp_pdfs: list[ReferencefileModel] of all supplement PDFs
        - main_pdf_available: bool
        - supp_pdf_available: bool
        - pending_main: list[PendingMainSource] — main-side sources still
            needing conversion (per-source dedup; nXML preferred over PDF)
        - pending_supplements: list[ReferencefileModel] — supplement PDFs
            still needing conversion (per-source dedup)
        - main_missing: bool — pending_main is non-empty
        - supp_missing: bool — pending_supplements is non-empty
        - needs_async: bool — at least one pending source needs PDFX
        - mod_abbreviation: str | None — first available mod abbrev for metadata

    Per-source dedup: a source is "already converted" iff a
    ``converted_merged_*`` row exists with the matching ``_nxml`` / ``_merged``
    display_name suffix (and ``_tei`` for legacy TEI-derived rows unless
    ``overwrite_tei_md`` is True). This means a later batch tick (or a later
    MOD's "conversion needed" tag) only processes sources that haven't been
    converted yet — supplements MOD A processed earlier are skipped while
    supplements MOD B added later are picked up.

    When ``overwrite_tei_md`` is True, ``_tei``-suffixed rows are NOT
    counted, so the endpoint re-converts from nXML or PDF and the
    higher-quality output supersedes the TEI-derived fallback.
    """
    main_cached = False
    supp_cached = False
    mod_abbreviation: Optional[str] = None
    for ref_file in reference.referencefiles or []:
        is_tei_md = (
            (ref_file.display_name or "").endswith("_tei")
            and ref_file.file_extension == "md"
        )
        if (
            ref_file.file_class == "converted_merged_main"
            and ref_file.file_extension == "md"
            and ref_file.file_publication_status == "final"
            and not (overwrite_tei_md and is_tei_md)
        ):
            main_cached = True
        if (
            ref_file.file_class == "converted_merged_supplement"
            and ref_file.file_extension == "md"
            and ref_file.file_publication_status == "final"
            and not (overwrite_tei_md and is_tei_md)
        ):
            supp_cached = True
        for ref_file_mod in ref_file.referencefile_mods:
            if mod_abbreviation is None and ref_file_mod.mod is not None:
                mod_abbreviation = ref_file_mod.mod.abbreviation

    nxml_source = get_nxml_referencefile(db, reference.reference_id)
    main_pdfs = get_pdf_files_for_reference(db, reference.reference_id, "main")
    supp_pdfs = get_pdf_files_for_reference(db, reference.reference_id, "supplement")

    pending_main: List[PendingMainSource] = pending_main_sources(
        db, reference.reference_id,
        prefer_nxml=True, ignore_tei_derived=overwrite_tei_md,
    )

    # Supplement conversion is restricted to references in corpus for WB/ZFIN/FB
    # (SCRUM-6026). Treat ineligible references as if they have no pending
    # supplements so the endpoint reports "converted" cleanly once main is
    # done and never schedules supplement work.
    supp_eligible = is_eligible_for_supplement_conversion(db, reference.reference_id)
    if supp_eligible:
        pending_supplements: List[Any] = pending_supplement_sources(
            db, reference.reference_id, ignore_tei_derived=overwrite_tei_md,
        )
    else:
        pending_supplements = []

    main_pdf_available = bool(main_pdfs)
    supp_pdf_available = bool(supp_pdfs)

    main_missing = bool(pending_main)
    supp_missing = bool(pending_supplements)

    # PDF-only pending sources need the async PDFX path; nXML can be done sync.
    main_needs_pdf = any(p["kind"] == "pdf" for p in pending_main)
    needs_async = main_needs_pdf or supp_missing

    return {
        "main_cached": main_cached,
        "supp_cached": supp_cached,
        "nxml_source": nxml_source,
        "main_pdfs": main_pdfs,
        "supp_pdfs": supp_pdfs,
        "main_pdf_available": main_pdf_available,
        "supp_pdf_available": supp_pdf_available,
        "pending_main": pending_main,
        "pending_supplements": pending_supplements,
        "main_missing": main_missing,
        "supp_missing": supp_missing,
        "needs_async": needs_async,
        "mod_abbreviation": mod_abbreviation,
    }


def per_mod_pending_status(
    db: Session,
    reference: ReferenceModel,
    overwrite_tei_md: bool = False,
) -> List[Dict[str, Any]]:
    """
    Return per-MOD conversion status for every MOD that currently has a
    ``text_convert_job`` workflow tag for this reference.

    Each entry: ``{mod_abbreviation, reference_workflow_tag_id,
    pending_main_count, pending_supplement_count, all_converted}``.
    ``all_converted`` is True iff that MOD has no pending main or
    supplement sources — i.e., that MOD's conversion is complete.
    """
    from agr_literature_service.api.crud.workflow_tag_crud import get_jobs
    from agr_literature_service.api.models import ModModel

    jobs = get_jobs(db, "text_convert_job", reference=reference.curie)
    out: List[Dict[str, Any]] = []
    for job in jobs:
        mod_id = job["mod_id"]
        mod_abbr_row = (
            db.query(ModModel.abbreviation)
            .filter(ModModel.mod_id == mod_id)
            .one_or_none()
        )
        if not mod_abbr_row:
            continue
        mod_abbreviation = mod_abbr_row.abbreviation
        pending_main = pending_main_sources(
            db, reference.reference_id,
            prefer_nxml=True, ignore_tei_derived=overwrite_tei_md,
            mod_abbreviation=mod_abbreviation,
        )
        # Only count supplements when the reference is supplement-eligible
        # (SCRUM-6026: WB/ZFIN/FB only). Otherwise this MOD has nothing to
        # do on the supplement side regardless of what supplement PDFs exist.
        if is_eligible_for_supplement_conversion(db, reference.reference_id):
            pending_supplements = pending_supplement_sources(
                db, reference.reference_id,
                ignore_tei_derived=overwrite_tei_md,
                mod_abbreviation=mod_abbreviation,
            )
        else:
            pending_supplements = []
        out.append({
            "mod_abbreviation": mod_abbreviation,
            "reference_workflow_tag_id": job["reference_workflow_tag_id"],
            "pending_main_count": len(pending_main),
            "pending_supplement_count": len(pending_supplements),
            "all_converted": (
                len(pending_main) == 0 and len(pending_supplements) == 0
            ),
        })
    return out


def transition_completed_text_convert_tags(
    db: Session,
    reference: ReferenceModel,
    overwrite_tei_md: bool = False,
) -> int:
    """
    For each ``text_convert_job`` "needed" tag on this reference, transition
    to ``on_success`` when no source files remain pending for that MOD.

    Returns the number of tags transitioned.

    Used by the on-demand endpoint to land all eligible MODs' workflow
    tags after conversion + mod-association sync. Only transitions tags
    whose MOD genuinely has nothing left to do — MODs with pending
    sources stay in their current state.
    """
    from agr_literature_service.api.crud.workflow_tag_crud import job_change_atp_code

    transitioned = 0
    for entry in per_mod_pending_status(db, reference, overwrite_tei_md):
        if entry["all_converted"]:
            try:
                job_change_atp_code(
                    db, entry["reference_workflow_tag_id"], "on_success"
                )
                transitioned += 1
            except Exception as exc:
                logger.warning(
                    f"Failed to transition text_convert_job tag "
                    f"{entry['reference_workflow_tag_id']} for "
                    f"{entry['mod_abbreviation']} on {reference.curie}: {exc}"
                )
    return transitioned


def _expected_source_files_from_assessment(
    assessment: Dict[str, Any],
) -> List[Dict[str, Optional[str]]]:
    """Build the list of eligible source files for the upcoming conversion job
    from the assessment's per-source pending lists.

    Used to seed ``per_file_progress`` with ``pending`` entries so callers
    polling while a job runs can see the full list of files in flight, not
    only the ones already finished. Each entry corresponds to a source file
    that genuinely still needs conversion — sources that were already
    converted in a prior run are not seeded here.
    """
    expected: List[Dict[str, Optional[str]]] = []

    for entry in assessment.get("pending_main") or []:
        ref_file = entry["ref_file"]
        if entry["kind"] == "nxml":
            expected.append({
                "source_display_name": ref_file.display_name,
                "source_file_class": "nXML",
                "source_referencefile_id": ref_file.referencefile_id,
                "expected_converted_display_name": f"{ref_file.display_name}_nxml",
                "expected_converted_file_class": "converted_merged_main",
            })
        else:
            expected.append({
                "source_display_name": ref_file.display_name,
                "source_file_class": "main",
                "source_referencefile_id": ref_file.referencefile_id,
                "expected_converted_display_name": f"{ref_file.display_name}_merged",
                "expected_converted_file_class": "converted_merged_main",
            })

    for ref_file in assessment.get("pending_supplements") or []:
        expected.append({
            "source_display_name": ref_file.display_name,
            "source_file_class": "supplement",
            "source_referencefile_id": ref_file.referencefile_id,
            "expected_converted_display_name": f"{ref_file.display_name}_merged",
            "expected_converted_file_class": "converted_merged_supplement",
        })

    return expected


def _job_progress_payload(job: Optional[ConversionJob]) -> List[Dict[str, Any]]:
    if job is None:
        return []
    out: List[Dict[str, Any]] = []
    for p in job.per_file_progress:
        converted_info: Optional[Dict[str, Any]] = None
        if p.converted_display_name and p.converted_file_class:
            converted_info = {
                "display_name": p.converted_display_name,
                "file_class": p.converted_file_class,
                "referencefile_id": p.converted_referencefile_id,
            }
        out.append({
            "source": {
                "display_name": p.source_display_name,
                "file_class": p.source_file_class,
                "referencefile_id": p.source_referencefile_id,
            },
            "converted": converted_info,
            "figures": [],
            "status": p.status,
            "error": p.error,
        })
    return out


_SUFFIX_TO_SOURCE_CLASS: Dict[str, str] = {
    # Direct mappings: suffix → source file_class.
    "_nxml": "nXML",
    "_tei": "tei",
}

# PDFX method suffixes — the source file_class depends on whether the
# converted row is main or supplement (resolved at lookup time).
_PDFX_METHOD_SUFFIXES = ("_merged", "_grobid", "_docling", "_marker")


def _infer_source_info(reference: ReferenceModel,
                       converted_display_name: str,
                       converted_file_class: str) -> Optional[Dict[str, Any]]:
    """Best-effort lookup of the source referencefile that produced a given
    converted Markdown row, based on the display_name suffix convention used
    by pdf2md_utils and the legacy TEI→MD batch. Returns the source info
    dict or None if the source can't be identified."""
    base_name: Optional[str] = None
    source_class: Optional[str] = None
    for suffix, src_class in _SUFFIX_TO_SOURCE_CLASS.items():
        if converted_display_name.endswith(suffix):
            base_name = converted_display_name[: -len(suffix)]
            source_class = src_class
            break
    if source_class is None:
        for method_suffix in _PDFX_METHOD_SUFFIXES:
            if converted_display_name.endswith(method_suffix):
                base_name = converted_display_name[: -len(method_suffix)]
                if converted_file_class == "converted_merged_main":
                    source_class = "main"
                elif converted_file_class == "converted_merged_supplement":
                    source_class = "supplement"
                break
    if base_name is None or source_class is None:
        return None
    for rf in reference.referencefiles or []:
        if rf.file_class == source_class and rf.display_name == base_name:
            return {
                "display_name": rf.display_name,
                "file_class": rf.file_class,
                "referencefile_id": int(rf.referencefile_id),
            }
    return None


def _db_derived_progress(reference: ReferenceModel) -> List[Dict[str, Any]]:
    """Produce per_file_progress entries for every converted Markdown row
    currently in the DB for this reference. Used to surface referencefile_ids
    even when no job ran in this process (e.g. the reference was converted in
    a prior session and the manager's in-memory state has since been lost).
    Source info is inferred from the display_name suffix convention."""
    out: List[Dict[str, Any]] = []
    for ref_file in reference.referencefiles or []:
        if ref_file.file_class not in ("converted_merged_main",
                                       "converted_merged_supplement"):
            continue
        if ref_file.file_extension != "md":
            continue
        if ref_file.file_publication_status != "final":
            continue
        out.append({
            "source": _infer_source_info(
                reference, ref_file.display_name or "", ref_file.file_class,
            ),
            "converted": {
                "display_name": ref_file.display_name,
                "file_class": ref_file.file_class,
                "referencefile_id": int(ref_file.referencefile_id),
            },
            "figures": [],
            "status": "success",
            "error": None,
        })
    return out


# Mapping from source PDF file_class to the figure file_class that
# process_extracted_images uploads. Kept in sync with FIGURE_FILE_CLASSES
# in pdf2md_utils — duplicated here to avoid an inter-module dependency.
_FIGURE_FILE_CLASS_FOR_SOURCE: Dict[str, str] = {
    "main": "converted_main_figure",
    "supplement": "converted_supplement_figure",
}


def _figures_for_source(reference: ReferenceModel,
                        source_display_name: Optional[str],
                        source_file_class: Optional[str]) -> List[Dict[str, Any]]:
    """Return ConversionFileInfo dicts for every extracted-figure row in the
    DB whose source PDF matches ``(source_display_name, source_file_class)``.

    Figures uploaded by ``pdf2md_utils.process_extracted_images`` use the
    ``{source_display_name}_image_{idx:03d}`` convention and one of the
    ``converted_main_figure`` / ``converted_supplement_figure`` file_classes,
    so we match on display_name prefix + the corresponding figure file_class.
    """
    if not source_display_name or not source_file_class:
        return []
    figure_file_class = _FIGURE_FILE_CLASS_FOR_SOURCE.get(source_file_class)
    if figure_file_class is None:
        return []
    prefix = f"{source_display_name}_image_"
    figures: List[Dict[str, Any]] = []
    for ref_file in reference.referencefiles or []:
        if ref_file.file_class != figure_file_class:
            continue
        if ref_file.file_publication_status != "final":
            continue
        if not (ref_file.display_name or "").startswith(prefix):
            continue
        figures.append({
            "display_name": ref_file.display_name,
            "file_class": ref_file.file_class,
            "referencefile_id": int(ref_file.referencefile_id),
        })
    figures.sort(key=lambda f: f["display_name"] or "")
    return figures


def _attach_figures(reference: ReferenceModel,
                    progress: List[Dict[str, Any]]) -> None:
    """Mutate ``progress`` in place: set each entry's ``figures`` list to the
    extracted-figure rows currently in the DB for that source PDF."""
    for entry in progress:
        source = entry.get("source") or {}
        entry["figures"] = _figures_for_source(
            reference,
            source.get("display_name"),
            source.get("file_class"),
        )


def _merge_progress(job_progress: List[Dict[str, Any]],
                    db_progress: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Merge job-recorded progress with DB-synthesized entries. Dedupes by
    the converted file's (display_name, file_class) — job entries win because
    they carry source info and accurate status (incl. pending/failed)."""
    seen: set = set()
    for entry in job_progress:
        conv = entry.get("converted") or {}
        if conv.get("display_name") and conv.get("file_class"):
            seen.add((conv["display_name"], conv["file_class"]))
    merged = list(job_progress)
    for db_entry in db_progress:
        conv = db_entry.get("converted") or {}
        key = (conv.get("display_name"), conv.get("file_class"))
        if key not in seen:
            merged.append(db_entry)
    return merged


def _converted_classes_from_assessment(assessment: Dict[str, Any]) -> List[str]:
    """List the converted file_class values currently present in the DB for
    this reference — regardless of whether they were produced in this call or
    written by a prior run."""
    out: List[str] = []
    if assessment.get("main_cached"):
        out.append("converted_merged_main")
    if assessment.get("supp_cached"):
        out.append("converted_merged_supplement")
    return out


def _status_payload(db: Session, reference: ReferenceModel, *, status_str: str,
                    converted_classes: List[str],
                    job: Optional[ConversionJob] = None,
                    error_message: Optional[str] = None,
                    started_at: Optional[str] = None,
                    completed_at: Optional[str] = None,
                    overwrite_tei_md: bool = False) -> Dict[str, Any]:
    """Build the endpoint response. ``job`` is the most recent job for the
    reference, if any — its id, timestamps, and per-file progress are surfaced.
    Per-file progress is merged with synthesized DB entries so converted rows
    produced in prior sessions still surface their referencefile_ids.

    ``per_mod_status`` lists every MOD with a text_convert_job tag for this
    reference and that MOD's conversion state (pending counts +
    all_converted boolean). Best-effort: any error computing per-MOD
    status returns an empty list rather than failing the whole response.
    """
    progress = _merge_progress(
        _job_progress_payload(job),
        _db_derived_progress(reference),
    )
    _attach_figures(reference, progress)
    try:
        per_mod = per_mod_pending_status(
            db, reference, overwrite_tei_md=overwrite_tei_md
        )
    except Exception:
        logger.exception(
            f"Failed to compute per-MOD status for {reference.curie}"
        )
        per_mod = []
    payload: Dict[str, Any] = {
        "reference_curie": reference.curie,
        "status": status_str,
        "job_id": job.job_id if job else None,
        "error_message": error_message,
        "started_at": started_at,
        "completed_at": completed_at,
        "converted_classes": converted_classes,
        "per_file_progress": progress,
        "per_mod_status": per_mod,
    }
    if job is not None and payload["started_at"] is None:
        payload["started_at"] = job.started_at.isoformat()
    if job is not None and payload["completed_at"] is None and job.completed_at is not None:
        payload["completed_at"] = job.completed_at.isoformat()
    if job is not None and payload["error_message"] is None and job.error_message:
        payload["error_message"] = job.error_message
    return payload


def _execute_sync_nxml(db: Session, reference: ReferenceModel,
                       assessment: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Run the NXML-only conversion inline. Returns (success, error_message)."""
    nxml_source = assessment["nxml_source"]
    if nxml_source is None:
        return False, "No nXML source available"
    success, error = process_nxml_to_markdown(
        db=db,
        nxml_ref_file=nxml_source,
        reference_curie=reference.curie,
        mod_abbreviation=assessment["mod_abbreviation"],
    )
    return success, error


def find_converted_referencefile_id(db: Session, reference_id: int,
                                    display_name: str,
                                    file_class: str) -> Optional[int]:
    """Look up a newly-uploaded converted Markdown row by its expected
    display_name + file_class. Returns the referencefile_id of the most
    recently created matching row, or None if not found."""
    row = (
        db.query(ReferencefileModel)
        .filter(
            ReferencefileModel.reference_id == reference_id,
            ReferencefileModel.display_name == display_name,
            ReferencefileModel.file_class == file_class,
            ReferencefileModel.file_extension == "md",
            ReferencefileModel.file_publication_status == "final",
        )
        .order_by(ReferencefileModel.date_created.desc())
        .first()
    )
    return int(row.referencefile_id) if row else None


def delete_tei_derived_md_rows(db: Session, reference: ReferenceModel,
                               file_classes: List[str]) -> int:
    """Remove converted Markdown rows produced by the legacy TEI→MD batch.
    Identified as rows with the given file_class(es), file_extension == 'md',
    and display_name ending with '_tei'. Returns the number of rows deleted."""
    removed = 0
    for ref_file in list(reference.referencefiles or []):
        if ref_file.file_class not in file_classes:
            continue
        if ref_file.file_extension != "md":
            continue
        if not (ref_file.display_name or "").endswith("_tei"):
            continue
        try:
            remove_from_s3_and_db(db, ref_file)
            removed += 1
        except Exception:
            logger.exception(
                f"Failed to delete TEI-derived MD row "
                f"referencefile_id={ref_file.referencefile_id} for "
                f"reference {reference.curie}"
            )
    if removed:
        logger.info(
            f"Deleted {removed} TEI-derived MD row(s) for reference {reference.curie}"
        )
    return removed


def handle_conversion_request(db: Session, curie_or_reference_id: str, wait: bool,
                              background_tasks: BackgroundTasks, mod_access: ModAccess,
                              user_id: str,
                              overwrite_tei_md: bool = False
                              ) -> Tuple[int, Dict[str, Any]]:
    """
    Orchestrator for
    ``GET /reference/referencefile/conversion_request/{curie_or_reference_id}``.

    Returns a ``(status_code, response_dict)`` tuple so the router can set the
    HTTP status to 200 or 202 as appropriate. The response reports conversion
    state plus the per-file progress of the most recent job (if any); callers
    use ``show_all`` to fetch the file listing once status is ``converted``.

    ``overwrite_tei_md`` — when True, converted Markdown rows produced by the
    legacy TEI→MD batch (display_name ends in ``_tei``) are ignored when
    deciding whether the reference is already converted, so a fresh nXML /
    PDFX conversion runs. After a successful new conversion, the TEI-derived
    rows are deleted.
    """
    reference = get_reference(db=db, curie_or_reference_id=curie_or_reference_id,
                              load_referencefiles=True)
    _check_permission(reference, mod_access)

    assessment = _assess_reference(db, reference, overwrite_tei_md=overwrite_tei_md)
    now_iso = datetime.utcnow().isoformat()
    recent_job = conversion_manager.get_last_job_for_reference(reference.reference_id)

    # If a job is still running for this reference, always report "running" —
    # even if the DB already reflects completed work. This closes the race
    # between file_upload committing a new row and the processor calling
    # record_file_progress, so callers only see status="converted" once every
    # per_file_progress entry (with referencefile_id) is finalized.
    existing = conversion_manager.get_active_job_for_reference(reference.reference_id)
    if existing is not None:
        return status.HTTP_202_ACCEPTED, _status_payload(
            db, reference, status_str=STATUS_RUNNING,
            converted_classes=_converted_classes_from_assessment(assessment),
            started_at=existing.started_at.isoformat(), job=existing,
        )

    nothing_missing = not assessment["main_missing"] and not assessment["supp_missing"]
    nothing_cached = not assessment["main_cached"] and not assessment["supp_cached"]
    no_sources = (
        not assessment["nxml_source"]
        and not assessment["main_pdf_available"]
        and not assessment["supp_pdf_available"]
    )

    if nothing_missing:
        if nothing_cached and no_sources:
            return status.HTTP_200_OK, _status_payload(
                db, reference, status_str=STATUS_NO_SOURCES,
                converted_classes=_converted_classes_from_assessment(assessment),
                job=recent_job,
            )
        # SCRUM-6041: nothing to convert, but still reconcile mod
        # associations on existing converted rows and transition any
        # text_convert_job tags whose MODs are now fully converted.
        # This is the "already converted by an earlier MOD-specific run,
        # need to fan out the result to other MODs" case.
        try:
            sync_converted_file_mods_to_sources(db, reference)
            transition_completed_text_convert_tags(
                db, reference, overwrite_tei_md=overwrite_tei_md
            )
            db.expire(reference)
            reference = get_reference(db=db, curie_or_reference_id=curie_or_reference_id,
                                      load_referencefiles=True)
        except Exception:
            logger.exception(
                f"Post-conversion sync/transition failed for {reference.curie}"
            )
        return status.HTTP_200_OK, _status_payload(
            db, reference, status_str=STATUS_CONVERTED,
            converted_classes=_converted_classes_from_assessment(assessment),
            completed_at=now_iso, job=recent_job,
        )

    # Sync-nxml shortcut applies when the only pending main source is the
    # nXML and there are no pending supplements — the nXML conversion is
    # fast enough to run inline.
    pending_main = assessment.get("pending_main") or []
    nxml_only = (
        bool(pending_main)
        and all(p["kind"] == "nxml" for p in pending_main)
        and not assessment["supp_missing"]
    )
    if nxml_only:
        success, error = _execute_sync_nxml(db, reference, assessment)
        if not success:
            return status.HTTP_200_OK, _status_payload(
                db, reference, status_str=STATUS_FAILED,
                converted_classes=_converted_classes_from_assessment(assessment),
                error_message=error or "nXML conversion failed",
                completed_at=now_iso, job=recent_job,
            )
        if overwrite_tei_md:
            delete_tei_derived_md_rows(
                db, reference,
                ["converted_merged_main", "converted_merged_supplement"],
            )
        db.expire(reference)
        reference = get_reference(db=db, curie_or_reference_id=curie_or_reference_id,
                                  load_referencefiles=True)
        post_assessment = _assess_reference(db, reference)
        return status.HTTP_200_OK, _status_payload(
            db, reference, status_str=STATUS_CONVERTED,
            converted_classes=_converted_classes_from_assessment(post_assessment),
            started_at=now_iso, completed_at=now_iso, job=recent_job,
        )

    if wait:
        job = conversion_manager.create_or_get_job(
            reference_id=reference.reference_id,
            reference_curie=reference.curie,
            user_id=user_id,
            expected_source_files=_expected_source_files_from_assessment(assessment),
        )
        run_conversion_job(
            job_id=job.job_id,
            reference_id=reference.reference_id,
            reference_curie=reference.curie,
            overwrite_tei_md=overwrite_tei_md,
        )
        db.expire(reference)
        reference = get_reference(db=db, curie_or_reference_id=curie_or_reference_id,
                                  load_referencefiles=True)
        post_assessment = _assess_reference(db, reference, overwrite_tei_md=overwrite_tei_md)
        final_job = conversion_manager.get_job(job.job_id)
        failed = (
            post_assessment["main_missing"]
            or post_assessment["supp_missing"]
            or (final_job is not None and final_job.status == "failed")
        )
        if failed:
            error_detail = ""
            if final_job is not None:
                if final_job.error_message:
                    error_detail = final_job.error_message
                per_file_errors = [
                    f"{p.source_display_name}: {p.error}"
                    for p in final_job.per_file_progress
                    if p.status == "failed" and p.error
                ]
                if per_file_errors:
                    error_detail = (error_detail + "; " if error_detail else "") \
                        + "; ".join(per_file_errors)
            if not error_detail:
                error_detail = "Conversion completed but some files are still missing."
            completed_at = (final_job.completed_at.isoformat()
                            if final_job and final_job.completed_at
                            else now_iso)
            return status.HTTP_200_OK, _status_payload(
                db, reference, status_str=STATUS_FAILED,
                converted_classes=_converted_classes_from_assessment(post_assessment),
                error_message=error_detail,
                started_at=job.started_at.isoformat(),
                completed_at=completed_at, job=final_job,
            )
        return status.HTTP_200_OK, _status_payload(
            db, reference, status_str=STATUS_CONVERTED,
            converted_classes=_converted_classes_from_assessment(post_assessment),
            started_at=job.started_at.isoformat(),
            completed_at=datetime.utcnow().isoformat(), job=final_job,
        )

    job = conversion_manager.create_or_get_job(
        reference_id=reference.reference_id,
        reference_curie=reference.curie,
        user_id=user_id,
        expected_source_files=_expected_source_files_from_assessment(assessment),
    )
    background_tasks.add_task(
        run_conversion_job,
        job.job_id,
        reference.reference_id,
        reference.curie,
        overwrite_tei_md,
    )
    return status.HTTP_202_ACCEPTED, _status_payload(
        db, reference, status_str=STATUS_RUNNING,
        converted_classes=_converted_classes_from_assessment(assessment),
        started_at=job.started_at.isoformat(), job=job,
    )


__all__ = [
    "_assess_reference",
    "handle_conversion_request",
]
