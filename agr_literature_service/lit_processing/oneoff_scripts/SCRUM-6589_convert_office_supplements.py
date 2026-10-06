"""
SCRUM-6589_convert_office_supplements.py
========================================

Backfill: convert the Office supplements (xlsx / xlsm / docx / docm) of the
papers published in the given years to ABC Markdown, through the same
in-process path the on-demand conversion endpoint and the pdf2md cron use
(``process_office_to_markdown``). Each file produces one
``converted_merged_supplement`` row named ``{display_name}_{extension}``.

Going forward no backfill is needed: new papers get their Office supplements
converted by the pdf2md cron together with their main file, and any
reference can be (re)converted on demand with
``GET /reference/referencefile/conversion_request/{curie}``. This script
only catches up the papers that were loaded before SCRUM-6589.

Selection: final ``file_class='supplement'`` rows with an Office extension
(``OFFICE_SUPPLEMENT_FORMATS``) whose reference was published in one of
``--year`` (``date_published``, else ``date_published_start``). Files that
already have their converted row are skipped (same per-source dedup as the
pipeline). MOD scope for the backfill (curator decision, 2026-10): every
MOD except RGD and AGR, i.e. the reference must be in corpus for at least
one MOD not in ``--exclude-mod`` (default ``RGD, AGR``); a paper in both the
AGR and the WB corpus is still converted, one only in RGD and/or AGR is
not. ``--mod ABBR`` narrows to one corpus and ``--all-mods`` lifts the
exclusion. This is deliberately wider
than the pipeline's WB/ZFIN/FB supplement-PDF rule, which is unchanged.
``htp_supplement`` rows are never selected. tsv / csv / txt supplements are
NOT covered: the parser has no reader for them yet.

Like the cron, the background job and the inline endpoint, the script
generates classifier embeddings once per reference after its Office
supplements converted (idempotent; dormant without OPENAI_API_KEY and
skipped outside classifier MODs), so the backfilled rows are not left
invisible to the classifiers.

References whose file upload is still in progress for any MOD (file-upload
workflow status ATP:0000139) are skipped by default: a curator is still
working on them (curator request on the first stage run). References with
no file-upload tag at all (PMC loads nobody has touched) or with "upload
needed" are converted. ``--include-upload-in-progress`` lifts the rule.

Uploads run with the interactive-upload guardrails suppressed
(``set_suppress_upload_guardrails``, as the figure-metadata backfill does):
a derived Markdown file must not move the reference's file-upload workflow
status, prune main PDFs or be refused because a curation job is running.
Without this, every upload re-fired the file-upload transition, which creates
a spurious "file upload in progress" tag and fails with a duplicate-WFT 422
from the second file of a reference on (first stage run, 2026-10-05).

Safe by default: without ``--commit`` the script only reports what it would
convert. ``--limit N`` stops after N conversion attempts (rows that are
eligible and not yet converted, so a trial slice always exercises real
conversions); ``--mod ABBR`` restricts to references in that MOD's corpus.
"""
import argparse
import logging
from os import path
from typing import Dict, Iterable, List, Optional, Sequence

from sqlalchemy import func
from sqlalchemy.orm import Session

from agr_literature_service.api.models import (
    ModCorpusAssociationModel,
    ModModel,
    ReferencefileModel,
    ReferenceModel,
    WorkflowTagModel,
)
from agr_literature_service.api.crud.referencefile_crud import (
    file_upload_in_progress_tag_atp_id,
    set_suppress_upload_guardrails,
)
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.pdf2md.pdf2md_utils import (
    OFFICE_SUPPLEMENT_FORMATS,
    is_office_supplement_converted,
    office_converted_display_name,
    process_office_to_markdown,
    recover_session,
)
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

DEFAULT_YEARS = (2025, 2026)
DEFAULT_EXCLUDED_MODS = ("RGD", "AGR")


def parse_years(values: Optional[Sequence[str]]) -> List[int]:
    """``--year`` values (``2025`` or ``2025,2026``) -> sorted unique ints."""
    if not values:
        return list(DEFAULT_YEARS)
    years = set()
    for value in values:
        for part in str(value).split(","):
            part = part.strip()
            if not part:
                continue
            if not (part.isdigit() and len(part) == 4):
                raise ValueError(f"--year expects 4-digit years, got '{part}'")
            years.add(int(part))
    return sorted(years)


def publication_year(date_published: Optional[str],
                     date_published_start: Optional[str]) -> Optional[int]:
    """Year of a reference's publication date: ``date_published``, else
    ``date_published_start`` (the classifier trainer's precedence). None when
    neither starts with a 4-digit year."""
    for value in (date_published, date_published_start):
        if value and value[:4].isdigit():
            return int(value[:4])
    return None


def source_mod_abbreviation(ref_file: ReferencefileModel) -> Optional[str]:
    """MOD to stamp on the converted row: the first MOD that owns the source
    file, or None for shared / PMC files (matches the pipeline's metadata)."""
    for ref_file_mod in ref_file.referencefile_mods or []:
        if ref_file_mod.mod is not None:
            return ref_file_mod.mod.abbreviation
    return None


def in_corpus_for_an_included_mod(db: Session, reference_id: int,
                                  excluded_mods: Sequence[str]) -> bool:
    """True when the reference is in corpus for at least one MOD that is not
    excluded. With the default exclusion (RGD, AGR) a reference that is only
    in RGD's and/or AGR's corpus (or in no corpus) is skipped."""
    query = (
        db.query(ModCorpusAssociationModel.mod_corpus_association_id)
        .join(ModModel, ModModel.mod_id == ModCorpusAssociationModel.mod_id)
        .filter(ModCorpusAssociationModel.reference_id == reference_id,
                ModCorpusAssociationModel.corpus.is_(True))
    )
    if excluded_mods:
        query = query.filter(~ModModel.abbreviation.in_([m.upper() for m in excluded_mods]))
    return query.first() is not None


def upload_in_progress(db: Session, reference_id: int) -> bool:
    """True when any MOD's file-upload workflow status for the reference is
    "file upload in progress": a curator is still adding files, so the
    backfill leaves the reference alone."""
    return (
        db.query(WorkflowTagModel.reference_workflow_tag_id)
        .filter(WorkflowTagModel.reference_id == reference_id,
                WorkflowTagModel.workflow_tag_id == file_upload_in_progress_tag_atp_id)
        .first()
        is not None
    )


def embed_reference(db: Session, reference_id: int, reference_curie: str) -> None:
    """Classifier embeddings for a reference's merged Markdown (main and
    supplements), as the cron and the endpoint do after converting. Imported
    lazily: the embedding stack is optional. Isolated: never raises."""
    try:
        from agr_literature_service.lit_processing.embedding.embedding_generation import (
            maybe_generate_classifier_embeddings,
        )
        maybe_generate_classifier_embeddings(db, reference_id, reference_curie)
    except Exception as e:  # noqa: BLE001 - embeddings must never fail the backfill
        logger.error("embeddings failed for %s: %s", reference_curie, e)


def candidate_office_supplements(db: Session, years: Iterable[int],
                                 mod_abbreviation: Optional[str] = None) -> List[ReferencefileModel]:
    """Final Office supplement rows of references published in ``years``,
    ordered by reference so the caller can act once per reference."""
    year_strings = [str(y) for y in years]
    best_date = func.coalesce(
        func.nullif(ReferenceModel.date_published, ""),
        func.nullif(ReferenceModel.date_published_start, ""),
    )
    query = (
        db.query(ReferencefileModel)
        .join(ReferenceModel, ReferenceModel.reference_id == ReferencefileModel.reference_id)
        .filter(
            ReferencefileModel.file_class == "supplement",
            func.lower(ReferencefileModel.file_extension).in_(list(OFFICE_SUPPLEMENT_FORMATS)),
            ReferencefileModel.file_publication_status == "final",
            ReferencefileModel.md5sum.isnot(None),
            func.substr(best_date, 1, 4).in_(year_strings),
        )
    )
    if mod_abbreviation:
        query = (
            query.join(ModCorpusAssociationModel,
                       ModCorpusAssociationModel.reference_id == ReferencefileModel.reference_id)
            .join(ModModel, ModModel.mod_id == ModCorpusAssociationModel.mod_id)
            .filter(ModCorpusAssociationModel.corpus.is_(True),
                    ModModel.abbreviation == mod_abbreviation)
        )
    query = query.order_by(ReferencefileModel.reference_id, ReferencefileModel.referencefile_id)
    return query.all()


def convert_office_supplements(years: Sequence[int], commit: bool = False,
                               limit: Optional[int] = None,
                               mod_abbreviation: Optional[str] = None,
                               all_mods: bool = False,
                               include_upload_in_progress: bool = False,
                               excluded_mods: Sequence[str] = DEFAULT_EXCLUDED_MODS) -> Dict[str, int]:
    db = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, script_name)

    rows = candidate_office_supplements(db, years, mod_abbreviation)
    excluded = () if all_mods else tuple(excluded_mods)
    logger.info("%s Office supplement row(s) (%s) on papers published in %s%s%s",
                len(rows), ", ".join(sorted(OFFICE_SUPPLEMENT_FORMATS)),
                ", ".join(str(y) for y in years),
                f" in the {mod_abbreviation} corpus" if mod_abbreviation else "",
                f"; skipping references only in corpus for {', '.join(excluded)}" if excluded else "")

    counts = {"converted": 0, "already_converted": 0, "excluded_mod": 0, "upload_in_progress": 0,
              "errors": 0, "embedded_references": 0}
    eligible_cache: Dict[int, bool] = {}
    if commit:
        set_suppress_upload_guardrails(True)
    try:
        _convert_rows(db, rows, commit, limit, excluded, counts, eligible_cache,
                      include_upload_in_progress)
    finally:
        if commit:
            set_suppress_upload_guardrails(False)

    logger.info("done: %s converted%s, %s already converted, %s on references outside the MOD scope, "
                "%s on references with a file upload in progress, %s errors, %s reference(s) embedded",
                counts["converted"], "" if commit else " (dry run)",
                counts["already_converted"], counts["excluded_mod"], counts["upload_in_progress"],
                counts["errors"], counts["embedded_references"])
    return counts


def _convert_rows(db: Session, rows: List[ReferencefileModel], commit: bool,
                  limit: Optional[int], excluded_mods: Sequence[str], counts: Dict[str, int],
                  eligible_cache: Dict[int, bool],
                  include_upload_in_progress: bool = False) -> None:
    attempted = 0
    in_progress_cache: Dict[int, bool] = {}
    # Rows are ordered by reference: embed a reference once, after its last
    # successful conversion, when the loop moves on to the next reference.
    pending_embed: Optional[tuple] = None

    def flush_embed() -> None:
        nonlocal pending_embed
        if pending_embed is not None:
            embed_reference(db, *pending_embed)
            counts["embedded_references"] += 1
            pending_embed = None

    for ref_file in rows:
        if limit and attempted >= limit:
            break
        reference_id = ref_file.reference_id
        if pending_embed is not None and pending_embed[0] != reference_id:
            flush_embed()
        if reference_id not in eligible_cache:
            eligible_cache[reference_id] = in_corpus_for_an_included_mod(db, reference_id, excluded_mods)
        if not eligible_cache[reference_id]:
            counts["excluded_mod"] += 1
            continue
        if not include_upload_in_progress:
            if reference_id not in in_progress_cache:
                in_progress_cache[reference_id] = upload_in_progress(db, reference_id)
            if in_progress_cache[reference_id]:
                counts["upload_in_progress"] += 1
                continue
        if is_office_supplement_converted(db, reference_id, ref_file):
            counts["already_converted"] += 1
            continue

        reference_curie = ref_file.reference.curie
        target = office_converted_display_name(ref_file)
        logger.info("%s %s referencefile_id=%s %s.%s -> %s.md",
                    "CONVERT" if commit else "WOULD CONVERT", reference_curie,
                    ref_file.referencefile_id, ref_file.display_name,
                    ref_file.file_extension, target)
        attempted += 1
        if not commit:
            counts["converted"] += 1
            continue
        try:
            success, error = process_office_to_markdown(
                db=db,
                office_ref_file=ref_file,
                reference_curie=reference_curie,
                mod_abbreviation=source_mod_abbreviation(ref_file),
            )
        except Exception as e:  # noqa: BLE001 - one bad file must not stop the run
            recover_session(db, e)
            success, error = False, str(e)
        if success:
            counts["converted"] += 1
            pending_embed = (reference_id, reference_curie)
        else:
            counts["errors"] += 1
            logger.error("FAILED %s referencefile_id=%s %s.%s: %s", reference_curie,
                         ref_file.referencefile_id, ref_file.display_name,
                         ref_file.file_extension, error)
    flush_embed()


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Convert the xlsx/docx supplements of papers published in the given "
                    "years to ABC Markdown (SCRUM-6589 backfill). Dry run unless --commit.")
    parser.add_argument("--year", action="append",
                        help="publication year(s) to backfill; repeat or comma-separate "
                             f"(default: {','.join(str(y) for y in DEFAULT_YEARS)})")
    parser.add_argument("--commit", action="store_true",
                        help="convert and store the Markdown (default: report only)")
    parser.add_argument("--limit", type=int, default=None,
                        help="stop after N conversion attempts (eligible, not-yet-converted files), "
                             "so a trial slice always exercises real conversions")
    parser.add_argument("--mod", default=None,
                        help="only references in this MOD's corpus (e.g. SGD)")
    parser.add_argument("--exclude-mod", action="append", default=None,
                        help="MOD(s) whose papers are skipped unless also in another MOD's corpus "
                             f"(repeatable; default: {', '.join(DEFAULT_EXCLUDED_MODS)})")
    parser.add_argument("--all-mods", action="store_true",
                        help="convert every MOD's papers, ignoring --exclude-mod")
    parser.add_argument("--include-upload-in-progress", action="store_true",
                        help="also convert references whose file upload is still in progress "
                             "for some MOD (skipped by default: a curator is working on them)")
    args = parser.parse_args()
    convert_office_supplements(parse_years(args.year), commit=args.commit, limit=args.limit,
                               mod_abbreviation=args.mod, all_mods=args.all_mods,
                               include_upload_in_progress=args.include_upload_in_progress,
                               excluded_mods=tuple(args.exclude_mod or DEFAULT_EXCLUDED_MODS))
