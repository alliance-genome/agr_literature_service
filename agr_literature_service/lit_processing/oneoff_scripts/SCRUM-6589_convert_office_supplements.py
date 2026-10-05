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
pipeline). The WB/ZFIN/FB supplement-conversion rule applies unless
``--all-mods``. ``htp_supplement`` rows are never selected. tsv / csv / txt
supplements are NOT covered: the parser has no reader for them yet.

Safe by default: without ``--commit`` the script only reports what it would
convert. ``--limit N`` for a trial slice, ``--mod ABBR`` to restrict to
references in that MOD's corpus.
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
)
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.pdf2md.pdf2md_utils import (
    OFFICE_SUPPLEMENT_FORMATS,
    is_eligible_for_supplement_conversion,
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


def candidate_office_supplements(db: Session, years: Iterable[int],
                                 mod_abbreviation: Optional[str] = None,
                                 limit: Optional[int] = None) -> List[ReferencefileModel]:
    """Final Office supplement rows of references published in ``years``."""
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
    if limit:
        query = query.limit(int(limit))
    return query.all()


def convert_office_supplements(years: Sequence[int], commit: bool = False,
                               limit: Optional[int] = None,
                               mod_abbreviation: Optional[str] = None,
                               all_mods: bool = False) -> Dict[str, int]:
    db = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, script_name)

    rows = candidate_office_supplements(db, years, mod_abbreviation, limit)
    logger.info("%s Office supplement row(s) (%s) on papers published in %s%s",
                len(rows), ", ".join(sorted(OFFICE_SUPPLEMENT_FORMATS)),
                ", ".join(str(y) for y in years),
                f" in the {mod_abbreviation} corpus" if mod_abbreviation else "")

    counts = {"converted": 0, "already_converted": 0, "ineligible": 0, "errors": 0}
    eligible_cache: Dict[int, bool] = {}
    for ref_file in rows:
        reference_id = ref_file.reference_id
        if not all_mods:
            if reference_id not in eligible_cache:
                eligible_cache[reference_id] = is_eligible_for_supplement_conversion(db, reference_id)
            if not eligible_cache[reference_id]:
                counts["ineligible"] += 1
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
        else:
            counts["errors"] += 1
            logger.error("FAILED %s referencefile_id=%s %s.%s: %s", reference_curie,
                         ref_file.referencefile_id, ref_file.display_name,
                         ref_file.file_extension, error)

    logger.info("done: %s converted%s, %s already converted, %s on ineligible references, %s errors",
                counts["converted"], "" if commit else " (dry run)",
                counts["already_converted"], counts["ineligible"], counts["errors"])
    return counts


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
                        help="only consider the first N candidate rows (trial slice)")
    parser.add_argument("--mod", default=None,
                        help="only references in this MOD's corpus (e.g. SGD)")
    parser.add_argument("--all-mods", action="store_true",
                        help="ignore the WB/ZFIN/FB supplement-conversion restriction")
    args = parser.parse_args()
    convert_office_supplements(parse_years(args.year), commit=args.commit, limit=args.limit,
                               mod_abbreviation=args.mod, all_mods=args.all_mods)
