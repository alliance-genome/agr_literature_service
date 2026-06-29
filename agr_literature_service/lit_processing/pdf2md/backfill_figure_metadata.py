"""Backfill JSON figure-metadata sidecars for already-converted references
(SCRUM-6246).

Earlier conversions persisted only the figure PNGs (``converted_main_figure``
/ ``converted_supplement_figure``); the PDFX manifest metadata (figure number,
caption/legend text, page index, bbox/polygon, image_review_*) was discarded.
This script finds references whose figures lack the JSON metadata sidecar,
re-runs each source PDF through PDFX in **marker-only** mode (``methods=marker``,
``merge=False``) — Marker is the only extractor that produces the image
manifest, so we skip Grobid, Docling and the merge/consensus step to save
credits — regenerates the transient image manifest, matches the manifest
entries back to the existing figure PNGs by order, and uploads one metadata
``.json`` sidecar per figure, sharing the PNG's display_name.

Attach-only: existing figure PNG rows are never re-uploaded — only the missing
metadata sidecars are added.

Idempotent and resumable: a figure whose sidecar already exists is skipped
(detected by display_name), and ``file_upload``'s md5sum dedup makes a re-run
of an identical sidecar a no-op. Safe to re-run after an interruption.

Note: each sidecar goes through the standard ``file_upload`` path, which fires
``transition_WFT_for_uploaded_file``. For already-converted references the
file-upload workflow is past its only actionable state, so this is a no-op —
the same side effect the original figure-PNG uploads already triggered.

Examples:
    # Dry run over everything that still needs metadata
    python3 backfill_figure_metadata.py --dry-run

    # Backfill a single reference (for testing)
    python3 backfill_figure_metadata.py --reference AGRKB:101000000000001
    python3 backfill_figure_metadata.py --curie PMID:12345678

    # Backfill the first 50 references, main figures only
    python3 backfill_figure_metadata.py --limit 50 --no-supplements
"""
import argparse
import logging
import re
import sys
from collections import defaultdict
from os import path
from typing import Callable, Dict, List, Optional, Tuple

from sqlalchemy import or_
from sqlalchemy.orm import Session, aliased

from agr_cognito_py import ModAccess, get_admin_token

from agr_literature_service.api.crud.referencefile_crud import download_file
from agr_literature_service.api.models import (
    ModModel,
    ReferencefileModel,
    ReferenceModel,
)
from agr_literature_service.api.models.referencefile_model import (
    ReferencefileModAssociationModel,
)
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.pdf2md.pdf2md_utils import (
    CONVERTED_FIGURE_FILE_EXTENSION,
    CONVERTED_FIGURE_METADATA_FILE_EXTENSION,
    FIGURE_FILE_CLASSES,
    FIGURE_METADATA_FILE_CLASSES,
    PdfType,
    _upload_figure_metadata_sidecar,
    download_pdfx_image_manifest,
    get_pdf_files_for_reference,
    poll_pdfx_status,
    resolve_curie_to_reference,
    submit_pdf_to_pdfx,
)
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import (
    create_postgres_session,
)

logger = logging.getLogger(__name__)

# Canonical marker figure naming: "{source_display_name}_image_NNN".
_FIGURE_INDEX_RE = re.compile(r"_image_(\d+)$")


def _configure_logging(log_file: str = "") -> None:
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
    root.setLevel(logging.INFO)


def _first_mod_abbreviation(ref_file: ReferencefileModel) -> Optional[str]:
    """Return the first non-null MOD abbreviation associated with a
    referencefile, or None (shared / PMC). Mirrors the mod-resolution in
    ``pdf2md_utils._process_single_pdf_file`` so the backfilled sidecar lands
    with the same MOD context the live path would use."""
    for ref_file_mod in ref_file.referencefile_mods or []:
        if ref_file_mod.mod is not None:
            return ref_file_mod.mod.abbreviation
    return None


def _figure_index_from_display_name(display_name: str, fallback: int) -> int:
    """Parse the 1-based ordinal encoded in a marker figure's display_name
    (``..._image_NNN``), falling back to ``fallback`` if the name is not
    canonical. Tying the persisted ``figure_index`` to the display_name
    ordinal (rather than a positional counter) keeps the value identical
    whether the sidecar is written by the live conversion path (which sets
    ``figure_index`` = manifest position, matching the ``_NNN`` it stamped
    into the PNG name) or by this backfill."""
    match = _FIGURE_INDEX_RE.search(display_name or "")
    return int(match.group(1)) if match else fallback


def plan_sidecars_for_source(
    png_rows: List[ReferencefileModel],
    manifest_images: List[Dict],
    existing_metadata_names: set,
) -> Tuple[List[Dict], int, Optional[str]]:
    """Pure planning step: decide which figure PNGs of one source PDF still
    need a metadata sidecar and which regenerated manifest entry feeds each.

    The existing figures are sorted by display_name (which sorts by the
    ``_image_NNN`` index) and paired positionally with the manifest images
    (PDFX/Marker emits images in a stable order for a given PDF). A figure
    whose display_name is already in ``existing_metadata_names`` is skipped
    (idempotency/resume). ``figure_index`` is parsed from each PNG's
    display_name so it stays consistent with the live conversion path.

    Returns ``(planned, skipped_existing, skip_reason)`` where:
      - ``planned`` is a list of ``{figure_index, figure_display_name, image}``
        dicts to upload (one per figure still missing its sidecar);
      - ``skipped_existing`` counts figures that already had a sidecar;
      - ``skip_reason`` is None when planning succeeded, else a human-readable
        reason the WHOLE source was skipped. We refuse to plan (rather than
        risk attaching a caption to the wrong figure — the exact failure the
        provenance feature must avoid) when either:
          * any existing figure PNG has a non-canonical name (e.g. a prior
            re-conversion left a ``_N`` rename suffix), so sorted order no
            longer reliably equals manifest order; or
          * the regenerated manifest's image count differs from the number of
            existing figures.
    """
    sorted_pngs = sorted(png_rows, key=lambda r: r.display_name or "")

    non_canonical = [
        p.display_name for p in sorted_pngs
        if not _FIGURE_INDEX_RE.search(p.display_name or "")
    ]
    if non_canonical:
        return [], 0, (
            f"{len(non_canonical)} figure PNG(s) have non-canonical names "
            f"(e.g. {non_canonical[0]!r}); cannot reliably align them to the "
            f"regenerated manifest by order"
        )
    if len(sorted_pngs) != len(manifest_images):
        return [], 0, (
            f"regenerated manifest has {len(manifest_images)} image(s) but "
            f"{len(sorted_pngs)} existing figure(s)"
        )

    planned: List[Dict] = []
    skipped_existing = 0
    for pos, (png, image) in enumerate(zip(sorted_pngs, manifest_images), start=1):
        display_name = png.display_name
        if display_name in existing_metadata_names:
            skipped_existing += 1
            continue
        planned.append({
            "figure_index": _figure_index_from_display_name(display_name, pos),
            "figure_display_name": display_name,
            "image": image,
        })
    return planned, skipped_existing, None


def select_references_needing_metadata(  # pragma: no cover
    db: Session, mod_abbreviation: str = "", limit: int = 0,
) -> List[int]:
    """Return reference_ids that have at least one converted figure PNG
    without a matching metadata sidecar.

    A figure PNG is considered backfilled when a ``json`` referencefile under
    one of the ``converted_*_figure_metadata`` classes shares its
    ``(reference_id, display_name)``. Optionally restrict to references whose
    figure PNGs are associated with ``mod_abbreviation`` (or are shared /
    null-mod)."""
    png = ReferencefileModel
    meta = aliased(ReferencefileModel)

    sidecar_exists = (
        db.query(meta.referencefile_id)
        .filter(
            meta.reference_id == png.reference_id,
            meta.display_name == png.display_name,
            meta.file_extension == CONVERTED_FIGURE_METADATA_FILE_EXTENSION,
            meta.file_publication_status == "final",
            meta.file_class.in_(list(FIGURE_METADATA_FILE_CLASSES.values())),
        )
        .exists()
    )

    q = (
        db.query(png.reference_id)
        .filter(
            png.file_class.in_(list(FIGURE_FILE_CLASSES.values())),
            png.file_extension == CONVERTED_FIGURE_FILE_EXTENSION,
            png.file_publication_status == "final",
            ~sidecar_exists,
        )
    )

    if mod_abbreviation:
        mod = (
            db.query(ModModel)
            .filter(ModModel.abbreviation == mod_abbreviation)
            .one_or_none()
        )
        if mod is None:
            logger.warning("MOD not found: %s", mod_abbreviation)
            return []
        q = (
            q.join(
                ReferencefileModAssociationModel,
                ReferencefileModAssociationModel.referencefile_id
                == png.referencefile_id,
            )
            .filter(or_(
                ReferencefileModAssociationModel.mod_id == mod.mod_id,
                ReferencefileModAssociationModel.mod_id.is_(None),
            ))
        )

    q = q.distinct().order_by(png.reference_id)
    if limit and limit > 0:
        q = q.limit(limit)
    return [row[0] for row in q.all()]


def _regenerate_image_manifest_marker_only(  # pragma: no cover
    file_content: bytes,
    token: str,
    reference_curie: str,
    mod_abbreviation: Optional[str],
) -> Dict:
    """Re-run a source PDF through PDFX using ONLY Marker (no Grobid/Docling,
    no merge/consensus) to regenerate the transient image manifest. Image
    review stays on (PDFX default) so image_review_* fields are populated."""
    process_id = submit_pdf_to_pdfx(
        file_content=file_content,
        token=token,
        methods="marker",
        merge=False,
        reference_curie=reference_curie,
        mod_abbreviation=mod_abbreviation,
        extract_images=True,
    )
    poll_pdfx_status(process_id, token)
    return download_pdfx_image_manifest(process_id, token)


def _figure_pngs_for_source(reference: ReferenceModel, figure_file_class: str,
                            prefix: str) -> List[ReferencefileModel]:
    return [
        rf for rf in (reference.referencefiles or [])
        if rf.file_class == figure_file_class
        and rf.file_extension == CONVERTED_FIGURE_FILE_EXTENSION
        and rf.file_publication_status == "final"
        and (rf.display_name or "").startswith(prefix)
    ]


def _existing_sidecar_names(reference: ReferenceModel, metadata_file_class: str,
                            prefix: str) -> set:
    return {
        rf.display_name for rf in (reference.referencefiles or [])
        if rf.file_class == metadata_file_class
        and rf.file_extension == CONVERTED_FIGURE_METADATA_FILE_EXTENSION
        and rf.file_publication_status == "final"
        and (rf.display_name or "").startswith(prefix)
    }


def backfill_reference(  # pragma: no cover
    db: Session,
    reference: ReferenceModel,
    get_token: Callable[[], str],
    dry_run: bool,
    include_supplements: bool,
    stats: Dict[str, int],
) -> bool:
    """Backfill metadata sidecars for one reference. Returns True if any
    sidecar was added (or would be, in dry-run mode)."""
    reference_curie = reference.curie
    source_file_classes: List[PdfType] = ["main"]
    if include_supplements:
        source_file_classes.append("supplement")

    any_added = False
    for source_file_class in source_file_classes:
        figure_file_class = FIGURE_FILE_CLASSES[source_file_class]
        metadata_file_class = FIGURE_METADATA_FILE_CLASSES[source_file_class]

        for pdf_file in get_pdf_files_for_reference(
            db, reference.reference_id, source_file_class
        ):
            prefix = f"{pdf_file.display_name}_image_"
            png_rows = _figure_pngs_for_source(
                reference, figure_file_class, prefix
            )
            if not png_rows:
                continue

            existing_names = _existing_sidecar_names(
                reference, metadata_file_class, prefix
            )
            missing = [
                r for r in png_rows if r.display_name not in existing_names
            ]
            if not missing:
                stats["sources_already_complete"] += 1
                continue

            if dry_run:
                logger.info(
                    "[dry-run] %s %s '%s': %d figure(s) need metadata "
                    "(would re-run marker-only PDFX)",
                    reference_curie, source_file_class,
                    pdf_file.display_name, len(missing),
                )
                stats["sidecars_would_add"] += len(missing)
                any_added = True
                continue

            mod_abbreviation = _first_mod_abbreviation(pdf_file)

            try:
                file_content = download_file(
                    db=db,
                    referencefile_id=pdf_file.referencefile_id,
                    mod_access=ModAccess.ALL_ACCESS,
                    use_in_api=False,
                )
            except Exception as exc:
                stats["sources_download_failed"] += 1
                logger.error(
                    "Failed to download %s PDF '%s' for %s: %s",
                    source_file_class, pdf_file.display_name,
                    reference_curie, exc,
                )
                continue
            if not file_content:
                stats["sources_download_failed"] += 1
                logger.error(
                    "Empty %s PDF '%s' for %s; skipping",
                    source_file_class, pdf_file.display_name, reference_curie,
                )
                continue

            try:
                manifest = _regenerate_image_manifest_marker_only(
                    file_content, get_token(), reference_curie, mod_abbreviation
                )
            except Exception as exc:
                stats["sources_pdfx_failed"] += 1
                logger.error(
                    "PDFX marker-only re-run failed for %s %s '%s': %s",
                    reference_curie, source_file_class,
                    pdf_file.display_name, exc,
                )
                continue

            images = manifest.get("images") or []
            planned, skipped, skip_reason = plan_sidecars_for_source(
                png_rows, images, existing_names
            )
            if skip_reason is not None:
                stats["sources_mismatch"] += 1
                logger.warning(
                    "%s %s '%s': %s; skipping to avoid mis-association",
                    reference_curie, source_file_class, pdf_file.display_name,
                    skip_reason,
                )
                continue
            stats["sources_processed"] += 1

            for plan in planned:
                try:
                    _upload_figure_metadata_sidecar(
                        db=db,
                        image=plan["image"],
                        figure_index=plan["figure_index"],
                        figure_display_name=plan["figure_display_name"],
                        figure_metadata_file_class=metadata_file_class,
                        source_display_name=pdf_file.display_name,
                        source_file_class=source_file_class,
                        reference_curie=reference_curie,
                        mod_abbreviation=mod_abbreviation,
                    )
                    stats["sidecars_added"] += 1
                    any_added = True
                    # Audit trail: record which regenerated manifest entry fed
                    # each sidecar so a human can verify the order-based match.
                    logger.info(
                        "%s: attached metadata sidecar '%s' "
                        "(figure_index=%d, manifest filename=%r)",
                        reference_curie, plan["figure_display_name"],
                        plan["figure_index"], plan["image"].get("filename"),
                    )
                except Exception as exc:
                    stats["sidecar_errors"] += 1
                    logger.error(
                        "Failed to upload metadata sidecar '%s' for %s: %s",
                        plan["figure_display_name"], reference_curie, exc,
                    )

    if any_added:
        stats["refs_with_new_metadata"] += 1
    return any_added


def _log_summary(stats: Dict[str, int], dry_run: bool) -> None:  # pragma: no cover
    verb = "would add" if dry_run else "added"
    count_key = "sidecars_would_add" if dry_run else "sidecars_added"
    logger.info(
        "Done (%s). refs_scanned=%d refs_with_new_metadata=%d "
        "sources_processed=%d sources_already_complete=%d "
        "sources_mismatch=%d sources_download_failed=%d "
        "sources_pdfx_failed=%d sidecars_%s=%d sidecar_errors=%d "
        "refs_failed=%d",
        "dry-run" if dry_run else "live",
        stats["refs_scanned"], stats["refs_with_new_metadata"],
        stats["sources_processed"], stats["sources_already_complete"],
        stats["sources_mismatch"], stats["sources_download_failed"],
        stats["sources_pdfx_failed"], verb, stats[count_key],
        stats["sidecar_errors"], stats["refs_failed"],
    )


def backfill(  # pragma: no cover
    mod_abbreviation: str = "",
    reference: str = "",
    limit: int = 0,
    include_supplements: bool = True,
    dry_run: bool = False,
) -> Dict[str, int]:
    db = create_postgres_session(False)
    set_global_user_id(db, path.basename(__file__).replace(".py", ""))
    stats: Dict[str, int] = defaultdict(int)

    def get_token() -> str:
        # Re-fetch per source PDF so a long run never trips an expired token.
        return get_admin_token()

    try:
        if reference:
            ref_obj = resolve_curie_to_reference(db, reference)
            if ref_obj is None:
                logger.error("Could not resolve reference: %s", reference)
                return dict(stats)
            references: List[ReferenceModel] = [ref_obj]
            logger.info("Backfilling single reference %s", ref_obj.curie)
        else:
            ref_ids = select_references_needing_metadata(
                db, mod_abbreviation=mod_abbreviation, limit=limit
            )
            scope = f" for {mod_abbreviation}" if mod_abbreviation else ""
            logger.info(
                "Found %d reference(s) with figures missing metadata "
                "sidecars%s", len(ref_ids), scope,
            )
            references = [
                db.query(ReferenceModel)
                .filter(ReferenceModel.reference_id == rid)
                .one()
                for rid in ref_ids
            ]

        for ref_obj in references:
            stats["refs_scanned"] += 1
            try:
                backfill_reference(
                    db, ref_obj, get_token, dry_run, include_supplements, stats
                )
            except Exception as exc:
                stats["refs_failed"] += 1
                logger.error("Error backfilling %s: %s", ref_obj.curie, exc)
                db.rollback()
    finally:
        _log_summary(stats, dry_run)
        db.close()
    return dict(stats)


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description=(
            "Backfill JSON figure-metadata sidecars for already-converted "
            "references (SCRUM-6246), re-running source PDFs through "
            "marker-only PDFX."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--mod", default="",
        help="Restrict to references whose figures are associated with this "
             "MOD (e.g. WB); shared / PMC figures are always included.",
    )
    parser.add_argument(
        "--reference", "--curie", dest="reference", default="",
        help="Backfill a single reference (AGRKB id or xref curie like "
             "PMID:12345678); ignores --mod/--limit. Useful for testing.",
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Cap the number of references processed (0 = no cap).",
    )
    parser.add_argument(
        "--no-supplements", dest="include_supplements", action="store_false",
        help="Skip supplement figures; backfill main figures only.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Report what would change without re-running PDFX or uploading.",
    )
    parser.add_argument(
        "--log-file", default="",
        help="Optional path to also write logs to a file.",
    )
    args = parser.parse_args()
    _configure_logging(args.log_file)
    backfill(
        mod_abbreviation=args.mod,
        reference=args.reference,
        limit=args.limit,
        include_supplements=args.include_supplements,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
