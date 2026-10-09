"""
SCRUM-6633_load_fb_pdf_archive.py
=================================

One-off load of the FlyBase PDF archive (~40,000 files) into ABC so the files
are not lost. Each file is named after the paper's FlyBase reference ID,
``FBrf[0-9]{7}.pdf``, and is attached to the reference that carries the
cross-reference ``FB:<FBrf>`` as an FB main / final / pdf file.

Curator decision (2026-10): these uploads must NOT send the papers to "text
conversion needed" (ATP:0000162). A normal upload moves the reference's FB
file-upload workflow status with ``transition_to_workflow_status``, whose FB
transition actions queue text conversion, and the pdf2md cron then converts the
paper. So the files are uploaded with the interactive-upload guardrails
suppressed (``set_suppress_upload_guardrails``, as the SCRUM-6589 and
figure-metadata backfills do), which skips that transition and the main-PDF
cleanup, and the FB file-upload status is then set to "files uploaded"
(ATP:0000134) directly: created when the paper has no file-upload tag yet,
otherwise overwritten in place. Neither path runs transition actions. The
text-conversion process tags are never touched.

Skipping ``cleanup_old_pdf_file`` also matters on its own: it keeps only the
newest FB main PDF of a reference, so an archive copy would have deleted a
newer FB PDF already in ABC. References that already have a *different* FB
main PDF are skipped by default for the same reason (``--include-existing``
uploads the archive copy alongside it instead).

Per file the script reports one of:

- ``bad_name``: the filename is not ``FBrf[0-9]{7}.pdf``;
- ``no_xref``: no live ``FB:<FBrf>`` cross-reference;
- ``not_in_corpus``: the reference is not in the FB corpus (the upload would
  be refused);
- ``already_loaded``: the reference already has this exact file (same md5) as
  an FB main PDF; with ``--commit`` its status is still set to "files
  uploaded", so a rerun after an interruption finishes the job;
- ``has_other_fb_pdf``: the reference already has a different FB main PDF;
- ``uploaded`` / ``errors``.

Safe by default: without ``--commit`` the script only reports what it would
upload. ``--limit N`` stops after N upload attempts; ``--report PATH`` writes
every file that was not uploaded, with the reason, as TSV (for FlyBase).

Note that manual pdf2md runs with ``--since YEAR`` or ``--newest N`` select
main PDFs by file, not by workflow status, so they WOULD convert archive
PDFs; the weekly cron (workflow mode) does not.

file_upload() writes its temporary gzip into the current directory, so run
the script from a writable one.
"""
import argparse
import csv
import hashlib
import logging
import os
import re
from os import path
from typing import Dict, Iterator, List, Optional, Set, Tuple

from fastapi import UploadFile
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.referencefile_crud import (
    check_if_paper_in_corpus,
    file_upload,
    file_upload_process_atp_id,
    file_uploaded_tag_atp_id,
    set_suppress_upload_guardrails,
)
from agr_literature_service.api.crud.workflow_tag_crud import (
    _get_current_workflow_tag_db_obj,
    create as create_wft,
)
from agr_literature_service.api.models import (
    CrossReferenceModel,
    ModModel,
    ReferenceModel,
    ReferencefileModAssociationModel,
    ReferencefileModel,
)
from agr_literature_service.api.schemas.workflow_tag_schemas import WorkflowTagSchemaPost
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

FB_MOD = "FB"
FBRF_FILENAME_RE = re.compile(r"^(FBrf[0-9]{7})\.pdf$")
SKIP_REASONS = ("bad_name", "no_xref", "not_in_corpus", "already_loaded", "has_other_fb_pdf")


def parse_fbrf(filename: str) -> Optional[str]:
    """``FBrf0123456.pdf`` -> ``FBrf0123456``; None for any other name."""
    match = FBRF_FILENAME_RE.match(filename)
    return match.group(1) if match else None


def archive_files(folder: str) -> Iterator[Tuple[str, str]]:
    """(filename, full path) of every regular file in ``folder``, by name."""
    with os.scandir(folder) as entries:
        names = sorted(entry.name for entry in entries if entry.is_file())
    for name in names:
        yield name, path.join(folder, name)


def file_md5(file_path: str) -> str:
    md5sum_hash = hashlib.md5()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(1 << 20), b""):
            md5sum_hash.update(byte_block)
    return md5sum_hash.hexdigest()


def find_reference(db: Session, fbrf: str) -> Optional[ReferenceModel]:
    """The reference carrying the live cross-reference ``FB:<fbrf>``."""
    return (
        db.query(ReferenceModel)
        .join(CrossReferenceModel, CrossReferenceModel.reference_id == ReferenceModel.reference_id)
        .filter(CrossReferenceModel.curie == f"{FB_MOD}:{fbrf}",
                CrossReferenceModel.is_obsolete.is_(False))
        .first()
    )


def fb_main_pdf_md5s(db: Session, reference_id: int) -> Set[str]:
    """md5sums of the reference's main / final / pdf files linked to FB."""
    rows = (
        db.query(ReferencefileModel.md5sum)
        .join(ReferencefileModAssociationModel,
              ReferencefileModAssociationModel.referencefile_id == ReferencefileModel.referencefile_id)
        .join(ModModel, ModModel.mod_id == ReferencefileModAssociationModel.mod_id)
        .filter(ReferencefileModel.reference_id == reference_id,
                ReferencefileModel.file_class == "main",
                ReferencefileModel.file_publication_status == "final",
                ReferencefileModel.pdf_type == "pdf",
                ModModel.abbreviation == FB_MOD)
        .all()
    )
    return {row.md5sum for row in rows}


def set_file_uploaded_status(db: Session, reference_curie: str) -> Optional[str]:
    """Set the reference's FB file-upload workflow status to "files uploaded"
    WITHOUT running transition actions, so nothing queues text conversion.

    Returns the change made: ``"created"``, ``"<old ATP> -> ATP:0000134"``, or
    None when the status already was "files uploaded"."""
    current = _get_current_workflow_tag_db_obj(db, reference_curie, file_upload_process_atp_id, FB_MOD)
    if current is None:
        create_wft(db, WorkflowTagSchemaPost(workflow_tag_id=file_uploaded_tag_atp_id,
                                             mod_abbreviation=FB_MOD,
                                             reference_curie=reference_curie))
        return "created"
    if current.workflow_tag_id == file_uploaded_tag_atp_id:
        return None
    old_tag = current.workflow_tag_id
    current.workflow_tag_id = file_uploaded_tag_atp_id
    db.commit()
    return f"{old_tag} -> {file_uploaded_tag_atp_id}"


def upload_archive_pdf(db: Session, reference_curie: str, fbrf: str, file_path: str) -> None:
    metadata = {
        "reference_curie": reference_curie,
        "display_name": fbrf,
        "file_class": "main",
        "file_publication_status": "final",
        "file_extension": "pdf",
        "pdf_type": "pdf",
        "is_annotation": False,
        "mod_abbreviation": FB_MOD,
    }
    with open(file_path, "rb") as f:
        file_upload(db=db, metadata=metadata,
                    file=UploadFile(file=f, filename=f"{fbrf}.pdf"),
                    upload_if_already_converted=True)


def load_fb_pdf_archive(folder: str, commit: bool = False, limit: Optional[int] = None,
                        include_existing: bool = False,
                        report_path: Optional[str] = None) -> Dict[str, int]:
    db = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, script_name)

    counts = {"files": 0, "uploaded": 0, "errors": 0, "wft_set": 0}
    counts.update({reason: 0 for reason in SKIP_REASONS})
    not_uploaded: List[Tuple[str, str, str]] = []
    if commit:
        set_suppress_upload_guardrails(True)
    try:
        _load_files(db, folder, commit, limit, include_existing, counts, not_uploaded)
    finally:
        if commit:
            set_suppress_upload_guardrails(False)

    if report_path:
        with open(report_path, "w", newline="") as report:
            writer = csv.writer(report, delimiter="\t")
            writer.writerow(["file", "reason", "detail"])
            writer.writerows(not_uploaded)
        logger.info("wrote %s file(s) not uploaded to %s", len(not_uploaded), report_path)

    logger.info("done: %s file(s); %s uploaded%s, %s already loaded, %s with another FB main PDF, "
                "%s not in the FB corpus, %s without an FB xref, %s badly named, %s errors; "
                "%s FB file-upload status(es) set to %s",
                counts["files"], counts["uploaded"], "" if commit else " (dry run)",
                counts["already_loaded"], counts["has_other_fb_pdf"], counts["not_in_corpus"],
                counts["no_xref"], counts["bad_name"], counts["errors"],
                counts["wft_set"], file_uploaded_tag_atp_id)
    return counts


def _load_files(db: Session, folder: str, commit: bool, limit: Optional[int],
                include_existing: bool, counts: Dict[str, int],
                not_uploaded: List[Tuple[str, str, str]]) -> None:
    attempted = 0

    def skip(filename: str, reason: str, detail: str = "") -> None:
        counts[reason] += 1
        not_uploaded.append((filename, reason, detail))

    for filename, file_path in archive_files(folder):
        if limit and attempted >= limit:
            break
        counts["files"] += 1
        fbrf = parse_fbrf(filename)
        if fbrf is None:
            skip(filename, "bad_name")
            continue
        try:
            reference = find_reference(db, fbrf)
            if reference is None:
                skip(filename, "no_xref", f"{FB_MOD}:{fbrf}")
                continue
            reference_curie = reference.curie
            if not check_if_paper_in_corpus(db, reference_curie, FB_MOD):
                skip(filename, "not_in_corpus", reference_curie)
                continue
            md5sum = file_md5(file_path)
            existing = fb_main_pdf_md5s(db, reference.reference_id)
            if md5sum in existing:
                skip(filename, "already_loaded", reference_curie)
                if commit:
                    _set_status(db, reference_curie, counts)
                continue
            if existing and not include_existing:
                skip(filename, "has_other_fb_pdf", reference_curie)
                continue

            logger.info("%s %s -> %s", "UPLOAD" if commit else "WOULD UPLOAD", filename, reference_curie)
            attempted += 1
            if not commit:
                counts["uploaded"] += 1
                continue
            upload_archive_pdf(db, reference_curie, fbrf, file_path)
            counts["uploaded"] += 1
            _set_status(db, reference_curie, counts)
        except Exception as e:  # noqa: BLE001 - one bad file must not stop the run
            db.rollback()
            counts["errors"] += 1
            not_uploaded.append((filename, "error", str(e)))
            logger.error("FAILED %s: %s", filename, e)


def _set_status(db: Session, reference_curie: str, counts: Dict[str, int]) -> None:
    change = set_file_uploaded_status(db, reference_curie)
    if change is not None:
        counts["wft_set"] += 1
        logger.info("%s FB file-upload status: %s", reference_curie, change)


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Load the FlyBase PDF archive (FBrf[0-9]{7}.pdf files) into ABC as FB main PDFs, "
                    "without queuing text conversion (SCRUM-6633). Dry run unless --commit.")
    parser.add_argument("--folder", required=True,
                        help="directory holding the archive PDFs")
    parser.add_argument("--commit", action="store_true",
                        help="upload the files and set the FB file-upload status (default: report only)")
    parser.add_argument("--limit", type=int, default=None,
                        help="stop after N upload attempts, so a trial slice always uploads files")
    parser.add_argument("--include-existing", action="store_true",
                        help="also upload to references that already have a different FB main PDF "
                             "(skipped by default; nothing is ever deleted)")
    parser.add_argument("--report", default=None,
                        help="write every file not uploaded, with the reason, to this TSV file")
    args = parser.parse_args()
    load_fb_pdf_archive(args.folder, commit=args.commit, limit=args.limit,
                        include_existing=args.include_existing, report_path=args.report)
