"""
Extract author emails from TEI reference files.

Two modes:
  1) streaming (default): download TEI bytes -> extract -> load (no files saved)
  2) files: optionally download TEI files to disk first, then read files -> extract -> load

Selection: for papers with "email extraction needed" tag AND have a tei file
"""

import os
import re
import logging
import argparse
from typing import Dict, List, Set, Tuple, Optional

from sqlalchemy import text

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.crud.reference_crud import set_reference_emails
from agr_literature_service.api.crud.workflow_tag_crud import transition_to_workflow_status
from agr_literature_service.api.crud.referencefile_crud import download_file
from agr_cognito_py import ModAccess

logger = logging.getLogger(__name__)

# Workflow tags
needed_tag = "ATP:0000358"
complete_tag = "ATP:0000355"
failed_tag = "ATP:0000356"

# Defaults
BATCH_SIZE = 200
DEFAULT_TEI_DIR = "tei_files_for_email_extraction/"


# ----------------------------------------------------------------------
# Text utils
# ----------------------------------------------------------------------
def _preclean_text(s: str) -> str:
    if not s:
        return s
    s = s.replace("\u00ad", "")  # soft hyphen
    s = s.replace("\u200b", "")  # zero-width space
    s = s.replace("\ufeff", "")  # ZERO WIDTH NO-BREAK SPACE
    return s


def _normalize_email(raw: str) -> str:
    raw = (raw or "").strip()
    raw = raw.strip(" \t\r\n<>()[]{}'\"")
    raw = raw.rstrip(".,;:!?)\u00bb\u201d")
    raw = re.sub(r"\s*@\s*", "@", raw)
    return raw.lower()


def _is_ascii(s: str) -> bool:
    try:
        s.encode("ascii")
        return True
    except UnicodeEncodeError:
        return False


# ----------------------------------------------------------------------
# Role/system email suppression
# ----------------------------------------------------------------------
_BLOCKED_EXACT = {
    "reprints@oup.com",
    "journals.permissions@oup.com",
    "journals.permissions@oxfordjournals.org",
    "journals.permission@oup.com",
    "permissions@oup.com",
    "journalpermissions@lww.com",
    "support@jstor.org",
    "pubs@aacr.org",
    "permissions@aacr.org",
    "bpgoffice@wjgnet.com",
    "reprints@futuremedicine.com",
    "sales@graphpad.com",
    "karger@karger.com",
    "karger@karger.comwww.karger.com",
}

_BLOCKED_KEYWORDS: Set[str] = {
    "reprint", "reprints",
    "permission", "permissions",
    "copyright",
    "editor", "editors",
    "support", "helpdesk", "help",
    "contact", "info", "admin", "webmaster",
    "data_request", "datarequest",
    "noreply", "no-reply", "do-not-reply",
    "postmaster", "mailer-daemon",
    "correspondence",
    "journal", "journals",
    "reviewer",
}

# Keep empty by default to avoid over-blocking.
_BLOCKED_DOMAINS: Set[str] = set()


def _is_role_or_system_email(email: str) -> bool:
    if not email:
        return True

    e = _normalize_email(email)
    if e in _BLOCKED_EXACT:
        return True

    if "@" not in e:
        return True

    local, domain = e.rsplit("@", 1)

    if domain in _BLOCKED_DOMAINS:
        return True

    for k in _BLOCKED_KEYWORDS:
        if k in local:
            return True

    # (redundant) keywords anywhere
    for k in _BLOCKED_KEYWORDS:
        if k in e:
            return True

    return False


def _looks_like_garbage_local(local: str) -> bool:
    """
    Drop obvious TEI concatenation / glue garbage in local-part.
    Conservative to avoid dropping legitimate addresses.
    """
    if not local:
        return True

    # very long local part with no separators is suspicious
    if len(local) > 30 and "." not in local and "_" not in local and "-" not in local:
        return True

    # reject very-short purely numeric local parts (often from bad splits)
    if re.fullmatch(r"\d{1,3}", local):
        return False

    # long local part containing a long digit run is suspicious
    if len(local) > 25 and re.search(r"\d{4,}", local):
        return True

    # long alphabetic name glued onto local-part-like pattern
    if re.match(r"^[a-z]{10,}[a-z0-9._%+-]*\.[a-z0-9._%+-]+$", local):
        return True

    # domain-like fragment appears inside local-part (strong TEI glue signal)
    if re.search(
        r"(?:^|[._-])[a-z0-9-]{1,30}\.(?:ac|edu|org|net|gov|com|info|io|co|me)\.(?:[a-z]{2})(?:[._-]|$)",
        local,
    ):
        return True

    # single-TLD domain fragments embedded in local-part (e.g. "gmail.com" / "simula.no")
    if re.search(
        r"(?:^|[._-])[a-z0-9-]{1,40}\.(?:ac|edu|org|net|gov|com|info|io|co|me)(?:[._-]|$)",
        local,
    ):
        return True

    return False


# ----------------------------------------------------------------------
# Email parsing / splitting
# ----------------------------------------------------------------------
_LOCAL_AT_RE = re.compile(
    r"(?i)(?<![a-z0-9._%+-])"
    r"[a-z0-9][a-z0-9._%+-]{0,63}@"
)

# top level domains
_ALLOWED_LONG_TLDS = {
    "com", "org", "net", "edu", "gov", "mil",
    "info", "io", "me", "co",
    "ac",
}

EMAIL_RE = re.compile(
    r"(?<![A-Za-z0-9._%+-])"
    r"([A-Za-z0-9][A-Za-z0-9._%+-]{0,63}"
    r"@(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,63})"
    r"(?![A-Za-z0-9._%+-])",
    re.IGNORECASE,
)

EMAIL_TAG_RE = re.compile(
    r"<email\b[^>]*>\s*([^<>\s]+@[^<>\s]+)\s*</email>",
    re.IGNORECASE,
)

FALLBACK_EMAIL_RE = re.compile(
    r"([A-Za-z0-9][A-Za-z0-9._%+-]{0,63}@(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,63})",
    re.IGNORECASE,
)


def _split_if_glued(raw: str) -> List[str]:
    """
    If raw contains multiple '@', try to split into separate candidate emails,
    without guessing or reconstructing domains.
    """
    s = _normalize_email(raw)
    if s.count("@") <= 1:
        return [s] if s else []

    starts = [m.start() for m in _LOCAL_AT_RE.finditer(s)]
    if len(starts) < 2:
        return [s] if s else []

    parts: List[str] = []
    for i, st in enumerate(starts):
        end = starts[i + 1] if i + 1 < len(starts) else len(s)
        part = s[st:end].strip(".,;:!?)")
        if part:
            parts.append(part)

    return parts


def _looks_valid(email: str) -> bool:
    if "@" not in email:
        return False

    if not _is_ascii(email):
        return False

    e = _normalize_email(email)

    if _is_role_or_system_email(e):
        return False

    local, domain = e.rsplit("@", 1)
    if not local or not domain:
        return False

    if _looks_like_garbage_local(local):
        return False

    if "." not in domain:
        return False

    if domain.startswith(".") or domain.endswith("."):
        return False

    tld = domain.rsplit(".", 1)[-1].lower()
    if len(tld) == 2:
        pass
    elif tld in _ALLOWED_LONG_TLDS:
        pass
    else:
        return False

    if len(local) > 64:
        return False

    return True


def _is_garbage_local_part(email: str) -> bool:
    """
    Drop if:
      - local part is exactly 1 char, OR
      - local part is digits-only
    """
    if not email or "@" not in email:
        return False

    local, _domain = email.split("@", 1)

    if len(local) == 1:
        return True

    if local.isdigit():
        return True

    return False


def _dedupe_add(
    out: List[str],
    seen: Set[str],
    exclude: Set[str],
    raw: str,
    bad: Optional[List[str]] = None,
) -> None:
    for cand in _split_if_glued(raw):
        e = _normalize_email(cand)

        if _is_garbage_local_part(e):
            continue

        if not e or e in seen or e in exclude:
            continue

        if not _looks_valid(e):
            if bad is not None and e:
                bad.append(e)
            continue

        seen.add(e)
        out.append(e)


def extract_emails_primary(content: str, exclude: Set[str]) -> Tuple[List[str], List[str]]:
    content = _preclean_text(content)
    out: List[str] = []
    bad: List[str] = []
    seen: Set[str] = set()

    # 1) <email> tags
    for m in EMAIL_TAG_RE.finditer(content):
        raw = m.group(1)

        strict_matches = [mm.group(1) for mm in EMAIL_RE.finditer(raw)]
        if strict_matches:
            for s in strict_matches:
                _dedupe_add(out, seen, exclude, s, bad=bad)
        else:
            _dedupe_add(out, seen, exclude, raw, bad=bad)

    # 2) Strict global regex
    for m in EMAIL_RE.finditer(content):
        _dedupe_add(out, seen, exclude, m.group(1), bad=bad)

    return out, bad


def extract_emails_fallback(content: str, exclude: Set[str]) -> Tuple[List[str], List[str]]:
    content = _preclean_text(content)
    out: List[str] = []
    bad: List[str] = []
    seen: Set[str] = set()

    for m in FALLBACK_EMAIL_RE.finditer(content):
        _dedupe_add(out, seen, exclude, m.group(1), bad=bad)

    return out, bad


def _extract_from_content(
    content: str,
    exclude: Set[str],
    print_suspicious: bool,
    curie: str,
) -> Tuple[List[str], List[str]]:
    emails, bad1 = extract_emails_primary(content, exclude)
    bad_all = list(bad1)

    if not emails:
        fb, bad2 = extract_emails_fallback(content, exclude)
        bad_all.extend(bad2)
        emails = fb  # only use fallback when primary is empty

    bad_all = sorted(set([b for b in bad_all if b]))
    if print_suspicious and bad_all:
        logger.info("suspicious: %s: %s", curie, bad_all)

    return emails, bad_all


# ----------------------------------------------------------------------
# DB mapping (TEI only, workflow-tag-based)
# ----------------------------------------------------------------------
def get_agrkb_tei_reffile_mapping(db) -> Dict[str, Tuple[int, int, str]]:
    """
    curie -> (tei_referencefile_id, reference_id, mod_abbreviation)

    Ensures TEI-only selection and deterministic choice when multiple TEI files exist.
    """
    rows = db.execute(
        text(
            """
            SELECT
              r.curie,
              r.reference_id,
              MIN(rf.referencefile_id) AS referencefile_id,
              m.abbreviation
            FROM reference r
            JOIN workflow_tag wft ON wft.reference_id = r.reference_id
            JOIN mod m ON m.mod_id = wft.mod_id
            JOIN referencefile rf ON rf.reference_id = r.reference_id
            WHERE wft.workflow_tag_id = :email_extraction_needed
              AND rf.file_class = 'tei'
            GROUP BY r.curie, r.reference_id, m.abbreviation
            """
        ),
        {"email_extraction_needed": needed_tag},
    ).fetchall()

    return {curie: (reffile_id, ref_id, mod) for curie, ref_id, reffile_id, mod in rows}


def transition_workflow(db, reference_id: int, mod: str, email_extraction_tag: str) -> None:
    transition_to_workflow_status(
        db,
        str(reference_id),
        mod,
        email_extraction_tag,
        transition_type="automated",
    )


# ----------------------------------------------------------------------
# File-cache helpers
# ----------------------------------------------------------------------
def _safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _tei_path(tei_dir: str, curie: str) -> str:
    return os.path.join(tei_dir, f"{curie}.tei")


def download_tei_files_first(
    tei_dir: str,
    mapping: Dict[str, Tuple[int, int, str]],
    overwrite: bool = False,
) -> None:
    """
    Downloads TEI files to disk for all curies in mapping.
    """
    _safe_mkdir(tei_dir)

    db = create_postgres_session(False)
    try:
        for curie, (reffile_id, _ref_id, _mod) in mapping.items():
            out_path = _tei_path(tei_dir, curie)

            if (not overwrite) and os.path.exists(out_path) and os.path.getsize(out_path) > 0:
                continue

            try:
                blob = download_file(
                    db=db,
                    referencefile_id=reffile_id,
                    mod_access=ModAccess.ALL_ACCESS,
                    use_in_api=False,
                )
                if not blob:
                    raise RuntimeError("Empty TEI content")

                # write data into a tmp file first so if the process crashes during writing,
                # only the .tmp file is affected, the original out_path remains intact
                tmp_path = out_path + ".tmp"
                with open(tmp_path, "wb") as f:
                    f.write(blob)
                os.replace(tmp_path, out_path)
            except Exception:
                logger.exception("Download failed for %s (referencefile_id=%s)", curie, reffile_id)
                # try to remove tmp if present
                try:
                    tmp_path = out_path + ".tmp"
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except Exception:
                    pass
    finally:
        db.close()


# ----------------------------------------------------------------------
# Processing implementations
# ----------------------------------------------------------------------
def process_and_load_streaming(
    mapping: Dict[str, Tuple[int, int, str]],
    exclude_list: List[str],
    print_suspicious: bool,
    batch_size: int,
) -> None:
    """
    Stream: download bytes -> extract -> load.
    """
    exclude = {_normalize_email(e) for e in (exclude_list or []) if e}

    db = create_postgres_session(False)
    count = 0

    try:
        for curie, (reffile_id, ref_id, mod) in mapping.items():
            count += 1

            try:
                blob = download_file(
                    db=db,
                    referencefile_id=reffile_id,
                    mod_access=ModAccess.ALL_ACCESS,
                    use_in_api=False,
                )
                if not blob:
                    transition_workflow(db, ref_id, mod, failed_tag)
                    logger.error("Empty TEI content for %s (referencefile_id=%s)", curie, reffile_id)
                    continue

                content = blob.decode("utf-8", errors="replace")
                emails, _bad = _extract_from_content(content, exclude, print_suspicious, curie)

                set_reference_emails(db, curie, emails)
                transition_workflow(db, ref_id, mod, complete_tag)

                # CRUD should have already committed, but still be safe
                db.commit()

                logger.info("Loaded emails for %s: %s", curie, emails)

            except Exception as e:
                db.rollback()
                logger.exception(
                    "Error processing/loading %s (referencefile_id=%s): %s",
                    curie,
                    reffile_id,
                    str(e),
                )
                # best effort: mark failed
                try:
                    transition_workflow(db, ref_id, mod, failed_tag)
                    db.commit()
                except Exception:
                    db.rollback()
                    logger.exception(
                        "Also failed to transition to failed_tag for %s (reference_id=%s mod=%s)",
                        curie,
                        ref_id,
                        mod,
                    )

            if batch_size > 0 and count % batch_size == 0:
                logger.info(
                    "Processed %s records (note: per-curie commits may happen inside CRUD).",
                    count,
                )

        logger.info("Done. Total processed: %s", count)

    finally:
        try:
            db.close()
        except Exception:
            logger.exception("Error closing DB session")


def process_and_load_from_files(
    tei_dir: str,
    mapping: Dict[str, Tuple[int, int, str]],
    exclude_list: List[str],
    print_suspicious: bool,
    batch_size: int,
    require_file: bool = True,
) -> None:
    """
    Files: read TEI from disk -> extract -> load.
    """
    exclude = {_normalize_email(e) for e in (exclude_list or []) if e}

    db = create_postgres_session(False)
    count = 0

    try:
        for curie, (_reffile_id, ref_id, mod) in mapping.items():
            count += 1

            path = _tei_path(tei_dir, curie)
            if (not os.path.exists(path)) or os.path.getsize(path) == 0:
                if require_file:
                    logger.warning("Missing/empty TEI file for %s: %s", curie, path)
                    try:
                        transition_workflow(db, ref_id, mod, failed_tag)
                        db.commit()
                    except Exception:
                        db.rollback()
                        logger.exception("Failed to transition failed_tag for %s", curie)
                continue

            try:
                # replaces them with the Unicode replacement character if bytes are not valid UTF-8
                with open(path, "r", encoding="utf-8", errors="replace") as f:
                    content = f.read()

                emails, _bad = _extract_from_content(content, exclude, print_suspicious, curie)

                set_reference_emails(db, curie, emails)
                transition_workflow(db, ref_id, mod, complete_tag)
                db.commit()

                logger.info("Loaded emails for %s: %s", curie, emails)

            except Exception as e:
                db.rollback()
                logger.exception("Error processing/loading %s from file %s: %s", curie, path, str(e))
                try:
                    transition_workflow(db, ref_id, mod, failed_tag)
                    db.commit()
                except Exception:
                    db.rollback()
                    logger.exception(
                        "Also failed to transition to failed_tag for %s (reference_id=%s mod=%s)",
                        curie,
                        ref_id,
                        mod,
                    )

            if batch_size > 0 and count % batch_size == 0:
                logger.info(
                    "Processed %s records (note: per-curie commits may happen inside CRUD).",
                    count,
                )

        logger.info("Done. Total processed: %s", count)

    finally:
        try:
            db.close()
        except Exception:
            logger.exception("Error closing DB session")


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--log-level", default="INFO")
    p.add_argument("--batch-size", type=int, default=BATCH_SIZE)

    # mode selection
    p.add_argument(
        "--mode",
        choices=["stream", "files"],
        default="stream",
        help="stream: download+extract+load in memory (default). files: read TEI from disk.",
    )

    # file-mode controls
    p.add_argument("--tei-dir", default=DEFAULT_TEI_DIR, help="Directory for cached TEI files.")
    p.add_argument(
        "--download-first",
        action="store_true",
        help="In --mode files, download TEIs to --tei-dir before processing.",
    )
    p.add_argument(
        "--download-only",
        action="store_true",
        help="Download TEIs to --tei-dir and exit (no extraction/load).",
    )
    p.add_argument(
        "--process-only",
        action="store_true",
        help="Only extract/load from existing files (no download). Implies --mode files.",
    )
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="When downloading, overwrite existing TEI files.",
    )

    p.add_argument(
        "--print-suspicious",
        action="store_true",
        help="Print rejected/invalid email candidates seen in TEI (debugging).",
    )
    p.add_argument(
        "--require-file",
        action="store_true",
        help="In file mode, mark missing/empty file as failed (default behavior).",
    )
    p.add_argument(
        "--skip-missing-file",
        action="store_true",
        help="In file mode, silently skip missing/empty file and do not transition failed_tag.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Resolve file-missing behavior
    require_file = True
    if args.skip_missing_file:
        require_file = False
    elif args.require_file:
        require_file = True  # explicit

    # Exclude list (FB handled by blocked sets)
    exclude_list: List[str] = []

    # Build mapping once (workflow-tag + TEI only)
    db = create_postgres_session(False)
    try:
        mapping = get_agrkb_tei_reffile_mapping(db)
    finally:
        db.close()

    logger.info("Found %s TEI files to process", len(mapping))

    # Resolve mode flags
    if args.process_only:
        args.mode = "files"
        args.download_first = False
        args.download_only = False

    if args.download_only:
        download_tei_files_first(args.tei_dir, mapping, overwrite=args.overwrite)
        logger.info("Download-only complete.")
        return

    if args.mode == "files":
        if args.download_first:
            download_tei_files_first(args.tei_dir, mapping, overwrite=args.overwrite)

        process_and_load_from_files(
            tei_dir=args.tei_dir,
            mapping=mapping,
            exclude_list=exclude_list,
            print_suspicious=args.print_suspicious,
            batch_size=args.batch_size,
            require_file=require_file,
        )
    else:
        process_and_load_streaming(
            mapping=mapping,
            exclude_list=exclude_list,
            print_suspicious=args.print_suspicious,
            batch_size=args.batch_size,
        )


if __name__ == "__main__":
    main()
