"""
Extract author emails from Markdown reference files.

The script reads the ABC-format Markdown produced by the conversion pipeline
(``file_class='converted_merged_main'``, ``file_extension='md'``) and falls
back to converting the TEI in-process via
``agr_abc_document_parsers.convert_xml_to_markdown`` when no main MD file is
available yet. Sub-articles are excluded from the extracted plain text.

Two modes:
  1) streaming (default): download file bytes -> convert if needed -> extract -> load
  2) files: optionally download MD files to disk first, then read files -> extract -> load

Selection: for papers with "email extraction needed" tag AND have either a
converted_merged_main MD file or a TEI file we can convert.
"""

import os
import re
import logging
import argparse
from typing import Dict, List, Optional, Set, Tuple

from sqlalchemy import text

from agr_abc_document_parsers import (
    convert_xml_to_markdown,
    extract_plain_text,
    read_markdown,
)

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
DEFAULT_MD_DIR = "md_files_for_email_extraction/"


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
    Drop obvious concatenation / glue garbage in local-part.
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

    # domain-like fragment appears inside local-part (strong glue signal)
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

    # Strict global regex
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
# Markdown plain-text extraction
# ----------------------------------------------------------------------
def _md_bytes_to_plain_text(md_bytes: bytes) -> str:
    """Parse an ABC-format Markdown blob and return the plain text used for
    email extraction. Sub-articles are excluded (per SCRUM-5893 acceptance
    criteria); author / correspondence / metadata / keyword sections are
    included so corresponding-author emails are not missed.
    """
    try:
        md_text = md_bytes.decode("utf-8")
    except UnicodeDecodeError:
        md_text = md_bytes.decode("latin-1", errors="replace")

    doc = read_markdown(md_text)
    return extract_plain_text(
        doc,
        include_authors=True,
        include_correspondence=True,
        include_metadata=True,
        include_keywords=True,
        include_sub_articles=False,
    )


# ----------------------------------------------------------------------
# DB mapping (workflow-tag-based, MD preferred with TEI fallback)
# ----------------------------------------------------------------------
def get_agrkb_md_reffile_mapping(db) -> Dict[str, Tuple[Optional[int], Optional[int], int, str]]:
    """
    curie -> (md_referencefile_id, tei_referencefile_id, reference_id, mod_abbreviation)

    Returns one row per (reference, mod) carrying the lowest-id main MD file
    and/or the lowest-id TEI file. Callers prefer MD and fall back to TEI on
    a per-curie basis.
    """
    rows = db.execute(
        text(
            """
            SELECT
              r.curie,
              r.reference_id,
              m.abbreviation,
              MIN(CASE
                    WHEN rf.file_class = 'converted_merged_main'
                     AND rf.file_extension = 'md'
                    THEN rf.referencefile_id
                  END) AS md_referencefile_id,
              MIN(CASE
                    WHEN rf.file_class = 'tei'
                     AND rf.file_extension = 'tei'
                    THEN rf.referencefile_id
                  END) AS tei_referencefile_id
            FROM reference r
            JOIN workflow_tag wft ON wft.reference_id = r.reference_id
            JOIN mod m ON m.mod_id = wft.mod_id
            JOIN referencefile rf ON rf.reference_id = r.reference_id
            WHERE wft.workflow_tag_id = :email_extraction_needed
              AND (
                    (rf.file_class = 'converted_merged_main' AND rf.file_extension = 'md')
                 OR (rf.file_class = 'tei' AND rf.file_extension = 'tei')
                  )
            GROUP BY r.curie, r.reference_id, m.abbreviation
            """
        ),
        {"email_extraction_needed": needed_tag},
    ).fetchall()

    return {
        curie: (md_id, tei_id, ref_id, mod)
        for curie, ref_id, mod, md_id, tei_id in rows
    }


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


def _md_path(md_dir: str, curie: str) -> str:
    return os.path.join(md_dir, f"{curie}.md")


def _fetch_md_bytes_via_db(
    db,
    md_referencefile_id: Optional[int],
    tei_referencefile_id: Optional[int],
    curie: str,
) -> Optional[bytes]:
    """Return Markdown bytes for ``curie``: prefer the main MD file, otherwise
    download the TEI and convert in-process. Returns None when neither yields
    usable content.
    """
    if md_referencefile_id is not None:
        try:
            blob = download_file(
                db=db,
                referencefile_id=md_referencefile_id,
                mod_access=ModAccess.ALL_ACCESS,
                use_in_api=False,
            )
            if blob:
                return blob
            logger.warning(
                "Main MD entry exists but download returned empty for %s "
                "(referencefile_id=%s); will try TEI fallback",
                curie,
                md_referencefile_id,
            )
        except Exception:
            logger.exception(
                "Main MD download failed for %s (referencefile_id=%s); "
                "will try TEI fallback",
                curie,
                md_referencefile_id,
            )

    if tei_referencefile_id is None:
        return None

    try:
        tei_blob = download_file(
            db=db,
            referencefile_id=tei_referencefile_id,
            mod_access=ModAccess.ALL_ACCESS,
            use_in_api=False,
        )
        if not tei_blob:
            logger.error(
                "TEI download returned empty for %s (referencefile_id=%s)",
                curie,
                tei_referencefile_id,
            )
            return None
        md_text = convert_xml_to_markdown(tei_blob, "tei")
        return md_text.encode("utf-8")
    except Exception:
        logger.exception(
            "TEI->MD conversion failed for %s (referencefile_id=%s)",
            curie,
            tei_referencefile_id,
        )
        return None


def download_md_files_first(
    md_dir: str,
    mapping: Dict[str, Tuple[Optional[int], Optional[int], int, str]],
    overwrite: bool = False,
) -> None:
    """
    Downloads main MD files to disk for all curies in mapping. Falls back to
    converting TEI to MD in-process when no main MD row is available.
    """
    _safe_mkdir(md_dir)

    db = create_postgres_session(False)
    try:
        for curie, (md_id, tei_id, _ref_id, _mod) in mapping.items():
            out_path = _md_path(md_dir, curie)

            if (not overwrite) and os.path.exists(out_path) and os.path.getsize(out_path) > 0:
                continue

            md_bytes = _fetch_md_bytes_via_db(db, md_id, tei_id, curie)
            if md_bytes is None:
                logger.error("Could not obtain MD bytes for %s", curie)
                continue

            # write into a tmp file first so a crash during writing only
            # affects the .tmp file, leaving the original out_path intact
            tmp_path = out_path + ".tmp"
            try:
                with open(tmp_path, "wb") as f:
                    f.write(md_bytes)
                os.replace(tmp_path, out_path)
            except Exception:
                logger.exception("Failed to write MD file for %s to %s", curie, out_path)
                try:
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
    mapping: Dict[str, Tuple[Optional[int], Optional[int], int, str]],
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
        for curie, (md_id, tei_id, ref_id, mod) in mapping.items():
            count += 1

            try:
                md_bytes = _fetch_md_bytes_via_db(db, md_id, tei_id, curie)
                if md_bytes is None:
                    transition_workflow(db, ref_id, mod, failed_tag)
                    db.commit()
                    logger.error(
                        "No usable MD/TEI content for %s (md_id=%s tei_id=%s)",
                        curie,
                        md_id,
                        tei_id,
                    )
                    continue

                content = _md_bytes_to_plain_text(md_bytes)
                emails, _bad = _extract_from_content(content, exclude, print_suspicious, curie)

                set_reference_emails(db, curie, emails)
                transition_workflow(db, ref_id, mod, complete_tag)

                # CRUD should have already committed, but still be safe
                db.commit()

                logger.info("Loaded emails for %s: %s", curie, emails)

            except Exception as e:
                db.rollback()
                logger.exception(
                    "Error processing/loading %s (md_id=%s tei_id=%s): %s",
                    curie,
                    md_id,
                    tei_id,
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
    md_dir: str,
    mapping: Dict[str, Tuple[Optional[int], Optional[int], int, str]],
    exclude_list: List[str],
    print_suspicious: bool,
    batch_size: int,
    require_file: bool = True,
) -> None:
    """
    Files: read Markdown from disk -> extract -> load.
    """
    exclude = {_normalize_email(e) for e in (exclude_list or []) if e}

    db = create_postgres_session(False)
    count = 0

    try:
        for curie, (_md_id, _tei_id, ref_id, mod) in mapping.items():
            count += 1

            path = _md_path(md_dir, curie)
            if (not os.path.exists(path)) or os.path.getsize(path) == 0:
                if require_file:
                    logger.warning("Missing/empty MD file for %s: %s", curie, path)
                    try:
                        transition_workflow(db, ref_id, mod, failed_tag)
                        db.commit()
                    except Exception:
                        db.rollback()
                        logger.exception("Failed to transition failed_tag for %s", curie)
                continue

            try:
                with open(path, "rb") as f:
                    md_bytes = f.read()

                content = _md_bytes_to_plain_text(md_bytes)
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
        help="stream: download+extract+load in memory (default). files: read MD from disk.",
    )

    # file-mode controls
    p.add_argument("--md-dir", default=DEFAULT_MD_DIR, help="Directory for cached MD files.")
    p.add_argument(
        "--download-first",
        action="store_true",
        help="In --mode files, download MDs to --md-dir before processing.",
    )
    p.add_argument(
        "--download-only",
        action="store_true",
        help="Download MDs to --md-dir and exit (no extraction/load).",
    )
    p.add_argument(
        "--process-only",
        action="store_true",
        help="Only extract/load from existing files (no download). Implies --mode files.",
    )
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="When downloading, overwrite existing MD files.",
    )

    p.add_argument(
        "--print-suspicious",
        action="store_true",
        help="Print rejected/invalid email candidates seen in Markdown (debugging).",
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

    # Build mapping once (workflow-tag + MD/TEI candidates)
    db = create_postgres_session(False)
    try:
        mapping = get_agrkb_md_reffile_mapping(db)
    finally:
        db.close()

    logger.info("Found %s references to process", len(mapping))

    # Resolve mode flags
    if args.process_only:
        args.mode = "files"
        args.download_first = False
        args.download_only = False

    if args.download_only:
        download_md_files_first(args.md_dir, mapping, overwrite=args.overwrite)
        logger.info("Download-only complete.")
        return

    if args.mode == "files":
        if args.download_first:
            download_md_files_first(args.md_dir, mapping, overwrite=args.overwrite)

        process_and_load_from_files(
            md_dir=args.md_dir,
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
