"""
Extract author emails from TEI reference files.

Features:
- Downloads TEI files from s3
- Extracts emails from <email> tags + strict regex scan
- Handles "glued" emails:
    * multiple '@' => split conservatively (no guessing/reconstruction)
    * name glued to front of a real email inside <email> => prefer strict match inside tag
- Rejects non-ASCII / mojibake addresses
- Drops role/system addresses (reprints@, permissions@, reviewer_*, data_request@, etc.)
- Optionally prints suspicious/dropped candidates per AGRKB id (--print-suspicious)
"""

import os
import re
import logging
import argparse
from typing import Dict, List, Set, Tuple, Optional

from sqlalchemy import text

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.crud.reference_crud import set_reference_emails
from agr_literature_service.api.crud.referencefile_crud import download_file
from agr_cognito_py import ModAccess

logger = logging.getLogger(__name__)

DEFAULT_TEI_DIR = "tei_files_for_email_extraction/"
BATCH_SIZE = 200


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

    # (redundant but cheap) keywords anywhere
    for k in _BLOCKED_KEYWORDS:
        if k in e:
            return True

    return False


def _looks_like_garbage_local(local: str) -> bool:
    """
    drop obvious TEI concatenation / glue garbage in local-part.
    keep conservative to avoid dropping legitimate addresses.
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

    # If the local-part looks like a long alphabetic name glued onto a real-looking email
    # local-part, reject it
    if re.match(r"^[a-z]{10,}[a-z0-9._%+-]*\.[a-z0-9._%+-]+$", local):
        return True

    # NEW: domain-like fragment appears inside local-part (strong TEI glue signal)
    # e.g. "nitrkl.ac.inkalpanaranidashdashkalpana44" (contains "ac.in")
    # Also catches "simula.no..." and similar.
    if re.search(r"(?:^|[._-])[a-z0-9-]{1,30}\.(?:ac|edu|org|net|gov|com|info|io|co|me)\.(?:[a-z]{2})(?:[._-]|$)", local):
        return True

    # Also catch single-TLD domain fragments embedded in local-part (e.g. "gmail.com" / "simula.no")
    if re.search(r"(?:^|[._-])[a-z0-9-]{1,40}\.(?:ac|edu|org|net|gov|com|info|io|co|me)(?:[._-]|$)", local):
        return True

    return False


# ----------------------------------------------------------------------
# Email parsing / splitting
# ----------------------------------------------------------------------
"""
Examples it matches:
john@
john.doe@
a.b-c_123@
x@
Examples it does not match:
@
.john@
john..@
john @ (space breaks it)
"""
_LOCAL_AT_RE = re.compile(
    r"(?i)(?<![a-z0-9._%+-])"
    r"[a-z0-9][a-z0-9._%+-]{0,63}@"
)

# we can remove  "io", "me", "co", "ac", but will leave them here
_ALLOWED_LONG_TLDS = {
    "com", "org", "net", "edu", "gov", "mil",
    "info", "io", "me", "co",
    "ac",
}

# strict regex with boundaries (prevents "...mcgill.ca1287..." style)
EMAIL_RE = re.compile(
    r"(?<![A-Za-z0-9._%+-])"  # LEFT BOUNDARY
    r"([A-Za-z0-9][A-Za-z0-9._%+-]{0,63}"  # EMAIL BODY (captured)
    r"@(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,63})"  # EMAIL BODY (captured)
    r"(?![A-Za-z0-9._%+-])",  # RIGHT BOUNDARY
    re.IGNORECASE,  # Case-insensitive matching
)

# extracts email text only when it appears inside a <email>...</email> tag.
# This is our highest-confidence source of emails.
EMAIL_TAG_RE = re.compile(
    r"<email\b[^>]*>\s*([^<>\s]+@[^<>\s]+)\s*</email>",
    re.IGNORECASE,
)

# This is our last-resort extractor.
# It runs only if:
# no emails were found via <email> tags
# and no strict matches were found via EMAIL_RE
# It scans raw text for anything that looks like an email.
# example: Correspondence should be addressed to Abc Xyz at axyz@ucmerced.edu.This work..
# pick up: axyz@ucmerced.edu
# example: <corresp>Contact author: abc.xyz@uni-heidelberg.deAffiliation</corresp>
# pick up: abc.xyz@uni-heidelberg.de
# example: Please contact abc.xyz@harvard.edu123for details.
# pick up: abc.xyz@harvard.edu
FALLBACK_EMAIL_RE = re.compile(
    r"([A-Za-z0-9][A-Za-z0-9._%+-]{0,63}@(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,63})",
    re.IGNORECASE,
)


def _split_if_glued(raw: str) -> List[str]:
    """
    If raw contains multiple '@', try to split into separate candidate emails,
    without guessing or reconstructing domains.
    """
    # <email>abc@nitrkl.ac.inxyz@gmail.com</email>
    # This contains two real emails smashed togethe
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
        # too long => suspicion
        return False

    return True


def _dedupe_add(
    out: List[str],
    seen: Set[str],
    exclude: Set[str],
    raw: str,
    bad: Optional[List[str]] = None,
) -> None:
    """
    Adds one or more emails derived from raw.
    - Splits glued emails (multiple '@') safely
    - Only keeps those that pass _looks_valid
    - Optionally records dropped candidates into `bad`
    """
    for cand in _split_if_glued(raw):
        e = _normalize_email(cand)
        if not e or e in seen or e in exclude:
            continue
        if not _looks_valid(e):
            if bad is not None and e:
                bad.append(e)
            continue
        seen.add(e)
        out.append(e)


def extract_emails_primary(content: str, exclude: Set[str]) -> Tuple[List[str], List[str]]:
    """
    Primary extraction:
      1) <email> tags: prefer strict EMAIL_RE matches *inside* the tag content.
         This fixes cases like:
           <email>oabcdefgzyxvutso.zyxvuts@unilag.edu.ng</email>
         where EMAIL_RE finds the real email "o.zyxvuts@unilag.edu.ng" inside.
      2) Strict EMAIL_RE scan over entire content.
    """
    content = _preclean_text(content)
    out: List[str] = []
    bad: List[str] = []
    seen: Set[str] = set()

    # 1) TEI <email> tags (prefer strict matches inside the tag)
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


def process_tei_files(
    tei_dir: str,
    exclude_list: List[str],
) -> Tuple[Dict[str, List[str]], Dict[str, List[str]], Dict[str, List[str]]]:
    """
    Returns:
      - ref_to_emails: curie -> extracted emails (primary or fallback)
      - ref_to_fallback: curie -> emails extracted by fallback only
      - ref_to_suspicious: curie -> rejected candidates we saw (optional debugging)
    """
    exclude = {_normalize_email(e) for e in (exclude_list or []) if e}

    ref_to_emails: Dict[str, List[str]] = {}
    ref_to_fallback: Dict[str, List[str]] = {}
    ref_to_suspicious: Dict[str, List[str]] = {}

    for fn in sorted(os.listdir(tei_dir)):
        if not fn.endswith(".tei"):
            continue

        curie = fn[:-4]
        path = os.path.join(tei_dir, fn)

        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()

            emails, bad1 = extract_emails_primary(content, exclude)
            bad_all = list(bad1)
            ref_to_emails[curie] = emails

            if not emails:
                fb, bad2 = extract_emails_fallback(content, exclude)
                bad_all.extend(bad2)
                if fb:
                    ref_to_fallback[curie] = fb

            bad_all = sorted(set([b for b in bad_all if b]))
            if bad_all:
                ref_to_suspicious[curie] = bad_all

        except Exception:
            logger.exception("Error processing %s", fn)

    return ref_to_emails, ref_to_fallback, ref_to_suspicious


# ----------------------------------------------------------------------
# Download TEI files from s3 - metadata is in the database
# ----------------------------------------------------------------------
def get_agrkb_reffile_id_mapping(db, since_date: str) -> Dict[str, int]:
    rows = db.execute(
        text(
            """
            SELECT DISTINCT r.curie, rf.referencefile_id
            FROM reference r
            JOIN referencefile rf ON rf.reference_id = r.reference_id
            JOIN mod_corpus_association mca ON mca.reference_id = r.reference_id
            JOIN mod m ON m.mod_id = mca.mod_id
            WHERE rf.file_class = 'tei'
              AND mca.corpus IS TRUE
              AND m.abbreviation IN ('FB', 'SGD', 'WB', 'XB', 'ZFIN')
              AND mca.date_updated >= DATE :since_date
            """
        ),
        {"since_date": since_date},
    ).fetchall()

    return {curie: reffile_id for curie, reffile_id in rows}


def download_tei_files(tei_dir: str, since_date: str) -> None:
    os.makedirs(tei_dir, exist_ok=True)

    db = create_postgres_session(False)
    try:
        mapping = get_agrkb_reffile_id_mapping(db, since_date)

        for curie, reffile_id in mapping.items():
            out_path = os.path.join(tei_dir, f"{curie}.tei")
            try:
                blob = download_file(
                    db=db,
                    referencefile_id=reffile_id,
                    mod_access=ModAccess.ALL_ACCESS,
                    use_in_api=False,
                )
                if not blob:
                    raise RuntimeError("Empty TEI content")
                with open(out_path, "wb") as f:
                    f.write(blob)
            except Exception:
                logger.exception("Download failed for %s (referencefile_id=%s)", curie, reffile_id)
    finally:
        db.close()


def load_emails(
    ref_to_emails: Dict[str, List[str]],
    ref_to_fallback: Dict[str, List[str]],
) -> None:
    """
    Load extracted emails into DB in batches.

    - Writes primary first, then fallback emails
    - Commits every BATCH_SIZE successful (or attempted) loads.
    - Rolls back on per-curie failure to keep the session usable.
    """
    db = create_postgres_session(False)
    count = 0

    try:
        # ---- primary ----
        for curie, emails in ref_to_emails.items():
            count += 1
            try:
                set_reference_emails(db, curie, emails)
                logger.info("Loading emails for %s: %s", curie, emails)
            except Exception as e:
                db.rollback()
                logger.exception("Error loading emails for %s: %s. Error=%s", curie, emails, str(e))

            if count % BATCH_SIZE == 0:
                db.commit()

        """
        only a few and one of them is not a valid email so skip this part for now
        # ---- fallback ----
        for curie, emails in ref_to_fallback.items():
            count += 1
            try:
                set_reference_emails(db, curie, emails)
                logger.info("Loading fallback emails for %s: %s", curie, emails)
            except Exception as e:
                db.rollback()
                logger.exception("Error loading fallback emails for %s: %s", curie, emails)

            if count % BATCH_SIZE == 0:
                db.commit()
        """
        db.commit()
    finally:
        try:
            db.close()
        except Exception:
            logger.exception("Error closing DB session")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--tei-dir", default=DEFAULT_TEI_DIR)
    p.add_argument("--download", action="store_true")
    p.add_argument("--since-date", default="2025-01-01")
    p.add_argument("--log-level", default="INFO")
    p.add_argument(
        "--print-suspicious",
        action="store_true",
        help="Print rejected/invalid email candidates seen in TEI (debugging).",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.download:
        download_tei_files(args.tei_dir, args.since_date)

    # Add known bad addresses here or import bad addresses from a file if we have a curated list
    # FB exclude list has been taken care of by _BLOCKED_EXACT & _BLOCKED_KEYWORDS
    exclude_list: List[str] = []

    ref_to_emails, ref_to_fallback, ref_to_suspicious = process_tei_files(args.tei_dir, exclude_list)

    load_emails(ref_to_emails, ref_to_fallback)

    if args.print_suspicious:
        for curie, bad in ref_to_suspicious.items():
            logger.info(f"suspicious: {curie}: {bad}")


if __name__ == "__main__":
    main()
