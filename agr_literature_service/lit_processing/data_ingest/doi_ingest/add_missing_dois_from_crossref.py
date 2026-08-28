"""
add_missing_dois_from_crossref.py
=================================

Monthly script (SCRUM-4525) to backfill DOIs from CrossRef.

Run this AFTER add_missing_dois_from_europepmc.py: it re-selects the
references still missing a (non-obsolete) DOI cross-reference, so only
the leftovers reach CrossRef.

CrossRef cannot be queried by PMID, so each reference is matched
bibliographically and CONSERVATIVELY: a CrossRef work is accepted only
when its normalized title is identical to the reference's title, it is
a linkable record type (no preprints/components), nothing we know about
the reference CONFLICTS with it (a differing volume, first page, or
journal vetoes the work even when the year agrees), AND a second field
corroborates it (publication year within one year, or volume plus first
page equal). No match means no write — a wrong DOI is worse than a
missing one.

Matches are flushed to the database every FLUSH_EVERY_QUERIES lookups,
so an interrupted run keeps everything committed so far. The crawl is
rate-limited (--delay, default 1 req/s), which makes the full candidate
set a multi-hour job — the cron entry passes --limit to bound each
monthly run; whatever is left simply surfaces again next month.

Usage:
    python add_missing_dois_from_crossref.py [--dry-run] [--limit N] [--delay SECONDS]

Options:
    --dry-run        Log what would be added without writing to the database
    --limit N        Process at most N candidate references (default: all)
    --delay SECONDS  Pause between CrossRef requests (default: 1.0)

CrossRef politeness: set CROSSREF_MAILTO (an email address) in the
environment so requests carry it, which routes them to CrossRef's
"polite" pool.
"""

import argparse
import html
import logging
import os
import re
import sys
import time
from typing import List, Optional, Tuple

import requests

from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.data_ingest.doi_ingest.doi_backfill_utils import (
    BackfillStats,
    Candidate,
    add_doi_cross_references,
    get_references_missing_doi,
)
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format='%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

CROSSREF_WORKS_URL = "https://api.crossref.org/works"
DEFAULT_REQUEST_DELAY_SECONDS = 1.0
REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 3
ROWS_PER_QUERY = 3
# titles shorter than this normalize into too few tokens to be a safe key
MIN_TITLE_LENGTH = 15
# flush accumulated matches to the DB every N CrossRef queries
FLUSH_EVERY_QUERIES = 200


def normalize_title(title: Optional[str]) -> Optional[str]:
    """Lowercase and strip everything but letters and digits, after unescaping
    HTML entities and dropping tags (CrossRef titles carry markup like
    &lt;i&gt;...&lt;/i&gt;), so punctuation, spacing, and markup differences
    between sources don't break equality."""
    if not title:
        return None
    unescaped = html.unescape(html.unescape(title))  # handles double-escaped entities too
    untagged = re.sub(r"<[^>]+>", "", unescaped)
    normalized = re.sub(r"[^a-z0-9]+", "", untagged.lower())
    return normalized or None


def first_page(page_range: Optional[str]) -> Optional[str]:
    if not page_range:
        return None
    m = re.match(r"\s*([A-Za-z]*\d+)", page_range)
    return m.group(1).lower() if m else None


def work_year(work: dict) -> Optional[int]:
    for key in ("published-print", "published-online", "issued"):
        parts = (work.get(key) or {}).get("date-parts") or []
        if parts and parts[0] and parts[0][0]:
            return int(parts[0][0])
    return None


# CrossRef record types that are never the version of record we want to link:
# preprints (a match would attach the preprint's DOI to the journal article),
# figure/supplement components, and peer-review records.
DISALLOWED_WORK_TYPES = {"posted-content", "component", "peer-review"}


def journals_agree(cand: Candidate, work: dict) -> Optional[bool]:
    """None when either side lacks a journal (neutral); otherwise True when
    any CrossRef container-title and any of our journal names agree under
    normalized containment. Containment (not equality) because the two sides
    render names differently ("Development" vs "Development (Cambridge,
    England)"); a lax True only skips the veto — it can never create a match
    on its own — so over-matching here is harmless."""
    ours: List[str] = []
    for name in (cand.journal, cand.journal_abbreviation):
        normalized = normalize_title(name)
        if normalized:
            ours.append(normalized)
    theirs: List[str] = []
    for container_title in (work.get("container-title") or []):
        normalized = normalize_title(container_title)
        if normalized:
            theirs.append(normalized)
    if not ours or not theirs:
        return None
    for cj in ours:
        for wj in theirs:
            if cj in wj or wj in cj:
                return True
    return False


def work_matches_candidate(work: dict, cand: Candidate) -> bool:
    """Accept only an exact normalized-title match that (a) is a linkable
    record type, (b) does not CONFLICT with anything we know — a known volume,
    first page, or journal that differs vetoes the work outright, even when
    the year agrees (audit finding: same journal+year but different pages was
    a neighbouring article, and a republication matched on year with the
    wrong volume) — and (c) is corroborated by year (within 1, print vs
    online years differ) or by volume + first page."""
    if work.get("type") in DISALLOWED_WORK_TYPES:
        return False
    titles = work.get("title") or []
    if not titles or normalize_title(titles[0]) != normalize_title(cand.title):
        return False
    # conflict vetoes: only fields known on BOTH sides can veto
    if cand.volume and work.get("volume") and work.get("volume") != cand.volume:
        return False
    cand_page = first_page(cand.page_range)
    work_page = first_page(work.get("page"))
    if cand_page and work_page and cand_page != work_page:
        return False
    if journals_agree(cand, work) is False:
        return False
    # corroboration
    year = work_year(work)
    if cand.year and year and abs(int(cand.year) - year) <= 1:
        return True
    # equality is implied here: a differing volume or page was vetoed above
    if cand.volume and work.get("volume") and cand_page and work_page:
        return True
    return False


def fetch_doi_for_candidate(session: requests.Session, cand: Candidate,
                            mailto: Optional[str]) -> Optional[str]:
    """Bibliographic CrossRef query for one reference; returns the DOI of the
    first work that passes work_matches_candidate, else None."""
    params = {"query.bibliographic": cand.title, "rows": ROWS_PER_QUERY}
    if mailto:
        params["mailto"] = mailto
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(CROSSREF_WORKS_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            if r.status_code == 429:
                logger.warning("CrossRef rate-limited; backing off")
                time.sleep(10 * attempt)
                continue
            r.raise_for_status()
            items = ((r.json().get("message") or {}).get("items")) or []
            for work in items:
                if work.get("DOI") and work_matches_candidate(work, cand):
                    return work["DOI"]
            return None
        except (requests.RequestException, ValueError) as e:
            logger.warning("CrossRef request failed for %s (attempt %s/%s): %s",
                           cand.curie, attempt, MAX_RETRIES, e)
            if attempt == MAX_RETRIES:
                return None
            time.sleep(5 * attempt)
    return None


def searchable_candidates(candidates: List[Candidate], stats: BackfillStats) -> List[Candidate]:
    """Split off the candidates whose title is missing or too short to be a
    safe match key, recording how many were skipped so the summary reconciles
    candidates vs. what was actually queried."""
    searchable = [c for c in candidates if c.title and len(c.title.strip()) >= MIN_TITLE_LENGTH]
    stats.no_searchable_title = len(candidates) - len(searchable)
    if stats.no_searchable_title:
        logger.info("Skipping %s candidate(s) with no/too-short title", stats.no_searchable_title)
    return searchable


def run(dry_run: bool = False, limit: Optional[int] = None,
        delay: float = DEFAULT_REQUEST_DELAY_SECONDS) -> BackfillStats:
    stats = BackfillStats()
    mailto = os.environ.get("CROSSREF_MAILTO")
    if not mailto:
        logger.warning("CROSSREF_MAILTO not set; requests will use CrossRef's public pool")
    db_session = create_postgres_session(False)
    try:
        if not dry_run:
            set_global_user_id(db_session, "add_missing_dois_from_crossref")
        candidates = get_references_missing_doi(db_session, require_pmid=False, limit=limit)
        stats.candidates = len(candidates)
        logger.info("%s reference(s) with no DOI", stats.candidates)
        searchable = searchable_candidates(candidates, stats)
        # The crawl is rate-limited and can run for hours, so matches are
        # flushed to the database as they accumulate rather than buffered for
        # a single write at the end — an interruption then loses at most one
        # flush window, not the whole run. A dry run instead evaluates all
        # matches in ONE call at the end: nothing is committed, so per-window
        # evaluation would miss intra-run duplicates that span windows and the
        # preview would differ from a real run.
        session = requests.Session()
        matches: List[Tuple[Candidate, str]] = []
        for n, cand in enumerate(searchable, start=1):
            doi = fetch_doi_for_candidate(session, cand, mailto)
            if doi:
                matches.append((cand, doi))
                stats.dois_found += 1
            if n % FLUSH_EVERY_QUERIES == 0 or n == len(searchable):
                if matches and not dry_run:
                    add_doi_cross_references(db_session, matches, stats, dry_run=False)
                    matches = []
                logger.info("CrossRef progress: %s/%s queried, %s matched, %s added",
                            n, len(searchable), stats.dois_found, stats.added)
            if n < len(searchable):
                time.sleep(delay)
        if matches:
            add_doi_cross_references(db_session, matches, stats, dry_run=dry_run)
        logger.info("Done: %s", stats.summary())
        if stats.conflicts:
            logger.warning("DOI conflicts needing curator review (reference, doi, current owner):")
            for ref_curie, doi_curie, owner_curie in stats.conflicts:
                logger.warning("  %s\t%s\t%s", ref_curie, doi_curie, owner_curie)
    finally:
        db_session.close()
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill missing DOIs from CrossRef (SCRUM-4525)")
    parser.add_argument("--dry-run", action="store_true", help="log without writing to the database")
    parser.add_argument("--limit", type=int, default=None, help="max candidate references to process")
    parser.add_argument("--delay", type=float, default=DEFAULT_REQUEST_DELAY_SECONDS,
                        help="seconds between CrossRef requests")
    args = parser.parse_args()
    run(dry_run=args.dry_run, limit=args.limit, delay=args.delay)
