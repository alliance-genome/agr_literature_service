"""
add_missing_dois_from_europepmc.py
==================================

Monthly script (SCRUM-4525) to backfill DOIs from Europe PMC.

For every reference that has a PMID but no (non-obsolete) DOI
cross-reference, look the PMID up in the Europe PMC REST API and, when
Europe PMC knows a DOI for it, add a 'DOI:...' cross-reference — so
curators can reach full text at the publisher.

(A companion CrossRef bibliographic-matching script existed and was
removed from the codebase/crontab; it is parked outside the repo
should the long tail ever be worth its slower fuzzy matching.)

Usage:
    python add_missing_dois_from_europepmc.py [--dry-run] [--limit N] [--batch-size N]

Options:
    --dry-run       Log what would be added without writing to the database
    --limit N       Process at most N candidate references (default: all)
    --batch-size N  PMIDs per Europe PMC search request (default: 100)
"""

import argparse
import logging
import sys
import time
from typing import Callable, Dict, List, Optional, Tuple, Union

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

EUROPEPMC_SEARCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
DEFAULT_BATCH_SIZE = 100
# Bounded by GET URL length, not by Europe PMC's pageSize cap of 1000: each
# PMID adds ~15 chars of "EXT_ID:… OR ", so 300 is ~4.5 KB — safely under
# common 8 KB URL limits, where 1000 (~14 KB) would be rejected outright and
# surface only as a silent per-batch failure.
MAX_BATCH_SIZE = 300
REQUEST_DELAY_SECONDS = 0.4  # stay well under Europe PMC's rate expectations
REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 3
# Hand accumulated matches to the DB writer every N Europe PMC batches, so an
# interrupted run keeps everything committed so far.
FLUSH_EVERY_BATCHES = 25
# Circuit breaker: abort the crawl after this many consecutively failed
# batches. Without it a Europe PMC outage still walks the whole candidate
# list at up to ~3x60s timeouts per batch — days of a cron job doing nothing
# but accumulating warnings. Everything found before the break is already
# flushed, and the summary still prints (with request_failures set).
MAX_CONSECUTIVE_FAILURES = 10


def fetch_dois_for_pmids(session: requests.Session, pmids: List[str],
                         stats: Optional[BackfillStats] = None) -> Optional[Dict[str, str]]:
    """Query Europe PMC for a batch of PMIDs and return {pmid: doi} for the
    records that carry a DOI; {} means the batch legitimately had none. Uses
    the MED (PubMed) source so the returned 'id' is the PMID itself. A batch
    that fails all retries returns None — distinguishable from an empty
    batch, so the caller's circuit breaker can act on it — and counts into
    stats.request_failures, otherwise a run with Europe PMC down would be
    indistinguishable from a clean nothing-to-add run."""
    query = "SRC:MED AND (" + " OR ".join(f"EXT_ID:{p}" for p in pmids) + ")"
    params: Dict[str, Union[str, int]] = {
        "query": query,
        "resultType": "lite",
        "format": "json",
        "pageSize": min(max(len(pmids), 25), 1000),
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(EUROPEPMC_SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            r.raise_for_status()
            payload = r.json()
            # Any deviation from the expected shape — missing/renamed/reshaped
            # envelope, non-dict items — must count as a FAILURE (ValueError ->
            # retried -> request_failures -> circuit breaker), not as "no DOIs
            # in this batch": it would otherwise walk the whole candidate list
            # as a clean zero-yield run. Validated explicitly rather than by
            # widening the except: a TypeError from our own code (e.g. a bad
            # session.get argument) should crash loudly, not be retried and
            # reported as a Europe PMC outage.
            if not isinstance(payload, dict) or "resultList" not in payload:
                keys = sorted(payload)[:5] if isinstance(payload, dict) else type(payload).__name__
                raise ValueError(f"unexpected Europe PMC response shape; top-level: {keys}")
            result_list = payload.get("resultList")
            if not isinstance(result_list, dict):
                raise ValueError(f"unexpected Europe PMC resultList type: {type(result_list).__name__}")
            # No `or []` default: a zero-hit query returns "result": [] with
            # the key PRESENT (verified against the live service), so a
            # missing/renamed/non-list result is a shape failure, not an
            # empty batch.
            results = result_list.get("result")
            if not isinstance(results, list) or not all(isinstance(x, dict) for x in results):
                raise ValueError("unexpected Europe PMC result shape inside resultList")
            return {x["id"]: x["doi"] for x in results if x.get("id") and x.get("doi")}
        except (requests.RequestException, ValueError) as e:
            logger.warning("Europe PMC request failed (attempt %s/%s): %s", attempt, MAX_RETRIES, e)
            if attempt == MAX_RETRIES:
                break
            time.sleep(5 * attempt)
    if stats is not None:
        stats.request_failures += 1
    return None


def collect_additions(candidates: List[Candidate], batch_size: int,
                      stats: BackfillStats,
                      session: Optional[requests.Session] = None,
                      on_flush: Optional[Callable[[List[Tuple[Candidate, str]]], None]] = None
                      ) -> List[Tuple[Candidate, str]]:
    """Look up every candidate's PMID in Europe PMC, in batches, and pair the
    candidates with the DOIs found. With on_flush, accumulated pairs are
    handed off every FLUSH_EVERY_BATCHES requests (and at the end) and the
    return value is empty; without it, all pairs are returned at the end."""
    session = session or requests.Session()
    by_pmid = {c.pmid: c for c in candidates if c.pmid}
    pmids = list(by_pmid.keys())
    additions: List[Tuple[Candidate, str]] = []
    consecutive_failures = 0
    for batch_number, i in enumerate(range(0, len(pmids), batch_size), start=1):
        batch = pmids[i:i + batch_size]
        pmid_to_doi = fetch_dois_for_pmids(session, batch, stats)
        if pmid_to_doi is None:
            consecutive_failures += 1
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                logger.error("Europe PMC failed %s batches in a row; aborting the crawl at "
                             "batch %s (PMIDs %s-%s of %s). Everything found so far is kept.",
                             consecutive_failures, batch_number, i + 1,
                             min(i + batch_size, len(pmids)), len(pmids))
                break
        else:
            consecutive_failures = 0
            for pmid, doi in pmid_to_doi.items():
                if pmid in by_pmid:
                    additions.append((by_pmid[pmid], doi))
                    stats.dois_found += 1
            logger.info("Europe PMC batch %s-%s of %s: %s DOI(s) found",
                        i + 1, min(i + batch_size, len(pmids)), len(pmids), len(pmid_to_doi))
        if on_flush and additions and batch_number % FLUSH_EVERY_BATCHES == 0:
            on_flush(additions)
            additions = []
        if i + batch_size < len(pmids):
            time.sleep(REQUEST_DELAY_SECONDS)
    if on_flush:
        if additions:
            on_flush(additions)
        return []
    return additions


def run(dry_run: bool = False, limit: Optional[int] = None,
        batch_size: int = DEFAULT_BATCH_SIZE) -> BackfillStats:
    stats = BackfillStats()
    if batch_size > MAX_BATCH_SIZE:
        logger.warning("batch size %s would exceed safe GET URL length; clamping to %s",
                       batch_size, MAX_BATCH_SIZE)
        batch_size = MAX_BATCH_SIZE
    db_session = create_postgres_session(False)
    try:
        if not dry_run:
            set_global_user_id(db_session, "add_missing_dois_from_europepmc")
        candidates = get_references_missing_doi(db_session, require_pmid=True, limit=limit)
        stats.candidates = len(candidates)
        logger.info("%s reference(s) with a PMID and no DOI", stats.candidates)

        # Real runs flush to the DB as matches accumulate, so an interruption
        # loses at most one flush window. Dry runs buffer everything and
        # evaluate once at the end instead: nothing is committed, so
        # per-window evaluation would miss intra-run duplicates that span
        # windows and the preview would differ from a real run. The per-flush
        # timing log is the signal for whether the case-insensitive DOI
        # lookup (unindexed since the lower(curie) migration was dropped)
        # ever needs its index back.
        def flush(chunk: List[Tuple[Candidate, str]]) -> None:
            started = time.monotonic()
            add_doi_cross_references(db_session, chunk, stats, dry_run=False)
            logger.info("flushed %s DOI(s) to the database in %.1fs (%s added so far)",
                        len(chunk), time.monotonic() - started, stats.added)

        if dry_run:
            additions = collect_additions(candidates, batch_size, stats)
            add_doi_cross_references(db_session, additions, stats, dry_run=True)
        else:
            collect_additions(candidates, batch_size, stats, on_flush=flush)
        logger.info("Done: %s", stats.summary())
        if stats.conflicts:
            logger.warning("DOI conflicts needing curator review (reference, doi, current owner):")
            for ref_curie, doi_curie, owner_curie in stats.conflicts:
                logger.warning("  %s\t%s\t%s", ref_curie, doi_curie, owner_curie)
    finally:
        db_session.close()
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill missing DOIs from Europe PMC (SCRUM-4525)")
    parser.add_argument("--dry-run", action="store_true", help="log without writing to the database")
    parser.add_argument("--limit", type=int, default=None, help="max candidate references to process")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                        help="PMIDs per Europe PMC request")
    args = parser.parse_args()
    final_stats = run(dry_run=args.dry_run, limit=args.limit, batch_size=args.batch_size)
    # Make failed batches legible to job monitoring: the crontab redirects all
    # output to a log file, so the exit code is the only out-of-band signal.
    if final_stats.request_failures:
        sys.exit(1)
