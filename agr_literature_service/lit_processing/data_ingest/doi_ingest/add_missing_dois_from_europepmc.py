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
from typing import Dict, List, Optional, Tuple, Union

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
MAX_BATCH_SIZE = 1000  # Europe PMC's pageSize cap
REQUEST_DELAY_SECONDS = 0.4  # stay well under Europe PMC's rate expectations
REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 3


def fetch_dois_for_pmids(session: requests.Session, pmids: List[str]) -> Dict[str, str]:
    """Query Europe PMC for a batch of PMIDs and return {pmid: doi} for the
    records that carry a DOI. Uses the MED (PubMed) source so the returned
    'id' is the PMID itself."""
    query = "SRC:MED AND (" + " OR ".join(f"EXT_ID:{p}" for p in pmids) + ")"
    params: Dict[str, Union[str, int]] = {
        "query": query,
        "resultType": "lite",
        "format": "json",
        "pageSize": min(max(len(pmids), 25), MAX_BATCH_SIZE),
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(EUROPEPMC_SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            r.raise_for_status()
            results = (r.json().get("resultList") or {}).get("result") or []
            return {x["id"]: x["doi"] for x in results if x.get("id") and x.get("doi")}
        except (requests.RequestException, ValueError) as e:
            logger.warning("Europe PMC request failed (attempt %s/%s): %s", attempt, MAX_RETRIES, e)
            if attempt == MAX_RETRIES:
                return {}
            time.sleep(5 * attempt)
    return {}


def collect_additions(candidates: List[Candidate], batch_size: int,
                      stats: BackfillStats,
                      session: Optional[requests.Session] = None) -> List[Tuple[Candidate, str]]:
    """Look up every candidate's PMID in Europe PMC, in batches, and pair the
    candidates with the DOIs found."""
    session = session or requests.Session()
    by_pmid = {c.pmid: c for c in candidates if c.pmid}
    pmids = list(by_pmid.keys())
    additions: List[Tuple[Candidate, str]] = []
    for i in range(0, len(pmids), batch_size):
        batch = pmids[i:i + batch_size]
        pmid_to_doi = fetch_dois_for_pmids(session, batch)
        for pmid, doi in pmid_to_doi.items():
            if pmid in by_pmid:
                additions.append((by_pmid[pmid], doi))
        logger.info("Europe PMC batch %s-%s of %s: %s DOI(s) found",
                    i + 1, min(i + batch_size, len(pmids)), len(pmids), len(pmid_to_doi))
        if i + batch_size < len(pmids):
            time.sleep(REQUEST_DELAY_SECONDS)
    stats.dois_found = len(additions)
    return additions


def run(dry_run: bool = False, limit: Optional[int] = None,
        batch_size: int = DEFAULT_BATCH_SIZE) -> BackfillStats:
    stats = BackfillStats()
    if batch_size > MAX_BATCH_SIZE:
        logger.warning("batch size %s exceeds Europe PMC's pageSize cap; clamping to %s",
                       batch_size, MAX_BATCH_SIZE)
        batch_size = MAX_BATCH_SIZE
    db_session = create_postgres_session(False)
    try:
        if not dry_run:
            set_global_user_id(db_session, "add_missing_dois_from_europepmc")
        candidates = get_references_missing_doi(db_session, require_pmid=True, limit=limit)
        stats.candidates = len(candidates)
        logger.info("%s reference(s) with a PMID and no DOI", stats.candidates)
        additions = collect_additions(candidates, batch_size, stats)
        add_doi_cross_references(db_session, additions, stats, dry_run=dry_run)
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
    run(dry_run=args.dry_run, limit=args.limit, batch_size=args.batch_size)
