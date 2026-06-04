"""Resolve NCBI GEO Series (GSE) accessions for one or more PMIDs.

Uses E-utilities `elink` (`dbfrom=pubmed&db=gds&linkname=pubmed_gds`) to find
linked GDS UIDs, then `esummary` to translate those UIDs into accession
strings (filtering down to GSE Series entries only).
"""
import logging
import os
import sys
import time
from typing import Dict, List, Optional

import requests

ELINK_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi"
ESUMMARY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"

# Conservative throttle: 3 req/s without an API key, 10 req/s with one.
_THROTTLE_SECONDS_NO_KEY = 0.34
_THROTTLE_SECONDS_WITH_KEY = 0.11

_MAX_RETRIES = 5
_RETRY_BACKOFF_BASE = 2

logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format='%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def _base_params() -> Dict[str, str]:
    api_key = os.environ.get("NCBI_API_KEY")
    return {"api_key": api_key} if api_key else {}


def _throttle() -> None:
    delay = _THROTTLE_SECONDS_WITH_KEY if os.environ.get("NCBI_API_KEY") else _THROTTLE_SECONDS_NO_KEY
    time.sleep(delay)


def _get_with_retry(url: str, params: Dict[str, str]) -> dict:
    last_exc: Optional[requests.exceptions.RequestException] = None
    for attempt in range(_MAX_RETRIES):
        wait = _RETRY_BACKOFF_BASE ** attempt
        r = None
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.JSONDecodeError as exc:
            body_preview = r.text[:500] if r is not None and r.text else ""
            logger.warning(
                "NCBI %s returned non-JSON (attempt %d/%d): %s; body[:500]=%r; retrying in %ds",
                url, attempt + 1, _MAX_RETRIES, exc, body_preview, wait,
            )
            last_exc = exc
        except requests.exceptions.RequestException as exc:
            logger.warning(
                "NCBI request to %s failed (attempt %d/%d): %s; retrying in %ds",
                url, attempt + 1, _MAX_RETRIES, exc, wait,
            )
            last_exc = exc
        time.sleep(wait)
    assert last_exc is not None
    raise last_exc


def get_geo_accessions_for_pmid(pmid: str) -> List[str]:
    """Return sorted, deduplicated list of GSE accessions linked from `pmid`."""
    result = get_geo_accessions_for_pmids([pmid])
    return result.get(pmid, [])


def get_geo_accessions_for_pmids(pmids: List[str]) -> Dict[str, List[str]]:
    """Batched form: returns {pmid: [GSE...], ...} for every input pmid."""
    if not pmids:
        return {}

    # elink is asked for the whole batch with comma-joined ids. NCBI merges these
    # into a single linkset (all source PMIDs pooled, no per-PMID attribution),
    # which is fine here: we only use elink to discover the set of candidate GDS
    # UIDs for the batch. The expensive alternative -- repeated `&id=` params to
    # force one linkset per PMID -- makes NCBI drop the chunked response for
    # batches of ~100, so we recover per-PMID attribution from esummary instead
    # (each GDS record carries its own `pubmedids`).
    elink_params = {
        "dbfrom": "pubmed",
        "db": "gds",
        "linkname": "pubmed_gds",
        "retmode": "json",
        "id": ",".join(pmids),
    }
    elink_params.update(_base_params())
    elink_data = _get_with_retry(ELINK_URL, elink_params)
    _throttle()

    candidate_uids: set = set()
    for linkset in elink_data.get("linksets", []) or []:
        for ldb in linkset.get("linksetdbs", []) or []:
            if ldb.get("linkname") != "pubmed_gds":
                continue
            for uid in ldb.get("links", []) or []:
                candidate_uids.add(str(uid))

    output: Dict[str, List[str]] = {p: [] for p in pmids}
    if not candidate_uids:
        return output

    unique_uids = sorted(candidate_uids)
    esummary_params = {"db": "gds", "id": ",".join(unique_uids), "retmode": "json"}
    esummary_params.update(_base_params())
    esummary_data = _get_with_retry(ESUMMARY_URL, esummary_params)
    _throttle()

    # Attribute each GSE accession back to the specific PMID(s) in our batch using
    # the record's own `pubmedids` reverse-link (GPL/GSM/GDS accessions are dropped).
    pmid_set = set(pmids)
    per_pmid: Dict[str, set] = {p: set() for p in pmids}
    result_block = esummary_data.get("result", {}) or {}
    for uid in unique_uids:
        record = result_block.get(uid) or {}
        accession = record.get("accession")
        if not (isinstance(accession, str) and accession.startswith("GSE")):
            continue
        for pm in record.get("pubmedids", []) or []:
            pm_str = str(pm)
            if pm_str in pmid_set:
                per_pmid[pm_str].add(accession)

    return {pmid: sorted(per_pmid[pmid]) for pmid in pmids}


def get_geo_accessions_for_pmids_with_split(pmids: List[str]) -> Dict[str, List[str]]:
    """Like get_geo_accessions_for_pmids, but if NCBI returns an unrecoverable
    failure for the batch we split the PMID list in half and retry each half.
    Recurses down to single-PMID granularity; a single PMID that still fails is
    logged at ERROR and dropped from the result dict (caller treats that the
    same as 'no GEO links found' for this run; next nightly run will retry it).
    Never raises for NCBI-side failures."""
    if not pmids:
        return {}
    try:
        return get_geo_accessions_for_pmids(pmids)
    except Exception as exc:
        if len(pmids) == 1:
            logger.error("elink lookup for PMID %s gave up after retries: %s", pmids[0], exc)
            return {}
        mid = len(pmids) // 2
        logger.warning(
            "elink batch of %d PMIDs failed after retries; splitting into %d + %d. Cause: %s",
            len(pmids), mid, len(pmids) - mid, exc,
        )
        left = get_geo_accessions_for_pmids_with_split(pmids[:mid])
        right = get_geo_accessions_for_pmids_with_split(pmids[mid:])
        return {**left, **right}


def _cli() -> None:  # pragma: no cover
    import argparse
    parser = argparse.ArgumentParser(description="Print GEO Series accessions for one or more PMIDs.")
    parser.add_argument("pmids", nargs="+", help="One or more PubMed IDs")
    args = parser.parse_args()
    result = get_geo_accessions_for_pmids(args.pmids)
    for pmid in args.pmids:
        print(f"{pmid}\t{','.join(result.get(pmid, []))}")


if __name__ == "__main__":  # pragma: no cover
    _cli()
