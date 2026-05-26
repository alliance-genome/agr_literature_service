"""Resolve NCBI GEO Series (GSE) accessions for one or more PMIDs.

Uses E-utilities `elink` (`dbfrom=pubmed&db=gds&linkname=pubmed_gds`) to find
linked GDS UIDs, then `esummary` to translate those UIDs into accession
strings (filtering down to GSE Series entries only).
"""
import logging
import os
import sys
import time
from typing import Dict, List

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
    last_exc = None
    for attempt in range(_MAX_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            wait = _RETRY_BACKOFF_BASE ** attempt
            logger.warning("NCBI request to %s failed (attempt %d/%d): %s; retrying in %ds",
                           url, attempt + 1, _MAX_RETRIES, exc, wait)
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

    pmid_to_uids: Dict[str, List[str]] = {p: [] for p in pmids}
    all_uids: List[str] = []
    for linkset in elink_data.get("linksets", []) or []:
        source_ids = linkset.get("ids") or []
        if not source_ids:
            continue
        source_pmid = str(source_ids[0])
        for ldb in linkset.get("linksetdbs", []) or []:
            if ldb.get("linkname") != "pubmed_gds":
                continue
            for uid in ldb.get("links", []) or []:
                uid_str = str(uid)
                pmid_to_uids.setdefault(source_pmid, []).append(uid_str)
                all_uids.append(uid_str)

    if not all_uids:
        return {p: [] for p in pmids}

    unique_uids = sorted(set(all_uids))
    esummary_params = {"db": "gds", "id": ",".join(unique_uids), "retmode": "json"}
    esummary_params.update(_base_params())
    esummary_data = _get_with_retry(ESUMMARY_URL, esummary_params)
    _throttle()

    uid_to_accession: Dict[str, str] = {}
    result_block = esummary_data.get("result", {}) or {}
    for uid in unique_uids:
        record = result_block.get(uid) or {}
        accession = record.get("accession")
        if isinstance(accession, str) and accession.startswith("GSE"):
            uid_to_accession[uid] = accession

    output: Dict[str, List[str]] = {}
    for pmid, uids in pmid_to_uids.items():
        accessions = {uid_to_accession[u] for u in uids if u in uid_to_accession}
        output[pmid] = sorted(accessions)
    return output


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
