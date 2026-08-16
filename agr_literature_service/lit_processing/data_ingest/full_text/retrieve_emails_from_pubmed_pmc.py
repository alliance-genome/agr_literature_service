"""
retrieve_emails_from_pubmed_pmc.py
==================================

Retrieve author emails for recently added SGD corpus papers from the free
structured sources -- PubMed metadata and PMC full-text XML -- and write them
to a flat file, to see how many of the emails the Markdown-based extraction
(extract_emails.py) misses could be backfilled without touching full text.

Two tiers, cheapest first (no AI, no PDF parsing):
  1. PubMed E-utilities efetch: AffiliationInfo strings often carry the
     corresponding author's email directly for papers from the last ~10 years.
  2. PMC full-text JATS XML (for papers with a PMCID): the <corresp> element
     in the front matter tags the corresponding author's email explicitly,
     with a <contrib-group>//<email> fallback for journals that attach the
     email to the contributor instead. By default only papers tier 1 found
     nothing for are fetched from PMC (the agreed cascade); --pmc-all fetches
     every paper with a PMCID so the two tiers can be compared.

Selection: references in the SGD corpus (mod_corpus_association.corpus IS
TRUE) whose corpus association was created in the --since/--until window
(default 2025-01-01 to 2027-01-01, i.e. papers added in 2025 and 2026) and
that have a non-obsolete PMID cross_reference. The DB is only read -- nothing
is written back; loading the addresses is a separate decision once the
output has been reviewed.

Every candidate address goes through the same normalization / role-account /
garbage filtering as the Markdown extraction (extract_emails.py), so the two
pipelines accept the same addresses.

Output: a tab-delimited file, one row per (reference, source, email):
    reference_curie  pmid  source(pubmed_metadata|pmc_corresp)  email  has_email_in_abc(Y|N)
has_email_in_abc says whether the reference already has reference_email rows
(from the Markdown extraction or a curator); rows with N are the backfill
candidates. A summary of per-tier and backfill coverage is logged at the end.

NCBI etiquette: requests are POSTed (large id lists), throttled to 3/s
(10/s when NCBI_API_KEY is set in the environment), and retried with backoff
on transient failures. elink pubmed->pmc MUST send one id parameter per PMID:
a single comma-joined list makes NCBI pool every input into one LinkSet and
almost no paper appears to have a PMCID.
"""
import argparse
import logging
import time
from os import environ
from typing import Dict, Iterator, List, Optional, Tuple
from xml.etree import ElementTree

import requests
from dotenv import load_dotenv

from agr_literature_service.lit_processing.data_ingest.full_text.extract_emails import (
    extract_emails_fallback,
    extract_emails_primary,
)
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import (
    create_postgres_session,
)
from sqlalchemy import text

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()

EUTILS_BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
NCBI_API_KEY = environ.get("NCBI_API_KEY", "")

MOD_ABBREVIATION = "SGD"
DEFAULT_SINCE = "2025-01-01"
DEFAULT_UNTIL = "2027-01-01"
DEFAULT_OUTPUT_FILE = "sgd_emails_from_pubmed_pmc.tsv"

SOURCE_PUBMED = "pubmed_metadata"
SOURCE_PMC = "pmc_corresp"

REQUEST_TIMEOUT = 300
REQUEST_RETRIES = 3
RETRY_BACKOFF_SECONDS = 10
# NCBI allows 3 requests/s without an API key, 10/s with one.
REQUEST_INTERVAL = 0.11 if NCBI_API_KEY else 0.34

EFETCH_PUBMED_CHUNK = 200
ELINK_CHUNK = 200
# PMC efetch returns the FULL JATS document per article, so keep chunks small.
EFETCH_PMC_CHUNK = 50

PROGRESS_LOG_INTERVAL = 1000

_last_request_time = 0.0


def _chunks(items: List[str], size: int) -> Iterator[List[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def eutils_post(endpoint: str, params: Dict[str, str], ids: List[str]) -> Optional[bytes]:
    """POST an E-utilities request with one id parameter per identifier
    (required for per-id elink results; harmless for efetch). Throttled and
    retried with backoff; returns None when all attempts fail."""
    global _last_request_time
    url = f"{EUTILS_BASE_URL}/{endpoint}.fcgi"
    data: List[Tuple[str, str]] = list(params.items())
    if NCBI_API_KEY:
        data.append(("api_key", NCBI_API_KEY))
    data.extend(("id", one_id) for one_id in ids)
    for attempt in range(1, REQUEST_RETRIES + 1):
        wait = REQUEST_INTERVAL - (time.monotonic() - _last_request_time)
        if wait > 0:
            time.sleep(wait)
        _last_request_time = time.monotonic()
        try:
            response = requests.post(url, data=data, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            logger.warning("%s attempt %d/%d failed (%d ids): %s",
                           endpoint, attempt, REQUEST_RETRIES, len(ids), e)
            if attempt < REQUEST_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    return None


def emails_from_text(content: str) -> List[str]:
    """Extract emails from a text fragment with the SAME normalization,
    role-account suppression, and garbage filtering as the Markdown pipeline
    (extract_emails.py), so both extractors accept identical addresses."""
    if not content:
        return []
    emails, _bad = extract_emails_primary(content, set())
    if not emails:
        emails, _bad = extract_emails_fallback(content, set())
    return emails


def get_sgd_corpus_papers(db, since: str, until: str) -> List[Tuple[str, str, bool]]:
    """Return (reference_curie, pmid, has_email_in_abc) for references added
    to the SGD corpus in the window (mod_corpus_association.date_created) that
    have a non-obsolete PMID cross_reference. has_email_in_abc flags papers
    that already carry reference_email rows (Markdown extraction or curator),
    so the output can separate backfill candidates from confirmations."""
    rows = db.execute(text(
        "SELECT r.curie, MIN(cr.curie), "
        "       EXISTS (SELECT 1 FROM reference_email re "
        "               WHERE re.reference_id = r.reference_id) "
        "FROM   mod_corpus_association mca "
        "JOIN   mod m ON mca.mod_id = m.mod_id "
        "JOIN   reference r ON mca.reference_id = r.reference_id "
        "JOIN   cross_reference cr ON cr.reference_id = r.reference_id "
        "       AND cr.curie_prefix = 'PMID' AND cr.is_obsolete IS FALSE "
        "WHERE  m.abbreviation = :abbr "
        "AND    mca.corpus IS TRUE "
        "AND    mca.date_created >= :since "
        "AND    mca.date_created < :until "
        "GROUP  BY r.curie, r.reference_id"
    ), {"abbr": MOD_ABBREVIATION, "since": since, "until": until}).fetchall()
    return [(row[0], row[1].replace("PMID:", ""), row[2]) for row in rows]


def fetch_pubmed_emails(pmids: List[str]) -> Dict[str, List[str]]:
    """Tier 1: efetch the PubMed records in chunks and extract emails from
    their AffiliationInfo strings. Returns pmid -> emails (only PMIDs with at
    least one email)."""
    emails_by_pmid: Dict[str, List[str]] = {}
    fetched = 0
    for chunk in _chunks(pmids, EFETCH_PUBMED_CHUNK):
        content = eutils_post("efetch", {"db": "pubmed", "retmode": "xml"}, chunk)
        if content is None:
            logger.warning("Skipping a PubMed chunk of %d PMIDs after retries", len(chunk))
            continue
        root = ElementTree.fromstring(content)
        for article in root.findall(".//PubmedArticle"):
            # direct child lookup: .//PMID would also match the PMIDs of
            # comments/corrections and erratum links
            pmid = article.findtext("MedlineCitation/PMID") or ""
            affiliations = " ".join(
                aff.text or "" for aff in article.findall(".//AffiliationInfo/Affiliation"))
            emails = emails_from_text(affiliations)
            if pmid and emails:
                emails_by_pmid[pmid] = emails
        fetched += len(chunk)
        if fetched % PROGRESS_LOG_INTERVAL < EFETCH_PUBMED_CHUNK:
            logger.info("PubMed: fetched %d/%d records (%d with emails so far)",
                        fetched, len(pmids), len(emails_by_pmid))
    return emails_by_pmid


def map_pmids_to_pmcids(pmids: List[str]) -> Dict[str, str]:
    """Map PMIDs to PMC ids via elink. Each PMID is sent as its own id
    parameter so NCBI returns one LinkSet per input (see module docstring)."""
    pmid_to_pmcid: Dict[str, str] = {}
    for chunk in _chunks(pmids, ELINK_CHUNK):
        content = eutils_post(
            "elink", {"dbfrom": "pubmed", "db": "pmc", "linkname": "pubmed_pmc"}, chunk)
        if content is None:
            logger.warning("Skipping an elink chunk of %d PMIDs after retries", len(chunk))
            continue
        root = ElementTree.fromstring(content)
        for linkset in root.findall(".//LinkSet"):
            pmid = linkset.findtext("IdList/Id")
            pmcid = linkset.findtext(".//LinkSetDb/Link/Id")
            if pmid and pmcid:
                pmid_to_pmcid[pmid] = pmcid
    return pmid_to_pmcid


def fetch_pmc_emails(pmid_to_pmcid: Dict[str, str]) -> Dict[str, List[str]]:
    """Tier 2: efetch the PMC JATS XML in chunks and extract emails from the
    front-matter <corresp> elements, falling back to <contrib-group>//<email>
    for journals that attach the address to the contributor instead. Returns
    pmid -> emails (only PMIDs with at least one email)."""
    emails_by_pmid: Dict[str, List[str]] = {}
    pmcid_to_pmid = {pmcid: pmid for pmid, pmcid in pmid_to_pmcid.items()}
    pmcids = sorted(pmid_to_pmcid.values())
    fetched = 0
    for chunk in _chunks(pmcids, EFETCH_PMC_CHUNK):
        content = eutils_post("efetch", {"db": "pmc", "retmode": "xml"}, chunk)
        if content is None:
            logger.warning("Skipping a PMC chunk of %d articles after retries", len(chunk))
            continue
        root = ElementTree.fromstring(content)
        for article in root.findall(".//article"):
            ids = {aid.get("pub-id-type"): (aid.text or "").strip()
                   for aid in article.findall("front/article-meta/article-id")}
            pmid = ids.get("pmid") or pmcid_to_pmid.get(ids.get("pmc", ""), "")
            corresp_text = " ".join(
                "".join(corresp.itertext()) for corresp in article.findall("front//corresp"))
            emails = emails_from_text(corresp_text)
            if not emails:
                contrib_text = " ".join(
                    "".join(email_el.itertext())
                    for email_el in article.findall("front//contrib-group//email"))
                emails = emails_from_text(contrib_text)
            if pmid and emails:
                emails_by_pmid[pmid] = emails
        fetched += len(chunk)
        logger.info("PMC: fetched %d/%d articles (%d with emails so far)",
                    fetched, len(pmcids), len(emails_by_pmid))
    return emails_by_pmid


def write_output_file(output_file: str, papers: List[Tuple[str, str, bool]],
                      pubmed_emails: Dict[str, List[str]],
                      pmc_emails: Dict[str, List[str]]) -> int:
    """Write one row per (reference, source, email); returns the row count."""
    row_count = 0
    with open(output_file, "w") as fw:
        fw.write("reference_curie\tpmid\tsource\temail\thas_email_in_abc\n")
        for curie, pmid, has_email in sorted(papers):
            flag = "Y" if has_email else "N"
            for source, emails_by_pmid in ((SOURCE_PUBMED, pubmed_emails),
                                           (SOURCE_PMC, pmc_emails)):
                for email in emails_by_pmid.get(pmid, []):
                    fw.write(f"{curie}\t{pmid}\t{source}\t{email}\t{flag}\n")
                    row_count += 1
    return row_count


def retrieve_emails(since: str, until: str, output_file: str, pmc_all: bool) -> None:
    db = create_postgres_session(False)
    try:
        papers = get_sgd_corpus_papers(db, since, until)
    finally:
        db.close()
    logger.info("%d SGD corpus papers with a PMID added between %s and %s",
                len(papers), since, until)
    if not papers:
        return

    pmids = sorted({pmid for _curie, pmid, _has_email in papers})
    pubmed_emails = fetch_pubmed_emails(pmids)
    logger.info("Tier 1 (PubMed metadata): emails for %d/%d papers",
                len(pubmed_emails), len(pmids))

    # The agreed cascade only consults PMC for papers tier 1 missed;
    # --pmc-all fetches everything with a PMCID so the tiers can be compared.
    pmc_candidates = pmids if pmc_all else [p for p in pmids if p not in pubmed_emails]
    pmid_to_pmcid = map_pmids_to_pmcids(pmc_candidates)
    logger.info("PMCIDs found for %d/%d candidate papers",
                len(pmid_to_pmcid), len(pmc_candidates))
    pmc_emails = fetch_pmc_emails(pmid_to_pmcid)
    logger.info("Tier 2 (PMC <corresp>): emails for %d papers", len(pmc_emails))

    row_count = write_output_file(output_file, papers, pubmed_emails, pmc_emails)

    covered = set(pubmed_emails) | set(pmc_emails)
    without_abc_email = {pmid for _curie, pmid, has_email in papers if not has_email}
    backfillable = without_abc_email & covered
    logger.info(
        "Done: %d papers, tier1=%d tier2=%d combined=%d (%.0f%%); "
        "%d papers have no email in ABC yet, %d of those are backfillable "
        "from these sources; %d rows written to %s",
        len(pmids), len(pubmed_emails), len(pmc_emails), len(covered),
        100 * len(covered) / len(pmids),
        len(without_abc_email), len(backfillable), row_count, output_file,
    )


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Retrieve author emails for SGD corpus papers added in the window "
                    "from PubMed metadata and PMC full-text XML, into a flat file"
    )
    parser.add_argument(
        "-s", "--since",
        default=DEFAULT_SINCE,
        help=f"Corpus-addition window start, inclusive (default: {DEFAULT_SINCE})",
    )
    parser.add_argument(
        "-u", "--until",
        default=DEFAULT_UNTIL,
        help=f"Corpus-addition window end, exclusive (default: {DEFAULT_UNTIL})",
    )
    parser.add_argument(
        "-o", "--output-file",
        default=DEFAULT_OUTPUT_FILE,
        help=f"Tab-delimited output file (default: {DEFAULT_OUTPUT_FILE})",
    )
    parser.add_argument(
        "--pmc-all",
        action="store_true",
        help="Fetch PMC XML for every paper with a PMCID, not only the papers "
             "PubMed metadata found no email for (for tier comparison)",
    )
    args = parser.parse_args()

    retrieve_emails(since=args.since, until=args.until,
                    output_file=args.output_file, pmc_all=args.pmc_all)
