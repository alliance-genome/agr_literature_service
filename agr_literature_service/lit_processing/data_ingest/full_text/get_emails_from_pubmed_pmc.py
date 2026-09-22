"""
get_emails_from_pubmed_pmc.py
=============================

Get author emails for corpus references from PubMed metadata and PMC
full-text XML, and set the email-extraction workflow tag accordingly
(SCRUM-6430).

Workflow (per the ticket):
    get reference -> download emails ->
        emails found -> load them + set "email extraction complete"
        no emails    -> set "email extraction needed"
so the Markdown/full-text extraction pipeline (extract_emails.py) only has
to handle the papers these free structured sources cannot cover.

Two tiers, cheapest first (no AI, no PDF parsing):
  1. PubMed E-utilities efetch: AffiliationInfo strings often carry the
     corresponding author's email directly for papers from the last ~10 years.
  2. PMC full-text JATS XML (for papers with a PMCID): the <corresp> element
     in the front matter tags the corresponding author's email explicitly,
     with a <contrib-group>//<email> fallback for journals that attach the
     email to the contributor instead. Only papers tier 1 found nothing for
     are fetched from PMC unless --pmc-all is given.

Selection: references in the corpus (mod_corpus_association.corpus IS TRUE)
of the MODs that run email extraction (detected from the workflow_transition
table; currently FB, WB, ZFIN, SGD and XB), whose corpus association was
created in the --since/--until window (default: 2024-01-01 to current), that
have a non-obsolete PMID cross_reference and no automation-added
reference_email rows yet (papers whose only emails were entered by a curator
ARE picked up -- the curator rows are never modified, see below). Untagged
reference+mod pairs and pairs pending in "email extraction needed"/"failed"
are processed; pairs already "complete"/"in progress" are never touched, so
repeated runs only process new papers and still-unresolved pairs: the first
run doubles as the backfill (verified against the production database
2026-08-17: ~10.9k/5.1k/4.9k email-less reference+mod pairs added in
2024/2025/2026; 2023 and earlier were left out -- nothing before 2024 was
ever email-processed and the 2022 numbers (~340k) are the initial bulk corpus
import, a separate decision).

Every candidate address goes through the same normalization / role-account /
garbage filtering as the Markdown extraction (extract_emails.py), so the two
pipelines accept the same addresses. Addresses are loaded through
set_reference_emails(), which -- exactly as in extract_emails.py -- never
deletes or duplicates human (curator)-added rows: only automation-added rows
are replaced.

Tagging per reference+mod pair (transition_to_workflow_status for every state
change; a direct WorkflowTagModel insert only seeds the initial "needed" tag,
which the transition function cannot create):
    emails found:  "email extraction needed" is transitioned to "email
                   extraction complete" (untagged pairs are seeded "needed"
                   first; "failed" pairs walk the retry transition
                   "failed" -> "needed" first).
    no emails:     "needed" stays "needed" -- the full-text pipeline
                   (extract_emails.py) processes the paper once a converted
                   file is available; untagged pairs are seeded "needed".
The proceed_on_value idempotency guard (SCRUM-6166) keeps the text-conversion
action from seeding a second "needed" tag later for pairs this script marked.

The default (no --commit) is a dry run: emails are downloaded and the planned
changes are reported, but nothing is written to the database.

NCBI etiquette: requests are POSTed (large id lists), throttled to 3/s
(10/s when NCBI_API_KEY is set in the environment), and retried with backoff
on transient failures. elink pubmed->pmc MUST send one id parameter per PMID:
a single comma-joined list makes NCBI pool every input into one LinkSet and
almost no paper appears to have a PMCID.

Usage:
    # dry run (default): 2024-01-01 to current, all email MODs
    python get_emails_from_pubmed_pmc.py

    # apply
    python get_emails_from_pubmed_pmc.py --commit

    # limit to specific MODs and/or a narrower window
    python get_emails_from_pubmed_pmc.py --commit --mods SGD,WB --since 2026-01-01
"""
import argparse
import logging
import time
from datetime import date, timedelta
from os import environ, path
from typing import Dict, Iterator, List, Optional, Tuple
from xml.etree import ElementTree

import requests
from dotenv import load_dotenv
from sqlalchemy import text

from agr_literature_service.api.crud.reference_crud import (
    CURATOR_USER_ID_PREFIX,
    set_reference_emails,
)
from agr_literature_service.api.crud.workflow_tag_crud import transition_to_workflow_status
from agr_literature_service.api.models import WorkflowTagModel
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.data_ingest.full_text.extract_emails import (
    extract_emails_fallback,
    extract_emails_primary,
)
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import (
    create_postgres_session,
)

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()

EUTILS_BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
NCBI_API_KEY = environ.get("NCBI_API_KEY", "")

# Email-extraction process (ATP:0000354) and its sub-states.
EMAIL_PROCESS_PARENT = "ATP:0000354"          # email extraction
EMAIL_IN_PROGRESS = "ATP:0000357"             # email extraction in progress
EMAIL_COMPLETE = "ATP:0000355"                # email extraction complete
EMAIL_FAILED = "ATP:0000356"                  # email extraction failed
EMAIL_NEEDED = "ATP:0000358"                  # email extraction needed

EMAIL_TAGS: List[str] = [
    EMAIL_PROCESS_PARENT,
    EMAIL_IN_PROGRESS,
    EMAIL_COMPLETE,
    EMAIL_FAILED,
    EMAIL_NEEDED,
]
# Pairs in one of these states are settled; never re-processed.
EMAIL_SETTLED_TAGS: List[str] = [EMAIL_PROCESS_PARENT, EMAIL_IN_PROGRESS, EMAIL_COMPLETE]

SOURCE_PUBMED = "pubmed_metadata"
SOURCE_PMC = "pmc_corresp"

DEFAULT_SINCE = "2024-01-01"

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


# ----------------------------------------------------------------------
# NCBI E-utilities layer
# ----------------------------------------------------------------------
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


def parse_pubmed_emails(content: bytes) -> Dict[str, List[str]]:
    """pmid -> emails from a PubMed efetch XML blob (AffiliationInfo strings)."""
    emails_by_pmid: Dict[str, List[str]] = {}
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
    return emails_by_pmid


def parse_pmc_emails(content: bytes, pmcid_to_pmid: Dict[str, str]) -> Dict[str, List[str]]:
    """pmid -> emails from a PMC efetch JATS XML blob: front-matter <corresp>
    first, then <contrib-group>//<email> for journals that attach the address
    to the contributor instead."""
    emails_by_pmid: Dict[str, List[str]] = {}
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
    return emails_by_pmid


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
        try:
            parsed = parse_pubmed_emails(content)
        except ElementTree.ParseError as e:
            logger.warning("Skipping an unparseable PubMed chunk of %d PMIDs: %s",
                           len(chunk), e)
            continue
        if b"<ERROR" in content:
            # a well-formed NCBI error document parses cleanly but carries no
            # (or not all) articles -- make the silent case visible
            logger.warning("NCBI error in a PubMed chunk of %d PMIDs", len(chunk))
        emails_by_pmid.update(parsed)
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
        try:
            root = ElementTree.fromstring(content)
        except ElementTree.ParseError as e:
            logger.warning("Skipping an unparseable elink chunk of %d PMIDs: %s",
                           len(chunk), e)
            continue
        if b"<ERROR" in content:
            logger.warning("NCBI error in an elink chunk of %d PMIDs", len(chunk))
        for linkset in root.findall(".//LinkSet"):
            pmid = linkset.findtext("IdList/Id")
            pmcid = linkset.findtext(".//LinkSetDb/Link/Id")
            if pmid and pmcid:
                pmid_to_pmcid[pmid] = pmcid
    return pmid_to_pmcid


def fetch_pmc_emails(pmid_to_pmcid: Dict[str, str]) -> Dict[str, List[str]]:
    """Tier 2: efetch the PMC JATS XML in chunks and extract emails from the
    front matter. Returns pmid -> emails (only PMIDs with at least one email)."""
    emails_by_pmid: Dict[str, List[str]] = {}
    pmcid_to_pmid = {pmcid: pmid for pmid, pmcid in pmid_to_pmcid.items()}
    pmcids = sorted(pmid_to_pmcid.values())
    fetched = 0
    for chunk in _chunks(pmcids, EFETCH_PMC_CHUNK):
        content = eutils_post("efetch", {"db": "pmc", "retmode": "xml"}, chunk)
        if content is None:
            logger.warning("Skipping a PMC chunk of %d articles after retries", len(chunk))
            continue
        try:
            parsed = parse_pmc_emails(content, pmcid_to_pmid)
        except ElementTree.ParseError as e:
            logger.warning("Skipping an unparseable PMC chunk of %d articles: %s",
                           len(chunk), e)
            continue
        if b"<ERROR" in content:
            logger.warning("NCBI error in a PMC chunk of %d articles", len(chunk))
        emails_by_pmid.update(parsed)
        fetched += len(chunk)
        logger.info("PMC: fetched %d/%d articles (%d with emails so far)",
                    fetched, len(pmcids), len(emails_by_pmid))
    return emails_by_pmid


def download_emails(pmids: List[str], pmc_all: bool = False) -> Dict[str, Tuple[List[str], str]]:
    """Run the two-tier cascade over the PMIDs. Returns
    pmid -> (emails, source) for every PMID at least one address was found
    for. Tier 2 is only consulted for the PMIDs tier 1 missed; with pmc_all,
    every paper with a PMCID is fetched from PMC as well and both tiers'
    addresses are unioned (PubMed's first) when both hit -- papers with two
    corresponding authors sometimes carry a different one in each source."""
    pubmed_emails = fetch_pubmed_emails(pmids)
    logger.info("Tier 1 (PubMed metadata): emails for %d/%d papers",
                len(pubmed_emails), len(pmids))

    pmc_candidates = pmids if pmc_all else [p for p in pmids if p not in pubmed_emails]
    pmid_to_pmcid = map_pmids_to_pmcids(pmc_candidates)
    logger.info("PMCIDs found for %d/%d candidate papers",
                len(pmid_to_pmcid), len(pmc_candidates))
    pmc_emails = fetch_pmc_emails(pmid_to_pmcid)
    logger.info("Tier 2 (PMC <corresp>): emails for %d papers", len(pmc_emails))

    results: Dict[str, Tuple[List[str], str]] = {}
    for pmid, emails in pmc_emails.items():
        results[pmid] = (emails, SOURCE_PMC)
    for pmid, emails in pubmed_emails.items():
        if pmid in results:
            # both tiers hit (only possible with pmc_all): union the addresses
            merged = emails + [e for e in results[pmid][0] if e not in emails]
            results[pmid] = (merged, SOURCE_PUBMED + "|" + SOURCE_PMC)
        else:
            results[pmid] = (emails, SOURCE_PUBMED)
    return results


# ----------------------------------------------------------------------
# Selection
# ----------------------------------------------------------------------
def get_email_extraction_mods(db) -> Dict[int, str]:
    """Return {mod_id: abbreviation} for MODs that run email extraction, i.e.
    those that have any workflow_transition within the email-extraction
    process (currently FB, WB, ZFIN, SGD, XB)."""
    rows = db.execute(
        text(
            """
            SELECT DISTINCT m.mod_id, m.abbreviation
            FROM workflow_transition wt
            JOIN mod m ON m.mod_id = wt.mod_id
            WHERE wt.transition_from = ANY(:email_tags)
               OR wt.transition_to = ANY(:email_tags)
               OR array_to_string(wt.actions, ',') LIKE '%email extraction%'
            ORDER BY m.mod_id
            """
        ),
        {"email_tags": EMAIL_TAGS},
    ).fetchall()
    return {row[0]: row[1] for row in rows}


def get_candidates(db, mod_ids: List[int], since: str, until: str,
                   limit: Optional[int] = None) -> List[Tuple[int, str, str, int, str, bool, bool]]:
    """Return [(reference_id, curie, pmid, mod_id, mod_abbreviation,
    has_needed, has_failed), ...] for in-corpus references of the given MODs
    with a non-obsolete PMID, no automation-added reference_email rows
    (curator-only or none), whose corpus association was created in
    [since, until).

    Untagged reference+mod pairs and pairs whose extraction is pending or
    failed ("needed"/"failed") are returned. Pairs already settled
    ("complete"/"in progress"/parent) never are.
    """
    limit_clause = "LIMIT :limit" if limit else ""
    rows = db.execute(
        text(
            f"""
            SELECT r.reference_id, r.curie, MIN(cr.curie) AS pmid_curie,
                   mca.mod_id, m.abbreviation, st.has_needed, st.has_failed
            FROM mod_corpus_association mca
            JOIN mod m ON m.mod_id = mca.mod_id
            JOIN reference r ON r.reference_id = mca.reference_id
            JOIN cross_reference cr ON cr.reference_id = r.reference_id
                 AND cr.curie_prefix = 'PMID' AND cr.is_obsolete IS FALSE
            JOIN LATERAL (
                 SELECT EXISTS (SELECT 1 FROM workflow_tag wt
                                WHERE wt.reference_id = r.reference_id
                                  AND wt.mod_id = mca.mod_id
                                  AND wt.workflow_tag_id = :needed) AS has_needed,
                        EXISTS (SELECT 1 FROM workflow_tag wt
                                WHERE wt.reference_id = r.reference_id
                                  AND wt.mod_id = mca.mod_id
                                  AND wt.workflow_tag_id = :failed) AS has_failed,
                        EXISTS (SELECT 1 FROM workflow_tag wt
                                WHERE wt.reference_id = r.reference_id
                                  AND wt.mod_id = mca.mod_id
                                  AND wt.workflow_tag_id = ANY(:settled_tags)) AS is_settled
            ) st ON TRUE
            WHERE mca.corpus IS TRUE
              AND mca.mod_id = ANY(:mod_ids)
              AND mca.date_created >= :since
              AND mca.date_created < :until
              AND NOT st.is_settled
              AND NOT EXISTS (SELECT 1 FROM reference_email re
                              WHERE re.reference_id = r.reference_id
                                AND (re.updated_by IS NULL
                                     OR re.updated_by NOT LIKE :curator_like))
            GROUP BY r.reference_id, r.curie, mca.mod_id, m.abbreviation,
                     st.has_needed, st.has_failed
            ORDER BY r.reference_id, mca.mod_id
            {limit_clause}
            """
        ),
        {
            "mod_ids": mod_ids,
            "since": since,
            "until": until,
            "needed": EMAIL_NEEDED,
            "failed": EMAIL_FAILED,
            "settled_tags": EMAIL_SETTLED_TAGS,
            "curator_like": CURATOR_USER_ID_PREFIX + "%",
            **({"limit": limit} if limit else {}),
        },
    ).fetchall()
    return [(row[0], row[1], row[2].replace("PMID:", ""), row[3], row[4],
             row[5], row[6]) for row in rows]


# ----------------------------------------------------------------------
# Tagging plan
# ----------------------------------------------------------------------
def plan_tag_actions(has_needed: bool, has_failed: bool,
                     emails_found: bool) -> List[Tuple[str, str]]:
    """Return the ordered tag operations for one reference+mod pair as
    (operation, tag) tuples; operation is 'seed' (insert a WorkflowTagModel
    row -- transition_to_workflow_status cannot create the first tag of a
    process) or 'transition' (transition_to_workflow_status call).

    emails found:  untagged pairs are seeded "needed" then transitioned to
                   "complete" so the normal state machine path is walked;
                   "failed" pairs walk the retry transition first.
    no emails:     untagged pairs are seeded "needed" for the full-text
                   pipeline; pairs already "needed"/"failed" are left alone.
    """
    if emails_found:
        actions: List[Tuple[str, str]] = []
        if not has_needed:
            if has_failed:
                actions.append(("transition", EMAIL_NEEDED))   # failed -> needed (retry)
            else:
                actions.append(("seed", EMAIL_NEEDED))
        actions.append(("transition", EMAIL_COMPLETE))
        return actions
    if not has_needed and not has_failed:
        return [("seed", EMAIL_NEEDED)]
    return []


# ----------------------------------------------------------------------
# Apply
# ----------------------------------------------------------------------
def apply_results(db, candidates: List[Tuple[int, str, str, int, str, bool, bool]],
                  results: Dict[str, Tuple[List[str], str]],
                  dry_run: bool) -> Dict[str, int]:
    """Load the downloaded emails and set the workflow tags. Returns counters.

    reference_email rows are per reference (mod-independent) and are set once;
    workflow tags are set per reference+mod pair. Every touched reference is
    committed on its own so one failure cannot poison the batch.
    """
    counters = {"emails_loaded": 0, "tagged_complete": 0, "tagged_needed": 0, "errors": 0}

    by_reference: Dict[int, List[Tuple[int, str, str, int, str, bool, bool]]] = {}
    for cand in candidates:
        by_reference.setdefault(cand[0], []).append(cand)

    for reference_id in sorted(by_reference):
        mod_rows = by_reference[reference_id]
        curie = mod_rows[0][1]
        pmid = mod_rows[0][2]
        emails, source = results.get(pmid, ([], ""))
        try:
            if emails:
                counters["emails_loaded"] += 1
                logger.info("%s PMID:%s: loading %s email(s) from %s: %s",
                            curie, pmid, len(emails), source, " | ".join(emails))
                if not dry_run:
                    set_reference_emails(db, curie, emails)

            for (_ref_id, _curie, _pmid, mod_id, mod_abbr,
                 has_needed, has_failed) in mod_rows:
                for operation, tag in plan_tag_actions(has_needed, has_failed, bool(emails)):
                    logger.info("%s PMID:%s mod=%s: %s %s",
                                curie, pmid, mod_abbr, operation, tag)
                    if dry_run:
                        continue
                    if operation == "seed":
                        db.add(WorkflowTagModel(reference_id=reference_id,
                                                mod_id=mod_id,
                                                workflow_tag_id=tag))
                        db.commit()
                    else:
                        transition_to_workflow_status(db, str(reference_id), mod_abbr,
                                                      tag, transition_type="automated")
                        db.commit()
                if emails:
                    counters["tagged_complete"] += 1
                elif not has_needed and not has_failed:
                    counters["tagged_needed"] += 1
        except Exception:
            counters["errors"] += 1
            db.rollback()
            logger.exception("Error processing %s (reference_id=%s pmid=%s)",
                             curie, reference_id, pmid)
    return counters


# ----------------------------------------------------------------------
# Driver
# ----------------------------------------------------------------------
def run(since: str, until: str, only_mods: Optional[List[str]] = None,
        limit: Optional[int] = None,
        pmc_all: bool = False, dry_run: bool = True) -> Dict[str, int]:
    db = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, script_name)
    try:
        mod_id_to_abbr = get_email_extraction_mods(db)
        if only_mods:
            wanted = {m.strip().upper() for m in only_mods}
            mod_id_to_abbr = {mid: abbr for mid, abbr in mod_id_to_abbr.items()
                              if abbr.upper() in wanted}
        if not mod_id_to_abbr:
            logger.warning("No email-extraction-enabled MODs selected; nothing to do.")
            return {}

        logger.info("Getting emails from PubMed/PMC (dry_run=%s) for MODs %s, "
                    "corpus additions in [%s, %s)",
                    dry_run, ", ".join(sorted(mod_id_to_abbr.values())), since, until)

        candidates = get_candidates(db, list(mod_id_to_abbr), since, until, limit=limit)
        pmids = sorted({cand[2] for cand in candidates})
        logger.info("%d reference+mod pairs (%d distinct papers) to process",
                    len(candidates), len(pmids))
        if not candidates:
            return {}

        results = download_emails(pmids, pmc_all=pmc_all)
        logger.info("Emails downloaded for %d/%d papers (%.0f%%)",
                    len(results), len(pmids), 100 * len(results) / len(pmids))

        counters = apply_results(db, candidates, results, dry_run)
        logger.info("%s: references with emails loaded: %s; pairs tagged complete: %s; "
                    "pairs tagged needed: %s; errors: %s",
                    "DRY RUN - nothing written" if dry_run else "COMMITTED",
                    counters["emails_loaded"], counters["tagged_complete"],
                    counters["tagged_needed"], counters["errors"])
        return counters
    finally:
        db.close()


def parse_args() -> argparse.Namespace:
    default_until = str(date.today() + timedelta(days=1))
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("-s", "--since", default=DEFAULT_SINCE,
                   help=f"Corpus-addition window start, inclusive (default: {DEFAULT_SINCE})")
    p.add_argument("-u", "--until", default=default_until,
                   help=f"Corpus-addition window end, exclusive (default: {default_until})")
    p.add_argument("--mods", default=None,
                   help="Comma-separated MOD abbreviations to limit to (e.g. SGD,WB); "
                        "default: all MODs with email-extraction workflow transitions")
    p.add_argument("--limit", type=int, default=None,
                   help="Cap the number of reference+mod pairs (testing)")
    p.add_argument("--pmc-all", action="store_true",
                   help="Fetch PMC XML for every paper with a PMCID, not only the "
                        "papers PubMed metadata found no email for, and union both "
                        "sources' addresses (slower: more NCBI requests)")
    p.add_argument("--commit", action="store_true",
                   help="Apply the changes. Without this flag the script only "
                        "downloads and reports (dry run).")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    logger.setLevel(getattr(logging, args.log_level.upper(), logging.INFO))
    only_mods = args.mods.split(",") if args.mods else None
    run(since=args.since, until=args.until, only_mods=only_mods,
        limit=args.limit, pmc_all=args.pmc_all, dry_run=not args.commit)


if __name__ == "__main__":  # pragma: no cover
    main()
