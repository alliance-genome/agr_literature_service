"""
Put the papers that the Complex Portal cites for yeast complexes on SGD's
needs-review list (SCRUM-6833).

SGD's load_complex.py links each complex to its Complex Portal papers. It used
to add any paper missing from SGD itself, which minted an SGD-only SGD ID for
a paper ABC did not know about. Now this script, run weekly, makes sure every
such paper is in ABC and in front of SGD's curators: papers not in ABC are
loaded from PubMed, and papers not yet associated with SGD are added to SGD's
needs-review list (corpus not set) with sort source complex_portal, so they
can be filtered for on the sort page. Papers SGD already has an association
with (in the corpus, rejected, or already waiting for review) are left alone.

A paper a curator accepts into the SGD corpus gets its SGD ID then, reaches
SGD through load_new_references_from_abc.py, and load_complex.py links it on
its next run.
"""
import argparse
import logging
import time
from os import environ, path
from typing import Any, Dict, Optional, Set, Tuple

import requests
from dotenv import load_dotenv
from sqlalchemy import text

from agr_literature_service.api.models import ModCorpusAssociationModel, ModModel
from agr_literature_service.api.schemas import ModCorpusSortSourceType
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.data_ingest.go_annotations.load_mod_gaf_papers import \
    load_sgd_new_papers
from agr_literature_service.lit_processing.data_ingest.utils.alliance_paper_utils import (
    clean_up_tmp_directories,
    search_pubmed_for_validity,
)
from agr_literature_service.lit_processing.utils.db_read_utils import (
    get_pmids_with_obsolete_mod_curie,
    retrieve_all_pmids,
)
from agr_literature_service.lit_processing.utils.report_utils import send_report
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()
init_tmp_dir()

# The same Complex Portal queries SGD's load_complex.py uses, so both see the
# same complexes and papers.
SEARCH_URL = ("https://www.ebi.ac.uk/intact/complex-ws/search/*?filters=species_f:"
              "(%22Saccharomyces%20cerevisiae%20(strain%20ATCC%20204508%20/%20S288c)%22)"
              "&facets=species_f")
DETAIL_URL_TEMPLATE = "https://www.ebi.ac.uk/intact/complex-ws/complex/{}"

MOD = "SGD"
REPORT_SUBJECT = "Complex Portal Paper Loading Report"

base_path = environ.get("XML_PATH", "")
json_path = base_path + "pubmed_json/"
xml_path = base_path + "pubmed_xml/"


def get_json(url: str, max_retries: int = 6, base_delay: int = 10, max_delay: int = 60,
             treat_404_as_missing: bool = True) -> Optional[Any]:
    """
    Fetch and parse JSON from the Complex Portal web service.

    The service returns 503 (and sometimes 404) while overloaded, so failures
    are retried with a linear backoff. A 404 on a per-complex call means the
    complex is gone and returns None; for the search call pass
    treat_404_as_missing=False so a 404 is retried like any other failure.
    A persistent failure raises RuntimeError.
    """
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, timeout=120)
            if response.status_code == 404 and treat_404_as_missing:
                return None
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            last_error = f"HTTP {e.response.status_code}"
        except requests.RequestException as e:
            last_error = str(e)
        except ValueError as e:
            # body was not JSON, e.g. an HTML overload page
            last_error = f"JSON decode error: {e}"
        if attempt < max_retries:
            delay = min(max_delay, base_delay * attempt)
            logger.info(f"{last_error} (attempt {attempt}/{max_retries}); retrying in {delay}s: {url}")
            time.sleep(delay)
    raise RuntimeError(f"get_json failed after {max_retries} attempts ({last_error}): {url}")


def extract_pmids_from_complex(complex_detail: Dict) -> Set[str]:
    """Return the PubMed IDs among a complex's cross references."""
    pmids = set()
    for xref in complex_detail.get("crossReferences") or []:
        identifier = str(xref.get("identifier") or "")
        if xref.get("database") == "pubmed" and identifier.isdigit():
            pmids.add(identifier)
    return pmids


def fetch_complex_pmids() -> Tuple[Dict[str, Set[str]], Set[str]]:
    """
    Fetch every yeast complex from the Complex Portal and collect its papers.

    Returns:
        Tuple of (PMID -> complex accessions citing it,
                  complex accessions whose details could not be fetched)
    """
    search = get_json(SEARCH_URL, treat_404_as_missing=False) or {}
    complex_acs = [x["complexAC"] for x in search.get("elements", [])]
    logger.info(f"{len(complex_acs)} yeast complexes in the Complex Portal")

    pmid_to_complexes: Dict[str, Set[str]] = {}
    failed: Set[str] = set()
    for complex_ac in complex_acs:
        try:
            detail = get_json(DETAIL_URL_TEMPLATE.format(complex_ac))
        except RuntimeError as e:
            logger.warning(str(e))
            failed.add(complex_ac)
            continue
        if detail is None:
            logger.warning(f"{complex_ac} not found in the Complex Portal")
            failed.add(complex_ac)
            continue
        for pmid in extract_pmids_from_complex(detail):
            pmid_to_complexes.setdefault(pmid, set()).add(complex_ac)
    return pmid_to_complexes, failed


def get_sgd_association_status(db_session) -> Dict[str, Optional[bool]]:
    """PMID -> corpus value of the paper's SGD association: True (in the
    corpus), False (rejected) or None (needs review)."""
    rows = db_session.execute(text(
        "SELECT cr.curie, mca.corpus "
        "FROM cross_reference cr "
        "JOIN mod_corpus_association mca ON mca.reference_id = cr.reference_id "
        "JOIN mod m ON m.id = mca.mod_id "
        "WHERE cr.curie_prefix = 'PMID' AND cr.obsolete = False "
        "AND m.abbreviation = :mod"), {"mod": MOD}).fetchall()
    return {curie.replace("PMID:", ""): corpus for curie, corpus in rows}


def classify_pmids(all_pmids: Set[str], all_pmids_db: Set[str], sgd_status: Dict[str, Optional[bool]],
                   obsolete_mod_curie: Set[str]) -> Dict[str, Set[str]]:
    """
    Sort the Complex Portal PMIDs by where they stand with SGD.

    Only papers with no SGD association are put on the needs-review list.
    Papers whose SGD ID is obsolete (deleted in SGD) are set aside.
    """
    in_corpus = {p for p in all_pmids if sgd_status.get(p, "") is True}
    rejected = {p for p in all_pmids if sgd_status.get(p, "") is False}
    needs_review = {p for p in all_pmids if p in sgd_status and sgd_status[p] is None}
    obsolete = (all_pmids - in_corpus) & obsolete_mod_curie
    return {
        "in_corpus": in_corpus,
        "rejected": rejected - obsolete,
        "needs_review": needs_review - obsolete,
        "not_associated": (all_pmids & all_pmids_db) - set(sgd_status) - obsolete,
        "not_in_db": all_pmids - all_pmids_db,
        "obsolete_mod_curie": obsolete,
    }


def add_to_sgd_needs_review(db_session, pmids: Set[str]) -> Set[str]:  # pragma: no cover
    """Give each paper an SGD association with corpus not set (needs
    review), unless it has an SGD association already. Returns the PMIDs
    added."""
    if not pmids:
        return set()
    sgd_mod_id = db_session.query(ModModel.mod_id).filter(ModModel.abbreviation == MOD).scalar()
    rows = db_session.execute(text(
        "SELECT cr.curie, cr.reference_id FROM cross_reference cr "
        "WHERE cr.curie = ANY(:curies) AND cr.obsolete = False "
        "AND NOT EXISTS (SELECT 1 FROM mod_corpus_association mca "
        "                WHERE mca.reference_id = cr.reference_id AND mca.mod_id = :mod_id)"),
        {"curies": [f"PMID:{p}" for p in pmids], "mod_id": sgd_mod_id}).fetchall()
    added: Set[str] = set()
    reference_ids: Set[int] = set()
    for curie, reference_id in rows:
        if reference_id in reference_ids:
            continue
        reference_ids.add(reference_id)
        db_session.add(ModCorpusAssociationModel(reference_id=reference_id, mod_id=sgd_mod_id, corpus=None,
                                                 mod_corpus_sort_source=ModCorpusSortSourceType.Complex_portal))
        added.add(curie.replace("PMID:", ""))
    if added:
        db_session.commit()
    return added


def format_pmid(pmid: str, pmid_to_complexes: Dict[str, Set[str]]) -> str:
    complexes = ", ".join(sorted(pmid_to_complexes.get(pmid, set())))
    return f"PMID:{pmid} [{complexes}]" if complexes else f"PMID:{pmid}"


def _pmid_lines(pmids: Set[str], pmid_to_complexes: Dict[str, Set[str]], label: str = "") -> str:
    return "".join(f"{format_pmid(p, pmid_to_complexes)}{label}<br>" for p in sorted(pmids, key=int))


def build_report_message(pmid_to_complexes: Dict[str, Set[str]], failed_complexes: Set[str],
                         groups: Dict[str, Set[str]], pmids_added_existing: Set[str],
                         pmids_added_new: Set[str], pmids_not_loaded: Set[str],
                         obsolete_pmids: Set[str], dry_run: bool = False) -> str:
    """Build the HTML report."""
    message = f"<b>{REPORT_SUBJECT}{' (dry run)' if dry_run else ''}</b><p>"
    message += "<ul>"
    message += f"<li>PMIDs cited by yeast complexes: {len(pmid_to_complexes)}"
    message += f"<li>In SGD corpus: {len(groups['in_corpus'])}"
    message += f"<li>Already on SGD's needs-review list: {len(groups['needs_review'])}"
    message += f"<li>Rejected by SGD (left alone): {len(groups['rejected'])}"
    if dry_run:
        message += f"<li>Would add to SGD needs review (in ABC, not associated): {len(groups['not_associated'])}"
        message += f"<li>Would load from PubMed and add to SGD needs review: {len(groups['not_in_db'])}"
    else:
        added = pmids_added_existing | pmids_added_new
        message += f"<li>Added to SGD needs review (in ABC, not associated): {len(pmids_added_existing)}"
        message += f"<li>New references loaded and added to SGD needs review: {len(pmids_added_new)}"
        if added:
            message += f"<li>Papers added to SGD needs review ({len(added)}):<br>"
            message += _pmid_lines(pmids_added_new, pmid_to_complexes, " (new)")
            message += _pmid_lines(pmids_added_existing, pmid_to_complexes, " (existing)")
    if pmids_not_loaded:
        message += f"<li>Not loaded (obsolete or unavailable in PubMed): {len(pmids_not_loaded)}<br>"
        message += "".join(f"{format_pmid(p, pmid_to_complexes)}{' (obsolete)' if p in obsolete_pmids else ''}<br>"
                           for p in sorted(pmids_not_loaded, key=int))
    if groups["obsolete_mod_curie"]:
        message += (f"<li>Not added, SGD ID is obsolete (paper deleted in SGD): "
                    f"{len(groups['obsolete_mod_curie'])}<br>")
        message += _pmid_lines(groups["obsolete_mod_curie"], pmid_to_complexes)
    if failed_complexes:
        message += f"<li>Complexes that could not be fetched ({len(failed_complexes)}): "
        message += ", ".join(sorted(failed_complexes))
    message += "</ul>"
    return message


def load_complex_papers(dry_run: bool = False) -> str:  # pragma: no cover
    """
    Put every paper the Complex Portal cites for a yeast complex, and that
    SGD has no association with yet, on SGD's needs-review list. With
    dry_run, report what would change and write nothing.

    Returns:
        HTML report message
    """
    pmid_to_complexes, failed_complexes = fetch_complex_pmids()
    all_pmids = set(pmid_to_complexes)

    db_session = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, script_name)

    all_pmids_db = set(retrieve_all_pmids(db_session))
    sgd_status = get_sgd_association_status(db_session)
    obsolete_mod_curie = get_pmids_with_obsolete_mod_curie(db_session, MOD)
    groups = classify_pmids(all_pmids, all_pmids_db, sgd_status, obsolete_mod_curie)
    logger.info(f"{len(all_pmids)} PMIDs: {len(groups['in_corpus'])} in SGD corpus, "
                f"{len(groups['needs_review'])} already need review, "
                f"{len(groups['rejected'])} rejected, "
                f"{len(groups['not_associated'])} in ABC not associated, "
                f"{len(groups['not_in_db'])} not in ABC, "
                f"{len(groups['obsolete_mod_curie'])} with an obsolete SGD ID")

    obsolete_pmids: Set[str] = set()
    if groups["not_in_db"]:
        obsolete_pmids, _ = search_pubmed_for_validity(groups["not_in_db"],
                                                       environ.get('NCBI_API_KEY', ''))

    pmids_added_existing: Set[str] = set()
    pmids_added_new: Set[str] = set()
    pmids_not_loaded: Set[str] = set(obsolete_pmids)
    if not dry_run:
        pmids_to_load = groups["not_in_db"] - obsolete_pmids
        pmids_loaded: Set[str] = set()
        if pmids_to_load:
            clean_up_tmp_directories([xml_path, json_path])
            pmids_loaded = load_sgd_new_papers(db_session, pmids_to_load)
            pmids_not_loaded |= pmids_to_load - pmids_loaded
        pmids_added_existing = add_to_sgd_needs_review(db_session, groups["not_associated"])
        pmids_added_new = add_to_sgd_needs_review(db_session, pmids_loaded)
    db_session.close()

    return build_report_message(pmid_to_complexes, failed_complexes, groups, pmids_added_existing,
                                pmids_added_new, pmids_not_loaded, obsolete_pmids, dry_run)


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Put the papers the Complex Portal cites for yeast complexes on SGD's needs-review list"
    )
    parser.add_argument('-d', '--dry-run', action='store_true',
                        help="Report what would change without writing to the database")
    parser.add_argument('-n', '--no-slack', action='store_true',
                        help="Do not send the Slack report; log it instead")
    args = parser.parse_args()

    report = load_complex_papers(dry_run=args.dry_run)
    if args.no_slack:
        logger.info(report)
    else:
        send_report(REPORT_SUBJECT, report)
