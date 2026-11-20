"""Add the papers SGD created itself to ABC, with the same SGD ID (SCRUM-6830).

ABC mints SGD reference IDs (SGD:S100...) and SGD loads its papers from ABC.
Some SGD scripts (load_complex, load_disease_data, load_pathway, ...) and the
SGD curation UI still create papers in SGD directly, with SGD's own IDs
(SGD:S000...). ABC never heard of those papers, and when it later put the same
paper into the SGD corpus (from GAF or interaction files) it minted a second
ID. SGD lists the papers it created at /references/created_in_sgd?days=N; this
script makes ABC agree with that list.

For each paper SGD lists:
- SGD's ID is already active on the paper in ABC: make sure the paper is in
  the SGD corpus.
- The paper is in ABC (by PMID, else DOI) without SGD's ID: give it SGD's ID,
  replacing an ABC-minted one if it has one, and put it in the SGD corpus.
- The PMID is not in ABC: load it from PubMed, then as above.
- No PMID or DOI match, or SGD's ID is on a different paper than the PMID:
  reported, nothing changed.

SGD's ID is added in the same flush as the corpus change, so
sgd_curie_listener sees it and does not mint another one. Every change goes
through the ORM, so it is versioned. SGD also lists the older S000 IDs ABC
reactivated and sent back; ABC already has those, so they need no change.

Usage:
    python load_sgd_created_references.py                # last 2 days (daily cron)
    python load_sgd_created_references.py --days 10000   # one-time full check
    python load_sgd_created_references.py --days 7 --dry-run
"""
import argparse
import logging
from collections import defaultdict
from os import environ, path
from typing import Dict, List, Optional, Set

import requests
from sqlalchemy import and_

from agr_literature_service.api.models import CrossReferenceModel, ModCorpusAssociationModel, ModModel
from agr_literature_service.api.schemas import ModCorpusSortSourceType
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SCRIPT_NAME = "load_sgd_created_references"
# its own setting: SGD_API_URL is already used, for SGD's entity-name lookup
# (https://backend.yeastgenome.org/entity/), and appending to it hit that endpoint
SGD_CREATED_REFERENCES_URL = environ.get("SGD_CREATED_REFERENCES_URL",
                                         "https://www.yeastgenome.org/backend/references/created_in_sgd")
SORT_SOURCE = ModCorpusSortSourceType.Dqm_files
CHUNK_SIZE = 2000
REPORT_LIST_LIMIT = 50

# outcomes
OK = "already in the SGD corpus with SGD's ID"
MOVED_INTO_CORPUS = "moved into the SGD corpus"
ID_ADDED = "given SGD's ID"
ID_REPLACED = "ABC SGD ID replaced with SGD's ID"
NEW_PMID = "PMID not in ABC"
NOT_LOADED = "PMID could not be loaded from PubMed"
NEEDS_CURATOR = "no PMID or DOI match in ABC, needs a curator"
CONFLICT = "needs a curator: SGD ID conflict"
CHANGED = (MOVED_INTO_CORPUS, ID_ADDED, ID_REPLACED)


def fetch_sgd_created_references(days: int) -> List[Dict]:
    response = requests.get(SGD_CREATED_REFERENCES_URL, params={"days": days}, timeout=600)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict) or "references" not in data:
        raise ValueError(f"{response.url} did not return SGD's created_in_sgd list: {str(data)[:200]}")
    return data["references"]


class Lookups:
    """ABC rows for one chunk of SGD papers, read with a few bulk queries."""

    def __init__(self, db, sgd_mod_id: int, papers: List[Dict]):
        sgdids = [p["sgdid"] for p in papers]
        id_curies = [p["pmid"] for p in papers if p.get("pmid")] + \
            [f"DOI:{p['doi']}" for p in papers if p.get("doi")]
        self.xrefs_by_curie: Dict[str, List[CrossReferenceModel]] = defaultdict(list)
        for x in db.query(CrossReferenceModel).filter(CrossReferenceModel.curie.in_(sgdids)).all():
            self.xrefs_by_curie[x.curie].append(x)
        self.reference_by_curie: Dict[str, int] = {}
        if id_curies:
            for curie, reference_id in db.query(CrossReferenceModel.curie, CrossReferenceModel.reference_id).filter(
                    CrossReferenceModel.curie.in_(id_curies), CrossReferenceModel.is_obsolete.is_(False),
                    CrossReferenceModel.reference_id.isnot(None)).all():
                self.reference_by_curie[curie] = reference_id
        reference_ids = set(self.reference_by_curie.values()) | {
            x.reference_id for rows in self.xrefs_by_curie.values() for x in rows if x.reference_id}
        self.sgd_xrefs_by_reference: Dict[int, List[CrossReferenceModel]] = defaultdict(list)
        # papers given an SGD ID earlier in this run (kept in a dry run too, so
        # it reports what the real run would do)
        self.assigned: Dict[int, str] = {}
        self.mca_by_reference: Dict[int, ModCorpusAssociationModel] = {}
        if reference_ids:
            for x in db.query(CrossReferenceModel).filter(CrossReferenceModel.reference_id.in_(reference_ids),
                                                          CrossReferenceModel.curie_prefix == "SGD").all():
                self.sgd_xrefs_by_reference[x.reference_id].append(x)
            for mca in db.query(ModCorpusAssociationModel).filter(
                    and_(ModCorpusAssociationModel.reference_id.in_(reference_ids),
                         ModCorpusAssociationModel.mod_id == sgd_mod_id)).all():
                self.mca_by_reference[mca.reference_id] = mca

    def reference_for(self, paper: Dict) -> Optional[int]:
        """The ABC paper with the PMID, else with the DOI. The DOI fallback
        matters: post_references skips a DOI already in ABC but still creates
        the paper, so loading such a PMID from PubMed would duplicate it."""
        if paper.get("pmid") and paper["pmid"] in self.reference_by_curie:
            return self.reference_by_curie[paper["pmid"]]
        if paper.get("doi"):
            return self.reference_by_curie.get(f"DOI:{paper['doi']}")
        return None


def _put_in_sgd_corpus(db, lookups: Lookups, sgd_mod_id: int, reference_id: int) -> bool:
    mca = lookups.mca_by_reference.get(reference_id)
    if mca is None:
        mca = ModCorpusAssociationModel(reference_id=reference_id, mod_id=sgd_mod_id, corpus=True,
                                        mod_corpus_sort_source=SORT_SOURCE)
        db.add(mca)
        lookups.mca_by_reference[reference_id] = mca
        return True
    if mca.corpus is not True:
        mca.corpus = True
        return True
    return False


def reconcile_paper(db, lookups: Lookups, sgd_mod_id: int, paper: Dict, apply: bool = True):
    """Make ABC agree with one paper SGD created. Returns (outcome, detail).
    With apply=False nothing is changed, only the outcome is worked out."""
    sgdid = paper["sgdid"]
    reference_id = lookups.reference_for(paper)
    active = [x for x in lookups.xrefs_by_curie.get(sgdid, []) if not x.is_obsolete and x.reference_id]

    if active:
        holder = active[0].reference_id
        if reference_id is not None and reference_id != holder:
            return CONFLICT, f"{sgdid} is on reference_id={holder}, {paper.get('pmid')} on {reference_id}"
        mca = lookups.mca_by_reference.get(holder)
        if mca is not None and mca.corpus is True:
            return OK, ""
        if apply:
            _put_in_sgd_corpus(db, lookups, sgd_mod_id, holder)
        return MOVED_INTO_CORPUS, f"{sgdid} reference_id={holder}"

    if reference_id is None:
        return (NEW_PMID, paper["pmid"]) if paper.get("pmid") else \
            (NEEDS_CURATOR, f"{sgdid} {paper.get('display_name') or ''} doi={paper.get('doi')}")

    if reference_id in lookups.assigned:
        # SGD lists two IDs for the same PMID/DOI
        return CONFLICT, (f"{paper.get('pmid') or paper.get('doi')} was given {lookups.assigned[reference_id]} "
                          f"in this run, SGD also lists {sgdid}")
    sgd_xrefs = lookups.sgd_xrefs_by_reference.get(reference_id, [])
    current = [x for x in sgd_xrefs if not x.is_obsolete]
    old_copy = next((x for x in sgd_xrefs if x.curie == sgdid), None)
    if current and not current[0].curie.startswith("SGD:S1"):
        # the paper's ID is an SGD-made S000 one too: SGD has two papers for
        # this PMID/DOI, which a curator has to resolve
        return CONFLICT, f"{paper.get('pmid') or paper.get('doi')} has {current[0].curie} in ABC, SGD lists {sgdid}"
    outcome, detail = (ID_REPLACED, f"{current[0].curie} -> {sgdid} ({paper.get('pmid')})") if current \
        else (ID_ADDED, f"{sgdid} ({paper.get('pmid')})")
    lookups.assigned[reference_id] = sgdid
    if not apply:
        return outcome, detail

    if current and old_copy is None:
        # an ABC-minted S1 ID (checked above) that SGD never used, since it
        # had the paper already: the row takes SGD's ID instead of leaving an
        # unused ID behind
        current[0].curie = sgdid
        lookups.xrefs_by_curie[sgdid].append(current[0])
    else:
        for x in current:
            # only one active SGD ID per paper: retire the other one first
            x.is_obsolete = True
        if current:
            db.flush()
        if old_copy is not None:
            old_copy.is_obsolete = False
        else:
            new_xref = CrossReferenceModel(curie=sgdid, curie_prefix="SGD", reference_id=reference_id,
                                           pages=["reference"], is_obsolete=False)
            db.add(new_xref)
            lookups.sgd_xrefs_by_reference[reference_id].append(new_xref)
            lookups.xrefs_by_curie[sgdid].append(new_xref)
    _put_in_sgd_corpus(db, lookups, sgd_mod_id, reference_id)
    return outcome, detail


def reconcile(db, sgd_mod_id: int, papers: List[Dict], apply: bool = True) -> Dict[str, List[str]]:
    results: Dict[str, List[str]] = defaultdict(list)
    for start in range(0, len(papers), CHUNK_SIZE):
        chunk = papers[start:start + CHUNK_SIZE]
        lookups = Lookups(db, sgd_mod_id, chunk)
        for paper in chunk:
            outcome, detail = reconcile_paper(db, lookups, sgd_mod_id, paper, apply=apply)
            results[outcome].append(detail)
        if apply:
            db.commit()
        else:
            db.rollback()
        logger.info(f"{min(start + CHUNK_SIZE, len(papers))}/{len(papers)} SGD papers checked")
    return results


def load_new_pmids(db, pmids: Set[str]) -> Set[str]:  # pragma: no cover
    """Load PMIDs from PubMed the way the interaction loader does. Returns the
    PMIDs that were loaded (an obsolete or unknown PMID is not)."""
    from agr_literature_service.lit_processing.data_ingest.post_reference_to_db import post_references
    from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_pubmed_xml import download_pubmed_xml
    from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_search_new_references import \
        add_md5sum_to_database
    from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.sanitize_pubmed_json import \
        sanitize_pubmed_json_list
    from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml_to_json import generate_json
    from agr_literature_service.lit_processing.data_ingest.utils.alliance_paper_utils import clean_up_tmp_directories
    from agr_literature_service.lit_processing.utils.s3_utils import upload_xml_file_to_s3
    from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

    init_tmp_dir()
    base_path = environ.get("XML_PATH", "")
    xml_path = base_path + "pubmed_xml/"
    clean_up_tmp_directories([base_path + "pubmed_json/", xml_path])
    numbers = sorted(p.replace("PMID:", "") for p in pmids)
    download_pubmed_xml(numbers)
    generate_json(numbers, [])
    sanitize_pubmed_json_list(numbers, [{}])
    post_references(base_path + "sanitized_reference_json/REFERENCE_PUBMED_PMID.json")
    loaded = {p for p in numbers if path.exists(xml_path + p + ".xml")}
    if environ.get("ENV_STATE") == "prod":
        for pmid in loaded:
            upload_xml_file_to_s3(pmid, "latest")
    add_md5sum_to_database(db, None, loaded)
    return {f"PMID:{p}" for p in loaded}


def compose_report(days: int, total: int, results: Dict[str, List[str]], dry_run: bool) -> str:
    title = f"SGD-created papers from the last {days} day(s): {total}" + (" (dry run, nothing changed)" if dry_run else "")
    message = f"<p><b>{title}</b></p><ul>"
    for outcome in (OK, MOVED_INTO_CORPUS, ID_ADDED, ID_REPLACED, NOT_LOADED, NEEDS_CURATOR, CONFLICT, NEW_PMID):
        details = results.get(outcome, [])
        if not details:
            continue
        message += f"<li>{outcome}: {len(details)}"
        if outcome != OK and any(details):
            shown = [d for d in details if d][:REPORT_LIST_LIMIT]
            message += "<ul>" + "".join(f"<li>{d}" for d in shown)
            if len(details) > len(shown):
                message += f"<li>... and {len(details) - len(shown)} more"
            message += "</ul>"
    return message + "</ul>"


def load_sgd_created_references(days: int = 2, dry_run: bool = False, send: bool = True) -> Dict[str, List[str]]:
    papers = fetch_sgd_created_references(days)
    logger.info(f"SGD lists {len(papers)} paper(s) it created in the last {days} day(s)")
    db = create_postgres_session(False)
    try:
        set_global_user_id(db, SCRIPT_NAME)
        sgd_mod_id = db.query(ModModel.mod_id).filter(ModModel.abbreviation == "SGD").scalar()
        results = reconcile(db, sgd_mod_id, papers, apply=not dry_run)

        new_pmids = set(results.pop(NEW_PMID, []))
        if new_pmids and dry_run:
            results[NEW_PMID] = sorted(new_pmids)
        elif new_pmids:
            loaded = load_new_pmids(db, new_pmids)
            results[NOT_LOADED].extend(sorted(new_pmids - loaded))
            again = reconcile(db, sgd_mod_id, [p for p in papers if p.get("pmid") in loaded])
            for outcome, details in again.items():
                results[outcome].extend(details)

        for outcome, details in results.items():
            logger.info(f"{outcome}: {len(details)}")
        if send:
            from agr_literature_service.lit_processing.utils.report_utils import send_report
            send_report("SGD-created papers loaded into ABC", compose_report(days, len(papers), results, dry_run))
        return results
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add the papers SGD created itself to ABC with the same SGD ID")
    parser.add_argument("--days", type=int, default=2, help="papers SGD created in the last N days (default 2)")
    parser.add_argument("--dry-run", action="store_true", help="report what would change, change nothing")
    parser.add_argument("--no-report", action="store_true", help="don't send the email/Slack report")
    args = parser.parse_args()
    load_sgd_created_references(args.days, dry_run=args.dry_run, send=not args.no_report)
