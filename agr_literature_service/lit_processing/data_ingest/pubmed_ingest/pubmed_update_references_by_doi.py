import time
import logging
from collections import Counter
import requests
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from xml.etree import ElementTree
from os import environ, path

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.utils.report_utils import send_report
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_references_single_mod \
    import update_data
from agr_literature_service.api.models import CrossReferenceModel
from agr_literature_service.api.user import set_global_user_id

base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
esummary_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
esummary_batch_size = 200

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def update_database(): # noqa

    db = create_postgres_session(False)

    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, scriptNm)

    logger.info("Reading data from Cross_reference table...")
    (reference_id_to_pmid, pmid_to_reference_id, reference_id_to_doi) = get_cross_reference_data(db)

    db.close()

    logger.info("Getting PMIDs for papers with_doi_only...")
    i = 0
    candidates = []
    for reference_id in reference_id_to_doi:
        if reference_id not in reference_id_to_pmid:
            i += 1
            doi = reference_id_to_doi[reference_id]
            logger.info(f"{i}: processing {doi}")
            pmids = get_pmid_for_doi(doi)
            if pmids is None:
                continue
            if len(pmids) == 1:
                candidates.append((reference_id, doi, pmids[0]))
            time.sleep(0.35)

    # PubMed's [DOI] search is not an exact match, so only keep a PMID whose
    # own record lists the DOI we searched for
    pmid_to_dois = get_dois_for_pmids([pmid for (_, _, pmid) in candidates])
    (papers_to_add_pmid, papers_to_merge, duplicate_papers, rejected) = \
        classify_doi_matches(candidates, pmid_to_dois, pmid_to_reference_id)
    for (reference_id, doi, pmid) in rejected:
        logger.info(f"REJECTED {pmid} for {doi} (reference_id = {reference_id}): "
                    f"the PubMed record does not list this DOI")
    for (doi, pmid) in papers_to_merge:
        logger.info(f"FOUND {pmid} for {doi}, but it is already in the database")
    for (doi, pmid) in duplicate_papers:
        logger.info(f"FOUND {pmid} for {doi}, but another paper with DOI also resolved to it")

    db = create_postgres_session(False)

    pmids_to_update = []
    if len(papers_to_add_pmid) > 0:
        logger.info(f"Adding PMID to papers ({len(papers_to_add_pmid)}) with_DOI:")
        added_pmids = add_pmid_to_existing_papers(db, papers_to_add_pmid)
        pmids_to_update = [pmid.replace("PMID:", "") for pmid in added_pmids]

    # db.rollback()
    db.commit()

    if len(pmids_to_update) > 0:
        logger.info(f"Updating papers ({len(pmids_to_update)}) with data from PubMed:")
        update_papers(db, pmids_to_update)

    # db.rollback()
    db.commit()
    db.close()

    if len(papers_to_merge) > 0 or len(duplicate_papers) > 0:
        logger.info("Sending report to slack:")
        send_report_for_merging_paper(papers_to_merge, duplicate_papers)

    logger.info("DONE!")


def build_doi_search_term(doi):
    """
    Return an esearch term for an exact DOI match.

    The DOI must be quoted: unquoted, PubMed falls back to automatic term
    mapping when the DOI is not indexed and splits it at the slash, e.g.
    10.1007/978-3-0348-8853-0_11 becomes "10.1007"[All Fields] AND "11"[Publisher ID],
    which matches unrelated papers.
    """
    if doi.upper().startswith("DOI:"):
        doi = doi[4:]
    return f'"{doi}"[DOI]'


def get_pmid_for_doi(doi): # noqa
    params = {
        "db": "pubmed",
        "term": build_doi_search_term(doi),
        'api_key': environ['NCBI_API_KEY']
    }
    try:
        response = requests.get(base_url, params=params)
        if response.status_code == 429:  # Too Many Requests
            time.sleep(10)  # Wait for 10 seconds before retrying
            response = requests.get(base_url, params=params)
        tree = ElementTree.fromstring(response.content)
        pmids = [elem.text for elem in tree.findall(".//Id")]
        return pmids
    except Exception as e:
        logger.info(f"Error(s) occurred when searching PubMed: {e}")


def get_dois_for_pmids(pmids):
    """
    Return {pmid: set of lower-cased DOIs} from the PubMed esummary records.
    A PMID whose summary could not be fetched is left out, so it gets rejected.
    """
    pmid_to_dois = {}
    for i in range(0, len(pmids), esummary_batch_size):
        batch = pmids[i:i + esummary_batch_size]
        data = {
            "db": "pubmed",
            "id": ",".join(batch),
            "retmode": "json",
            "api_key": environ['NCBI_API_KEY']
        }
        try:
            response = requests.post(esummary_url, data=data)
            result = response.json()["result"]
        except Exception as e:
            logger.info(f"Error(s) occurred when fetching PubMed summaries: {e}")
            continue
        for pmid in batch:
            article_ids = result.get(pmid, {}).get("articleids", [])
            pmid_to_dois[pmid] = {a["value"].lower() for a in article_ids if a.get("idtype") == "doi"}
        time.sleep(0.35)
    return pmid_to_dois


def classify_doi_matches(candidates, pmid_to_dois, pmid_to_reference_id):
    """
    Split (reference_id, doi, pmid) search results into
    - papers_to_add_pmid: [(reference_id, "PMID:n")], verified and new
    - papers_to_merge: [(doi, "PMID:n")], the PMID is already on another reference
    - duplicate_papers: [(doi, "PMID:n")], more than one DOI-only reference
      resolved to the same new PMID, so none of them gets it
    - rejected: [(reference_id, doi, "PMID:n")], the PubMed record does not list the DOI
    """
    verified = []
    rejected = []
    for (reference_id, doi, pmid) in candidates:
        bare_doi = doi[4:] if doi.upper().startswith("DOI:") else doi
        if bare_doi.lower() in pmid_to_dois.get(pmid, set()):
            verified.append((reference_id, doi, "PMID:" + pmid))
        else:
            rejected.append((reference_id, doi, "PMID:" + pmid))

    new_pmid_count = Counter(pmid for (_, _, pmid) in verified if pmid not in pmid_to_reference_id)
    papers_to_add_pmid = []
    papers_to_merge = []
    duplicate_papers = []
    for (reference_id, doi, pmid) in verified:
        if pmid in pmid_to_reference_id:
            papers_to_merge.append((doi, pmid))
        elif new_pmid_count[pmid] > 1:
            duplicate_papers.append((doi, pmid))
        else:
            papers_to_add_pmid.append((reference_id, pmid))
    return (papers_to_add_pmid, papers_to_merge, duplicate_papers, rejected)


def add_pmid_to_existing_papers(db: Session, papers_to_add_pmid): # noqa
    """
    Add each PMID in its own savepoint, so one conflict (e.g. the PMID was added
    to another reference while this script was running) only skips that paper.
    Returns the PMIDs that were added.
    """
    added_pmids = []
    for (reference_id, pmid) in papers_to_add_pmid:
        try:
            with db.begin_nested():
                db.add(CrossReferenceModel(reference_id=reference_id,
                                           curie_prefix='PMID',
                                           curie=pmid,
                                           is_obsolete=False))
            added_pmids.append(pmid)
            logger.info(f"Adding {pmid} to cross_reference table for reference_id = {reference_id}")
        except IntegrityError as e:
            logger.info(f"Skipped {pmid} for reference_id = {reference_id}: {e.orig}")
    return added_pmids


def update_papers(db: Session, pmids_to_update): # noqa

    pmids = "|".join(pmids_to_update)

    try:
        update_data(None, pmids)
    except Exception as e:
        logger.info(f"Error(s) occurred when updating papers with the data from PubMed: {e}")


def send_report_for_merging_paper(papers_to_merge, duplicate_papers=None): # noqa

    email_subject = "Duplicate Paper Pairs Detected: Merge Required"

    email_message = "During our routine checks, we've identified pairs of papers in our database that appear to be duplicates. One paper in each pair has a DOI ID, while the other has a PMID. We believe these pairs correspond to the same paper and need to be merged.<p>Below is the list of detected duplicate pairs:<p>"

    rows = "<tr><th style='text-align:left' width='300'>Paper with DOI</th><th style='text-align:left' width='200'>Paper with PMID</th></tr>"

    for (doi, pmid) in papers_to_merge:
        rows = rows + f"<tr><td style='text-align:left' width='300'>{doi}</th><td style='text-align:left' width='200'>{pmid}</td></tr>"
    email_message = email_message + "<table></tbody>" + rows + "</tbody></table><p>"

    if duplicate_papers:
        email_message = email_message + "The following papers with DOI resolved to the same PubMed record, which is not yet in our database, so no PMID was added to them:<p>"
        rows = "<tr><th style='text-align:left' width='300'>Paper with DOI</th><th style='text-align:left' width='200'>PMID</th></tr>"
        for (doi, pmid) in duplicate_papers:
            rows = rows + f"<tr><td style='text-align:left' width='300'>{doi}</th><td style='text-align:left' width='200'>{pmid}</td></tr>"
        email_message = email_message + "<table></tbody>" + rows + "</tbody></table><p>"

    email_message = email_message + "Please review and take the necessary actions to merge the records."

    try:
        send_report(email_subject, email_message)
    except Exception as e:
        logger.info(f"An error occurred when sending email to slack: {e}")


def get_cross_reference_data(db: Session): # noqa

    rows = db.execute(text("SELECT reference_id, curie "
                           "FROM cross_reference "
                           "WHERE curie_prefix in ('PMID', 'DOI') "
                           "AND is_obsolete is False")).fetchall()

    # reference_id_pmid = {row[0]: row[1] for row in rows if row[1].startswith('PMID')}
    # reference_id_doi = {row[0]: row[1] for row in rows if row[1].startswith('DOI')}

    reference_id_pmid = {}
    pmid_to_reference_id = {}
    reference_id_doi = {}

    for row in rows:
        if row[1].startswith('PMID'):
            reference_id_pmid[row[0]] = row[1]
            pmid_to_reference_id[row[1]] = row[0]
        elif row[1].startswith('DOI'):
            reference_id_doi[row[0]] = row[1]

    return (reference_id_pmid, pmid_to_reference_id, reference_id_doi)


if __name__ == "__main__":

    update_database()
