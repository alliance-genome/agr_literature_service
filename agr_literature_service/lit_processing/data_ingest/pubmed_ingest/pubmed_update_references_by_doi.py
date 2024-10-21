import time
import logging
import requests
from sqlalchemy import text
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
    papers_to_merge = []
    pmids_to_update = []
    papers_to_add_pmid = []
    for reference_id in reference_id_to_doi:
        if reference_id not in reference_id_to_pmid:
            i += 1
            doi = reference_id_to_doi[reference_id]
            logger.info(f"{i}: processing {doi}")
            pmids = get_pmid_for_doi(doi)
            if pmids is None:
                continue
            if len(pmids) == 1:
                pmid = "PMID:" + pmids[0]
                if pmid in pmid_to_reference_id:
                    logger.info(f"FOUND {pmid}, but it is already in the database")
                    papers_to_merge.append((doi, pmid))
                else:
                    logger.info(f"FOUND {pmid} and it is a new one")
                    papers_to_add_pmid.append((reference_id, pmid))
                    pmids_to_update.append(pmid.replace("PMID:", ""))
            time.sleep(0.35)

    db = create_postgres_session(False)

    if len(papers_to_add_pmid) > 0:
        logger.info(f"Adding PMID to papers ({len(papers_to_add_pmid)}) with_DOI:")
        add_pmid_to_existing_papers(db, papers_to_add_pmid)

    # db.rollback()
    db.commit()

    if len(pmids_to_update) > 0:
        logger.info(f"Updating papers ({len(pmids_to_update)}) with data from PubMed:")
        update_papers(db, pmids_to_update)

    # db.rollback()
    db.commit()
    db.close()

    if len(papers_to_merge) > 0:
        logger.info("Sending report to slack:")
        send_report_for_merging_paper(papers_to_merge)

    logger.info("DONE!")


def get_pmid_for_doi(doi): # noqa
    params = {
        "db": "pubmed",
        "term": f"{doi}[DOI]",
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


def add_pmid_to_existing_papers(db: Session, papers_to_add_pmid): # noqa

    try:
        for (reference_id, pmid) in papers_to_add_pmid:
            x = CrossReferenceModel(reference_id=reference_id,
                                    curie_prefix='PMID',
                                    curie=pmid,
                                    is_obsolete=False)
            db.add(x)
            logger.info(f"Adding {pmid} to cross_reference table for reference_id = {reference_id}")
    except Exception as e:
        logger.info(f"Error(s) occurred when adding PMID(s) into cross_reference table: {e}")


def update_papers(db: Session, pmids_to_update): # noqa

    pmids = "|".join(pmids_to_update)

    try:
        update_data(None, pmids)
    except Exception as e:
        logger.info(f"Error(s) occurred when updating papers with the data from PubMed: {e}")


def send_report_for_merging_paper(papers_to_merge): # noqa

    email_subject = "Duplicate Paper Pairs Detected: Merge Required"

    email_message = "During our routine checks, we've identified pairs of papers in our database that appear to be duplicates. One paper in each pair has a DOI ID, while the other has a PMID. We believe these pairs correspond to the same paper and need to be merged.<p>Below is the list of detected duplicate pairs:<p>"

    rows = "<tr><th style='text-align:left' width='300'>Paper with DOI</th><th style='text-align:left' width='200'>Paper with PMID</th></tr>"

    for (doi, pmid) in papers_to_merge:
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
