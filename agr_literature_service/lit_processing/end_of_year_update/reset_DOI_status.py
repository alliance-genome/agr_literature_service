import logging
import requests
from sqlalchemy import text
from sqlalchemy.orm import Session
from os import path
from agr_literature_service.api.models import CrossReferenceModel
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.user import set_global_user_id

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

doi_root_url = "https://doi.org/"
bad_doi = "DOI:10.1016/j.bbagen.xxxx.130210"
batch_commit_size = 200


def reset_DOI_status():

    db_session = create_postgres_session(False)

    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)

    rows = db_session.execute(text("select reference_id, curie "
                                   "from   cross_reference "
                                   "where  curie_prefix = 'DOI' "
                                   "and    reference_id in "
                                   "(select reference_id from cross_reference "
                                   "where curie_prefix = 'PMID' and is_obsolete is True)")).fetchall()

    found = check_for_doi_in_doi_resolver(bad_doi)
    if found:
        logger.info("Something wrong with DOI resolver")
        return

    record = 0
    for row in rows:
        valid_pmid_status = check_for_valid_pmid(db_session, row['reference_id'])
        if valid_pmid_status:
            # it is a valid PubMed paper so let PubMed update script take care of it
            continue
        doi = row['curie']
        isValidDOI = check_for_doi_in_doi_resolver(doi)

        x = db_session.query(CrossReferenceModel).filter_by(curie=doi).one_or_none()
        record += 1
        if x:
            try:
                if x.is_obsolete is False and not isValidDOI:
                    x.is_obsolete = True
                    logger.info(f"{record} The XREF for {doi} is set to obsolete")
                elif x.is_obsolete is True and isValidDOI:
                    x.is_obsolete = False
                    logger.info(f"{record} The XREF for {doi} is set to valid")
            except Exception as e:
                logger.info(f"{record} An error occurred when setting the XREF for {doi} status: {e}")
        if record % batch_commit_size == 0:
            db_session.commit()
            # db_session.rollback()
    db_session.commit()
    # db_session.rollback()


def check_for_doi_in_doi_resolver(doi):
    url = doi_root_url + doi.replace("DOI:", "doi:")
    response = requests.get(url)
    result = response.content.decode('UTF-8')
    if "DOI Not Found" in result:
        return False
    return True


"""
def check_for_doi_in_pubmed(doi): # noqa
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
        xml_string = response.content.decode('UTF-8')
        tree = ElementTree.fromstring(xml_string)
        return tree.find('Count').text
    except Exception as e:
        logger.info(f"Error(s) occurred when searching PubMed: {e}")
"""


def check_for_valid_pmid(db_session: Session, reference_id):

    rows = db_session.execute(text(f"SELECT cross_reference_id "
                                   f"FROM   cross_reference "
                                   f"WHERE  reference_id = {reference_id} "
                                   f"AND    curie_prefix = 'PMID' "
                                   f"AND    is_obsolete is False")).fetchall()
    if len(rows) > 0:
        return True
    return False


if __name__ == "__main__":

    reset_DOI_status()
