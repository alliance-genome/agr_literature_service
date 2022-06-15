import logging
from datetime import datetime
import time

from agr_literature_service.api.models import CrossReferenceModel
from agr_literature_service.lit_processing.helper_sqlalchemy import create_postgres_session
from agr_literature_service.lit_processing.update_resource_pubmed_nlm import update_resource_pubmed_nlm
from agr_literature_service.lit_processing.get_pubmed_xml import download_pubmed_xml
from agr_literature_service.lit_processing.update_mod_pubmed_papers import update_data
from agr_literature_service.lit_processing.filter_dqm_md5sum import load_s3_md5data, save_s3_md5data

logging.basicConfig(format='%(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)

download_xml_max_size = 50000
sleep_time = 60


def update_all_data():

    ## take 18 sec
    log.info("Updating resource: ", datetime.now())
    try:
        update_resource_pubmed_nlm()
    except Exception as e:
        log.info("Error occurred when updating resource info.\n" + str(e))
        return

    ## take 8 sec
    log.info("Retrieving all pmids: ", datetime.now())
    try:
        pmids_all = retrieve_all_pmids()
        pmids_all.sort()
    except Exception as e:
        log.info("Error occurred when retrieving pmid list from database.\n" + str(e))
        return

    ## take 2hrs
    log.info("Downloading all xml files: ", datetime.now())
    try:
        download_all_xml_files(pmids_all)
    except Exception as e:
        log.info("Error occurred when downloading the xml files from PubMed.\n" + str(e))
        return

    for mod in ['WB', 'ZFIN', 'FB', 'SGD', 'RGD', 'MGI', 'NONE']:
        if mod == 'NONE':
            log.info("Updating pubmed papers that are not associated with a mod: ", datetime.now())
        else:
            log.info("Updating pubmed papers for " + mod + ": ", datetime.now())
        md5dict = load_s3_md5data(['PMID'])
        try:
            update_data(mod, None, md5dict)
        except Exception as e:
            log.info("Error occurred when updating pubmed papers for " + mod + "\n" + str(e))
            save_s3_md5data(md5dict, ['PMID'])
        time.sleep(sleep_time)


def download_all_xml_files(pmids_all):

    for index in range(0, len(pmids_all), download_xml_max_size):
        pmids_slice = pmids_all[index:index + download_xml_max_size]
        download_pubmed_xml(pmids_slice)
        time.sleep(sleep_time)


def retrieve_all_pmids():

    db_session = create_postgres_session(False)

    pmids = []
    for x in db_session.query(CrossReferenceModel).filter(CrossReferenceModel.curie.like('PMID:%')).all():
        if x.is_obsolete:
            continue
        pmids.append(x.curie.replace("PMID:", ""))

    db_session.close()

    return pmids


if __name__ == "__main__":

    update_all_data()
