import logging
import time
from dotenv import load_dotenv
from os import environ, makedirs, path, listdir, stat, remove
import shutil

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_resources_nlm import \
    update_resource_pubmed_nlm
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_pubmed_xml import \
    download_pubmed_xml
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_references_single_mod import \
    update_data
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.md5sum_utils import \
    load_s3_md5data, save_s3_md5data
from agr_literature_service.lit_processing.data_ingest.utils.db_utils import retrieve_newly_added_pmids,\
    retrieve_all_pmids
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

logging.basicConfig(format='%(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)

download_xml_max_size = 50000
sleep_time = 60

init_tmp_dir()


def update_all_data():  # pragma: no cover

    ## take 18 sec
    log.info("Updating resource:")
    try:
        update_resource_pubmed_nlm()
    except Exception as e:
        log.info("Error occurred when updating resource info.\n" + str(e))
        return

    db_session = create_postgres_session(False)

    ## take 8 sec
    log.info("Retrieving all pmids:")
    try:
        pmids_all = retrieve_all_pmids(db_session)
        pmids_all.sort()
    except Exception as e:
        log.info("Error occurred when retrieving pmid list from database.\n" + str(e))
        db_session.close()
        return

    log.info("Retrieving recently added pmids:")
    pmids_new = []
    try:
        pmids_new = retrieve_newly_added_pmids(db_session)
    except Exception as e:
        log.info("Error occurred when retrieving new pmid list from database.\n" + str(e))
        db_session.close()
        return

    db_session.close()

    ## take 1 to 2hrs
    log.info("Downloading all xml files:")
    try:
        download_all_xml_files(pmids_all)
    except Exception as e:
        log.info("Error occurred when downloading the xml files from PubMed.\n" + str(e))
        return

    for mod in ['WB', 'ZFIN', 'XB', 'FB', 'SGD', 'RGD', 'MGI', 'NONE']:
        if mod == 'NONE':
            log.info("Updating pubmed papers that are not associated with a mod:")
        else:
            log.info("Updating pubmed papers for " + mod + ":")
        md5dict = load_s3_md5data(['PMID'])
        try:
            update_data(mod, None, md5dict, pmids_new)
        except Exception as e:
            log.info("Error occurred when updating pubmed papers for " + mod + "\n" + str(e))
            save_s3_md5data(md5dict, ['PMID'])
        time.sleep(sleep_time)


def download_all_xml_files(pmids_all):  # pragma: no cover

    load_dotenv()
    base_path = environ.get('XML_PATH', "")
    xml_path = base_path + "pubmed_xml/"
    json_path = base_path + "pubmed_json/"

    try:
        if path.exists(xml_path):
            shutil.rmtree(xml_path)
        if path.exists(json_path):
            shutil.rmtree(json_path)
    except OSError as e:
        log.info("Error deleting old xml/json: %s" % (e.strerror))

    makedirs(xml_path)
    makedirs(json_path)

    for index in range(0, len(pmids_all), download_xml_max_size):
        pmids_slice = pmids_all[index:index + download_xml_max_size]
        download_pubmed_xml(pmids_slice)
        time.sleep(sleep_time)

    # get pmids with a xml file and file size != 0
    found_xml = get_pmids_with_xml(xml_path)

    # try to download the xml files one more time for the pmids with no xml downloaded
    # or with xml file size = 0 during previous xml download process
    missingXmlPmidList = []
    for pmid in pmids_all:
        if pmid not in found_xml:
            missingXmlPmidList.append(pmid)
    found_xml.clear()

    if len(missingXmlPmidList) > 0:
        log.info("Downloading xml file(s) for " + str(len(missingXmlPmidList)) + " PMID(s)")
        download_pubmed_xml(missingXmlPmidList)

    remove_empty_xml_file(xml_path)


def get_pmids_with_xml(xml_path):

    found_xml = {}

    for filename in listdir(xml_path):
        file = path.join(xml_path, filename)
        if filename.endswith('.xml') and stat(file).st_size > 0:
            pmid = filename.replace('.xml', '')
            found_xml[pmid] = 1

    return found_xml


def remove_empty_xml_file(xml_path):  # pragma: no cover

    for filename in listdir(xml_path):
        file = path.join(xml_path, filename)
        if stat(file).st_size == 0:
            log.info(filename + ": file size = 0")
            remove(file)


if __name__ == "__main__":

    update_all_data()
