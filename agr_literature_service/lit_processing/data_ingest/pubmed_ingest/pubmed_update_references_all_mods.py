import logging
import gzip
import re
import time
import requests
from dotenv import load_dotenv
from os import environ, makedirs, path, remove
import shutil

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_resources_nlm import \
    update_resource_pubmed_nlm
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import \
    download_file
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_references_single_mod \
    import update_data
from agr_literature_service.lit_processing.utils.db_read_utils import sort_pmids, \
    retrieve_all_pmids, get_mod_abbreviations
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

updatefileRootURL = "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/"
sleep_time = 30
default_days = 8

init_tmp_dir()


def update_all_data():  # pragma: no cover

    ## take 18 sec
    logger.info("Updating resource:")
    try:
        update_resource_pubmed_nlm()
    except Exception as e:
        logger.info("Error occurred when updating resource info.\n" + str(e))
        return

    db_session = create_postgres_session(False)

    ## take 8 sec
    logger.info("Retrieving all pmids:")
    try:
        pmids_all = retrieve_all_pmids(db_session)
    except Exception as e:
        logger.info("Error occurred when retrieving pmid list from database.\n" + str(e))
        db_session.close()
        return

    logger.info("Retrieving pmids from PubMed daily update file:")
    (updated_pmids_for_mod, deleted_pmids_for_mod) = download_and_parse_daily_update(db_session,
                                                                                     set(pmids_all))
    db_session.close()

    resourceUpdated = 1
    for mod in [*get_mod_abbreviations(), 'NONE']:
        if mod == 'NONE':
            logger.info("Updating pubmed papers that are not associated with a mod:")
        else:
            logger.info("Updating pubmed papers for " + mod + ":")
        pmids = updated_pmids_for_mod.get(mod, set())
        try:
            update_data(mod, '|'.join(list(pmids)), resourceUpdated)
        except Exception as e:
            logger.info("Error occurred when updating pubmed papers for " + mod + "\n" + str(e))
        time.sleep(sleep_time)


def download_and_parse_daily_update(db_session, pmids_all):  # pragma: no cover

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
        logger.info("Error deleting old xml/json: %s" % (e.strerror))

    makedirs(xml_path)
    makedirs(json_path)

    updated_pmids_for_mod = {}
    deleted_pmids = []
    dailyfileNames = get_daily_update_files()
    for dailyfileName in dailyfileNames:
        updated_pmids = []
        dailyFileUrl = updatefileRootURL + dailyfileName
        dailyFile = base_path + dailyfileName
        download_file(dailyFileUrl, dailyFile)
        with gzip.open(dailyFile, 'rb') as f_in:
            decompressed_content = f_in.read()
            records = decompressed_content.decode('utf-8').split("</PubmedArticle>")
            deleteRecords = records.pop().split('\n')
            header = None
            for record in records:
                if header is None:
                    header_lines = record.split("<PubmedArticleSet>")
                    header = header_lines[0].replace('\n', '')
                    record = header_lines[1]
                lines = record.split('\n')
                for line in lines:
                    if '<PMID Version="1">' in line:
                        pmid = line.split('>')[1].split('<')[0]
                        if pmid in pmids_all:
                            updated_pmids.append(pmid)
                            logger.info(f"generating xml file for PMID:{pmid}")
                            record = re.sub(r'\s*\n\s*', '', record)
                            record = record.strip()
                            with open(xml_path + pmid + ".xml", "w") as f_out:
                                f_out.write(header + "<PubmedArticleSet>" + record + "</PubmedArticle></PubmedArticleSet>\n")

            for record in deleteRecords:
                if record.startswith('<PMID Version'):
                    pmid = record.split('>')[1].split('<')[0]
                    if pmid in pmids_all and pmid not in deleted_pmids:
                        deleted_pmids.append(pmid)

        logger.info(f"{dailyfileName}: {len(updated_pmids)} PMIDs")
        if len(updated_pmids) > 0:
            sort_pmids(db_session, updated_pmids, updated_pmids_for_mod)
        remove(dailyFile)

    logger.info(f"deleted PMIDs: {len(deleted_pmids)}")
    deleted_pmids_for_mod = {}
    if len(deleted_pmids) > 0:
        sort_pmids(db_session, deleted_pmids, deleted_pmids_for_mod)

    for mod in updated_pmids_for_mod:
        print(mod, len(updated_pmids_for_mod[mod]))

    return (updated_pmids_for_mod, deleted_pmids_for_mod)


def get_daily_update_files(days=None):

    """
    some examples of pubmed daily update files:
    pubmed23n1424.xml.gz     2023-07-27
    pubmed23n1425.xml.gz     2023-07-28
    pubmed23n1426.xml.gz     2023-07-29
    pubmed23n1427.xml.gz     2023-07-30
    pubmed23n1428.xml.gz     2023-07-31
    pubmed23n1429.xml.gz     2023-08-01
    pubmed23n1430.xml.gz     2023-08-02
    pubmed23n1431.xml.gz     2023-08-03
    pubmed23n1432.xml.gz     2023-08-04
    pubmed23n1433.xml.gz     2023-08-05
    pubmed23n1434.xml.gz     2023-08-06
    pubmed23n1435.xml.gz     2023-08-07
    pubmed23n1436.xml.gz     2023-08-08
    """

    if days is None:
        days = default_days
    response = requests.request("GET", updatefileRootURL)
    files = response.text.split("<a href=")
    dailyFiles = []
    files.pop()
    while len(files) > 0:
        file = files.pop()
        if len(dailyFiles) > days:
            break
        if ".html" not in file and ".gz.md5" not in file and ".xml.gz" in file:
            dailyFiles.append(file.split(">")[0].replace('"', ''))

    return dailyFiles


if __name__ == "__main__":

    update_all_data()
