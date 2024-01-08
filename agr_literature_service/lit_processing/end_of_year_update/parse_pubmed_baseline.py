import logging
import gzip
import re
import requests
from dotenv import load_dotenv
from os import environ, remove

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import \
    download_file
from agr_literature_service.lit_processing.utils.db_read_utils import retrieve_all_pmids
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

baselinefileRootURL = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
sleep_time = 30

init_tmp_dir()


def update_all_data():  # pragma: no cover

    db_session = create_postgres_session(False)

    ## take 8 sec
    logger.info("Retrieving all pmids:")
    try:
        pmids_all = retrieve_all_pmids(db_session)
    except Exception as e:
        logger.info("Error occurred when retrieving pmid list from database.\n" + str(e))
        db_session.close()
        return

    logger.info("Generating xml files from PubMed annual baseline files:")
    download_and_parse_baseline_files(db_session, set(pmids_all))
    db_session.close()


def download_and_parse_baseline_files(db_session, pmids_all):  # pragma: no cover

    load_dotenv()
    # fresh created the empty directories
    base_path = environ.get('XML_PATH', "")
    xml_path = base_path + "baseline_update/pubmed_xml/"

    baselinefileNames = get_baseline_files()
    for baselinefileName in baselinefileNames:
        logger.info(f"{baselinefileName}")
        baselineFileUrl = baselinefileRootURL + baselinefileName
        baselineFile = base_path + baselinefileName
        download_file(baselineFileUrl, baselineFile)
        with gzip.open(baselineFile, 'rb') as f_in:
            decompressed_content = f_in.read()
            records = decompressed_content.decode('utf-8').split("</PubmedArticle>")
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
                            logger.info(f"generating xml file for PMID:{pmid}")
                            record = re.sub(r'\s*\n\s*', '', record)
                            record = record.strip()
                            with open(xml_path + pmid + ".xml", "w") as f_out:
                                f_out.write(header + "<PubmedArticleSet>" + record + "</PubmedArticle></PubmedArticleSet>\n")
        remove(baselineFile)


def get_baseline_files():

    """
    Baseline Data
    -------------
    NLM produces a baseline set of PubMed citation records in XML format
    for download on an annual basis. The annual baseline is released in
    December of each year. The complete baseline consists of files
    pubmed24n0001.xml through pubmed24n1219.xml.
    ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline
    some example below:
    pubmed24n0001.xml.gz
    pubmed24n0002.xml.gz
    pubmed24n0003.xml.gz
    ...
    pubmed24n1219.xml.gz
    """

    response = requests.request("GET", baselinefileRootURL)
    files = response.text.split("<a href=")
    baselineFiles = []
    for file in files:
        if ".gz.md5" not in file and ".xml.gz" in file:
            baselineFiles.append(file.split(">")[0].replace('"', ''))

    return baselineFiles


if __name__ == "__main__":

    update_all_data()
