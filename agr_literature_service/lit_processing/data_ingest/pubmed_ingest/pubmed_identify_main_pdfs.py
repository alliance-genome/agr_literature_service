import logging
import time
import requests
from sqlalchemy import text
from os import environ
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session
from agr_literature_service.api.models import CrossReferenceModel, ReferencefileModel
from agr_literature_service.lit_processing.data_ingest.utils.db_write_utils import \
    add_file_uploaded_workflow
from agr_literature_service.api.crud.referencefile_crud import cleanup_old_pdf_file

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

rootUrl = "https://www.ncbi.nlm.nih.gov"
pmcRootUrl = 'https://ftp.ncbi.nlm.nih.gov/pub/pmc/'

## the following file is created by main PMC downloading program
## (pubmed_download_pmc_files.py) that calls this program
## to identify the main PDFs after downloading PMC packages
## from PubMed Central, uploading the files to s3, loading
## the metadata into ABC
infile = "data/pmc_oa_files_uploaded.txt"
batch_size = 20


def identify_main_pdfs(identify_all=False):

    if identify_all:
        logger.info("Reading PMCID list that are missing main PDFs from the database...")
        db_session = create_postgres_session(False)
        pmcid_set = get_pmcids_without_main_pdf(db_session)
        db_session.close()
    else:
        logger.info("Reading PMCID list from pmc_oa_files_uploaded.txt...")
        pmcid_set = get_pmcid_for_recent_downloaded_pmc_packages()

    logger.info("Searching PMC for PDF full texts...")

    batch_count = 0
    pmcids = []
    pmcid_to_pdf_name = {}
    for pmcid in pmcid_set:
        if len(pmcids) >= batch_size:
            batch_count += 1
            logger.info("batch-" + str(batch_count) + ": Searching PMC and extract PDF file names")
            search_pmc_and_extract_pdf_file_names(pmcids, pmcid_to_pdf_name)
            pmcids = []
            time.sleep(1)
        pmcids.append(pmcid)

    if len(pmcids) > 0:
        search_pmc_and_extract_pdf_file_names(pmcids, pmcid_to_pdf_name)

    logger.info("Updating file_class for identified PDF files in the database...")

    db_session = create_postgres_session(False)

    for pmcid in pmcid_to_pdf_name:
        crossRef = db_session.query(CrossReferenceModel).filter_by(
            curie='PMCID:' + pmcid, is_obsolete=False).one_or_none()
        if crossRef:
            for x in db_session.query(ReferencefileModel).filter_by(reference_id=crossRef.reference_id).all():
                if x.file_extension == 'pdf':
                    file_name = x.display_name + ".pdf"
                    if file_name.lower() == pmcid_to_pdf_name[pmcid].lower():
                        logger.info(pmcid + ": Found main PDF file " + pmcid_to_pdf_name[pmcid])
                        if x.file_class != 'main':
                            x.file_class = 'main'
                            db_session.add(x)
                            db_session.commit()
                            logger.info(pmcid + ": update the file_class to 'main' for the main PDF file " + pmcid_to_pdf_name[pmcid])
                            ref = x.reference
                            add_file_uploaded_workflow(db_session, str(x.reference_id), logger=logger)
                            cleanup_old_pdf_file(db_session, ref.curie, 'all_access')
    db_session.close()


def get_pmcids_without_main_pdf(db_session):

    pmcid_set = set()

    rows = db_session.execute(text("SELECT distinct cr.curie "
                                   "FROM cross_reference cr, referencefile rf, referencefile_mod rfm "
                                   "WHERE cr.curie_prefix = 'PMCID' "
                                   "AND cr.reference_id = rf.reference_id "
                                   "AND rf.file_class = 'supplement' "
                                   "AND rf.referencefile_id = rfm.referencefile_id "
                                   "AND rfm.mod_id is NULL "
                                   "AND NOT EXISTS ( "
                                   "SELECT 1 "
                                   "FROM referencefile "
                                   "WHERE reference_id = rf.reference_id "
                                   "AND file_class = 'main')")).fetchall()
    for x in rows:
        pmcid_set.add(x[0].replace("PMCID:", ''))
    return pmcid_set


def get_pmcid_for_recent_downloaded_pmc_packages():

    pmcid_set = set()
    with open(infile) as f:
        for line in f:
            # 35857496      PMC9278858      sciadv.abm9875-f5.jpg   17ef0e061fcdc9bd1f4338809f738d72
            pieces = line.strip().split("\t")
            pmcid_set.add(pieces[1])
    return pmcid_set


def search_pmc_and_extract_pdf_file_names(pmcids, pmcid_to_pdf_name):

    url = rootUrl + "/pmc/?term=" + "+OR+".join(pmcids)

    if environ.get('NCBI_API_KEY'):
        url = url + "&api_key=" + environ['NCBI_API_KEY']

    response = requests.get(url)
    content = str(response.content)
    if ">PDF" not in content:
        return
    records = content.split('>PDF')
    records.pop()
    for record in records:
        for line in record.split("\n"):
            if "pmc.ncbi.nlm.nih.gov/articles/PMC" in line and '.pdf' in line:
                for item in line.split(" "):
                    if "pmc.ncbi.nlm.nih.gov/articles/PMC" in item and '.pdf' in item:
                        # href="https://pmc.ncbi.nlm.nih.gov/articles/PMC3248519/pdf/pnas.201114118.pdf"
                        fulltext_url = item.replace("href=", '').replace('"', '')
                        url_pieces = fulltext_url.split('/')
                        pmcid = url_pieces[-3]
                        pdf_name = url_pieces[-1]
                        logger.info(f"fulltext_url={fulltext_url}, pmcid={pmcid}, pdf_name={pdf_name}")
                        if pmcid.startswith('PMC') and pdf_name.endswith('.pdf'):
                            pmcid_to_pdf_name[pmcid] = pdf_name


if __name__ == "__main__":

    identify_main_pdfs(True)
