import logging
import time
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session
from agr_literature_service.api.models import CrossReferenceModel, ReferencefileModel
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_identify_main_pdfs import \
    search_pmc_and_extract_pdf_file_names
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
batch_size = 20


def identify_main_pdfs():

    logger.info("Retriving PMCIDs without a main PDF...")

    db_session = create_postgres_session(False)

    pmcid_set = get_pmcids_without_main_pdf(db_session)

    db_session.close()

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
                            cleanup_old_pdf_file(db_session, ref.curie, 'all_access')
    db_session.close()


def get_pmcids_without_main_pdf(db_session):

    pmcid_set = set()

    rows = db_session.execute("SELECT distinct cr.curie "
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
                              "AND file_class = 'main')").fetchall()
    for x in rows:
        pmcid_set.add(x[0].replace("PMCID:", ''))
    return pmcid_set


if __name__ == "__main__":

    identify_main_pdfs()
