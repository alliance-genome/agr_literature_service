import logging
from os import environ, listdir

from agr_literature_service.lit_processing.utils.s3_utils import upload_xml_file_to_s3

if not environ.get('ENV_STATE') or environ['ENV_STATE'] != 'prod':
    exit()

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

base_path = environ.get('XML_PATH', "")
xml_path = base_path + "pubmed_xml/"

pmids = [f.replace(".xml", "") for f in listdir(xml_path) if f.endswith('.xml')]
pmids.sort()

logger.info("Uploading xml files to s3...")
i = 0
for pmid in pmids:
    i += 1
    logger.info(f"{i}: uploading xml file for PMID:{pmid} to s3")
    upload_xml_file_to_s3(pmid, 'latest')
logger.info("DONE!\n\n")
