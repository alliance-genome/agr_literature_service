import argparse
import logging
import gzip
import shutil
from os import environ, makedirs, path
from dotenv import load_dotenv
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import \
    download_file
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_pubmed_xml import \
    download_pubmed_xml
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml_to_json import generate_json
from agr_literature_service.lit_processing.utils.db_read_utils import retrieve_all_pmids
from agr_literature_service.lit_processing.data_ingest.post_reference_to_db import post_references
from agr_literature_service.lit_processing.utils.s3_utils import upload_xml_file_to_s3
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_search_new_references \
    import add_md5sum_to_database
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.sanitize_pubmed_json import \
    sanitize_pubmed_json_list
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()
init_tmp_dir()

download_url = "https://fms.alliancegenome.org/download/"

has_interactions = {
    "GEN": [
        "HUMAN", "SGD", "WB", "FB", "ZFIN", "MGI", "RGD", "XBXL"
    ],
    "MOL": [
        "HUMAN", "SGD", "WB", "FB", "ZFIN", "MGI", "RGD", "XBXL", "XBXT", "SARS-CoV-2"
    ]
}

base_path = environ.get("XML_PATH", "")
file_path = base_path + "interaction_data/"
json_path = base_path + "pubmed_json/"
xml_path = base_path + "pubmed_xml/"


def load_data(datasetName, dataType):

    if not has_interactions.get(dataType) or datasetName not in has_interactions[dataType]:
        logger.error(f"We don't have {dataType} interaction data for {datasetName}")
        return

    db_session = create_postgres_session(False)
    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)

    (new_pmids, pmids_in_db) = extract_pmids(db_session, datasetName, dataType)

    clean_up_tmp_directories()

    download_pubmed_xml(list(new_pmids))
    generate_json(list(new_pmids), [])

    inject_object = {}
    sanitize_pubmed_json_list(new_pmids, [inject_object])

    json_filepath = base_path + 'sanitized_reference_json/REFERENCE_PUBMED_PMID.json'
    post_references(json_filepath)

    if environ.get('ENV_STATE') and environ['ENV_STATE'] == 'prod':
        for pmid in new_pmids:
            upload_xml_file_to_s3(pmid, 'latest')

    add_md5sum_to_database(db_session, None, new_pmids)

    db_session.close()


def clean_up_tmp_directories():

    try:
        if path.exists(file_path):
            shutil.rmtree(file_path)
        if path.exists(xml_path):
            shutil.rmtree(xml_path)
        if path.exists(json_path):
            shutil.rmtree(json_path)
    except OSError as e:
        logger.info("Error deleting old interaction/xml/json files: %s" % (e.strerror))

    makedirs(file_path)
    makedirs(xml_path)
    makedirs(json_path)


def extract_pmids(db_session, datasetName, dataType):

    file_name = f"INTERACTION-{dataType}_{datasetName}.tsv.gz"
    url_to_download = f"{download_url}{file_name}"
    file_with_path = f"{file_path}{file_name}"
    download_file(url_to_download, file_with_path)
    all_pmids = []
    all_other_ids = []
    total_annotations = 0
    with gzip.open(file_with_path, "rt") as f:
        for line in f:
            if line.startswith("#"):
                continue
            total_annotations += 1
            items = line.split("\t")
            pub_ids = items[8].split("|")
            pmid = None
            for id in pub_ids:
                if id.isdigit():
                    pmid = id
                elif id.startswith("pubmed:"):
                    pmid = id.replace("pubmed:", "")
            if pmid:
                all_pmids.append(pmid)
            else:
                all_other_ids.append(items[8])

    all_pmids_db = retrieve_all_pmids(db_session)
    new_pmids = set(all_pmids) - set(all_pmids_db)
    pmids_in_db = set(all_pmids) - new_pmids
    return (new_pmids, pmids_in_db)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--datasetName', action='store', type=str,
                        help='datasetName to update',
                        choices=['SGD', 'WB', 'FB', 'ZFIN', 'MGI', 'RGD', 'XBXL',
                                 'XBXT', 'HUMAN', 'SARS-CoV-2'])
    parser.add_argument('-t', '--type', action='store', type=str,
                        help='data type to update: MOL or GEN',
                        choices=['MOL', 'GEN'])

    args = vars(parser.parse_args())
    if not any(args.values()):
        parser.error('No arguments provided.')

    load_data(args['datasetName'], args['type'])
