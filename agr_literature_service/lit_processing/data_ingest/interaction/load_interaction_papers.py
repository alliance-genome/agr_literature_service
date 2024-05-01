import argparse
import logging
import requests
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
from agr_literature_service.lit_processing.utils.db_read_utils import retrieve_all_pmids, get_mod_papers
from agr_literature_service.lit_processing.utils.report_utils import send_report
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

pubmed_efetch_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
pubmed_search_url = f"{pubmed_efetch_url}?api_key={environ['NCBI_API_KEY']}&db=pubmed&id="
download_url = "https://fms.alliancegenome.org/download/"

has_interactions = {
    "GEN": [
        "HUMAN", "SGD", "WB", "FB", "ZFIN", "MGI", "RGD", "XBXL",
    ],
    "MOL": [
        "HUMAN", "SGD", "WB", "FB", "ZFIN", "MGI", "RGD", "XBXL", "XBXT", "SARS-CoV-2"
    ]
}

base_path = environ.get("XML_PATH", "")
file_path = base_path + "interaction_data/"
json_path = base_path + "pubmed_json/"
xml_path = base_path + "pubmed_xml/"


def load_data(datasetName, dataType, message):

    if not has_interactions.get(dataType) or datasetName not in has_interactions[dataType]:
        logger.error(f"We don't have {dataType} interaction data for {datasetName}")
        return

    db_session = create_postgres_session(False)
    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)

    file_name, all_pmids, pmid_to_src = extract_pmids(db_session, datasetName, dataType)
    all_pmids_db = retrieve_all_pmids(db_session)
    new_pmids = all_pmids - set(all_pmids_db)

    if len(new_pmids) == 0:
        check_pmids_and_compose_message(db_session, datasetName, file_name,
                                        all_pmids, new_pmids, message)
        return

    clean_up_tmp_directories()

    download_pubmed_xml(list(new_pmids))
    generate_json(list(new_pmids), [])

    inject_object = {}
    sanitize_pubmed_json_list(new_pmids, [inject_object])

    json_filepath = base_path + 'sanitized_reference_json/REFERENCE_PUBMED_PMID.json'
    post_references(json_filepath)

    pmids_loaded = set()
    for pmid in new_pmids:
        if path.exists(xml_path + pmid + ".xml"):
            pmids_loaded.add(pmid)
            if environ.get('ENV_STATE') and environ['ENV_STATE'] == 'prod':
                logger.info(f"uploading xml file to s3 for PMID:{pmid}")
                upload_xml_file_to_s3(pmid, 'latest')

    add_md5sum_to_database(db_session, None, pmids_loaded)

    check_pmids_and_compose_message(db_session, datasetName, file_name,
                                    all_pmids, pmids_loaded, message)


def extract_pmids(db_session, datasetName, dataType):

    file_name = f"INTERACTION-{dataType}_{datasetName}.tsv.gz"
    url_to_download = f"{download_url}{file_name}"
    file_with_path = f"{file_path}{file_name}"
    download_file(url_to_download, file_with_path)
    all_pmids = set()
    # all_other_ids = []
    pmid_to_src = {}
    with gzip.open(file_with_path, "rt") as f:
        for line in f:
            if line.startswith("#"):
                continue
            items = line.split("\t")
            pub_ids = items[8].split("|")
            pmid = None
            for id in pub_ids:
                if id.isdigit():
                    pmid = id
                elif id.startswith("pubmed:"):
                    pmid = id.replace("pubmed:", "")
            if pmid:
                all_pmids.add(pmid)
                if len(items) > 12:
                    pmid_to_src[pmid] = items[12]
            # else:
            #    all_other_ids.append(items[8])

    return file_name, all_pmids, pmid_to_src


def check_pmids_and_compose_message(db_session, datasetName, file_name, all_pmids, pmids_loaded, message):

    logger.info(f"{file_name}:\n")
    logger.info(f"{len(pmids_loaded)} new reference(s) added into the database")

    message += f"<strong>Loading new papers from {file_name}</strong>:<p>"
    message += f"{len(pmids_loaded)} new reference(s) added into the database<br>"

    all_pmids_db = retrieve_all_pmids(db_session)
    pmids_out_db_set = all_pmids - set(all_pmids_db)

    pmids_in_corpus_set = set()
    pmids_associated_with_mod_but_out_corpus_set = set()
    pmids_in_db_but_not_associated_with_mod_set = set()
    if datasetName in ["SGD", "WB", "FB", "ZFIN", "MGI", "RGD", "XBXL", "XBXT"]:
        mod = datasetName
        mod.replace("XL", "").replace("XT", "")
        in_corpus_set, out_corpus_set = get_mod_papers(db_session, mod)
        for pmid in set(all_pmids):
            if pmid in in_corpus_set:
                pmids_in_corpus_set.add(pmid)
            elif pmid in out_corpus_set:
                pmids_associated_with_mod_but_out_corpus_set.add(pmid)
            elif pmid in all_pmids_db:
                pmids_in_db_but_not_associated_with_mod_set.add(pmid)
        logger.info(f"{len(pmids_in_corpus_set)} references are in {mod} corpus")
        message += f"{len(pmids_in_corpus_set)} references are in {mod} corpus<br>"
        logger.info(f"{len(pmids_associated_with_mod_but_out_corpus_set)} references are associated with {mod}, but are out of {mod} corpus")
        message += f"{len(pmids_associated_with_mod_but_out_corpus_set)} reference(s) are associated with {mod}, but are out of {mod} corpus<br>"
        logger.info(f"{len(pmids_in_db_but_not_associated_with_mod_set)} reference(s) are in the database, but not associated with {mod}")
        message += f"{len(pmids_in_db_but_not_associated_with_mod_set)} reference(s) are in the database, but not associated with {mod}<br>"
    else:
        pmids_in_db_but_not_associated_with_mod_set = set(all_pmids) - pmids_out_db_set
        logger.info(f"{len(pmids_in_db_but_not_associated_with_mod_set)} references are in the database")
        message += f"{len(pmids_in_db_but_not_associated_with_mod_set)} references are in the database<br>"
    obsolete_pmids, valid_pmids = search_pubmed(pmids_out_db_set)
    if len(obsolete_pmids) > 0:
        logger.info(f"Obsolete PMIDs={obsolete_pmids}")
        message += f"Obsolete PMIDs={obsolete_pmids}<br>"
    if len(valid_pmids) > 0:
        logger.info(f"Valid new PMIDs, but not loaded={valid_pmids}")
        message += f"Valid new PMIDs, but not loaded={valid_pmids}<br>"
    message += "<br>"
    logger.info("\n")


def search_pubmed(pmids):

    obsolete_pmids = []
    valid_pmids = []
    for pmid in pmids:
        url = f"{pubmed_search_url}{pmid}"
        response = requests.get(url)
        content = response.text.replace("\n", "")
        if "<PubmedArticleSet></PubmedArticleSet>" in content:
            obsolete_pmids.append(pmid)
        else:
            valid_pmids.append(pmid)

    return obsolete_pmids, valid_pmids


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


def load_all(message):

    combined_dataset_list = has_interactions["GEN"] + has_interactions["MOL"]
    unique_dataset_set = list(set(combined_dataset_list))
    unique_dataset_set.sort()
    for datasetName in unique_dataset_set:
        if datasetName in has_interactions["GEN"]:
            load_data(datasetName, "GEN", message)
        if datasetName in has_interactions["MOL"]:
            load_data(datasetName, "MOL", message)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--all', action='store', type=str, help="update all")
    parser.add_argument('-d', '--datasetName', action='store', type=str,
                        help='datasetName to update',
                        choices=['SGD', 'WB', 'FB', 'ZFIN', 'MGI', 'RGD', 'XBXL',
                                 'XBXT', 'HUMAN', 'SARS-CoV-2'])
    parser.add_argument('-t', '--type', action='store', type=str,
                        help='data type to update: MOL or GEN',
                        choices=['MOL', 'GEN'])
    message = ''
    args = vars(parser.parse_args())
    if not any(args.values()) or args['all']:
        load_all(message)
    elif args['datasetName'] and args['type']:
        load_data(args['datasetName'], args['type'], message)
    elif args['datasetName']:
        if args['datasetName'] in has_interactions["GEN"]:
            load_data(args['datasetName', "GEN"], message)
        if args['datasetName'] in has_interactions["MOL"]:
            load_data(args['datasetName', "MOL"], message)
    elif args['type']:
        for datasetName in has_interactions[args['type']]:
            load_data(datasetName, args['type'], message)

    email_subject = "Interaction Reference Loading Report"
    send_report(email_subject, message)
