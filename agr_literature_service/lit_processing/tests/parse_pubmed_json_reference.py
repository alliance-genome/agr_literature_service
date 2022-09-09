import argparse
import logging.config
from os import environ, path
from typing import List

from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.sanitize_pubmed_json import \
    sanitize_pubmed_json_list
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

# pipenv run python parse_pubmed_json_reference.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/pubmed_only_pmids
# enter a file of pmids as an argument, sanitize, post to api

log_file_path = path.join(path.dirname(path.abspath(__file__)), '../../../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('parse_pubmed_json_reference')

init_tmp_dir()


def parse_pubmed_json_reference(pmids: List[str] = None, load_pmids_from_file_path: str = None):
    if load_pmids_from_file_path:
        base_path = environ.get("XML_PATH", "")
        logger.info("Processing file input from %s", load_pmids_from_file_path)
        pmids = [line.strip() for line in open(base_path + load_pmids_from_file_path, 'r')]

    sanitize_pubmed_json_list(pmids, [])

    # do below if wanting to post from here, instead of from post_reference_to_api.py
    # base_path = environ.get('XML_PATH')
    # json_filepath = base_path + 'sanitized_reference_json/REFERENCE_PUBMED_PMID.json'
    # process_results = post_references(json_filepath)

    logger.info("Done Processing")


if __name__ == "__main__":
    """
    call main start function
    """

    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-c', '--commandline', nargs='*', action='store', help='take input from command line flag')
    group.add_argument('-f', '--file', action='store', help='take input from entries in file with full path')

    args = vars(parser.parse_args())
    parse_pubmed_json_reference(pmids=args['commandline'], load_pmids_from_file_path=args['file'])
