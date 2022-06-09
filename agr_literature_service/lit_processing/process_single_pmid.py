import argparse
import json
import logging
import logging.config
from os import environ, path

import requests

from agr_literature_service.lit_processing.get_pubmed_xml import download_pubmed_xml
from agr_literature_service.lit_processing.post_reference_to_api import post_references
from agr_literature_service.lit_processing.sanitize_pubmed_json import sanitize_pubmed_json_list
from agr_literature_service.lit_processing.xml_to_json import generate_json
from agr_literature_service.lit_processing.helper_s3 import upload_xml_file_to_s3

# pipenv run python process_single_pmid.py -c 12345678
# enter a single pmid as an argument, download xml, convert to json, sanitize, post to api

log_file_path = path.join(path.dirname(path.abspath(__file__)), '../../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


def check_pmid_cross_reference(pmid):
    """

    :param pmid:
    :return:
    """

    api_port = environ.get('API_PORT')
    api_server = environ.get('API_SERVER', 'localhost')
    url = 'http://' + api_server + ':' + api_port + '/cross_reference/PMID:' + pmid
    #     'Authorization': 'Bearer <token_goes_here>',
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    process_results = []
    process_result = dict()
    process_result['text'] = 'cross_reference not found'
    process_result['status_code'] = 999
    process_result['found'] = False
    post_return = requests.get(url, headers=headers)
    process_status_code = post_return.status_code
    if process_status_code == 200:
        process_result['found'] = True
        process_result['text'] = post_return.json()['reference_curie']
        process_result['status_code'] = process_status_code
    process_results.append(process_result)
    return process_results


def output_message_json(process_results):
    """

    :param process_results:
    :return:
    """

    process_result = dict()
    if process_results:
        process_result = process_results.pop()
        process_result['status_code'] = int(process_result['status_code'])
        if process_result['text'].startswith('"') and process_result['text'].endswith('"'):
            process_result['text'] = process_result['text'][1:-1]
    else:
        process_result['text'] = 'Failure processing POST to API'
        process_result['status_code'] = 999
    return json.dumps(process_result)


def process_pmid(pmid):
    """

    :param pmid:
    :return:
    """

    process_results = check_pmid_cross_reference(pmid)
    if not process_results[0]['found']:
        base_path = environ.get('XML_PATH')
        pmids_wanted = [pmid]
        download_pubmed_xml(pmids_wanted)
        generate_json(pmids_wanted, [])
        sanitize_pubmed_json_list(pmids_wanted, [])
        # json_filepath = base_path + 'sanitized_reference_json/REFERENCE_PUBMED_' + pmid + '.json'
        json_filepath = base_path + 'sanitized_reference_json/REFERENCE_PUBMED_PMID.json'
        process_results = post_references(json_filepath, 'no_file_check')
        upload_xml_file_to_s3(pmid)
    return output_message_json(process_results)


if __name__ == "__main__":
    """
    call main start function
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--commandline', nargs='*', action='store', help='take input from command line flag')

    args = vars(parser.parse_args())

    pmids_wanted = []

#    python process_single_pmid.py -c 1234 4576 1828
    if args['commandline']:
        logger.info("Processing commandline input")
        for pmid in args['commandline']:
            pmids_wanted.append(pmid)

    else:
        logger.info("Must enter a PMID through command line")

    for pmid in pmids_wanted:
        process_pmid(pmid)
