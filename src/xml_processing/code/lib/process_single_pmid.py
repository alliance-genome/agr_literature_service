"""
# pipenv run python process_single_pmid.py -c 12345678
# enter a single pmid as an argument, download xml, convert to json, sanitize, post to api
"""

import argparse
import json
import logging
import logging.config
from os import environ, path

import requests

from get_pubmed_xml import download_pubmed_xml

# from parse_dqm_json_reference import write_json
from post_reference_to_api import post_references
from sanitize_pubmed_json import sanitize_pubmed_json_list
from xml_to_json import generate_json

log_file_path = path.join(path.dirname(path.abspath(__file__)), "../logging.conf")
logging.config.fileConfig(log_file_path)
logger = logging.getLogger("literature logger")

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--commandline", nargs="*", action="store", help="take input from command line flag",)

args = vars(parser.parse_args())


def check_pmid_cross_reference(pmid):
    """

    :param pmid:
    :return:
    """

    api_port = environ.get("API_PORT")
    api_server = environ.get("API_SERVER", "localhost")
    url = "http://" + api_server + ":" + api_port + "/cross_reference/PMID:" + pmid
    #     'Authorization': 'Bearer <token_goes_here>',
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    process_results = []
    process_result = {"text": "cross_reference not found", "status_code": 999, "found": False}
    post_return = requests.get(url, headers=headers)
    process_status_code = post_return.status_code
    if process_status_code == 200:
        process_result["found"] = True
        process_result["text"] = post_return.json()["reference_curie"]
        process_result["status_code"] = process_status_code
    process_results.append(process_result)

    return process_results


# def sanitize_pubmed_json(pmid):
#     base_path = environ.get('XML_PATH')
#     pubmed_json_filepath = base_path + 'pubmed_json/' + pmid + '.json'
#     sanitized_reference_json_path = base_path + 'sanitized_reference_json/'
#     if not path.exists(sanitized_reference_json_path):
#         makedirs(sanitized_reference_json_path)
#
#     pmid_fields = ['authors', 'volume', 'title', 'pages', 'issueName', 'issueDate', 'datePublished', 'dateArrivedInPubmed', 'dateLastModified', 'abstract', 'pubMedType', 'publisher', 'meshTerms', 'plainLanguageAbstract', 'pubmedAbstractLanguages', 'crossReferences']
#     single_value_fields = ['volume', 'title', 'pages', 'issueName', 'issueDate', 'datePublished', 'dateArrivedInPubmed', 'dateLastModified', 'abstract', 'publisher', 'plainLanguageAbstract', 'pubmedAbstractLanguages']
#     replace_value_fields = ['authors', 'pubMedType', 'meshTerms', 'crossReferences']
#     date_fields = ['issueDate', 'dateArrivedInPubmed', 'dateLastModified']
#
#     pubmed_data = dict()
#     try:
#         with open(pubmed_json_filepath, 'r') as f:
#             pubmed_data = json.load(f)
#             f.close()
#         entry = dict()
#         entry['primaryId'] = 'PMID:' + pmid
#         if 'nlm' in pubmed_data:
#             entry['resource'] = 'NLM:' + pubmed_data['nlm']
#         entry['category'] = 'unknown'
#         for pmid_field in pmid_fields:
#             if pmid_field in single_value_fields:
#                 pmid_data = ''
#                 if pmid_field in pubmed_data:
#                     if pmid_field in date_fields:
#                         pmid_data = pubmed_data[pmid_field]['date_string']
#                     else:
#                         pmid_data = pubmed_data[pmid_field]
#                 if pmid_data != '':
#                     entry[pmid_field] = pmid_data
#             elif pmid_field in replace_value_fields:
#                 if pmid_field in pubmed_data:
#                     entry[pmid_field] = pubmed_data[pmid_field]
#         sanitized_data = []
#         sanitized_data.append(entry)
#         json_filename = sanitized_reference_json_path + 'REFERENCE_PUBMED_' + pmid + '.json'
#         write_json(json_filename, sanitized_data)
#     except IOError:
#         print(pubmed_json_filepath + ' not found in filesystem')


def output_message_json(process_results):
    """

    :param process_results:
    :return:
    """

    process_result = {}
    if process_results:
        process_result = process_results.pop()
        process_result["status_code"] = int(process_result["status_code"])
        if process_result["text"].startswith('"') and process_result["text"].endswith("'"):
            process_result["text"] = process_result["text"][1:-1]
    else:
        process_result["text"] = "Failure processing POST to API"
        process_result["status_code"] = 999
    process_message_json = json.dumps(process_result)

    print(process_message_json)


def process_pmid(pmid):
    """

    :param pmid:
    :return:
    """

    process_results = check_pmid_cross_reference(pmid)
    if not process_results[0]["found"]:
        base_path = environ.get("XML_PATH")
        pmids_wanted = [pmid]
        download_pubmed_xml(pmids_wanted)
        generate_json(pmids_wanted, [])
        sanitize_pubmed_json_list(pmids_wanted)
        # json_filepath = base_path + 'sanitized_reference_json/REFERENCE_PUBMED_' + pmid + '.json'
        json_filepath = (
            base_path + "sanitized_reference_json/REFERENCE_PUBMED_PMID.json"
        )
        process_results = post_references(json_filepath, "no_file_check")
    output_message_json(process_results)
    # print('finished')


if __name__ == "__main__":
    """
    call main start function
    """

    pmids_wanted = []

    #    python process_single_pmid.py -c 1234 4576 1828
    if args["commandline"]:
        logger.info("Processing commandline input")
        for pmid in args["commandline"]:
            pmids_wanted.append(pmid)
    else:
        logger.info("Must enter a PMID through command line")
    for pmid in pmids_wanted:
        process_pmid(pmid)
