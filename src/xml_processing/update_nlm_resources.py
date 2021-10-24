import json
# import urllib.request
import requests

import argparse
# import re

from os import environ, path, makedirs
import logging
import logging.config

# from helper_post_to_api import generate_headers, get_authentication_token, process_api_request
from helper_post_to_api import generate_headers, get_authentication_token

from helper_file_processing import load_pubmed_resource_basic, load_ref_xref, save_pubmed_resource

from dotenv import load_dotenv

# import bs4
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

load_dotenv()

# pipenv run python update_nlm_resources.py

# first run  get_datatypes_cross_references.py  to generate mappings from references to xrefs and resources to xrefs
# and  generate_pubmed_nlm_resource.py  to generate pubmed_resource_json/resource_pubmed_all.json

# Attention Paulo: I'm actively making changes to this script, testing it, and cleaning it up

# Workflow for DQM updates
# 1 - run get_datatypes_cross_references.py  to generate mappings from references to xrefs and resources to xrefs
# 2 - Get pubmed nlm resources with generate_pubmed_nlm_resource.py
# 3 - TODO new script - compare pubmed resources with database resources-xref, update existing, create new ones
# 4 - TODO new script - compare MOD (FB/ZFIN) resources with database, update existing, create new ones, update FB_resourceAbbreviation_to_NLM
# 5 - generate new mappings from resources to xrefs (get_datatypes_cross_references.py)
# 6 - run this script to update reference cross references, report to curators, update mod-specific references - TODO update reference-resource connections, generate dqm files for creating new references
# 7 - create new references off of dqm references that are completely new through the get_pubmed_xml -> xml_to_json -> parse_dqm_json_reference pipeline (TODO check how it interacts with updates to FB_resourceAbbreviation_to_NLM)


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file', action='store', help='take input from REFERENCE files in full path')
parser.add_argument('-m', '--mod', action='store', help='which mod, use all or leave blank for all')
# parser.add_argument('-p', '--generate-pmid-data', action='store_true', help='generate pmid outputs')
# parser.add_argument('-c', '--commandline', nargs='*', action='store', help='placeholder for process_single_pmid.py')

args = vars(parser.parse_args())


def update_nlm_resources():
    base_path = environ.get('XML_PATH')
    api_port = environ.get('API_PORT')    # noqa: F841

    json_storage_path = base_path + 'sanitized_resource_json_updates/'
    if not path.exists(json_storage_path):
        makedirs(json_storage_path)

    token = get_authentication_token()
    headers = generate_headers(token)

    xref_ref, ref_xref_valid, ref_xref_obsolete = load_ref_xref('resource')
    pubmed_by_nlm = load_pubmed_resource_basic()
    resources_to_update = dict()
    resources_to_create = dict()

    counter = 0
    live_changes = False
    for nlm in pubmed_by_nlm:
        counter = counter + 1
        # if counter > 2:
        #     break
        found = False
        pubmed_data = pubmed_by_nlm[nlm]
        # logger.info("nlm %s pubmed %s", nlm, pubmed_data)
        prefix = 'NLM'
        if prefix in xref_ref:
            if nlm in xref_ref[prefix]:
                agr = xref_ref[prefix][nlm]
                if agr in resources_to_update:
                    logger.info("ERROR agr %s has multiple values to update", agr)
                resources_to_update[agr] = pubmed_data
                # logger.info("update nlm %s db %s", nlm, agr)
                found = True
        if not found:
            counter = counter + 1
            resources_to_create[nlm] = pubmed_data
            logger.info("create nlm %s", nlm)

    save_pubmed_resource(json_storage_path, resources_to_create)  # this needs to post_resource_to_api, figure out appending to resource_primary_id_to_curie

    update_resources(live_changes, headers, resources_to_update)


def update_resources(live_changes, headers, resources_to_update):
    api_port = environ.get('API_PORT')

    counter = 0
    # max_counter = 10000000
    max_counter = 1

    for agr in resources_to_update:
        counter = counter + 1
        if counter > max_counter:
            break

        pm_entry = resources_to_update[agr]
        print(pm_entry)

        url = 'http://localhost:' + api_port + '/resource/' + agr
        logger.info("get AGR resource info from database %s", url)
        get_return = requests.get(url)
        db_entry = json.loads(get_return.text)
        logger.info("title %s", db_entry['title'])   # for debugging which reference was found


if __name__ == "__main__":
    """
    call main start function
    """

    logger.info("starting update_nlm_resources.py")

    update_nlm_resources()

    logger.info("ending update_nlm_resources.py")
