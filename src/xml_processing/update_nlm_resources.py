import json
import requests
import argparse

from os import environ, path, makedirs
import logging
import logging.config

from helper_post_to_api import generate_headers, get_authentication_token, process_api_request

from helper_file_processing import load_pubmed_resource_basic, load_ref_xref, save_pubmed_resource

from dotenv import load_dotenv

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

load_dotenv()

# pipenv run python update_nlm_resources.py

# first run  get_datatypes_cross_references.py  to generate mappings from references to xrefs and resources to xrefs
# and  generate_pubmed_nlm_resource.py  to generate pubmed_resource_json/resource_pubmed_all.json

# Attention Paulo: This is still in progress, need to test it against a newly populated database after hearing back about oddly high-numbered NLMs


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')

parser = argparse.ArgumentParser()

args = vars(parser.parse_args())


def update_nlm_resources():
    """

    :return:
    """

    base_path = environ.get('XML_PATH')
    api_port = environ.get('API_PORT')    # noqa: F841

    json_storage_path = base_path + 'sanitized_resource_json_updates/'
    if not path.exists(json_storage_path):
        makedirs(json_storage_path)

    token = get_authentication_token()
    headers = generate_headers(token)

    xref_ref, ref_xref_valid, ref_xref_obsolete = load_ref_xref('resource')
    pubmed_by_nlm = load_pubmed_resource_basic()  # from pubmed_resource_json/resource_pubmed_all.json
    resources_to_update = dict()
    resources_to_create = dict()

    counter = 0
    # make this True for live changes
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
            # logger.info("create nlm %s", nlm)
            resources_to_create[nlm] = pubmed_data

    save_pubmed_resource(json_storage_path, resources_to_create)  # this needs to post_resource_to_api, figure out appending to resource_primary_id_to_curie

    update_resources(live_changes, headers, resources_to_update)


def update_resources(live_changes, headers, resources_to_update):
    """
    This takes 11 minutes to query 34284 resources one by one through the API

    :param live_changes:
    :param headers:
    :param resources_to_update:
    :return:
    """

    pubmed_fields = ['isoAbbreviation', 'crossReferences', 'onlineISSN', 'medlineAbbreviation', 'printISSN', 'title', 'primaryId', 'nlm']
    keys_to_remove = {'nlm', 'primaryId', 'crossReferences'}   # these are all the nlm, which is the key to find this, so it cannot change
    remap_keys = dict()
    remap_keys['isoAbbreviation'] = 'iso_abbreviation'
    remap_keys['medlineAbbreviation'] = 'medline_abbreviation'
    remap_keys['crossReferences'] = 'cross_references'
    remap_keys['printISSN'] = 'print_issn'
    remap_keys['onlineISSN'] = 'online_issn'

    api_port = environ.get('API_PORT')

    # counter = 0
    # max_counter = 10000000
    # max_counter = 1

    for agr in resources_to_update:
        # counter = counter + 1
        # if counter > max_counter:
        #     break

        # to test only on something that gets a new online_issn
        # if agr != 'AGR:AGR-Resource-0000015274':
        #     continue

        pm_entry = resources_to_update[agr]
        # logger.info("pm title %s", pm_entry['title'])   # for debugging which reference was found

        url = 'http://localhost:' + api_port + '/resource/' + agr
        logger.info("get AGR resource info from database %s", url)
        get_return = requests.get(url)
        db_entry = json.loads(get_return.text)
        # logger.info("db title %s", db_entry['title'])   # for debugging which reference was found

        update_json = dict()
        for field_camel in pubmed_fields:
            if field_camel in keys_to_remove:
                continue
            field_snake = field_camel
            if field_camel in remap_keys:
                field_snake = remap_keys[field_camel]
            pm_value = None
            db_value = None
            if field_camel in pm_entry:
                pm_value = pm_entry[field_camel]
            if field_snake in db_entry:
                db_value = db_entry[field_snake]
            if pm_value != db_value:
                logger.info("patch %s field %s from db %s to pm %s", agr, field_snake, db_value, pm_value)
                update_json[field_snake] = pm_value
        if update_json:
            # for debugging changes
            # update_text = json.dumps(update_json, indent=4)
            # print('update ' + update_text)
            if live_changes:
                api_response_tuple = process_api_request('PATCH', url, headers, update_json, agr, None, None)
                headers = api_response_tuple[0]
                response_text = api_response_tuple[1]
                response_status_code = api_response_tuple[2]
                log_info = api_response_tuple[3]
                if log_info:
                    logger.info(log_info)
                if response_status_code == 202:
                    response_dict = json.loads(response_text)
                    response_dict = str(response_dict).replace('"', '')
                    logger.info("%s\t%s", agr, response_dict)


if __name__ == "__main__":
    """
    call main start function
    """

    logger.info("starting update_nlm_resources.py")

    update_nlm_resources()

    logger.info("ending update_nlm_resources.py")
