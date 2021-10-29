import json
import requests
import argparse

from os import environ, path, makedirs
import logging
import logging.config

from helper_post_to_api import generate_headers, get_authentication_token, process_api_request

from helper_file_processing import load_pubmed_resource_basic, load_ref_xref, save_resource_file, split_identifier

from dotenv import load_dotenv

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

load_dotenv()

# pipenv run python update_nlm_resources.py

# first run  get_datatypes_cross_references.py  to generate mappings from references to xrefs and resources to xrefs
# and  generate_pubmed_nlm_resource.py  to generate pubmed_resource_json/resource_pubmed_all.json

# Attention Paulo: This is still in progress, need to test it against a newly populated database after hearing back about oddly high-numbered NLMs

# rename this to sort_dqm_json_resource_updates
# work off of sanitized_resource_json  mod + NLM files
# should it also update NLM resources ?  yes, 13.5 minutes is not long
# test time to get all resources 0000042513 - 13.5 minutes.
# keep working off of lit-4003, comparing data from 20211025 files (loaded at lit-4005)


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')

parser = argparse.ArgumentParser()

args = vars(parser.parse_args())


def load_sanitized_resource(datatype):
    """

    :return:
    """

    base_path = environ.get('XML_PATH')
    filename = base_path + 'sanitized_resource_json/RESOURCE_' + datatype + '.json'
    sanitized_resources = dict()
    try:
        with open(filename, 'r') as f:
            whole_dict = json.load(f)
            if 'data' in whole_dict:
                sanitized_resources = whole_dict['data']
    except IOError:
        pass
    return sanitized_resources


def update_sanitized_resources(datatype):
    """
    Replace this with checking against sanitized_resource_json/ mod + NLM data

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
#     pubmed_by_nlm = load_pubmed_resource_basic()  # from pubmed_resource_json/resource_pubmed_all.json
    sanitized_resources = load_sanitized_resource(datatype)
    resources_to_update = dict()
    resources_to_create = dict()

# e.g. create ZFIN:ZDB-JRNL-210824-1
    counter = 0
    # make this True for live changes
    live_changes = False
    for resource_dict in sanitized_resources:
        counter = counter + 1
        # if counter > 2:
        #     break
        found = False
        primary_id = resource_dict['primaryId']
        prefix, identifier, separator = split_identifier(primary_id)
        # logger.info("primary_id %s pubmed %s", primary_id, resource_dict)
        if prefix in xref_ref:
            if identifier in xref_ref[prefix]:
                agr = xref_ref[prefix][identifier]
                if agr in resources_to_update:
                    logger.info("ERROR agr %s has multiple values to update", agr)
                resources_to_update[agr] = resource_dict
                # logger.info("update primary_id %s db %s", primary_id, agr)
                found = True
        if not found:
            # logger.info("create primary_id %s", primary_id)
            resources_to_create[primary_id] = resource_dict

    save_resource_file(json_storage_path, resources_to_create, datatype)  # this needs to post_resource_to_api, figure out appending to resource_primary_id_to_curie

    update_resources(live_changes, headers, resources_to_update)


def update_resources(live_changes, headers, resources_to_update):
    """
    This takes 11 minutes to query 34284 resources one by one through the API
    This takes 17 seconds to query   978 zfin resources one by one through the API

    :param live_changes:
    :param headers:
    :param resources_to_update:
    :return:
    """

    # pubmed_fields = ['isoAbbreviation', 'crossReferences', 'onlineISSN', 'medlineAbbreviation', 'printISSN', 'title', 'primaryId', 'nlm']
    # keys_to_remove = {'nlm', 'primaryId', 'crossReferences'}   # these are all the nlm, which is the key to find this, so it cannot change
    remap_keys = dict()
    remap_keys['isoAbbreviation'] = 'iso_abbreviation'
    remap_keys['medlineAbbreviation'] = 'medline_abbreviation'
    remap_keys['printISSN'] = 'print_issn'
    remap_keys['onlineISSN'] = 'online_issn'
    remap_keys['abbreviationSynonyms'] = 'abbreviation_synonyms'
    remap_keys['crossReferences'] = 'cross_references'
    remap_keys['editorsOrAuthors'] = 'editors'

    # to account for editors and xrefs later
    # editor_keys_to_remove = {'referenceId'}
    # remap_editor_keys = dict()
    # remap_editor_keys['authorRank'] = 'order'
    # remap_editor_keys['firstName'] = 'first_name'
    # remap_editor_keys['lastName'] = 'last_name'
    # remap_editor_keys['middleNames'] = 'middle_names'
    # cross_references_keys_to_remove = dict()
    # remap_cross_references_keys = dict()
    # remap_cross_references_keys['id'] = 'curie'


    # no one is sending abstractOrSummary / 'abstract', 'summary' ; titleSynonyms ; copyrightDate data
    simple_fields = ['title', 'isoAbbreviation', 'medlineAbbreviation', 'printISSN', 'onlineISSN', 'publisher', 'pages']
    list_fields = ['abbreviationSynonyms', 'volumes']
    complex_fields = ['crossReferences', 'editorsOrAuthors']

    api_port = environ.get('API_PORT')

    counter = 0
    max_counter = 10000000
#     max_counter = 1

    for agr in resources_to_update:
        counter = counter + 1
        if counter > max_counter:
            break

        # to test only on something that gets a new online_issn
        # if agr != 'AGR:AGR-Resource-0000015274':
        #     continue

        pm_entry = resources_to_update[agr]
        # logger.info("pm title %s", pm_entry['title'])   # for debugging which reference was found
        # logger.info("%s", pm_entry)
        # live_changes = True

        url = 'http://localhost:' + api_port + '/resource/' + agr
        logger.info("get AGR resource info from database %s", url)
        get_return = requests.get(url)
        db_entry = json.loads(get_return.text)
        # logger.info("db title %s", db_entry['title'])   # for debugging which reference was found

        update_json = dict()
        for field_camel in simple_fields:
            # if field_camel in keys_to_remove:
            #     continue
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
            update_text = json.dumps(update_json, indent=4)
            print('update ' + update_text)
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


def test_get_from_list():
    """
    To test making a GET on :4005 to get multiple references at once vs one-by-one.  It's just as slow, but leaving it in to test future different methods for getting data from database
    20 seconds for 1000 resources
    13.5 minutes for all 42513 resources

    :return:
    """

    print('json_data')
    method = 'GET'
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    json_data = []
    # for i in range(1, 1001):
    for i in range(1, 42514):
        agr_id = 'AGR:AGR-Resource-' + str(i).zfill(10)
        url = 'http://dev.alliancegenome.org:4005/resource/' + agr_id
        print(url)
        request_return = requests.request(method, url=url, headers=headers, json=json_data)
        process_text = str(request_return.text)
        print(process_text)
    # print(json_data)


if __name__ == "__main__":
    """
    call main start function
    """

    logger.info("starting sort_dqm_json_resource_updates.py")

    # test_get_from_list()
    update_sanitized_resources('ZFIN')
#     update_sanitized_resources('FB')
#     update_sanitized_resources('NLM')

    logger.info("ending sort_dqm_json_resource_updates.py")
