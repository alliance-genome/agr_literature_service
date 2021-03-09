
import json
# import urllib.request

# import argparse
# import re

from os import path
import logging
import logging.config


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')

# pipenv run python parse_dqm_json_resource.py

base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'


def split_identifier(identifier, ignore_error=False):
    """Split Identifier.

    Does not throw exception anymore. Check return, if None returned, there was an error
    """
    prefix = None
    identifier_processed = None
    separator = None

    if ':' in identifier:
        prefix, identifier_processed = identifier.split(':', 1)  # Split on the first occurrence
        separator = ':'
    elif '-' in identifier:
        prefix, identifier_processed = identifier.split('-', 1)  # Split on the first occurrence
        separator = '-'
    else:
        if not ignore_error:
            logger.critical('Identifier does not contain \':\' or \'-\' characters.')
            logger.critical('Splitting identifier is not possible.')
            logger.critical('Identifier: %s', identifier)
        prefix = identifier_processed = separator = None

    return prefix, identifier_processed, separator


def write_json(json_filename, dict_to_output):
    with open(json_filename, "w") as json_file:
        logger.info("Generating JSON for %s", json_filename)
        json_data = json.dumps(dict_to_output, indent=4, sort_keys=True)
#         logger.info("Writing JSON")
        json_file.write(json_data)
#         logger.info("Closing JSON file")
        json_file.close()
#         logger.info("Done with JSON")


# TODO
# read FB resources.  parse references to generate mapping of pubmed NLM to FB resource ID (e.g. FB:FBmultipub_7576) into a file.  then this script uses that file to replace FB resource data with this data.

def load_zfin_resource(nlm_in_pubmed):
    # resource_fields = ['primaryId', 'title', 'isoAbbreviation', 'medlineAbbreviation', 'printISSN', 'onlineISSN']
    # resource_to_mod = dict()
    resource_fields_from_pubmed = ['title', 'isoAbbreviation', 'medlineAbbreviation', 'printISSN', 'onlineISSN']
    filename = base_path + 'dqm_data/RESOURCE_ZFIN.json'
    try:
        with open(filename, 'r') as f:
            dqm_data = json.load(f)
            sanitized_data = []
            for entry in dqm_data['data']:
                primary_id = entry['primaryId']
                if primary_id in nlm_in_pubmed:
                    entry['primaryId'] = 'NLM:' + primary_id
                    for field in resource_fields_from_pubmed:
                        if field in entry:
                            del entry[field]
                    sanitized_data.append(entry)
                else:
                    prefix, identifier, separator = split_identifier(primary_id)
                    if prefix == 'ZFIN':
                        sanitized_data.append(entry)
                    else:
                        logger.info("unexpected DQM ZFIN resource %s : %s", prefix, primary_id)
            dqm_data['data'] = sanitized_data
            json_storage_path = base_path + 'sanitized_resource_json/'
            json_filename = json_storage_path + 'RESOURCE_ZFIN.json'
            write_json(json_filename, dqm_data)
    except IOError:
        pass


def load_pubmed_resource():
    filename = base_path + 'pubmed_resource_json/resource_pubmed_all.json'
    f = open(filename)
    resource_data = json.load(f)
#     resource_to_nlm = dict()
#     resource_nlm_to_title = dict()
#     resource_fields = ['primaryId', 'nlm', 'title', 'isoAbbreviation', 'medlineAbbreviation', 'printISSN', 'onlineISSN']
    nlm_in_pubmed = set()
    for entry in resource_data:
        # primary_id = entry['primaryId']
        nlm = entry['nlm']
        nlm_in_pubmed.add(nlm)
    return nlm_in_pubmed
#         title = entry['title']
#         resource_nlm_to_title[nlm] = title
#         for field in resource_fields:
#             if field in entry:
#                 # value = entry[field].lower()
#                 value = simplify_text(entry[field])
#                 # if value == '2985088r':
#                 #     print("2985088r loaded\n")
#                 if value in resource_to_nlm:
#                     # if value == '2985088r':
#                     #     print("already in 2985088r to %s loaded\n" % (value))
#                     if primary_id not in resource_to_nlm[value]:
#                         resource_to_nlm[value].append(primary_id)
#                         # if value == '2985088r':
#                         #     print("append in 2985088r to %s loaded\n" % (value))
#                 else:
#                     resource_to_nlm[value] = [primary_id]
#                     # if value == '2985088r':
#                     #     print("orig 2985088r to %s loaded\n" % (value))
#     return resource_to_nlm, resource_nlm_to_title


if __name__ == "__main__":
    """ call main start function """
    logger.info("starting parse_dqm_json_resource.py")

    nlm_in_pubmed = load_pubmed_resource()
    load_zfin_resource(nlm_in_pubmed)

# pipenv run python parse_dqm_json_resource.py

    logger.info("ending parse_dqm_json_resource.py")
