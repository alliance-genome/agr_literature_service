
import json
from os import environ, path, makedirs
import logging
import logging.config
import re

from helper_file_processing import load_pubmed_resource_basic, write_json, save_pubmed_resource
# from helper_file_processing import split_identifier

from dotenv import load_dotenv

load_dotenv()

log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')

# pipenv run python parse_dqm_json_resource.py

# 50 seconds to run, mostly from generate_resource_abbreviation_to_nlm_from_dqm_references for FB data,
# because their resource file has resourceAbbreviation that can be matched from their reference, and
# their reference can be matched to an NLM from PubMed XML, so both can be combined to find that a FB
# resource is a resource that comes from J_Medline, and avoid creating a duplicate resource.  This
# requires reading all the FB references and their matching PubMed XML, which takes 49 seconds.


# base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'
# base_path = environ.get('XML_PATH')
# json_storage_path = base_path + 'sanitized_resource_json/'

# resource_fields = ['primaryId', 'nlm', 'title', 'isoAbbreviation', 'medlineAbbreviation', 'printISSN', 'onlineISSN']
# resource_fields_from_pubmed = ['title', 'isoAbbreviation', 'medlineAbbreviation', 'printISSN', 'onlineISSN']
# resource_fields_not_in_pubmed = ['titleSynonyms', 'abbreviationSynonyms', 'copyrightDate',
#                                  'publisher', 'editorsOrAuthors', 'volumes', 'pages', 'abstractOrSummary']

# TODO  when done developing and running scripts posting to API, switch back from DEV folder and file to live one


def create_storage_path(json_storage_path):
    """

    :return:
    """

    if not path.exists(json_storage_path):
        makedirs(json_storage_path)


# def split_identifier(identifier, ignore_error=False):
#     """
#
#     Split Identifier.
#
#     Does not throw exception anymore. Check return, if None returned, there was an error
#
#     :param identifier:
#     :param ignore_error:
#     :return:
#     """
#
#     prefix = None
#     identifier_processed = None
#     separator = None
#
#     if ':' in identifier:
#         prefix, identifier_processed = identifier.split(':', 1)  # Split on the first occurrence
#         separator = ':'
#     elif '-' in identifier:
#         prefix, identifier_processed = identifier.split('-', 1)  # Split on the first occurrence
#         separator = '-'
#     else:
#         if not ignore_error:
#             logger.critical('Identifier does not contain \':\' or \'-\' characters.')
#             logger.critical('Splitting identifier is not possible.')
#             logger.critical('Identifier: %s', identifier)
#         prefix = identifier_processed = separator = None
#
#     return prefix, identifier_processed, separator
#
#
# def write_json(json_filename, dict_to_output):
#     """
#
#     :param json_filename:
#     :param dict_to_output:
#     :return:
#     """
#
#     with open(json_filename, "w") as json_file:
#         logger.info("Generating JSON for %s", json_filename)
#         json_data = json.dumps(dict_to_output, indent=4, sort_keys=True)
#         json_file.write(json_data)
#         json_file.close()
#
#
# def save_pubmed_resource(pubmed_by_nlm):
#     """
#
#     :param pubmed_by_nlm:
#     :return:
#     """
#
#     pubmed_data = dict()
#     pubmed_data['data'] = []
#     for nlm in pubmed_by_nlm:
#         pubmed_data['data'].append(pubmed_by_nlm[nlm])
#     json_filename = json_storage_path + 'RESOURCE_NLM.json'
#     write_json(json_filename, pubmed_data)
#
#
# def load_zfin_resource(json_storage_path, pubmed_by_nlm):
#     """
#
#     :param pubmed_by_nlm:
#     :return:
#     """
#
#     base_path = environ.get('XML_PATH')
#     filename = base_path + 'dqm_data/RESOURCE_ZFIN.json'
#     try:
#         with open(filename, 'r') as f:
#             dqm_data = json.load(f)
#             sanitized_data = []
#             for entry in dqm_data['data']:
#                 primary_id = entry['primaryId']
#                 if primary_id in pubmed_by_nlm:
#                     if 'crossReferences' in entry:
#                         for cross_ref in entry['crossReferences']:
#                             pubmed_by_nlm[primary_id]['crossReferences'].append(cross_ref)
#                 else:
#                     prefix, identifier, separator = split_identifier(primary_id)
#                     if prefix == 'ZFIN':
#                         sanitized_data.append(entry)
#                     else:
#                         logger.info("unexpected DQM ZFIN resource %s : %s", prefix, primary_id)
#             dqm_data['data'] = sanitized_data
#             json_filename = json_storage_path + 'RESOURCE_ZFIN.json'
#             write_json(json_filename, dqm_data)
#     except IOError:
#         pass
#     return pubmed_by_nlm
#
#
# def load_fb_resource(json_storage_path, pubmed_by_nlm):
#     """
#     load_fb_resource and load_zfin_resource can be combined into single function, treating FB a bit different for the fb_to_nlm
#     if primary_id in pubmed_by_nlm:    means primary_id is the nlm
#     will work on this later
#
#     :param pubmed_by_nlm:
#     :return:
#     """
#
#     resource_fields_not_in_pubmed = ['titleSynonyms', 'abbreviationSynonyms', 'copyrightDate',
#                                      'publisher', 'editorsOrAuthors', 'volumes', 'pages', 'abstractOrSummary']
#
#     base_path = environ.get('XML_PATH')
#     filename = base_path + 'dqm_data/RESOURCE_FB.json'
#     fb_to_nlm = load_fb_resource_to_nlm()
#     try:
#         with open(filename, 'r') as f:
#             dqm_data = json.load(f)
#             sanitized_data = []
#             for entry in dqm_data['data']:
#                 nlm = ''
#                 if 'abbreviationSynonyms' in entry:
#                     for abbreviation in entry['abbreviationSynonyms']:
#                         if abbreviation in fb_to_nlm:
#                             nlm = fb_to_nlm[abbreviation]
#                 if nlm != '':
#                     if nlm in pubmed_by_nlm:
#                         if 'crossReferences' in entry:
#                             for cross_ref in entry['crossReferences']:
#                                 pubmed_by_nlm[nlm]['crossReferences'].append(cross_ref)
#                         if 'primaryId' in entry:
#                             cross_ref = dict()
#                             cross_ref['id'] = entry['primaryId']
#                             pubmed_by_nlm[nlm]['crossReferences'].append(cross_ref)
#                         # this causes conflicts if different MODs match an NLM and they send different non-pubmed information
#                         # whichever mod runs last will have the final value
#                         for field in resource_fields_not_in_pubmed:
#                             if field in entry:
#                                 pubmed_by_nlm[nlm][field] = entry[field]
#                 else:
#                     if 'primaryId' in entry:
#                         cross_ref = dict()
#                         cross_ref['id'] = entry['primaryId']
#                         if 'crossReferences' in entry:
#                             entry['crossReferences'].append(cross_ref)
#                         else:
#                             entry['crossReferences'] = [cross_ref]
#                     sanitized_data.append(entry)
#             dqm_data['data'] = sanitized_data
#             json_filename = json_storage_path + 'RESOURCE_FB.json'
#             write_json(json_filename, dqm_data)
#     except IOError:
#         pass
#     return pubmed_by_nlm


def load_mod_resource_to_nlm(mod):
    """

    :return:
    """

    base_path = environ.get('XML_PATH')
    filename = base_path + mod + '_resourceAbbreviation_to_NLM.json'
    mod_to_nlm = dict()
    try:
        with open(filename, 'r') as f:
            mod_to_nlm = json.load(f)
    except IOError:
        pass
    return mod_to_nlm


def generate_resource_abbreviation_to_nlm_from_dqm_references(input_path, mod):      # noqa: C901
    # fb have fb ids for resources, but from the resourceAbbreviation and pubmed xml's nlm, we can update
    # fb resource data to primary key off of nlm
    mod_resource_abbreviation_to_nlm = dict()
    filename = input_path + 'REFERENCE_' + mod + '.json'
    logger.info("Processing %s", filename)
    dqm_data = dict()
    with open(filename, 'r') as f:
        dqm_data = json.load(f)
        f.close()
    entries = dqm_data['data']
    for entry in entries:
        is_pubmod = True
        pmid = None
        primary_id = entry['primaryId']
        orig_primary_id = entry['primaryId']
#         print("primaryId %s" % (entry['primaryId']))

        pmid_group = re.search(r"^PMID:([0-9]+)", primary_id)
        if pmid_group is not None:
            pmid = pmid_group[1]
            # print(pmid)
            filename = base_path + 'pubmed_json/' + pmid + '.json'
            # print("primary_id %s reading %s" % (primary_id, filename))
            pubmed_data = dict()
            try:
                with open(filename, 'r') as f:
                    pubmed_data = json.load(f)
                    f.close()
                    is_pubmod = False
            except IOError:
                # fh_mod_report[mod].write("Warning: PMID %s does not have PubMed xml, from Mod %s primary_id %s\n" % (pmid, mod, orig_primary_id))
                logger.info("Warning: PMID %s does not have PubMed xml, from Mod %s primary_id %s", pmid, mod, orig_primary_id)

        if not is_pubmod:
            if 'nlm' in pubmed_data:
                nlm = pubmed_data['nlm']
                if 'resourceAbbreviation' in entry:
                    mod_resource_abbreviation_to_nlm[entry['resourceAbbreviation']] = nlm

    # fb have fb ids for resources, but from the resourceAbbreviation and pubmed xml's nlm, we can update fb resource data to primary key off of nlm
    # parse_dqm_json_resource takes one second, but generating this takes 49 seconds for FB data, 56s for SGD, 150s for MGI, 15s WB
    # so save to a file for ease of altering this script in the future, being able to load the file with load_mod_resource_to_nlm(mod)
    json_filename = base_path + 'DEV_' + mod + '_resourceAbbreviation_to_NLM.json'
    write_json(json_filename, mod_resource_abbreviation_to_nlm)

    return mod_resource_abbreviation_to_nlm


def load_mod_resource(json_storage_path, pubmed_by_nlm, mod):      # noqa: C901
    """

    :param pubmed_by_nlm:
    :return:
    """

    resource_fields_not_in_pubmed = ['titleSynonyms', 'abbreviationSynonyms', 'copyrightDate',
                                     'publisher', 'editorsOrAuthors', 'volumes', 'pages', 'abstractOrSummary']
    base_path = environ.get('XML_PATH')

    mod_to_nlm = dict()
    if mod == 'FB':   # only FB sending abbreviationSynonyms in dqm resource file that can be mapped to resource via NLM
        # mod_to_nlm = load_mod_resource_to_nlm(mod)   # for modifying this script without re-generating mod_to_nlm from input_path
        input_path = base_path + 'dqm_data/'
        mod_to_nlm = generate_resource_abbreviation_to_nlm_from_dqm_references(input_path, mod)

    filename = base_path + 'dqm_data/RESOURCE_' + mod + '.json'
    try:
        with open(filename, 'r') as f:
            dqm_data = json.load(f)
            sanitized_data = []
            for entry in dqm_data['data']:
                nlm = ''
                if 'primaryId' in entry:
                    primary_id = entry['primaryId']
                if primary_id in pubmed_by_nlm:
                    nlm = primary_id
                elif 'abbreviationSynonyms' in entry:
                    for abbreviation in entry['abbreviationSynonyms']:
                        if abbreviation in mod_to_nlm:
                            nlm = mod_to_nlm[abbreviation]
                if nlm != '':
                    if nlm in pubmed_by_nlm:
                        nlm_cross_refs = set()
                        for cross_ref in pubmed_by_nlm[nlm]['crossReferences']:
                            nlm_cross_refs.add(cross_ref['id'])
                        if 'crossReferences' in entry:
                            for cross_ref in entry['crossReferences']:
                                if cross_ref['id'] not in nlm_cross_refs:
                                    nlm_cross_refs.add(cross_ref['id'])
                                    pubmed_by_nlm[nlm]['crossReferences'].append(cross_ref)
                        if 'primaryId' in entry:
                            if entry['primaryId'] not in nlm_cross_refs:
                                nlm_cross_refs.add(entry['primaryId'])
                                cross_ref = dict()
                                cross_ref['id'] = entry['primaryId']
                                pubmed_by_nlm[nlm]['crossReferences'].append(cross_ref)
                        # this causes conflicts if different MODs match an NLM and they send different non-pubmed information
                        # whichever mod runs last will have the final value
                        for field in resource_fields_not_in_pubmed:
                            if field in entry:
                                pubmed_by_nlm[nlm][field] = entry[field]
                else:
                    if 'primaryId' in entry:
                        entry_cross_refs = set()
                        if 'crossReferences' in entry:
                            for cross_ref in entry['crossReferences']:
                                entry_cross_refs.add(cross_ref['id'])
                        if entry['primaryId'] not in entry_cross_refs:
                            entry_cross_refs.add(entry['primaryId'])
                            cross_ref = dict()
                            cross_ref['id'] = entry['primaryId']
                            if 'crossReferences' in entry:
                                entry['crossReferences'].append(cross_ref)
                            else:
                                entry['crossReferences'] = [cross_ref]
                    sanitized_data.append(entry)

            dqm_data['data'] = sanitized_data
            json_filename = json_storage_path + 'RESOURCE_' + mod + '.json'
            write_json(json_filename, dqm_data)
    except IOError:
        pass
    return pubmed_by_nlm


if __name__ == "__main__":
    """
    call main start function
    """

    logger.info("Starting parse_dqm_json_resource.py")

    base_path = environ.get('XML_PATH')
    json_storage_path = base_path + 'DEV_sanitized_resource_json/'
    create_storage_path(json_storage_path)

    mods = ['RGD', 'MGI', 'SGD', 'FB', 'ZFIN', 'WB']

    pubmed_by_nlm = load_pubmed_resource_basic()
    for mod in mods:
        pubmed_by_nlm = load_mod_resource(json_storage_path, pubmed_by_nlm, mod)
    save_pubmed_resource(json_storage_path, pubmed_by_nlm)

    logger.info("ending parse_dqm_json_resource.py")

# pipenv run python parse_dqm_json_resource.py
