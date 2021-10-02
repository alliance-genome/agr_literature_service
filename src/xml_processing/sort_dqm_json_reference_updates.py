import json
# import urllib.request

import argparse
# import re

from os import environ, path, makedirs, listdir
import logging
import logging.config

from dotenv import load_dotenv

# import bs4
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

load_dotenv()

# pipenv run python sort_dqm_json_reference_updates.py -f dqm_data -m WB

# pipenv run python sort_dqm_json_reference_updates.py -f dqm_data -m all > asdf_sanitized


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file', action='store', help='take input from REFERENCE files in full path')
parser.add_argument('-m', '--mod', action='store', help='which mod, use all or leave blank for all')
# parser.add_argument('-p', '--generate-pmid-data', action='store_true', help='generate pmid outputs')
# parser.add_argument('-c', '--commandline', nargs='*', action='store', help='placeholder for process_single_pmid.py')

args = vars(parser.parse_args())


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


def load_ref_xref():
    # 7 seconds to populate file with 2476879 rows
    ref_xref = dict()
    xref_ref = dict()
    base_path = environ.get('XML_PATH')
#     reference_primary_id_to_curie_file = base_path + 'reference_curie_to_xref_sample'
    reference_primary_id_to_curie_file = base_path + 'reference_curie_to_xref'
    if path.isfile(reference_primary_id_to_curie_file):
        with open(reference_primary_id_to_curie_file, 'r') as read_fh:
            for line in read_fh:
                line_data = line.rstrip().split("\t")
                agr = line_data[0]
                xref = line_data[1]
                prefix, identifier, separator = split_identifier(xref)

                if agr not in ref_xref:
                    ref_xref[agr] = dict()
                    if prefix not in ref_xref[agr]:
                        ref_xref[agr][prefix] = []
                    if identifier not in ref_xref[agr][prefix]:
                        ref_xref[agr][prefix].append(identifier)

                if prefix not in xref_ref:
                    xref_ref[prefix] = dict()
                if identifier not in xref_ref[prefix]:
                    xref_ref[prefix][identifier] = agr
            read_fh.close
    return ref_xref, xref_ref


def load_pmids_not_found():
    pmids_not_found = set()
    base_path = environ.get('XML_PATH')
    pmids_not_found_file = base_path + 'pmids_not_found'
    if path.isfile(pmids_not_found_file):
        with open(pmids_not_found_file, 'r') as read_fh:
            for line in read_fh:
                pmids_not_found.add(line.rstrip())
    return pmids_not_found


def sort_dqm_references(input_path, input_mod):
    # base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'
    base_path = environ.get('XML_PATH')

    mods = ['RGD', 'MGI', 'SGD', 'FB', 'ZFIN', 'WB']
    if input_mod in mods:
        mods = [input_mod]

    ref_xref, xref_ref = load_ref_xref()
    pmids_not_found = load_pmids_not_found()

#     for prefix in xref_ref:
#         for identifier in xref_ref[prefix]:
#             agr = xref_ref[prefix][identifier]
#             logger.info("agr %s prefix %s ident %s", agr, prefix, identifier)
#
#     for agr in ref_xref:
#         for prefix in ref_xref[agr]:
#             for identifier in ref_xref[agr][prefix]:
#                 logger.info("agr %s prefix %s ident %s", agr, prefix, identifier)

    input_file = 'sanitized'
#     input_file = 'dqm'
    files_to_process = []
    if input_file == 'sanitized':
        json_storage_path = base_path + 'sanitized_reference_json/'
        dir_list = listdir(json_storage_path)
        for filename in dir_list:
            # logger.info("%s", filename)
            if 'REFERENCE_' in filename and '.REFERENCE_' not in filename:
                # logger.info("%s", filename)
                files_to_process.append(json_storage_path + filename)
    else:
        for mod in mods:
            filename = input_path + '/REFERENCE_' + mod + '.json'
            files_to_process.append(filename)

    dqm = dict()
    dqm['mod'] = dict()
    dqm['pmid'] = dict()
    dqm['pmid']['PMID'] = set()
    # 2 seconds to read full WB file
    for mod in mods:
        dqm['mod'][mod] = set()
        dqm['pmid'][mod] = set()

#         filename = input_path + '/REFERENCE_' + mod + '.json'

    for filename in sorted(files_to_process):
        logger.info(filename)
        dqm_data = dict()
        with open(filename, 'r') as f:
            dqm_data = json.load(f)
            f.close()
        entries = dqm_data
        if input_file == 'dqm':
            entries = dqm_data['data']
        hash = []
        counter = 0
#         max_counter = 100
        max_counter = 100000000
        for entry in entries:
            counter = counter + 1
            if counter > max_counter:
                break
#             mod_xrefs = []
#             pmid_xrefs = []
            dqm_xrefs = dict()
            xrefs = []
            if 'crossReferences' in entry:
                for cross_reference in entry['crossReferences']:
                    if "id" in cross_reference:
                        xrefs.append(cross_reference["id"])
            if entry['primaryId'] not in xrefs:
                xrefs.append(entry['primaryId'])
            for cross_reference in xrefs:
                prefix, identifier, separator = split_identifier(cross_reference)
                # when looping through specific mods
                # if prefix == mod:
                if prefix in mods:
                    if prefix not in dqm_xrefs:
                        dqm_xrefs[prefix] = []
                    dqm['mod'][prefix].add(identifier)
                    dqm_xrefs[prefix].append(identifier)
                if prefix == 'PMID':
                    if 'PMID' not in dqm_xrefs:
                        dqm_xrefs['PMID'] = []
                    dqm['pmid'][prefix].add(identifier)
                    dqm_xrefs['PMID'].append(identifier)
#             print(xrefs)
#             print(mod_xrefs)
#             print(pmid_xrefs)

#             agr = xref_ref[prefix][identifier]
#             for mod_xref in mod_xrefs:
#                 for mod in mods:
            for mod in dqm_xrefs:
                for ident in dqm_xrefs[mod]:
                    if mod in xref_ref:
                        mod_xref_found = False
                        if ident in xref_ref[mod]:
#                             agr = xref_ref[mod][ident]
#                             logger.info("Mod submitted Yes Found in DB : agr %s prefix %s ident %s", agr, mod, ident)
                            mod_xref_found = True
                        if not mod_xref_found:
                            accounted = False
                            if prefix == 'PMID':
                                if ident in pmids_not_found:
                                    accounted = True
                                    logger.info("Mod submitted PMID Not Found before at PubMed : prefix %s ident %s", mod, ident)
                            if not accounted:
                                logger.info("Mod submitted Not Found in DB : prefix %s ident %s", mod, ident)

#                 # when looping through specific mods
#             for mod_xref in mod_xrefs:
#                 mod_xref_found = False
#                 if mod in xref_ref:
#                     if mod_xref in xref_ref[mod]:
#                         agr = xref_ref[mod][mod_xref]
# #                         logger.info("Mod Id Yes Found : agr %s prefix %s ident %s", agr, mod, mod_xref)
#                         mod_xref_found = True
#                 if not mod_xref_found:
#                     logger.info("Mod Id Submitted Not Found in DB : prefix %s ident %s", mod, mod_xref)

#             for pmid_xref in pmid_xrefs:
#                 pmid_xref_found = False
#                 if 'PMID' in xref_ref:
#                     if pmid_xref in xref_ref['PMID']:
#                         agr = xref_ref['PMID'][pmid_xref]
# #                         logger.info("Mod Id Yes Found : agr %s prefix %s ident %s", agr, mod, pmid_xref)
#                         pmid_xref_found = True
#                 if not pmid_xref_found:
#                     logger.info("Mod PMID Submitted Not Found in DB : prefix %s ident %s", mod, pmid_xref)


if __name__ == "__main__":
    """ call main start function """
    logger.info("starting sort_dqm_json_reference_updates.py")

    if args['file']:
        if args['mod']:
            sort_dqm_references(args['file'], args['mod'])
        else:
            sort_dqm_references(args['file'], 'all')

    logger.info("ending sort_dqm_json_reference_updates.py")
