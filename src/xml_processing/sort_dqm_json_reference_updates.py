import argparse
import json
import logging
import logging.config
# import bs4
import warnings
from os import environ, listdir, makedirs, path

# import urllib.request
import requests
from dotenv import load_dotenv

from helper_file_processing import (clean_up_keywords,
                                    compare_authors_or_editors, load_ref_xref,
                                    split_identifier, write_json)
from helper_post_to_api import (generate_headers, get_authentication_token,
                                process_api_request)

# import re

warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

load_dotenv()
api_server = environ.get('API_SERVER', 'localhost')
# pipenv run python sort_dqm_json_reference_updates.py -f dqm_data -m WB

# pipenv run python sort_dqm_json_reference_updates.py -f dqm_data -m all > asdf_sanitized

# first run  get_datatypes_cross_references.py  to generate mappings from references to xrefs and resources to xrefs

# Attention Paulo: I'm actively making changes to this script, testing it, and cleaning it up

# Workflow for DQM updates
# 1 - get_datatypes_cross_references.py - to generate mappings from references to xrefs and resources to xrefs
# 2 - generate_pubmed_nlm_resource.py - get pubmed nlm resources
# 3 - sort_dqm_json_resource_updates.py - compare pubmed and MOD resources with database resources-xref, update existing, create new ones
# 4 - get_datatypes_cross_references.py - generate new mappings from resources to xrefs
# 5 - run this script to update reference cross references, report to curators, update mod-specific references - TODO update reference-resource connections, generate dqm files for creating new references
# 6 - create new references off of dqm references that are completely new through the get_pubmed_xml -> xml_to_json -> parse_dqm_json_reference pipeline


# When new data comes from DQMs, how do we update, possible cases
# 1 - A MOD sends data with a PMID.  We created an AgrID.  Future submission has the same ModId to AgrId.  No change because we get stuff from PubMed, but possibly check the stuff that didn't come from PubMed to see if that changed, and update those fields. Good
# 2 - Again MOD+PMID+AgrID, future submission doesn't have a PMID.  Do we remove the PMID from the AgrId ?  What if other MODs have also sent the PMID ?  Do we create a whole new AgrId to that ModID, but remove the ModId from the previous AgrId ? - Don't make changes, notify a curator
# 3 - A MOD sends data without a PMID.  We created an AgrID.  Future submission is the same, update Biblio data. Good
# 4 - Again MOD+NoPmid+AgrID, future submissions have a PMID.  If the PMID existed before for a different MOD, do we need to merge the references, and do they need to be done manually ? Don't make changes, notify a curator. If the PMID didn't exist for a different MOD, do we add the PMID but then remove all its data and replace it from the PubMed data ? Good
#
# PubMed updates.  PMID existed, PMID removed: Notify curator, do not delete, do not flag PMID as obsolete.
# PubMed updates, merge by DOI match for micropublications.  (check if DOI exists in agrDb, connect PMID to that agrId instead of creating new agrId, update that PMID's data normally)
#
# What does obsolete crossreference PMID mean:
# - not a valid connection, do not update from pubmed.
# - If a DQM sends the same modId-PMID connection, treat is as if already connected, mention to curator in report file.
#
# Script that updates from PubMed should output to curators a list of non-obsolete PMIDs that no longer exist at PubMed (Deletions of papers at PubMed need to become obsolete at Alliance)

# dqms can send modId / PMID / other.  modId is required, trigger update.  PMID is optional, check if added/removed.  For all xref types, if matches valid do nothing or trigger update from modId/PMID, if matches obsolete notify curator, if no match add connection.

# all IDs should have at most one identifier per each type (PMID, DOI, ModId, PMC, PMCID, ISBN)
# always aggregate all xrefs from MOD + pubmed.  If 2+ for a given type take the PubMed one and notify curator.
# If there are multiple identifiers for a given type from MOD, reject reference and notify curator.


# database
#  map agrId to xref
#   valid / obsolete
#  map xref to agrId
#   valid only
# dqm - each entry
#   get all valid mappings of dqm xref->agrId + all valid dqm xrefs
#     check each xref prefix only has one value per reference, if not notify curator and skip reference
#     no agrId -> sort to create reference
#     2+ agrId -> sort to notify curator
#     1 agrId
#       check modId matches DB->modId
#         if modId matches valid DB->modId, flag ok to aggregate biblio data
#         elsif modId matches obsolete DB->modId, notify curator or ignore ? (was removed from agr by mistake / needs update at mod to not send ?)
#         else modId is new, add agrId->modId to list to attach
#         modId cannot be attached to another agrId or would be in 2+ agrId before
#       if PMID, check PMID matches DB->PMID
#         if PMID matches valid DB->PMID, flag ok to aggregate mod-specific data
#         elsif PMID matches obsolete DB->PMID, sort to notify curator (was removed from agr by mistake / needs update at mod)
#         else PMID does not match any DB->PMID
#           if PMID prefix already in DB, notify curator
#           else add agrId->PMID to list to attach (previously came from modId or DOI)
#       elsif no PMID, check DB->agrId does not have PMID
#         if no valid agrId->PMID, do nothing
#         if valid agrId->PMID, notify curator (needs connection at mod ?)
#       if DOI, check DOI matches DB->DOI
#         if DOI matches valid DB->DOI, do nothing
#         elsif DOI matches obsolete DB->DOI, notify curator (was removed from agr by mistake / needs update at mod)
#         else DOI does not match any DB->DOI
#           if DOI prefix already in DB, notify curator
#           else add agrId->DOI to list to attach
#       elsif no DOI, check DB->agrId does not have DOI
#         if no valid agrId->DOI, do nothing
#         if valid agrId->DOI, notify curator (needs connection at mod ?)
#       check other xref types in database
#         if xref matches valid DB->xref, do nothing
#         elsif xref matches obsolete DB->xref, notify curator or ignore ?
#         else xref does not match any DB->xref
#           if xref prefix already in DB, notify curator
#           else add agrId->xref to list to attach
#         xref cannot be attached to another agrId or would be in 2+ agrId before
#       if has PMID and flagged ok to aggregate mod-specific data, do that
#       elsif (has modId and) flagged ok to aggregate biblio data
#         if multiple mods have data, notify curator
#         elsif only one mods has data, aggregate biblio data
# database - each entry
#   get valid agrId->modId
#     each mod
#       modId not in dqm, notify curator or remove modId / mod_reference_types / tags ?
#     if no mods have data, do nothing (probably created at agr and does not need to be removed, or data timing issue)
# list to attach - each agr and prefix
#   if more than one identifier notify curators
#   elsif exactly one identifier add agr->xref to DB

# when adding PMIDs make sure that there isn't already another valid PMID

# when a pmid is new, check it's at pubmed before attaching ?  yes.  on UI don't overengineer for now, but in future might want restriction on adding PMID xrefs to existing agr references.

# this ticket https://agr-jira.atlassian.net/browse/AGR-3114
# When mods send new data that does not have PMID, only update the mod_reference_types. PubMed will update all other info, including alliance category. 'tags' are another field coming only from the mods, but there's no API for updating it, and it will likely be replaced by in/outside corpus when that's implemented AGR-3115 , and the ATP tags SCRUM-529 .

# zfin files at
# https://zfin.org/downloads/ZFIN_1.0.1.4_Reference.json
# https://zfin.org/downloads/ZFIN_1.0.1.4_Resource.json


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file', action='store', help='take input from REFERENCE files in full path')
parser.add_argument('-m', '--mod', action='store', help='which mod, use all or leave blank for all')

args = vars(parser.parse_args())


def load_pmids_not_found():
    """

    :return:
    """

    pmids_not_found = set()
    base_path = environ.get('XML_PATH')
    pmids_not_found_file = base_path + 'pmids_not_found'
    if path.isfile(pmids_not_found_file):
        with open(pmids_not_found_file, 'r') as read_fh:
            for line in read_fh:
                pmids_not_found.add(line.rstrip())
    return pmids_not_found


def make_url_ref_curie_prefix():
    api_port = environ.get('API_PORT')    # noqa: F841
    url_ref_curie_prefix = 'https://dev' + api_port + '-literature-rest.alliancegenome.org/reference/'
    return url_ref_curie_prefix


def sort_dqm_references(input_path, input_mod):      # noqa: C901
    """

    :param input_path:
    :param input_mod:
    :return:
    """

    # base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'
    base_path = environ.get('XML_PATH')
    api_port = environ.get('API_PORT')    # noqa: F841
    # url_ref_curie_prefix = 'https://dev' + api_port + '-literature-rest.alliancegenome.org/reference/'
    url_ref_curie_prefix = make_url_ref_curie_prefix()

    token = get_authentication_token()
    headers = generate_headers(token)

    mods = ['RGD', 'MGI', 'SGD', 'FB', 'ZFIN', 'WB']
    if input_mod in mods:
        mods = [input_mod]

    xref_ref, ref_xref_valid, ref_xref_obsolete = load_ref_xref('reference')
    # xref_ref, ref_xref_valid, ref_xref_obsolete = load_ref_xref('reference2')   # to test against older database mappings
    pmids_not_found = load_pmids_not_found()

    # make this True for live changes
    # live_changes = False
    live_changes = True

    # test data structure content
    # for prefix in xref_ref:
    #     for identifier in xref_ref[prefix]:
    #         agr = xref_ref[prefix][identifier]
    #         logger.info("agr %s prefix %s ident %s", agr, prefix, identifier)
    #
    # for agr in ref_xref_valid:
    #     for prefix in ref_xref_valid[agr]:
    #         for identifier in ref_xref_valid[agr][prefix]:
    #             logger.info("agr %s valid prefix %s ident %s", agr, prefix, identifier)
    #
    # for agr in ref_xref_obsolete:
    #     for prefix in ref_xref_obsolete[agr]:
    #         for identifier in ref_xref_obsolete[agr][prefix]:
    #             logger.info("agr %s obsolete prefix %s ident %s", agr, prefix, identifier)

    # input_file = 'sanitized'	# set to sanitized to check after posting references to database, that all references are accounted for
    input_file = 'dqm'
    files_to_process = dict()
    if input_file == 'sanitized':
        files_to_process['sanitized'] = []
        json_storage_path = base_path + 'sanitized_reference_json/'
        dir_list = listdir(json_storage_path)
        for filename in dir_list:
            # logger.info("%s", filename)
            if 'REFERENCE_' in filename and '.REFERENCE_' not in filename:
                # logger.info("%s", filename)
                files_to_process['sanitized'].append(json_storage_path + filename)
    else:
        for mod in mods:
            if mod not in files_to_process:
                files_to_process[mod] = []
            filename = base_path + input_path + '/REFERENCE_' + mod + '.json'
            files_to_process[mod].append(filename)

    dqm = dict()
    for mod in mods:
        dqm[mod] = set()

    # filename = input_path + '/REFERENCE_' + mod + '.json'

    xrefs_to_add = dict()
    aggregate_mod_reference_types_only = dict()
    aggregate_mod_biblio_all = dict()

    fh_mod_report = dict()
    report_file_path = base_path + 'report_files/'
    if not path.exists(report_file_path):
        makedirs(report_file_path)
    for mod in mods:
        filename = report_file_path + mod + '_updates'
        fh_mod_report.setdefault(mod, open(filename, 'w'))
    sanitized_report_filename = base_path + 'report_files/sanitized_updates'
    fh_mod_report.setdefault('sanitized', open(sanitized_report_filename, 'w'))

    xref_to_pages = dict()
    for mod in sorted(files_to_process):
        references_to_create = []
        for filename in sorted(files_to_process[mod]):
            logger.info(filename)
            dqm_data = dict()
            with open(filename, 'r') as f:
                dqm_data = json.load(f)
                f.close()
            entries = dqm_data
            if input_file == 'dqm':
                entries = dqm_data['data']
            # get rid of counter
            counter = 0
            # max_counter = 1
            max_counter = 100000000
            for entry in entries:
                counter = counter + 1
                if counter > max_counter:
                    break

                # for debugging changes
                # dqm_entry_text = json.dumps(entry, indent=4)
                # print('dqm ')
                # print(dqm_entry_text)

                dqm_xrefs = dict()
                xrefs = []
                agrs_found = set()
                if 'crossReferences' in entry:
                    for cross_reference in entry['crossReferences']:
                        if "id" in cross_reference:
                            xrefs.append(cross_reference["id"])
                            # logger.info("append xref %s", cross_reference["id"])
                            if "pages" in cross_reference:
                                xref_to_pages[cross_reference["id"]] = cross_reference["pages"]
                if entry['primaryId'] not in xrefs:
                    xrefs.append(entry['primaryId'])
                    # logger.info("append primaryId %s", entry['primaryId'])
                for cross_reference in xrefs:
                    prefix, identifier, separator = split_identifier(cross_reference)
                    if prefix not in dqm_xrefs:
                        dqm_xrefs[prefix] = set()
                    dqm_xrefs[prefix].add(identifier)
                    if prefix in xref_ref:
                        if identifier in xref_ref[prefix]:
                            agr = xref_ref[prefix][identifier]
                            agrs_found.add(agr)
                    if prefix in mods:
                        dqm[prefix].add(identifier)

                flag_dqm_prefix_fail = False
                for prefix in dqm_xrefs:
                    if len(dqm_xrefs[prefix]) > 1:
                        flag_dqm_prefix_fail = True
                        # logger.info("Notify curator, filename %s, dqm %s has too many identifiers for %s %s", filename, entry['primaryId'], prefix, ', '.join(sorted(dqm_xrefs[prefix])))
                        fh_mod_report[mod].write("dqm %s has too many identifiers for %s %s\n" % (entry['primaryId'], prefix, ', '.join(sorted(dqm_xrefs[prefix]))))
                if flag_dqm_prefix_fail:
                    continue

                if len(agrs_found) == 0:
                    # logger.info("Action : Create New mod %s", entry['primaryId'])
                    references_to_create.append(entry)
                elif len(agrs_found) > 1:
                    # logger.info("Notify curator, dqm %s too many matches %s", entry['primaryId'], ', '.join(sorted(map(lambda x: url_ref_curie_prefix + x, agrs_found))))
                    fh_mod_report[mod].write("dqm %s too many matches %s\n" % (entry['primaryId'], ', '.join(sorted(map(lambda x: url_ref_curie_prefix + x, agrs_found)))))
                elif len(agrs_found) == 1:
                    # logger.info("Normal %s", entry['primaryId'])
                    agr = agrs_found.pop()
                    agr_url = url_ref_curie_prefix + agr
                    flag_aggregate_biblio = False
                    flag_aggregate_mod = False
                    for prefix in dqm_xrefs:
                        for ident in dqm_xrefs[prefix]:
                            # logger.info("looking for %s %s", prefix, ident)
                            dqm_xref_valid_found = False
                            agr_had_prefix = False
                            if agr in ref_xref_valid:
                                # logger.info("agr found %s", agr)
                                if prefix == 'PMID' and ident in pmids_not_found:
                                    # logger.info("Notify curator dqm has PMID not in PubMed %s %s in agr %s", prefix, ident, agr_url)
                                    fh_mod_report[mod].write("dqm has PMID not in PubMed %s %s in agr %s\n" % (prefix, ident, agr_url))
                                elif prefix in ref_xref_valid[agr]:
                                    agr_had_prefix = True
                                    # logger.info("agr prefix found %s %s", agr, prefix)
                                    if ident.lower() == ref_xref_valid[agr][prefix].lower():
                                        # logger.info("agr prefix ident found %s %s %s", agr, prefix, ident)
                                        dqm_xref_valid_found = True
                                        if prefix == 'PMID':
                                            flag_aggregate_mod = True
                                            # logger.info("valid PMID xref %s %s to update agr %s", prefix, ident, agr)
                                        if prefix in mods:
                                            flag_aggregate_biblio = True
                                            # logger.info("valid MOD xref %s %s to update agr %s", prefix, ident, agr)
                            dqm_xref_obsolete_found = False
                            if agr in ref_xref_obsolete:
                                if prefix in ref_xref_obsolete[agr]:
                                    if ident.lower() in ref_xref_obsolete[agr][prefix]:
                                        dqm_xref_obsolete_found = True
                            if dqm_xref_obsolete_found:
                                # logger.info("Notify curator dqm has obsolete xref %s %s in agr %s", prefix, ident, agr_url)
                                fh_mod_report[mod].write("dqm has obsolete xref %s %s in agr %s\n" % (prefix, ident, agr_url))
                            if not dqm_xref_valid_found:
                                if agr_had_prefix:
                                    # logger.info("Notify curator, %s had %s %s, dqm submitted %s", agr_url, prefix, ref_xref_valid[agr][prefix], ident)
                                    fh_mod_report[mod].write("%s had %s %s, dqm submitted %s\n" % (agr_url, prefix, ref_xref_valid[agr][prefix], ident))
                                elif not dqm_xref_obsolete_found:
                                    if agr not in xrefs_to_add:
                                        xrefs_to_add[agr] = dict()
                                    if prefix not in xrefs_to_add[agr]:
                                        xrefs_to_add[agr][prefix] = dict()
                                    if ident not in xrefs_to_add[agr][prefix]:
                                        xrefs_to_add[agr][prefix][ident] = set()
                                    xrefs_to_add[agr][prefix][ident].add(filename)
                                    # logger.info("Action : Add dqm xref %s %s to agr %s", prefix, ident, agr)  # dealt with below, not needed

                    if flag_aggregate_mod:
                        # logger.info("Action : aggregate PMID mod data %s", agr)
                        aggregate_mod_reference_types_only[agr] = entry
                    elif flag_aggregate_biblio:
                        if 'keywords' in entry:
                            entry = clean_up_keywords(mod, entry)
                        # logger.info("Action : aggregate MOD biblio data %s", agr)
                        aggregate_mod_biblio_all[agr] = entry
                        pass
                    # check if dqm has no pmid/doi, but pmid/doi in DB
                    if 'PMID' not in dqm_xrefs:
                        if 'PMID' in ref_xref_valid[agr]:
                            # logger.info("Notify curator %s has PMID %s, dqm %s does not", agr, ref_xref_valid[agr]['PMID'], entry['primaryId'])
                            fh_mod_report[mod].write("%s has PMID %s, dqm %s does not\n" % (agr_url, ref_xref_valid[agr]['PMID'], entry['primaryId']))
                    if 'DOI' not in dqm_xrefs:
                        if 'DOI' in ref_xref_valid[agr]:
                            # logger.info("Notify curator %s has DOI %s, dqm %s does not", agr, ref_xref_valid[agr]['DOI'], entry['primaryId'])
                            fh_mod_report[mod].write("%s has DOI %s, dqm %s does not\n" % (agr_url, ref_xref_valid[agr]['DOI'], entry['primaryId']))

        save_new_references_to_file(references_to_create, mod)

        # check all db agrId->modId, check each dqm mod still had modId
        for agr in ref_xref_valid:
            agr_url = url_ref_curie_prefix + agr
            for prefix in ref_xref_valid[agr]:
                if prefix in mods:
                    # for identifier in ref_xref_valid[agr][prefix]:
                    identifier = ref_xref_valid[agr][prefix]
                    ident_found = False
                    if prefix in dqm:
                        if identifier in dqm[prefix]:
                            ident_found = True
                    if not ident_found:
                        # logger.info("Notify curator %s %s %s not in dqm submission", agr_url, prefix, identifier)
                        fh_mod_report[mod].write("%s %s %s not in dqm submission\n" % (agr_url, prefix, identifier))

        for agr in xrefs_to_add:
            agr_url = url_ref_curie_prefix + agr
            for prefix in xrefs_to_add[agr]:
                if len(xrefs_to_add[agr][prefix]) > 1:
                    conflict_list = []
                    for ident in xrefs_to_add[agr][prefix]:
                        filenames = ' '.join(sorted(xrefs_to_add[agr][prefix][ident]))
                        conflict_list.append(ident + ' ' + filenames)
                    conflict_string = ', '.join(conflict_list)
                    # logger.info("Notify curator %s %s has multiple identifiers from dqms %s", agr_url, prefix, conflict_string)
                    fh_mod_report[mod].write("%s %s has multiple identifiers from dqms %s\n" % (agr_url, prefix, conflict_string))
                elif len(xrefs_to_add[agr][prefix]) == 1:
                    for ident in xrefs_to_add[agr][prefix]:
                        xref_id = prefix + ':' + ident
                        new_entry = dict()
                        new_entry["curie"] = xref_id
                        new_entry["reference_curie"] = agr
                        if xref_id in xref_to_pages:
                            new_entry["pages"] = xref_to_pages[xref_id]
                        if live_changes:
                            logger.info("add validated dqm xref %s s to agr %s", xref_id, agr)
                            url = 'http://' + api_server + ':' + api_port + '/cross_reference/'
                            headers = generic_api_post(live_changes, url, headers, new_entry, agr, None, None)

        # these take hours for each mod, process about 200 references per minute
        headers = update_db_entries(headers, aggregate_mod_reference_types_only, live_changes, fh_mod_report[mod], 'mod_reference_types_only')
        headers = update_db_entries(headers, aggregate_mod_biblio_all, live_changes, fh_mod_report[mod], 'mod_biblio_all')

    for mod in fh_mod_report:
        fh_mod_report[mod].close()
    fh_mod_report['sanitized'].close()


def save_new_references_to_file(references_to_create, mod):
    base_path = environ.get('XML_PATH')
    json_storage_path = base_path + 'dqm_data_updates_new/'
    if not path.exists(json_storage_path):
        makedirs(json_storage_path)
    dqm_data = dict()
    dqm_data['data'] = references_to_create
    json_filename = json_storage_path + 'REFERENCE_' + mod + '.json'
    write_json(json_filename, dqm_data)


def update_db_entries(headers, entries, live_changes, report_fh, processing_flag):      # noqa: C901
    """
    Take a dict of Alliance Reference curies and DQM MODReferenceTypes to compare against data stored in DB and update to match DQM data.

    :param entries:
    :param processing_flag:
    :return:
    """

    remap_keys = dict()
    remap_keys['datePublished'] = 'date_published'
    remap_keys['dateArrivedInPubmed'] = 'date_arrived_in_pubmed'
    remap_keys['dateLastModified'] = 'date_last_modified'
    remap_keys['crossReferences'] = 'cross_references'
    remap_keys['issueName'] = 'issue_name'
    remap_keys['issueDate'] = 'issue_date'
    remap_keys['pubMedType'] = 'pubmed_type'
    remap_keys['meshTerms'] = 'mesh_terms'
    remap_keys['allianceCategory'] = 'category'
    remap_keys['MODReferenceType'] = 'mod_reference_types'
    remap_keys['MODReferenceTypes'] = 'mod_reference_types'
    remap_keys['plainLanguageAbstract'] = 'plain_language_abstract'
    remap_keys['pubmedAbstractLanguages'] = 'pubmed_abstract_languages'
    remap_keys['publicationStatus'] = 'pubmed_publication_status'
    # remap_keys['resourceAbbreviation'] = 'resource_title'

    # MODReferenceTypes and allianceCategory cannot be auto converted from camel to snake, so have two lists
    # fields_simple_snake = ['title', 'category', 'citation', 'volume', 'pages', 'language', 'abstract', 'publisher', 'issue_name', 'issue_date', 'date_published', 'date_last_modified']
    fields_simple_camel = ['title', 'allianceCategory', 'citation', 'volume', 'pages', 'language', 'abstract', 'publisher', 'issueName', 'issueDate', 'datePublished', 'dateLastModified']
    # there's no API to update tags

    api_port = environ.get('API_PORT')
    # url_ref_curie_prefix = make_url_ref_curie_prefix()

    counter = 0
    max_counter = 10000000
    # max_counter = 3

    for agr in entries:
        counter = counter + 1
        if counter > max_counter:
            break

        # agr_url = url_ref_curie_prefix + agr    # noqa: F841
        api_server = environ.get('API_SERVER', 'localhost')
        url = 'http://' + api_server + ':' + api_port + '/reference/' + agr
        logger.info("get AGR reference info from database %s", url)
        get_return = requests.get(url)
        db_entry = json.loads(get_return.text)
        # logger.info("title %s", response_dict['title'])   # for debugging which reference was found

        dqm_entry = entries[agr]

        if processing_flag == 'mod_biblio_all':
            # for debugging changes
            # dqm_entry_text = json.dumps(dqm_entry, indent=4)
            # db_entry_text = json.dumps(db_entry, indent=4)
            # print('db ')
            # print(db_entry_text)
            # print('dqm2 ')
            # print(dqm_entry_text)

            update_json = dict()
            for field_camel in fields_simple_camel:
                field_snake = field_camel
                if field_camel in remap_keys:
                    field_snake = remap_keys[field_camel]
                dqm_value = None
                db_value = None
                if field_camel in dqm_entry:
                    dqm_value = dqm_entry[field_camel]
                    if field_snake == 'category':
                        dqm_value = dqm_value.lower().replace(" ", "_")
                if field_snake in db_entry:
                    db_value = db_entry[field_snake]
                if dqm_value != db_value:
                    logger.info("patch %s field %s from db %s to dqm %s", agr, field_snake, db_value, dqm_value)
                    update_json[field_snake] = dqm_value
            keywords_changed = compare_keywords(db_entry, dqm_entry)
            if keywords_changed[0]:
                logger.info("patch %s field keywords from db %s to dqm %s", agr, keywords_changed[2], keywords_changed[1])
                update_json['keywords'] = keywords_changed[1]
            authors_changed = compare_authors_or_editors(db_entry, dqm_entry, 'authors')
            if authors_changed[0]:
                # live_changes = True
                for patch_data in authors_changed[1]:
                    patch_dict = patch_data['patch_dict']
                    patch_dict['reference_curie'] = agr
                    logger.info("patch %s author_id %s patch_dict %s", agr, patch_data['author_id'], patch_dict)
                    author_patch_url = 'http://' + api_server + ':' + api_port + '/author/' + str(patch_data['author_id'])
                    headers = generic_api_patch(live_changes, author_patch_url, headers, patch_dict, str(patch_data['author_id']), None, None)
                for create_dict in authors_changed[2]:
                    create_dict['reference_curie'] = agr
                    logger.info("add to %s create_dict %s", agr, create_dict)
                    author_post_url = 'http://' + api_server + ':' + api_port + '/author/'
                    headers = generic_api_post(live_changes, author_post_url, headers, create_dict, agr, None, None)

            # if curators want to get reports of how resource change, put this back, but we're comparing resource titles with dqm resource abbreviations, so they often differ even if they would match if we had a resource lookup by names and synonyms.
            # e.g. WBPaper00000007 has db title "Comptes rendus des seances de l'Academie des sciences. Serie D, Sciences naturelles" and dqm abbreviation "C R Seances Acad Sci D"
            # resource_changed = compare_resource(db_entry, dqm_entry)
            # if resource_changed[0]:
            #     logger.info("%s dqm resource differs db %s dqm %s", agr_url, resource_changed[2], resource_changed[1])
            #     report_fh.write("%s dqm resource differs db '%s' dqm '%s'\n" % (agr_url, resource_changed[2], resource_changed[1]))
            if update_json:
                # for debugging changes
                # update_text = json.dumps(update_json, indent=4)
                # print('update ' + update_text)
                headers = generic_api_patch(live_changes, url, headers, update_json, agr, None, None)

        # always update mod reference types, whether 'mod_reference_types_only' or 'mod_biblio_all'
        headers = update_mod_reference_types(live_changes, headers, agr, dqm_entry, db_entry)

    return headers


def compare_resource(db_entry, dqm_entry):
    db_resource_title = ''
    dqm_resource_abbreviation = ''
    if 'resource_title' in db_entry:
        if db_entry['resource_title'] is not None:
            db_resource_title = db_entry['resource_title']
    if 'resourceAbbreviation' in dqm_entry:
        if dqm_entry['resourceAbbreviation'] is not None:
            dqm_resource_abbreviation = dqm_entry['resourceAbbreviation']
    if db_resource_title.lower() == dqm_resource_abbreviation.lower():
        return False, None, None
    else:
        return True, dqm_resource_abbreviation, db_resource_title


def compare_keywords(db_entry, dqm_entry):
    # e.g. ZFIN:ZDB-PUB-150828-18
    db_keywords = []
    dqm_keywords = []
    if 'keywords' in db_entry:
        if db_entry['keywords'] is not None:
            db_keywords = db_entry['keywords']
    lower_db_keywords = [i.lower() for i in db_keywords]
    if 'keywords' in dqm_entry:
        if dqm_entry['keywords'] is not None:
            dqm_keywords = dqm_entry['keywords']
    lower_dqm_keywords = [i.lower() for i in dqm_keywords]
    if set(lower_db_keywords) == set(lower_dqm_keywords):
        return False, None, None
    else:
        return True, dqm_keywords, db_keywords


def update_mod_reference_types(live_changes, headers, agr, dqm_entry, db_entry):
    api_port = environ.get('API_PORT')
    dqm_mod_ref_types = []
    if 'MODReferenceTypes' in dqm_entry:
        dqm_mod_ref_types = dqm_entry['MODReferenceTypes']
    dqm_mrt_data = dict()
    for mrt in dqm_mod_ref_types:
        source = mrt['source']
        ref_type = mrt['referenceType']
        if source not in dqm_mrt_data:
            dqm_mrt_data[source] = []
        dqm_mrt_data[source].append(ref_type)

    db_mod_ref_types = []
    if 'mod_reference_types' in db_entry:
        db_mod_ref_types = db_entry['mod_reference_types']

    # for debugging changes
    # dqm_mod_ref_types_json = json.dumps(dqm_mod_ref_types, indent=4)
    # db_mod_ref_types_json = json.dumps(db_mod_ref_types, indent=4)
    # logger.info("Action : aggregate PMID mod data %s was %s now %s", agr, db_mod_ref_types_json, dqm_mod_ref_types_json)

    db_mrt_data = dict()
    for mrt in db_mod_ref_types:
        source = mrt['source']
        ref_type = mrt['reference_type']
        mrt_id = mrt['mod_reference_type_id']
        if source not in db_mrt_data:
            db_mrt_data[source] = dict()
        db_mrt_data[source][ref_type] = mrt_id

    # live_changes = False
    # try AGR:AGR-Reference-0000382879	WBPaper00000292
    for mod in dqm_mrt_data:
        lc_dqm = [x.lower() for x in dqm_mrt_data[mod]]
        for dqm_mrt in dqm_mrt_data[mod]:
            create_it = True
            if mod in db_mrt_data:
                for db_mrt in db_mrt_data[mod]:
                    if db_mrt.lower() in lc_dqm:
                        create_it = False
            if create_it:
                logger.info("add %s %s to %s", mod, dqm_mrt, agr)
                url = 'http://' + api_server + ':' + api_port + '/reference/mod_reference_type/'
                new_entry = dict()
                new_entry["reference_type"] = dqm_mrt
                new_entry["source"] = mod
                new_entry["reference_curie"] = agr
                headers = generic_api_post(live_changes, url, headers, new_entry, agr, None, None)
                # process_post_tuple = process_post('POST', url, headers, new_entry, agr, mapping_fh, error_fh)    # noqa: F841
        if mod in db_mrt_data:
            lc_db_dict = {x.lower(): x for x in db_mrt_data[mod]}
            lc_db = set(lc_db_dict.keys())
            for db_mrt in db_mrt_data[mod]:
                delete_it = True
                for dqm_mrt in dqm_mrt_data[mod]:
                    if dqm_mrt.lower() in lc_db:
                        delete_it = False
                if delete_it:
                    mod_reference_type_id = str(db_mrt_data[mod][db_mrt])
                    logger.info("remove %s %s from %s via %s", mod, db_mrt, agr, mod_reference_type_id)
                    url = 'http://' + api_server + ':' + api_port + '/reference/mod_reference_type/' + mod_reference_type_id
                    headers = generic_api_delete(live_changes, url, headers, None, agr, None, None)
                    # process_post_tuple = process_post('DELETE', url, headers, None, agr, mapping_fh, error_fh)    # noqa: F841
    return headers


def generic_api_post(live_changes, url, headers, new_entry, agr, mapping_fh, error_fh):
    if live_changes:
        api_response_tuple = process_api_request('POST', url, headers, new_entry, agr, mapping_fh, error_fh)
        headers = api_response_tuple[0]
        response_text = api_response_tuple[1]
        response_status_code = api_response_tuple[2]
        log_info = api_response_tuple[3]
        if log_info:
            logger.info(log_info)
        if response_status_code == 201:
            response_dict = json.loads(response_text)
            response_dict = str(response_dict).replace('"', '')
            logger.info("%s\t%s", agr, response_dict)
    return headers


def generic_api_patch(live_changes, url, headers, update_json, agr, mapping_fh, error_fh):
    if live_changes:
        api_response_tuple = process_api_request('PATCH', url, headers, update_json, agr, mapping_fh, error_fh)
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
    return headers


def generic_api_delete(live_changes, url, headers, json_data, agr, mapping_fh, error_fh):
    if live_changes:
        api_response_tuple = process_api_request('DELETE', url, headers, json_data, agr, mapping_fh, error_fh)
        headers = api_response_tuple[0]
        response_text = api_response_tuple[1]    # noqa: F841
        response_status_code = api_response_tuple[2]
        log_info = api_response_tuple[3]
        if log_info:
            logger.info(log_info)
        if response_status_code == 204:
            logger.info("%s\t%s\tdelete success", agr, url)
    return headers


if __name__ == "__main__":
    """
    call main start function
    """

    logger.info("starting sort_dqm_json_reference_updates.py")

    if args['file']:
        if args['mod']:
            sort_dqm_references(args['file'], args['mod'])
        else:
            sort_dqm_references(args['file'], 'all')

    logger.info("ending sort_dqm_json_reference_updates.py")
