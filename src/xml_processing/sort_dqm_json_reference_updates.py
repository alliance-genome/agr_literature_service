import json
# import urllib.request
import argparse
# import re
from os import environ, path, listdir
# from os import makedirs
import logging
import logging.config

from dotenv import load_dotenv

# import bs4
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

load_dotenv()

# pipenv run python sort_dqm_json_reference_updates.py -f dqm_data -m WB

# pipenv run python sort_dqm_json_reference_updates.py -f dqm_data -m all > asdf_sanitized


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
    """

    Split Identifier.

    Does not throw exception anymore. Check return, if None returned, there was an error

    :param identifier:
    :param ignore_error:
    :return:
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
    """

    :return:
    """

    # 7 seconds to populate file with 2476879 rows
    ref_xref_valid = dict()
    ref_xref_obsolete = dict()
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
                status = line_data[2]
                prefix, identifier, separator = split_identifier(xref)
                if status == 'valid':
                    if agr not in ref_xref_valid:
                        ref_xref_valid[agr] = dict()
                    ref_xref_valid[agr][prefix] = identifier
                    # previously a reference and prefix could have multiple values
                    # if prefix not in ref_xref_valid[agr]:
                    #     ref_xref_valid[agr][prefix] = set()
                    # if identifier not in ref_xref_valid[agr][prefix]:
                    #     ref_xref_valid[agr][prefix].add(identifier)
                    if prefix not in xref_ref:
                        xref_ref[prefix] = dict()
                    if identifier not in xref_ref[prefix]:
                        xref_ref[prefix][identifier] = agr
                elif status == 'obsolete':
                    if agr not in ref_xref_obsolete:
                        ref_xref_obsolete[agr] = dict()
                    # a reference and prefix can still have multiple obsolete values
                    if prefix not in ref_xref_obsolete[agr]:
                        ref_xref_obsolete[agr][prefix] = set()
                    if identifier not in ref_xref_obsolete[agr][prefix]:
                        ref_xref_obsolete[agr][prefix].add(identifier.lower())
            read_fh.close
    return xref_ref, ref_xref_valid, ref_xref_obsolete


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


def sort_dqm_references(input_path, input_mod):
    """

    :param input_path:
    :param input_mod:
    :return:
    """

    # base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'
    base_path = environ.get('XML_PATH')

    mods = ['RGD', 'MGI', 'SGD', 'FB', 'ZFIN', 'WB']
    if input_mod in mods:
        mods = [input_mod]

    xref_ref, ref_xref_valid, ref_xref_obsolete = load_ref_xref()
    pmids_not_found = load_pmids_not_found()

#     # test data structure content
#     for prefix in xref_ref:
#         for identifier in xref_ref[prefix]:
#             agr = xref_ref[prefix][identifier]
#             logger.info("agr %s prefix %s ident %s", agr, prefix, identifier)
#
#     for agr in ref_xref_valid:
#         for prefix in ref_xref_valid[agr]:
#             for identifier in ref_xref_valid[agr][prefix]:
#                 logger.info("agr %s valid prefix %s ident %s", agr, prefix, identifier)
#
#     for agr in ref_xref_obsolete:
#         for prefix in ref_xref_obsolete[agr]:
#             for identifier in ref_xref_obsolete[agr][prefix]:
#                 logger.info("agr %s obsolete prefix %s ident %s", agr, prefix, identifier)

    # input_file = 'sanitized'	# set to sanitized to check after posting references to database, that all references are accounted for
    input_file = 'dqm'
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
    for mod in mods:
        dqm[mod] = set()

    # filename = input_path + '/REFERENCE_' + mod + '.json'

    xrefs_to_add = dict()

    for filename in sorted(files_to_process):
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
        # max_counter = 10
        max_counter = 100000000
        for entry in entries:
            counter = counter + 1
            if counter > max_counter:
                break

            dqm_xrefs = dict()
            xrefs = []
            agrs_found = set()
            if 'crossReferences' in entry:
                for cross_reference in entry['crossReferences']:
                    if "id" in cross_reference:
                        xrefs.append(cross_reference["id"])
            if entry['primaryId'] not in xrefs:
                xrefs.append(entry['primaryId'])
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
                    logger.info("Notify curator, filename %s, dqm %s has too many identifiers for %s %s", filename, entry['primaryId'], prefix, ', '.join(sorted(dqm_xrefs[prefix])))
            if flag_dqm_prefix_fail:
                continue

            if len(agrs_found) == 0:
                logger.info("Action : Create New mod %s", entry['primaryId'])
                # TODO  shunt this to set of new to create to use old pipeline on
            elif len(agrs_found) > 1:
                logger.info("Notify curator, dqm %s too many matches %s", entry['primaryId'], ', '.join(sorted(agrs_found)))
            elif len(agrs_found) == 1:
                # logger.info("Normal %s", entry['primaryId'])
                agr = agrs_found.pop()
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
                                logger.info("Notify curator dqm has PMID not in PubMed %s %s in agr %s", prefix, ident, agr)
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
                            logger.info("Notify curator dqm has obsolete xref %s %s in agr %s", prefix, ident, agr)
                        if not dqm_xref_valid_found:
                            if agr_had_prefix:
                                logger.info("Notify curator, %s had %s %s, dqm submitted %s", agr, prefix, ref_xref_valid[agr][prefix], ident)
                            elif not dqm_xref_obsolete_found:
                                if agr not in xrefs_to_add:
                                    xrefs_to_add[agr] = dict()
                                if prefix not in xrefs_to_add[agr]:
                                    xrefs_to_add[agr][prefix] = dict()
                                if ident not in xrefs_to_add[agr][prefix]:
                                    xrefs_to_add[agr][prefix][ident] = set()
                                xrefs_to_add[agr][prefix][ident].add(filename)
                                # logger.info("Action : Add dqm xref %s %s to agr %s", prefix, ident, agr)

                if flag_aggregate_mod:
                    logger.info("Action : aggregate PMID mod data %s", agr)
                    # TODO  figure out what to patch
                elif flag_aggregate_biblio:
                    logger.info("Action : aggregate MOD biblio data %s", agr)
                    # TODO  figure out what to patch
                # check if dqm has no pmid/doi, but pmid/doi in DB
                if 'PMID' not in dqm_xrefs:
                    if 'PMID' in ref_xref_valid[agr]:
                        logger.info("Notify curator %s has PMID %s, dqm %s does not", agr, ref_xref_valid[agr]['PMID'], entry['primaryId'])
                if 'DOI' not in dqm_xrefs:
                    if 'DOI' in ref_xref_valid[agr]:
                        logger.info("Notify curator %s has DOI %s, dqm %s does not", agr, ref_xref_valid[agr]['DOI'], entry['primaryId'])

    # check all db agrId->modId, check each dqm mod still had modId
    for agr in ref_xref_valid:
        for prefix in ref_xref_valid[agr]:
            if prefix in mods:
                # for identifier in ref_xref_valid[agr][prefix]:
                identifier = ref_xref_valid[agr][prefix]
                ident_found = False
                if prefix in dqm:
                    if identifier in dqm[prefix]:
                        ident_found = True
                if not ident_found:
                    logger.info("Notify curator %s %s %s not in dqm submission", agr, prefix, identifier)

    for agr in xrefs_to_add:
        for prefix in xrefs_to_add[agr]:
            if len(xrefs_to_add[agr][prefix]) > 1:
                conflict_list = []
                for ident in xrefs_to_add[agr][prefix]:
                    filenames = ' '.join(sorted(xrefs_to_add[agr][prefix][ident]))
                    conflict_list.append(ident + ' ' + filenames)
                conflict_string = ', '.join(conflict_list)
                logger.info("Notify curator %s %s has multiple identifiers from dqms %s", agr, prefix, conflict_string)
            elif len(xrefs_to_add[agr][prefix]) == 1:
                for ident in xrefs_to_add[agr][prefix]:
                    logger.info("Action : add validated dqm xref %s %s to agr %s", prefix, ident, agr)
                    # TODO   create new xref


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
