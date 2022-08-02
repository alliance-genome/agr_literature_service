import argparse
import json
import sys
import logging
import logging.config
import warnings
from os import environ, makedirs, path

import requests
from dotenv import load_dotenv

from fastapi.encoders import jsonable_encoder

from agr_literature_service.lit_processing.filter_dqm_md5sum import load_s3_md5data, generate_new_md5, save_s3_md5data

from agr_literature_service.api.models import ReferenceModel
from agr_literature_service.api.crud.reference_crud import update_citation

from agr_literature_service.lit_processing.helper_file_processing import (compare_authors_or_editors,
                                                                          split_identifier, write_json)
from agr_literature_service.lit_processing.helper_post_to_api import (generate_headers, get_authentication_token,
                                                                      process_api_request)
from agr_literature_service.lit_processing.helper_sqlalchemy import (create_postgres_session,
                                                                     sqlalchemy_load_ref_xref)
from agr_literature_service.lit_processing.helper_email import send_email

from agr_literature_service.lit_processing.parse_dqm_json_reference import (generate_pmid_data,
                                                                            aggregate_dqm_with_pubmed)
from agr_literature_service.lit_processing.get_pubmed_xml import download_pubmed_xml
from agr_literature_service.lit_processing.xml_to_json import generate_json
from agr_literature_service.lit_processing.post_reference_to_api import post_references
from agr_literature_service.lit_processing.post_comments_corrections_to_api import post_comments_corrections
from agr_literature_service.lit_processing.update_resource_pubmed_nlm import update_resource_pubmed_nlm
from agr_literature_service.lit_processing.get_dqm_data import download_dqm_json

# For WB needing 57578 references checked for updating,
# It would take 48 hours to query the database through the API one by one.
# It takes 24 minutes to query in batches of 1000 through batch alchemy.


warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

load_dotenv()
api_server = environ.get('API_SERVER', 'localhost')

# pipenv run python sort_dqm_json_reference_updates.py -f dqm_data -m WB

# pipenv run python sort_dqm_json_reference_updates.py -f tests/dqm_update_sample -m WB

# pipenv run python sort_dqm_json_reference_updates.py -f dqm_data -m all > asdf_sanitized

# first run  get_datatypes_cross_references.py  to generate mappings from references to xrefs and resources to xrefs

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


logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format= '%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


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


def filter_from_md5sum(mod):
    return


def sort_dqm_references(input_path, input_mod):      # noqa: C901
    """

# TODO
# DATA  WBPaper00061683 was primaryId in 2021 11 04, became PMID:34345807 in 2022 04 25 update, check that can be found via xref and associated

    :param input_path:
    :param input_mod:
    :return:
    """

    base_path = environ.get('XML_PATH')
    api_port = environ.get('API_PORT')    # noqa: F841
    # url_ref_curie_prefix = 'https://dev' + api_port + '-literature-rest.alliancegenome.org/reference/'
    url_ref_curie_prefix = make_url_ref_curie_prefix()

    # download the dqm file(s) from mod(s)
    env_state = environ.get('ENV_STATE', 'build')
    if env_state != 'test':
        # download the dqm file(s) from mod(s)
        download_dqm_json()
        # to pull in new journal info from pubmed
        update_resource_pubmed_nlm()
    token = get_authentication_token()
    headers = generate_headers(token)

    mods = ['RGD', 'MGI', 'XB', 'SGD', 'FB', 'ZFIN', 'WB']
    if input_mod in mods:
        mods = [input_mod]

    # these tags are allowed from dqms, but we don't want them in the database
    dqm_keys_to_remove = {'tags', 'issueDate', 'dateArrivedInPubmed', 'dateLastModified', 'keywords', 'citation'}

    # to debug, save 9 seconds per run by generating xref mappings only once and load from flatfile
    # generate_cross_references_file('reference')
    # xref_ref, ref_xref_valid, ref_xref_obsolete = load_ref_xref_api_flatfile('reference')

    # in production load xref mappings through sqlalchemy, which takes 14 seconds for all cross references
    xref_ref, ref_xref_valid, ref_xref_obsolete = sqlalchemy_load_ref_xref('reference')

    # test data structure content
    # for prefix in xref_ref:
    #     for identifier in xref_ref[prefix]:
    #         agr = xref_ref[prefix][identifier]
    #         logger.info("agr %s prefix %s ident %s", agr, prefix, identifier)
    #
    # for agr in ref_xref_valid:
    #     for prefix in ref_xref_valid[agr]:
    #         logger.info("agr %s valid prefix %s ident %s", agr, prefix, ref_xref_valid[agr][prefix])
    #
    # for agr in ref_xref_obsolete:
    #     for prefix in ref_xref_obsolete[agr]:
    #         logger.info("agr %s obsolete prefix %s ident %s", agr, prefix, ref_xref_obsolete[agr][prefix])

    pmids_not_found = load_pmids_not_found()

    # make this True for live changes
#     live_changes = False
# PUT THIS BACK
    live_changes = True

    dqm = dict()
    for mod in mods:
        if mod == 'XB':
            prefix = "Xenbase"
        else:
            prefix = mod
        dqm[prefix] = set()

    # filename = input_path + '/REFERENCE_' + mod + '.json'

    xrefs_to_add = dict()
    aggregate_mod_specific_fields_only = dict()
    aggregate_mod_biblio_all = dict()

    fh_mod_report = dict()
    report_file_path = ''
    if environ.get('LOG_PATH'):
        report_file_path = path.join(environ['LOG_PATH'], 'dqm_load/')
    if report_file_path and not path.exists(report_file_path):
        makedirs(report_file_path)

    xref_to_pages = dict()
    # for mod in sorted(files_to_process):
    report = {}
    # report2 = {}
    # report3 = {}
    for mod in sorted(mods):

        report[mod] = []
        # report2[mod] = []
        # report3[mod] = []
        filename = report_file_path + mod + '_dqm_loading.log'
        fh_mod_report.setdefault(mod, open(filename, 'w'))
        references_to_create = []
        curies_for_citation_update = []
        logger.info("loading old md5")
        old_md5dict = load_s3_md5data([mod])

#         print("old_md5dict")
#         db_entry_text = json.dumps(old_md5dict, indent=4, sort_keys=True)
#         print(db_entry_text)

        logger.info("generating new md5")
        new_md5dict = generate_new_md5(input_path, [mod])

        filename = base_path + input_path + '/REFERENCE_' + mod + '.json'
        logger.info(f"Processing {filename}")
        dqm_data = dict()
        with open(filename, 'r') as f:
            dqm_data = json.load(f)
            f.close()
        entries = dqm_data['data']
        # get rid of counter
        counter = 0
        # max_counter = 1
        max_counter = 100000000
#         max_counter = 10
        for entry in entries:
            counter = counter + 1
            if counter > max_counter:
                break

            if 'primaryId' not in entry or entry['primaryId'] is None:
                continue

            # dbid = None
            # ## grab all MOD IDs (eg, SGDID) from qdm submission and save them in memory (in hash dqm)
            # if 'crossReferences' in entry:
            #    for cross_reference in entry['crossReferences']:
            #        if "id" in cross_reference:
            #            items = cross_reference['id'].split(":")
            #            if items[0] in dqm:
            #                dqm[items[0]].add(items[1])
            #                dbid = cross_reference['id']
            #                break
            ## end grabbing all MOD IDs section

            primary_id = entry['primaryId']
            old_md5 = 'none'
            if mod in old_md5dict and primary_id in old_md5dict[mod] and old_md5dict[mod][primary_id] is not None:
                old_md5 = old_md5dict[mod][primary_id]
            new_md5 = 'none'
            if mod in new_md5dict and primary_id in new_md5dict[mod] and new_md5dict[mod][primary_id] is not None:
                new_md5 = new_md5dict[mod][primary_id]
#             logger.info(f"primaryId {primary_id} old {old_md5}")
#             logger.info(f"primaryId {primary_id} new {new_md5}")

            if old_md5 == new_md5:
                continue

            if old_md5 == 'none':
                logger.info(f"primaryId {primary_id} is new for {mod} but could pre-exist for other mod")
            elif new_md5 == 'none':
                # logger.info(f"{primary_id} in previous dqm submission, not in current")
                fh_mod_report[mod].write(f"{primary_id} in previous dqm submission, not in current")
            else:
                logger.info(f"primaryId {primary_id} has changed")

            # inject the mod corpus association data because if it came from that mod dqm file it should have this entry
            mod_corpus_associations = [{"mod_abbreviation": mod, "mod_corpus_sort_source": "dqm_files", "corpus": True}]
            entry['mod_corpus_associations'] = mod_corpus_associations

            # for debugging changes
            # dqm_entry_text = json.dumps(entry, indent=4)
            # print('dqm ')
            # print(dqm_entry_text)

            dqm_xrefs = dict()
            xrefs = []
            agrs_found = set()
            dbid = None
            if 'crossReferences' in entry:
                for cross_reference in entry['crossReferences']:
                    if "id" in cross_reference:
                        xrefs.append(cross_reference["id"])
                        # logger.info("append xref %s", cross_reference["id"])
                        if "pages" in cross_reference:
                            xref_to_pages[cross_reference["id"]] = cross_reference["pages"]
                        items = cross_reference['id'].split(":")
                        if items[0] in dqm:
                            dqm[items[0]].add(items[1])
                            dbid = cross_reference['id']
            if entry['primaryId'] not in xrefs:
                xrefs.append(entry['primaryId'])
                # logger.info("append primaryId %s", entry['primaryId'])
            for cross_reference in xrefs:
                prefix, identifier, separator = split_identifier(cross_reference, True)
                if prefix not in dqm_xrefs:
                    dqm_xrefs[prefix] = set()
                dqm_xrefs[prefix].add(identifier)
                if prefix in xref_ref:
                    if identifier in xref_ref[prefix]:
                        agr = xref_ref[prefix][identifier]
                        agrs_found.add(agr)

            flag_dqm_prefix_fail = False
            for prefix in dqm_xrefs:
                if len(dqm_xrefs[prefix]) > 1:
                    flag_dqm_prefix_fail = True
                    # logger.info("Notify curator, filename %s, dqm %s has too many identifiers for %s %s", filename, entry['primaryId'], prefix, ', '.join(sorted(dqm_xrefs[prefix])))
                    fh_mod_report[mod].write("dqm %s has too many identifiers for %s %s\n" % (entry['primaryId'], prefix, ', '.join(sorted(dqm_xrefs[prefix]))))
                    report[mod].append((dbid, str(len(dqm_xrefs[prefix])) + prefix + " : " + ', '.join(sorted(dqm_xrefs[prefix])) + " in dqm file"))

            if flag_dqm_prefix_fail:
                continue

            if len(agrs_found) == 0:
                # logger.info("Action : Create New mod %s", entry['primaryId'])
                for key in dqm_keys_to_remove:
                    if key in entry:
                        del entry[key]
                references_to_create.append(entry)
                logger.info(f"create {entry['primaryId']}")
            elif len(agrs_found) > 1:
                # logger.info("Notify curator, dqm %s too many matches %s", entry['primaryId'], ', '.join(sorted(map(lambda x: url_ref_curie_prefix + x, agrs_found))))
                fh_mod_report[mod].write("dqm %s too many matches %s\n" % (entry['primaryId'], ', '.join(sorted(map(lambda x: url_ref_curie_prefix + x, agrs_found)))))
            elif len(agrs_found) == 1:
                # logger.info("Normal %s", entry['primaryId'])
                agr = agrs_found.pop()
                curies_for_citation_update.append(agr)
                agr_url = url_ref_curie_prefix + agr
                flag_aggregate_biblio = False
                flag_aggregate_mod_specific = False
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
                                        flag_aggregate_mod_specific = True
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
                            report[mod].append((dbid, prefix + ":" + ident + " from dqm file is obsolete"))

                        if not dqm_xref_valid_found:
                            if agr_had_prefix:
                                # logger.info("Notify curator, %s had %s %s, dqm submitted %s", agr_url, prefix, ref_xref_valid[agr][prefix], ident)
                                fh_mod_report[mod].write("%s had %s:%s, dqm submitted %s:%s\n" % (agr_url, prefix, ref_xref_valid[agr][prefix], prefix, ident))
                                report[mod].append((dbid, prefix + ":" + ref_xref_valid[agr][prefix] + " in the database doesn't match " + prefix + ":" + ident + " from dqm file"))
                            elif not dqm_xref_obsolete_found:
                                if agr not in xrefs_to_add:
                                    xrefs_to_add[agr] = dict()
                                if prefix not in xrefs_to_add[agr]:
                                    xrefs_to_add[agr][prefix] = dict()
                                if ident not in xrefs_to_add[agr][prefix]:
                                    xrefs_to_add[agr][prefix][ident] = set()
                                xrefs_to_add[agr][prefix][ident].add(filename)
                                # logger.info("Action : Add dqm xref %s %s to agr %s", prefix, ident, agr)  # dealt with below, not needed

                if flag_aggregate_mod_specific:
                    # logger.info("Action : aggregate PMID mod data %s", agr)
                    aggregate_mod_specific_fields_only[agr] = entry
                elif flag_aggregate_biblio:
                    # ignore keywords after initial 2021 Nov load
                    # if 'keywords' in entry:
                    #     entry = clean_up_keywords(mod, entry)
                    # logger.info("Action : aggregate MOD biblio data %s", agr)
                    aggregate_mod_biblio_all[agr] = entry
                    pass
                # check if dqm has no pmid/doi, but pmid/doi in DB
                if 'PMID' not in dqm_xrefs:
                    if 'PMID' in ref_xref_valid[agr]:
                        # logger.info("Notify curator %s has PMID %s, dqm %s does not", agr, ref_xref_valid[agr]['PMID'], entry['primaryId'])
                        fh_mod_report[mod].write("%s has PMID %s, dqm %s does not\n" % (agr_url, ref_xref_valid[agr]['PMID'], entry['primaryId']))
                        # report2[mod].append((dbid, "PMID:" + ref_xref_valid[agr]['PMID'] + " is in the database, but no PMID for this paper in dqm file"))
                if 'DOI' not in dqm_xrefs:
                    if 'DOI' in ref_xref_valid[agr]:
                        # logger.info("Notify curator %s has DOI %s, dqm %s does not", agr, ref_xref_valid[agr]['DOI'], entry['primaryId'])
                        fh_mod_report[mod].write("%s has DOI %s, dqm %s does not\n" % (agr_url, ref_xref_valid[agr]['DOI'], entry['primaryId']))
                        # report2[mod].append((dbid, "DOI:" + ref_xref_valid[agr]['DOI'] + " is in the database, but no DOI for this paper in dqm file"))

        save_new_references_to_file(references_to_create, mod)

        # ## check all db agrId->modId, check each dqm mod still had modId
        # for agr in ref_xref_valid:
        #    agr_url = url_ref_curie_prefix + agr
        #    for prefix in ref_xref_valid[agr]:
        #        if prefix in mods:
        #            # for identifier in ref_xref_valid[agr][prefix]:
        #            identifier = ref_xref_valid[agr][prefix]
        #            ident_found = False
        #            if prefix in dqm:
        #                if identifier in dqm[prefix]:
        #                    ident_found = True
        #            if not ident_found:
        #                # logger.info("Notify curator %s %s %s not in dqm submission", agr_url, prefix, identifier)
        #                fh_mod_report[mod].write("%s %s %s not in dqm submission\n" % (agr_url, prefix, identifier))
        #                dbid = prefix + ":" + identifier
        #                # report3[mod].append((dbid, dbid + " is not in the dqm submission"))

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
                    report[mod].append((dbid, "This paper has multiple identifiers from dqm file: " + conflict_string))
                elif len(xrefs_to_add[agr][prefix]) == 1:
                    for ident in xrefs_to_add[agr][prefix]:
                        xref_id = prefix + ':' + ident
                        new_entry = dict()
                        new_entry["curie"] = xref_id
                        new_entry["reference_curie"] = agr
                        if xref_id in xref_to_pages:
                            new_entry["pages"] = xref_to_pages[xref_id]
                        if live_changes:
                            logger.info(f"add validated dqm xref {xref_id} to agr {agr}")
                            url = 'http://' + api_server + ':' + api_port + '/cross_reference/'
                            headers = generic_api_post(live_changes, url, headers, new_entry, agr, None, None)

        # these take hours for each mod, process about 200 references per minute
        headers = update_db_entries(headers, aggregate_mod_specific_fields_only, live_changes, fh_mod_report[mod], 'mod_specific_fields_only')
        headers = update_db_entries(headers, aggregate_mod_biblio_all, live_changes, fh_mod_report[mod], 'mod_biblio_all')

        output_directory_name = 'process_dqm_update_' + mod
        output_directory_path = base_path + output_directory_name
        if not path.exists(output_directory_path):
            makedirs(output_directory_path)
        if not path.exists(output_directory_path + '/inputs'):
            makedirs(output_directory_path + '/inputs')

        # get list of pmids to process from dqm papers filtered down to references_to_create
        # equivalent to
        # python3 parse_dqm_json_reference.py -f dqm_data_updates_new/ -p
        generate_pmid_data('dqm_data_updates_new/', output_directory_name + '/', mod)

        # read list of pmids to process from file
        pmids_wanted = read_pmid_file(output_directory_name + '/inputs/alliance_pmids')

        # download xml from pubmed into base_path pubmed_xml/
        # equivalent to
        # python3 get_pubmed_xml.py -f inputs/alliance_pmids
        download_pubmed_xml(pmids_wanted)

        # convert xml from base_path pubmed_xml/ to base_path pubmed_json/
        # equivalent to
        # python3 xml_to_json.py -f inputs/alliance_pmids
        generate_json(pmids_wanted, [])

        # if wanting to recursively download comments and corrections, which Ceri does not want
        # untested equivalent to
        # python3 process_many_pmids_to_json.py -s -f inputs/alliance_pmids > logs/log_process_many_pmids_to_json_update_create
        # download_and_convert_pmids(pmids_wanted, True)

        # aggregate dqm data with pubmed data from dqm_data_updates_new/ into <output_directory_name>/sanitized_reference_json/REFERENCE_PUBM[EO]D_<mod>_1.json
        # equivalent to
        # python3 parse_dqm_json_reference.py -f dqm_data_updates_new/ -m all
        aggregate_dqm_with_pubmed('dqm_data_updates_new/', mod, output_directory_name + '/')

        # if wanting to process the pmids from recursive download of comments and corrections, which Ceri does not want
        # untested equivalent to
        # python3 parse_pubmed_json_reference.py -f inputs/pubmed_only_pmids > logs/log_parse_pubmed_json_reference_update_create
        # sanitize_pubmed_json_list(pmids_wanted, [])

        # post generated json to api
        # equivalent to
        # python3 post_reference_to_api.py > logs/log_post_reference_to_api_update_create
        json_filepath = base_path + 'process_dqm_update_' + mod + '/sanitized_reference_json/REFERENCE_PUBMED_' + mod + '_1.json'
        process_results = post_references(json_filepath, 'yes_file_check')
        logger.info(process_results)
        json_filepath = base_path + 'process_dqm_update_' + mod + '/sanitized_reference_json/REFERENCE_PUBMOD_' + mod + '_1.json'
        process_results = post_references(json_filepath, 'yes_file_check')
        logger.info(process_results)

        # post generated json to api
        # equivalent to
        # python3 post_comments_corrections_to_api.py -f inputs/alliance_pmids
        # but if doing recursive should take inputs/all_pmids instead of inputs/alliance_pmids
        post_comments_corrections(pmids_wanted)

        # update s3 md5sum only if prod, to test develop copy file from s3 prod to s3 develop
        # https://s3.console.aws.amazon.com/s3/buckets/agr-literature?prefix=develop%2Freference%2Fmetadata%2Fmd5sum%2F&region=us-east-1&showversions=false#
        # env_state = environ.get('ENV_STATE', 'prod')
        # if env_state == 'build':
        env_state = environ.get('ENV_STATE', 'build')
        if env_state == 'prod':
            merge_md5dict = {}
            merge_md5dict[mod] = {**old_md5dict[mod], **new_md5dict[mod]}
            save_s3_md5data(merge_md5dict, [mod])

        fh_mod_report[mod].close()

        ## update citations
        db_session = create_postgres_session(False)
        for curie in curies_for_citation_update:
            logger.info("Update citation for curie:" + curie)
            update_citation(db_session, curie)
        db_session.close()

        # rows_to_report = report[mod] + report2[mod] + report3[mod]
        send_loading_report(mod, report[mod], report_file_path)


def send_loading_report(mod, rows_to_report, log_path):

    email_recipients = None
    if environ.get('CRONTAB_EMAIL'):
        email_recipients = environ['CRONTAB_EMAIL']
    sender_email = None
    if environ.get('SENDER_EMAIL'):
        sender_email = environ['SENDER_EMAIL']
    sender_password = None
    if environ.get('SENDER_PASSWORD'):
        sender_password = environ['SENDER_PASSWORD']
    reply_to = sender_email
    if environ.get('REPLY_TO'):
        reply_to = environ['REPLY_TO']
    log_url = None
    if environ.get('LOG_URL'):
        log_url = environ['LOG_URL'] + "dqm_load/"
    if email_recipients is None or sender_email is None:
        return

    email_subject = mod + " DQM Loading Report"
    email_message = "<h3>" + mod + " DQM Loading Report</h3>"

    if len(rows_to_report) > 0:
        rows = ''
        i = 0
        (dbid, error) = rows_to_report[0]
        width = len(dbid) * 11

        for x in rows_to_report:
            i += 1
            if i >= 15:
                break
            (dbid, error) = x
            rows = rows + "<tr><th style='text-align:left' width='" + str(width) + "'>" + dbid + ":</th><td>" + error + "</td></tr>"
        email_message = email_message + "<table></tbody>" + rows + "</tbody></table>"

    if log_url:
        email_message = email_message + "<p>Loading log file is available at " + "<a href=" + log_url + ">" + log_url + "</a><p>"
    else:
        email_message = email_message + "<p>Loading log file is available at " + log_path

    (status, message) = send_email(email_subject, email_recipients,
                                   email_message, sender_email, sender_password, reply_to)
    if status == 'error':
        logger.info("Failed sending email to slack: " + message + "\n")


def read_pmid_file(local_path):
    pmids_wanted = []
    base_path = environ.get('XML_PATH')
    file = base_path + local_path
    logger.info(f"Processing file input from {file}")
    with open(file, 'r') as fp:
        pmid = fp.readline()
        while pmid:
            pmids_wanted.append(pmid.rstrip())
            pmid = fp.readline()
    return pmids_wanted


def save_new_references_to_file(references_to_create, mod):
    base_path = environ.get('XML_PATH')
    json_storage_path = base_path + 'dqm_data_updates_new/'
    if not path.exists(json_storage_path):
        makedirs(json_storage_path)
    dqm_data = dict()
    dqm_data['data'] = references_to_create
    # dqm_data['data'] = references_to_create[0:100]	# sample for less papers
    json_filename = json_storage_path + 'REFERENCE_' + mod + '.json'
    write_json(json_filename, dqm_data)


def batch_alchemy(curies, db_dict, batch_size, count_start=0, verbose=False):
    session = create_postgres_session(verbose)
    batch_list = curies[count_start:(count_start + batch_size)]
    refs = session.query(ReferenceModel).\
        filter(ReferenceModel.curie.in_(batch_list)).all()
    session.close()
    for item in refs:
        item_dict = jsonable_encoder(item)
        db_dict[item.curie] = item_dict
    return count_start + batch_size, db_dict


def update_db_entries(headers, entries, live_changes, report_fh, processing_flag):      # noqa: C901
    """
    Take a dict of Alliance Reference curies and DQM MODReferenceTypes to compare against data stored in DB and update to match DQM data.

    :param entries:
    :param processing_flag:
    :return:
    """

    logger.info("processing %s entries for %s", len(entries.keys()), processing_flag)

    remap_keys = dict()
    remap_keys['datePublished'] = 'date_published'
    remap_keys['dateArrivedInPubmed'] = 'date_arrived_in_pubmed'
    remap_keys['dateLastModified'] = 'date_last_modified_in_pubmed'
    remap_keys['crossReferences'] = 'cross_references'
    remap_keys['issueName'] = 'issue_name'
    remap_keys['pubMedType'] = 'pubmed_type'
    remap_keys['meshTerms'] = 'mesh_terms'
    remap_keys['allianceCategory'] = 'category'
    remap_keys['MODReferenceType'] = 'mod_reference_types'
    remap_keys['MODReferenceTypes'] = 'mod_reference_types'
    remap_keys['plainLanguageAbstract'] = 'plain_language_abstract'
    remap_keys['pubmedAbstractLanguages'] = 'pubmed_abstract_languages'
    remap_keys['publicationStatus'] = 'pubmed_publication_status'
    remap_keys['pages'] = 'page_range'
    remap_keys['pageRange'] = 'page_range'
    # remap_keys['resourceAbbreviation'] = 'resource_title'

    # MODReferenceTypes and allianceCategory cannot be auto converted from camel to snake, so have two lists
    # fields_simple_snake = ['title', 'category', 'citation', 'volume', 'pages', 'language', 'abstract', 'publisher', 'issue_name', 'issue_date', 'date_published', 'date_last_modified']
    # fields_simple_camel = ['title', 'allianceCategory', 'citation', 'volume', 'pages', 'language', 'abstract', 'publisher', 'issueName', 'issueDate', 'datePublished', 'dateLastModified']
    # removed some fields that Ceri and Kimberly don't want to update anymore  2022 04 25
    fields_simple_camel = ['title', 'allianceCategory', 'volume', 'pageRange', 'language', 'abstract', 'publisher', 'issueName', 'datePublished']
    # there's no API to update tags

    api_port = environ.get('API_PORT')
    # url_ref_curie_prefix = make_url_ref_curie_prefix()

    # retrieve_method can be fast directly through sqlalchemy in batch mode, or slow one by one through the api
    retrieve_method = 'batch_alchemy'
    # retrieve_method = 'api_one_by_one'

    db_dict = dict()
    if retrieve_method == 'batch_alchemy':
        curies = list(entries.keys())
        curies_count = len(curies)
        start_index = 0
        # verbose = True
        verbose = False
        # api_server = environ.get('API_SERVER', 'localhost')
        # 10000 freezes the server, 1000 works
        # size_per_batch = 1000
        size_per_batch = 200
        while start_index < curies_count:
            for batch_size in [size_per_batch]:
                start_index, db_dict = batch_alchemy(curies, db_dict, batch_size, count_start=start_index, verbose=verbose)
        # for debugging what came from the database
        # print('curies')
        # for agr in db_dict:
        #     print(agr)
        #     db_entry_json = json.dumps(db_dict[agr], indent=4)
        #     print(db_entry_json)

    for agr in entries:
        dqm_entry = entries[agr]
        # to test a particular reference curie
        # PUT THIS BACK
        # if agr != 'AGR:AGR-Reference-0000658372':
        #     continue

        if retrieve_method == 'api_one_by_one':
            # agr_url = url_ref_curie_prefix + agr    # noqa: F841
            api_server = environ.get('API_SERVER', 'localhost')
            url = 'http://' + api_server + ':' + api_port + '/reference/' + agr
            logger.info("get AGR reference info from database %s", url)
            get_return = requests.get(url)
            db_entry = json.loads(get_return.text)
            # logger.info("title %s", response_dict['title'])   # for debugging which reference was found
        elif retrieve_method == 'batch_alchemy':
            db_entry = db_dict[agr]
        else:
            continue

        # to debug
        # db_entry_text = json.dumps(db_entry, indent=4, sort_keys=True)
        # print('db ')
        # print(db_entry_text)

        api_server = environ.get('API_SERVER', 'localhost')
        reference_patch_url = 'http://' + api_server + ':' + api_port + '/reference/' + agr

        # always update mod_reference_types and mod_corpus_associations, whether 'mod_specific_fields_only' or 'mod_biblio_all'
        headers = update_mod_specific_fields(live_changes, headers, agr, dqm_entry, db_entry)

        if processing_flag == 'mod_biblio_all':
            # for debugging changes
            # dqm_entry_text = json.dumps(dqm_entry, indent=4, sort_keys=True)
            # db_entry_text = json.dumps(db_entry, indent=4, sort_keys=True)
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
                    logger.info(f"patch {agr} {dqm_entry['primaryId']} field {field_snake} from db {db_value} to dqm {dqm_value}")
                    update_json[field_snake] = dqm_value

            # ignore keywords after initial 2021 Nov load
            # keywords_changed = compare_keywords(db_entry, dqm_entry)
            # if keywords_changed[0]:
            #     logger.info("patch %s field keywords from db %s to dqm %s", agr, keywords_changed[2], keywords_changed[1])
            #     update_json['keywords'] = keywords_changed[1]

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
                headers = generic_api_patch(live_changes, reference_patch_url, headers, update_json, agr, None, None)

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


# keywords only from ZFIN old papers, will not need in the future
# def compare_keywords(db_entry, dqm_entry):
#     # e.g. ZFIN:ZDB-PUB-150828-18
#     db_keywords = []
#     dqm_keywords = []
#     if 'keywords' in db_entry:
#         if db_entry['keywords'] is not None:
#             db_keywords = db_entry['keywords']
#     lower_db_keywords = [i.lower() for i in db_keywords]
#     if 'keywords' in dqm_entry:
#         if dqm_entry['keywords'] is not None:
#             dqm_keywords = dqm_entry['keywords']
#     lower_dqm_keywords = [i.lower() for i in dqm_keywords]
#     if set(lower_db_keywords) == set(lower_dqm_keywords):
#         return False, None, None
#     else:
#         return True, dqm_keywords, db_keywords


# always update mod_reference_types and mod_corpus_associations, whether 'mod_specific_fields_only' or 'mod_biblio_all'
def update_mod_specific_fields(live_changes, headers, agr, dqm_entry, db_entry):  # noqa: C901
    api_port = environ.get('API_PORT')

    # to debug
    # db_entry_text = json.dumps(db_entry, indent=4, sort_keys=True)
    # print(db_entry_text)

    db_mod_corpus_association = {}
    if 'mod_corpus_association' in db_entry and db_entry['mod_corpus_association'] is not None:
        for db_mca_entry in db_entry['mod_corpus_association']:
            if 'mod' in db_mca_entry and db_mca_entry['mod'] is not None:
                if 'abbreviation' in db_mca_entry['mod'] and db_mca_entry['mod']['abbreviation'] is not None:
                    mod = db_mca_entry['mod']['abbreviation']
                    if mod not in db_mod_corpus_association:
                        db_mod_corpus_association[mod] = {}
                    db_mod_corpus_association[mod]['id'] = db_mca_entry['mod_corpus_association_id']
                    db_mod_corpus_association[mod]['corpus'] = db_mca_entry['corpus']
    # logger.info(agr)
    # logger.info(db_mod_corpus_association)
    if 'mod_corpus_associations' in dqm_entry:
        for dqm_mca_entry in dqm_entry['mod_corpus_associations']:
            if 'mod_abbreviation' in dqm_mca_entry and dqm_mca_entry['mod_abbreviation'] is not None:
                mod = dqm_mca_entry['mod_abbreviation']
                dqm_mca_entry['reference_curie'] = agr
                if mod not in db_mod_corpus_association:
                    logger.info(f"Action : add mod corpus association for {mod} to {agr}")
                    logger.info(dqm_mca_entry)
                    mca_post_url = 'http://' + api_server + ':' + api_port + '/reference/mod_corpus_association/'
                    headers = generic_api_post(live_changes, mca_post_url, headers, dqm_mca_entry, agr, None, None)
                elif dqm_mca_entry['corpus'] != db_mod_corpus_association[mod]['corpus']:
                    logger.info(f"Action : update existing mod corpus association for {db_mod_corpus_association[mod]['id']} to {dqm_mca_entry}")
                    mca_patch_url = 'http://' + api_server + ':' + api_port + '/reference/mod_corpus_association/' + str(db_mod_corpus_association[mod]['id'])
                    logger.info(mca_patch_url)
                    headers = generic_api_patch(live_changes, mca_patch_url, headers, dqm_mca_entry, str(db_mod_corpus_association[mod]['id']), None, None)

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

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', action='store', help='take input from REFERENCE files in full path')
    parser.add_argument('-m', '--mod', action='store', help='which mod, use all or leave blank for all')

    args = vars(parser.parse_args())

    logger.info("starting sort_dqm_json_reference_updates.py")

#     test_ref_xref()

    if args['file']:
        if args['mod']:
            sort_dqm_references(args['file'], args['mod'])
        else:
            sort_dqm_references(args['file'], 'all')

    logger.info("ending sort_dqm_json_reference_updates.py")
