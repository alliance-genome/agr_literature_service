import argparse
from sqlalchemy import text
import json
import sys
import html
import logging.config
import warnings
from os import environ, makedirs, path
from dotenv import load_dotenv
from collections import defaultdict

from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.md5sum_utils import \
    load_database_md5data, generate_new_md5, update_md5sum
from agr_literature_service.api.models import ReferenceModel, ModModel
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import \
    write_json
from agr_literature_service.lit_processing.utils.generic_utils import split_identifier
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import \
    create_postgres_session, sqlalchemy_load_ref_xref
from agr_literature_service.lit_processing.utils.report_utils import send_dqm_loading_report, \
    send_report
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.parse_dqm_json_reference import \
    generate_pmid_data, aggregate_dqm_with_pubmed
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_pubmed_xml import \
    download_pubmed_xml
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import \
    get_pmids_from_exclude_list
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml_to_json import \
    generate_json
from agr_literature_service.lit_processing.data_ingest.post_reference_to_db import post_references
# from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_resources_nlm import \
#    update_resource_pubmed_nlm
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.get_dqm_data import \
    download_dqm_reference_json
from agr_literature_service.lit_processing.utils.db_read_utils import \
    get_references_by_curies, get_curie_to_title_mapping, get_mod_abbreviations
from agr_literature_service.lit_processing.data_ingest.utils.db_write_utils import \
    add_cross_references, update_authors, update_mod_corpus_associations, \
    update_mod_reference_types, mark_not_in_mod_papers_as_out_of_corpus, change_mod_curie_status, \
    add_file_needed_for_new_papers
from agr_literature_service.lit_processing.data_ingest.utils.date_utils import parse_date
from agr_literature_service.api.user import set_global_user_id

# For WB needing 57578 references checked for updating,
# It would take 48 hours to query the database through the API one by one.
# It takes 24 minutes to query in batches of 1000 through batch alchemy.

warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

load_dotenv()

live_change = True
# live_change = False  # for testing purpose
batch_size_for_commit = 250

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

base_path = environ.get('XML_PATH')


def load_pmids_not_found():
    """

    :return:
    """

    pmids_not_found = set()
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


def sort_dqm_references(input_path, input_mod, update_all_papers=False, base_dir=base_path):      # noqa: C901
    """

    # TODO
    # DATA  WBPaper00061683 was primaryId in 2021 11 04, became PMID:34345807
    # in 2022 04 25 update, check that can be found via xref and associated

    :param input_path:
    :param input_mod:
    :return:
    """

    db_session = create_postgres_session(False)
    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)
    mod_to_mod_id = {x.abbreviation: x.mod_id for x in db_session.query(ModModel).all()}
    XREF_to_resource_id = {x["curie"]: x["resource_id"] for x in db_session.execute(text(
        "SELECT curie, resource_id FROM cross_reference WHERE resource_id is not null "
        "AND is_obsolete is False")).mappings().fetchall()}
    rows = db_session.execute(text(
        "SELECT r.curie, c.curie_prefix, c.curie FROM reference r, cross_reference c "
        "WHERE c.curie_prefix = 'CGC' "
        "AND c.is_obsolete is False "
        "AND r.reference_id = c.reference_id")).mappings().fetchall()
    ref_cgcs_valid = defaultdict(lambda: defaultdict(set))
    for agr, prefix, identifier in rows:
        ref_cgcs_valid[agr][prefix].add(identifier)
    db_session.close()

    url_ref_curie_prefix = make_url_ref_curie_prefix()

    mods = get_mod_abbreviations()
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

    dqm = dict()
    for mod in mods:
        if mod == 'XB':
            prefix = "Xenbase"
        else:
            prefix = mod
        dqm[prefix] = set()

    # filename = input_path + '/REFERENCE_' + mod + '.json'

    # xrefs_to_add = dict()
    # aggregate_mod_specific_fields_only = dict()
    # aggregate_mod_biblio_all = dict()

    fh_mod_report = dict()
    report_file_path = ''
    if environ.get('LOG_PATH'):
        report_file_path = path.join(environ['LOG_PATH'], 'dqm_load/')
    if report_file_path and not path.exists(report_file_path):
        makedirs(report_file_path)

    # xref_to_pages = dict()
    # for mod in sorted(files_to_process):
    report = {}
    # report2 = {}
    missing_papers_in_mod = {}
    missing_agr_in_mod = {}
    exclude_pmids = get_pmids_from_exclude_list()

    for mod in sorted(mods):
        filename = path.join(base_dir, input_path) + '/REFERENCE_' + mod + '.json'
        if not path.exists(filename):
            continue
        fp_pmids = get_pmids_from_exclude_list(mod)
        xrefs_to_add = dict()
        clashed_pmids = []
        aggregate_mod_specific_fields_only = dict()
        aggregate_mod_biblio_all = dict()
        xref_to_pages = dict()
        report[mod] = []
        md5sum_to_update = []
        # report2[mod] = []
        missing_papers_in_mod[mod] = []
        missing_agr_in_mod[mod] = []
        log_filename = report_file_path + mod + '_dqm_loading.log'
        fh_mod_report.setdefault(mod, open(log_filename, 'w'))
        references_to_create = []
        cross_reference_to_add = []
        agr_list_for_cross_refs_to_add = []
        logger.info("loading old md5")
        old_md5dict = load_database_md5data([mod])

        logger.info("generating new md5")
        new_md5dict = generate_new_md5(input_path, [mod], base_dir=base_dir)

        mod_ids_used_in_resource = []
        mod_curie_set = set()
        dbid2pmid = {}

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
        for entry in entries:
            counter = counter + 1
            if counter > max_counter:
                break
            if 'primaryId' not in entry or entry['primaryId'] is None:
                continue

            primary_id = entry['primaryId']

            ## do not load the paper that is in the exclude list or
            ## in the false positive list
            pmid = primary_id.replace('PMID:', '')
            if pmid in exclude_pmids or pmid in fp_pmids:
                if pmid in fp_pmids:
                    clashed_pmids.append(pmid)
                continue

            dbid = None
            ## grab all MOD IDs (eg, SGDID) from qdm submission and
            ## save them in memory (in hash dqm)
            if 'crossReferences' in entry:
                for cross_reference in entry['crossReferences']:
                    if "id" in cross_reference:
                        items = cross_reference['id'].split(":")
                        if items[0] in dqm:
                            dqm[items[0]].add(items[1])
                            dbid = cross_reference['id']
                            mod_curie_set.add(dbid)
                            break
            ## end grabbing all MOD ID (curies) section

            if primary_id.startswith("PMID:"):
                dbid2pmid[dbid] = primary_id

            old_md5 = 'none'
            if mod in old_md5dict and primary_id in old_md5dict[mod] and old_md5dict[mod][primary_id] is not None:
                old_md5 = old_md5dict[mod][primary_id]
            new_md5 = 'none'
            if mod in new_md5dict and primary_id in new_md5dict[mod] and new_md5dict[mod][primary_id] is not None:
                new_md5 = new_md5dict[mod][primary_id]
#             logger.info(f"primaryId {primary_id} old {old_md5}")
#             logger.info(f"primaryId {primary_id} new {new_md5}")

            if not update_all_papers and old_md5 == new_md5:
                continue
            if old_md5 == 'none':
                logger.info(f"primaryId {primary_id} is new for {mod} but could pre-exist for other mod")
                md5sum_to_update.append((primary_id, None, new_md5))
            elif new_md5 == 'none':
                # logger.info(f"{primary_id} in previous dqm submission, not in current")
                fh_mod_report[mod].write(f"{primary_id} in previous dqm submission, not in current")
            else:
                logger.info(f"primaryId {primary_id} has changed")
                md5sum_to_update.append((primary_id, old_md5, new_md5))
            # inject the mod corpus association data because if it came from
            # that mod dqm file it should have this entry
            mod_corpus_associations = [{"mod_abbreviation": mod, "mod_corpus_sort_source": "dqm_files", "corpus": True}]
            entry['mod_corpus_associations'] = mod_corpus_associations

            # for debugging changes
            # dqm_entry_text = json.dumps(entry, indent=4)
            # print('dqm ')
            # print(dqm_entry_text)

            dqm_xrefs = dict()
            xrefs = []
            agrs_found = set()
            # dbid = None
            if 'crossReferences' in entry:
                for cross_reference in entry['crossReferences']:
                    if "id" in cross_reference:
                        xref_id = cross_reference["id"]
                        if xref_id in exclude_pmids:
                            continue
                        xref_id = xref_id.replace("DOI:http://dx.doi.org/", "DOI:")
                        xref_id = xref_id.replace("DOI:https://doi.org/", "DOI:")
                        xref_id = xref_id.replace("DOI:doi.org/", "DOI:").replace(" ", "")
                        if xref_id in ["DOI:10.1042/", "DOI:10.1042"]:
                            continue
                        xrefs.append(xref_id)
                        if xref_id in XREF_to_resource_id:
                            mod_ids_used_in_resource.append((dbid, xref_id))
                        if "pages" in cross_reference:
                            xref_to_pages[xref_id] = cross_reference["pages"]

            ### handle special characters
            if 'title' in entry:
                entry['title'] = html.unescape(entry['title'])
            if 'abstract' in entry:
                entry['abstract'] = html.unescape(entry['abstract'])
            if 'publisher' in entry:
                entry['publisher'] = html.unescape(entry['publisher'])
            if 'authors' in entry:
                authors = []
                for author in entry['authors']:
                    for key in author:
                        if 'name' in key:
                            author[key] = html.unescape(author[key])
                    authors.append(author)
                entry['authors'] = authors
            if 'resourceAbbreviation' in entry:
                entry['resourceAbbreviation'] = html.unescape(entry['resourceAbbreviation'])

            if entry['primaryId'] not in xrefs:
                xrefs.append(entry['primaryId'])
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
                if len(dqm_xrefs[prefix]) > 1 and prefix != 'CGC':
                    flag_dqm_prefix_fail = True
                    fh_mod_report[mod].write("dqm %s has too many identifiers for %s %s\n" % (entry['primaryId'], prefix, ', '.join(sorted(dqm_xrefs[prefix]))))
                    report[mod].append((dbid, str(len(dqm_xrefs[prefix])) + " " + prefix + " : " + ', '.join(sorted(dqm_xrefs[prefix])) + " in dqm file"))

            if flag_dqm_prefix_fail:
                continue

            xref_conflict = False

            if len(agrs_found) == 0:
                # logger.info("Action : Create New mod %s", entry['primaryId'])
                for key in dqm_keys_to_remove:
                    if key in entry:
                        del entry[key]
                if dbid not in XREF_to_resource_id:
                    references_to_create.append(entry)
                    logger.info(f"create {entry['primaryId']}")
                else:
                    continue
            elif len(agrs_found) > 1:
                if primary_id.startswith('PMID:'):
                    identifier = primary_id.replace("PMID:", "")
                    if identifier not in xref_ref['PMID']:
                        ## it is a new pmid, so add it
                        references_to_create.append(entry)
                fh_mod_report[mod].write("dqm %s too many matches %s\n" % (entry['primaryId'], ', '.join(sorted(map(lambda x: url_ref_curie_prefix + x, agrs_found)))))
            elif len(agrs_found) == 1:
                if primary_id.startswith('PMID:'):
                    identifier = primary_id.replace("PMID:", "")
                    if identifier not in xref_ref['PMID']:
                        ## it is a new pmid, so add it
                        references_to_create.append(entry)
                agr = agrs_found.pop()
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
                                # agr_had_prefix = True
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
                                else:
                                    agr_had_prefix = True

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
                                xref_conflict = True
                                if prefix == 'CGC' and prefix + ":" + ident in ref_cgcs_valid[agr][prefix]:
                                    xref_conflict = False
                                if xref_conflict:
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

                ## do not do anything if there is any XREF ID conflict
                if xref_conflict:
                    continue

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

        ## save all new papers (both pubmed and non-pubmed) to a json file
        ## an example json file: lit_processing/dqm_data_updates_new/REFERENCE_SGD.json
        try:
            save_new_references_to_file(references_to_create, mod)
        except Exception as e:
            logger.info(f"An error occurred when saving new references to file. ERROR={e}")

        ## check all db agrId->modId, check each dqm mod still had modId
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
                        # fh_mod_report[mod].write("%s %s %s not in dqm submission\n" % (agr_url, prefix, identifier))
                        dbid = prefix + ":" + identifier
                        pmid = None
                        if 'PMID' in ref_xref_valid[agr]:
                            pmid = "PMID:" + ref_xref_valid[agr]['PMID']
                        missing_papers_in_mod[mod].append((dbid, agr, pmid))
                        missing_agr_in_mod[mod].append(agr)

        for agr in xrefs_to_add:
            agr_url = url_ref_curie_prefix + agr
            for prefix in xrefs_to_add[agr]:
                if len(xrefs_to_add[agr][prefix]) > 1 and prefix != 'CGC':
                    conflict_list = []
                    for ident in xrefs_to_add[agr][prefix]:
                        # filenames = ' '.join(sorted(xrefs_to_add[agr][prefix][ident]))
                        conflict_list.append(ident)
                    conflict_string = ', '.join(conflict_list)
                    fh_mod_report[mod].write("%s %s has multiple identifiers from dqms %s\n" % (agr_url, prefix, conflict_string))
                    report[mod].append((dbid, "This paper has multiple identifiers from dqm file: " + conflict_string))
                elif len(xrefs_to_add[agr][prefix]) == 1 or (len(xrefs_to_add[agr][prefix]) > 1 and prefix == 'CGC'):
                    for ident in xrefs_to_add[agr][prefix]:
                        xref_id = prefix + ':' + ident
                        new_entry = dict()
                        new_entry["curie"] = xref_id
                        new_entry["reference_curie"] = agr
                        if xref_id in xref_to_pages:
                            new_entry["pages"] = xref_to_pages[xref_id]
                        agr_list_for_cross_refs_to_add.append(agr)
                        cross_reference_to_add.append(new_entry)

        # print ("aggregate_mod_specific_fields_only=", len(aggregate_mod_specific_fields_only))
        # print ("aggregate_mod_specific_fields_only=", len(aggregate_mod_specific_fields_only))
        # print ("cross_reference_to_add=", len(cross_reference_to_add))

        ## add new cross_reference XREF IDs
        try:
            add_cross_references(cross_reference_to_add, agr_list_for_cross_refs_to_add, logger, live_change)
        except Exception as e:
            logger.info(f"An error occurred when adding cross references. Error={e}")
        ## update references with md5sum changed
        try:
            curie_to_reftypesConflict = update_db_entries(mod, mod_to_mod_id,
                                                          aggregate_mod_specific_fields_only,
                                                          fh_mod_report[mod],
                                                          'mod_specific_fields_only')
        except Exception as e:
            logger.info(f"An error occurred when updating mod specific db entries. Error={e}")
        try:
            update_db_entries(mod, mod_to_mod_id, aggregate_mod_biblio_all, fh_mod_report[mod],
                              'mod_biblio_all')
        except Exception as e:
            logger.info(f"An error occurred when updating mod biblio db entries. Error={e}")
        output_directory_name = 'process_dqm_update_' + mod
        output_directory_path = base_path + output_directory_name
        if not path.exists(output_directory_path):
            makedirs(output_directory_path)
        if not path.exists(output_directory_path + '/inputs'):
            makedirs(output_directory_path + '/inputs')

        # get list of pmids to process from dqm papers filtered down to references_to_create
        # equivalent to python3 parse_dqm_json_reference.py -f dqm_data_updates_new/ -p
        # create a file of the pmids (stored in output_directory_name + '/inputs/alliance_pmids)
        # from this newly generated json file (eg, dqm_data_updates_new/REFERENCE_SGD.json)
        try:
            generate_pmid_data('dqm_data_updates_new/', output_directory_name + '/', mod)
        except Exception as e:
            logger.info(f"An error occurred when generating pmid data. Error={e}")
        # read list of new pmids to process from file (alliance_pmids file)
        try:
            pmids_wanted = read_pmid_file(output_directory_name + '/inputs/alliance_pmids')
        except Exception as e:
            logger.info(f"An error occurred when reading pmid file. Error={e}")
        # download xml from pubmed into base_path pubmed_xml/
        # equivalent to
        # python3 get_pubmed_xml.py -f inputs/alliance_pmids
        try:
            download_pubmed_xml(pmids_wanted)
        except Exception as e:
            logger.info(f"An error occurred when downloading xml files. Error={e}")
        # convert xml from base_path pubmed_xml/ to base_path pubmed_json/
        # equivalent to
        # python3 xml_to_json.py -f inputs/alliance_pmids
        try:
            generate_json(pmids_wanted, [], base_dir=base_dir)
        except Exception as e:
            logger.info(f"An error occurred when generating json files. Error={e}")
        # if wanting to recursively download reference_relations, which Ceri does not want
        # untested equivalent to
        # python3 process_many_pmids_to_json.py -s -f inputs/alliance_pmids > logs/log_process_many_pmids_to_json_update_create
        # download_and_convert_pmids(pmids_wanted, True)

        # aggregate dqm data with pubmed data from dqm_data_updates_new/
        # into <output_directory_name>/sanitized_reference_json/REFERENCE_PUBM[EO]D_<mod>_1.json
        # PubMed papers in REFERENCE_PUBMED_<mod>_1.json
        # non-Pubmed papers in REFERENCE_PUBMOD_<mod>_1.json
        # equivalent to python3 parse_dqm_json_reference.py -f dqm_data_updates_new/ -m all
        try:
            aggregate_dqm_with_pubmed('dqm_data_updates_new/', mod, output_directory_name + '/')
        except Exception as e:
            logger.info(f"An error occurred when calling aggregate_dqm_with_pubmed. Error={e}")
        # if wanting to process the pmids from recursive download of reference_relations, which Ceri does not want
        # untested equivalent to
        # python3 parse_pubmed_json_reference.py -f inputs/pubmed_only_pmids > logs/log_parse_pubmed_json_reference_update_create
        # sanitize_pubmed_json_list(pmids_wanted, [])

        bad_date_published = {}
        # load new PubMed papers (REFERENCE_PUBMED_<mod>_1.json) into database
        json_filepath = base_path + 'process_dqm_update_' + mod + '/sanitized_reference_json/REFERENCE_PUBMED_' + mod + '_1.json'
        try:
            find_unparsable_date_published(json_filepath, bad_date_published)
        except Exception as e:
            logger.info(f"An error occurred when calling find_unparsable_date_published for PubMed papers. Error={e}")
        try:
            post_references(json_filepath, live_change)
        except Exception as e:
            logger.info(f"An error occurred when calling post_references for PubMed papers. Error={e}")

        # load new non PubMed papers (REFERENCE_PUBMOD_<mod>_1.json) into database
        json_filepath = base_path + 'process_dqm_update_' + mod + '/sanitized_reference_json/REFERENCE_PUBMOD_' + mod + '_1.json'
        try:
            find_unparsable_date_published(json_filepath, bad_date_published)
        except Exception as e:
            logger.info(f"An error occurred when calling find_unparsable_date_published for non PubMed papers. Error={e}")
        try:
            post_references(json_filepath, live_change)
        except Exception as e:
            logger.info(f"An error occurred when calling post_references for non PubMed papers. Error={e}")
        # update s3 md5sum only if prod, to test develop copy file from s3 prod to s3 develop
        # https://s3.console.aws.amazon.com/s3/buckets/agr-literature?prefix=develop%2Freference%2Fmetadata%2Fmd5sum%2F&region=us-east-1&showversions=false#
        # env_state = environ.get('ENV_STATE', 'prod')
        # if env_state == 'build':
        env_state = environ.get('ENV_STATE', 'build')
        if env_state != 'test':
            try:
                update_md5sum(md5sum_to_update, mod)
            except Exception as e:
                logger.info(f"An error occurred when updating md5sum for {mod} DQM papers:" + str(e))
        fh_mod_report[mod].close()

        try:
            mark_not_in_mod_papers_as_out_of_corpus(mod, missing_papers_in_mod[mod], logger)
        except Exception as e:
            logger.info("An error occurred when marking not in mod papers as out of corpus:" + str(e))

        try:
            change_mod_curie_status(db_session, mod, mod_curie_set, dbid2pmid, logger)
        except Exception as e:
            logger.info("An error occurred when changing mod curie status:" + str(e))

        agr_to_title = get_curie_to_title_mapping(missing_agr_in_mod[mod])

        try:
            add_file_needed_for_new_papers(db_session, mod, logger=logger)
        except Exception as e:
            logger.info("An error occurred when adding file needed for new papers:" + str(e))

        try:
            send_dqm_loading_report(mod, report[mod], missing_papers_in_mod[mod],
                                    agr_to_title, bad_date_published,
                                    mod_ids_used_in_resource, clashed_pmids,
                                    report_file_path,
                                    curie_to_reftypesConflict)
        except Exception as e:
            logger.info("An error occurred when sending dqm loading report:" + str(e))


def find_unparsable_date_published(json_file, bad_date_published):
    if path.exists(json_file):
        with open(json_file, 'r') as f:
            json_data = json.load(f)
        json_new_data = []
        for entry in json_data:
            primaryId = entry.get('primaryId')
            if entry.get('datePublished'):
                try:
                    # use extended unpacking to capture extra values (if any) without error.
                    parsed_result = parse_date(entry['datePublished'].strip(), False)
                    date_range = parsed_result[0]
                    if date_range is not False:
                        datePublishedStart, datePublishedEnd = date_range
                        entry['datePublishedStart'] = datePublishedStart
                        entry['datePublishedEnd'] = datePublishedEnd
                    else:
                        bad_date_published[primaryId] = entry['datePublished']
                except Exception:
                    bad_date_published[primaryId] = entry['datePublished']
            else:
                bad_date_published[primaryId] = 'No datePublished provided'
            json_new_data.append(entry)
        with open(json_file, 'w') as fw:
            fw.write(json.dumps(json_new_data, indent=4, sort_keys=True))


def read_pmid_file(local_path):
    pmids_wanted = []
    file = base_path + local_path
    logger.info(f"Processing file input from {file}")
    with open(file, 'r') as fp:
        pmid = fp.readline()
        while pmid:
            pmids_wanted.append(pmid.rstrip())
            pmid = fp.readline()
    return pmids_wanted


def save_new_references_to_file(references_to_create, mod):

    json_storage_path = base_path + 'dqm_data_updates_new/'
    if not path.exists(json_storage_path):
        makedirs(json_storage_path)
    dqm_data = dict()
    dqm_data['data'] = references_to_create
    # dqm_data['data'] = references_to_create[0:100]	# sample for less papers
    json_filename = json_storage_path + 'REFERENCE_' + mod + '.json'
    write_json(json_filename, dqm_data)


def update_db_entries(mod, mod_to_mod_id, dqm_entries, report_fh, processing_flag):      # noqa: C901
    """
    Take a dict of Alliance Reference curies and DQM MODReferenceTypes to compare against
    data stored in DB and update to match DQM data.

    :param entries:
    :param processing_flag:
    :return:
    """

    logger.info("processing %s entries for %s", len(dqm_entries.keys()), processing_flag)

    remap_keys = dict()
    remap_keys['datePublished'] = 'date_published'
    remap_keys['datePublishedStart'] = 'date_published_start'
    remap_keys['datePublishedEnd'] = 'date_published_end'
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

    # MODReferenceTypes and allianceCategory cannot be auto converted from camel to snake,
    # so have two lists
    # fields_simple_snake = ['title', 'category', 'citation', 'volume', 'pages', 'language',
    #                        'abstract', 'publisher', 'issue_name', 'issue_date',
    #                        'date_published', 'date_last_modified']
    # fields_simple_camel = ['title', 'allianceCategory', 'citation', 'volume', 'pages',
    #                        'language', 'abstract', 'publisher', 'issueName', 'issueDate',
    #                        'datePublished', 'dateLastModified']
    # removed some fields that Ceri and Kimberly don't want to update anymore  2022 04 25
    fields_simple_camel = ['title', 'allianceCategory', 'volume', 'pageRange', 'language',
                           'abstract', 'publisher', 'issueName', 'datePublished',
                           'datePublishedStart', 'datePublishedEnd']

    # always use sqlalchemy in batch mode to speed up the database query
    batch_db_connection_size = 7500
    curies = list(dqm_entries.keys())
    curies_count = len(curies)
    start_index = 0
    batch_size = 750
    j = 0
    k = 0
    db_session = create_postgres_session(False)
    curie_to_reftypesConflict = {}
    while start_index < curies_count:
        if k > batch_db_connection_size:
            k = 0
            db_session.close()
            db_session = create_postgres_session(False)
        batch_curie_list = curies[start_index:(start_index + batch_size)]
        end_index = start_index + batch_size
        if end_index > len(curies):
            end_index = len(curies)
        logger.info("processing #%s to %s out of %s entries for %s", start_index, end_index, len(curies), processing_flag)
        db_entries = get_references_by_curies(db_session, batch_curie_list)
        start_index = start_index + batch_size

        i = 0
        for agr in batch_curie_list:
            j += 1
            i += 1
            k += 1
            if i > batch_size_for_commit:
                i = 0
                if live_change:
                    db_session.commit()
                else:
                    db_session.rollback()

            dqm_entry = dqm_entries[agr]
            db_entry = db_entries[agr]
            # reference_id = db_entry['reference_id']
            # always update mod_reference_types and mod_corpus_associations, whether
            # 'mod_specific_fields_only' or 'mod_biblio_all'

            logger.info("processing #%s out of %s entries for %s", j, len(dqm_entries.keys()), processing_flag)
            try:
                update_mod_corpus_associations(db_session, mod_to_mod_id, db_entry['reference_id'],
                                               db_entry.get('mod_corpus_association', []),
                                               dqm_entry.get('mod_corpus_associations', []),
                                               logger)
            except Exception as e:
                logger.info(f"An error occurred when calling update_mod_corpus_associations. error={e}")

            db_entry_pubmed_types = db_entry.get('pubmed_types', [])
            if db_entry_pubmed_types is None:
                db_entry_pubmed_types = []
            dqm_entry_pubmed_types = dqm_entry.get('pubmedTypes', [])
            if dqm_entry_pubmed_types is None:
                dqm_entry_pubmed_types = []

            try:
                referenceTypesConflict = update_mod_reference_types(db_session, db_entry['reference_id'],
                                                                    db_entry.get('mod_referencetypes', []),
                                                                    dqm_entry.get('MODReferenceTypes', []),
                                                                    set(db_entry_pubmed_types) | set(dqm_entry_pubmed_types),
                                                                    logger)
                curie_to_reftypesConflict[agr] = referenceTypesConflict
            except Exception as e:
                logger.info(f"{agr}: update_mod_reference_types: error={e}")

            if processing_flag == 'mod_biblio_all':
                update_json = dict()
                for field_camel in fields_simple_camel:
                    if field_camel == 'datePublished' and dqm_entry.get(field_camel):
                        datePublished = str(dqm_entry[field_camel])
                        try:
                            parsed_result = parse_date(datePublished.strip(), False)
                            date_range = parsed_result[0]
                            if date_range is not False:
                                datePublishedStart, datePublishedEnd = date_range
                                dqm_entry['datePublishedStart'] = str(datePublishedStart)[0:10]
                                dqm_entry['datePublishedEnd'] = str(datePublishedEnd)[0:10]
                        except Exception as e:
                            logger.error(f"Error parsing date for {dqm_entry.get('primaryId', 'Unknown')}: {e}")
                    field_snake = field_camel
                    if field_camel in remap_keys:
                        field_snake = remap_keys[field_camel]
                    dqm_value = None
                    db_value = None
                    if field_camel in dqm_entry:
                        dqm_value = dqm_entry[field_camel]
                        if field_snake == 'category' and dqm_value:
                            dqm_value = dqm_value.lower().replace(" ", "_")
                    if field_snake in db_entry:
                        db_value = db_entry[field_snake]
                    if field_camel in ['datePublishedStart', 'datePublishedEnd']:
                        ## db_value looks something like 2020-05-15 00:00:00
                        ## dqm_value looks something like 2020-05-15
                        ## so we don't want to update this
                        db_value = str(db_value)[0:10]
                        if db_value == str(dqm_value):
                            continue
                    if dqm_value != db_value:
                        logger.info(f"patch {agr} {dqm_entry['primaryId']} field {field_snake} from db {db_value} to dqm {dqm_value}")
                        update_json[field_snake] = dqm_value
                        if field_snake == 'category':
                            update_json[field_snake] = update_json[field_snake].replace(' ', '_')

                # ignore keywords after initial 2021 Nov load
                # keywords_changed = compare_keywords(db_entry, dqm_entry)
                # if keywords_changed[0]:
                #     logger.info("patch %s field keywords from db %s to dqm %s", agr, keywords_changed[2], keywords_changed[1])
                #     update_json['keywords'] = keywords_changed[1]

                pub_status_changed = "publication status change place holder"
                pmids_with_pub_status_changed = {}
                try:
                    update_authors(db_session, db_entry['reference_id'],
                                   db_entry.get('author', []),
                                   dqm_entry.get('authors', []),
                                   pub_status_changed,
                                   pmids_with_pub_status_changed,
                                   logger)
                except Exception as e:
                    logger.info(f"An error occurred when calling update_authors: error={e}")

                # if curators want to get reports of how resource change, put this back,
                # but we're comparing resource titles with dqm resource abbreviations,
                # so they often differ even if they would match if we had a resource
                # lookup by names and synonyms. e.g. WBPaper00000007 has db title
                # "Comptes rendus des seances de l'Academie des sciences. Serie D,
                # Sciences naturelles" and dqm abbreviation "C R Seances Acad Sci D"
                # resource_changed = compare_resource(db_entry, dqm_entry)
                # if resource_changed[0]:
                #     logger.info("%s dqm resource differs db %s dqm %s", agr_url, resource_changed[2], resource_changed[1])
                #     report_fh.write("%s dqm resource differs db '%s' dqm '%s'\n" % (agr_url, resource_changed[2], resource_changed[1]))
                if update_json:
                    try:
                        db_session.query(ReferenceModel).filter_by(curie=agr).update(update_json)
                        logger.info("The reference row for curie = " + agr + " has been updated.")
                    except Exception as e:
                        logger.info("An error occurred when updating reference row for curie = " + agr + " " + str(e))

        if live_change:
            db_session.commit()
        else:
            db_session.rollback()
    db_session.close()
    return curie_to_reftypesConflict


# def compare_resource(db_entry, dqm_entry):
#    db_resource_title = ''
#    dqm_resource_abbreviation = ''
#    if 'resource_title' in db_entry:
#        if db_entry['resource_title'] is not None:
#            db_resource_title = db_entry['resource_title']
#    if 'resourceAbbreviation' in dqm_entry:
#        if dqm_entry['resourceAbbreviation'] is not None:
#            dqm_resource_abbreviation = dqm_entry['resourceAbbreviation']
#    if db_resource_title.lower() == dqm_resource_abbreviation.lower():
#        return False, None, None
#    else:
#        return True, dqm_resource_abbreviation, db_resource_title


if __name__ == "__main__":
    """
    call main start function
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', action='store', help='take input from REFERENCE files in full path')
    parser.add_argument('-m', '--mod', action='store', help='which mod, use all or leave blank for all')
    parser.add_argument('-a', '--all', action='store_true', help='update all papers, not just ones with md5sum changed')

    args = vars(parser.parse_args())

    logger.info("starting sort_dqm_json_reference_updates.py")

    # download the dqm file(s) from mod(s)
    env_state = environ.get('ENV_STATE', 'build')
    if env_state != 'test':
        download_dqm_reference_json()
        # update_resource_pubmed_nlm()

    dqm_path = args['file'] if args['file'] else "dqm_data"
    mods = [args['mod']] if args['mod'] else get_mod_abbreviations()
    for mod in mods:
        if mod == 'SGD':
            continue
        try:
            sort_dqm_references(dqm_path, mod, args['all'])
        except Exception as e:
            send_report(f"{mod} DQM Loading Failed",
                        f"Error message: {e}")
    logger.info("ending sort_dqm_json_reference_updates.py")
