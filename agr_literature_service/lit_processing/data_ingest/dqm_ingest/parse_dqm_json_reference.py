import argparse
import json
import logging.config
import re
import sys
import warnings
from collections import defaultdict
from os import environ, makedirs, path
from typing import Dict

from dotenv import load_dotenv

from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.dqm_processing_utils import \
    simplify_text_keep_digits, strip_string_to_integer
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.report_writer import ReportWriter
from agr_literature_service.lit_processing.data_ingest.reference import PMID_FIELDS, Reference, \
    write_sanitized_references_to_json, load_references_data_from_dqm_json, EXCLUDE_XREF_TYPES
from agr_literature_service.lit_processing.utils.generic_utils import split_identifier
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

init_tmp_dir()

warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

load_dotenv()


# TODO  -p  should also be able to take directory so that dqm updates can run on dqm_data_updates_new/

# pipenv run python parse_dqm_json_reference.py -f dqm_data/ -p  takes about 90 seconds to run
# pipenv run python parse_dqm_json_reference.py -f dqm_data/ -m all   takes 3.5 minutes without looking at pubmed json
# pipenv run python parse_dqm_json_reference.py -f dqm_data/ -m all   takes 13.5 minutes with comparing to pubmed json into output chunks without comparing fields for differences
# pipenv run python parse_dqm_json_reference.py -f dqm_data/ -m all   takes 19 minutes with comparing to pubmed json into output chunks and comparing fields for differences
# pipenv run python parse_dqm_json_reference.py -f dqm_data/ -m all   takes 17 minutes with comparing to pubmed json into output chunks, without comparing fields for differences, splitting into unmerged_pubmed_data for multi_mod pmids.
# pipenv run python parse_dqm_json_reference.py -f dqm_data/ -m all   takes 20.5 minutes with comparing to pubmed json into output chunks and comparing fields for differences, and processing keywords, without using bs4 on dqm inputs
# pipenv run python parse_dqm_json_reference.py -f dqm_data/ -m all   takes 33 minutes with comparing to pubmed json into output chunks and comparing fields for differences, and processing keywords, while using bs4 on dqm inputs

# pipenv run python parse_dqm_json_reference.py -f dqm_data/ -m all   takes 1 hour 32 minutes on agr-literature-dev

#  pipenv run python parse_dqm_json_reference.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/dqm_data/ -m MGI > log_mgi
# Loading .env environment variables...
# Killed
# in 4.5 minutes, logs show it read the last pmid
# rewrote to split into chunks of 100000 entries by pubmed vs pubmod, MGI now runs in 3.5 minutes (without doing data comparison)

# TODO when creating authors, make sure that  first_author: false, corresponding_author: false  otherwise they get a null, which looks different than false when toggling on/off the flags in the UI


logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format='%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',
                    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

base_path = environ.get("XML_PATH", "")

_PMID_RE = re.compile(r'^\s*(?:PMID:)?(\d+)\s*$')


def _numeric_pmid_or_none(val):
    s = str(val)
    m = _PMID_RE.match(s)
    return int(m.group(1)) if m else None


def generate_pmid_data(input_path, output_directory, input_mod, base_input_dir=base_path):  # noqa: C901
    """

    output set of PMID identifiers that will need XML downloaded
    output pmids and the mods that have them

    :param input_path:
    :return:
    """

    logger.info("generating pmid sets from dqm data")

    # RGD should be first in mods list.  if conflicting allianceCategories the later mod gets priority
    mods = ['RGD', 'MGI', 'XB', 'SGD', 'FB', 'ZFIN', 'WB']
    # mods = ['XB']

    if input_mod in mods:
        mods = [input_mod]

    pmid_stats = dict()
    unknown_prefix = set()
    pmid_references = dict()
    for mod in mods:
        pmid_references[mod] = []
    non_pmid_references = dict()
    for mod in mods:
        non_pmid_references[mod] = []

    check_primary_id_is_unique = True
    check_pmid_is_unique = True

    for mod in mods:
        filename = base_input_dir + input_path + '/REFERENCE_' + mod + '.json'
        logger.info("Loading %s data from %s", mod, filename)
        dqm_data = dict()
        try:
            with open(filename, 'r') as f:
                dqm_data = json.load(f)
                f.close()
        except IOError:
            logger.info("No reference data to update from MOD %s", mod)
        if not dqm_data:
            continue

        primary_id_unique = dict()
        pmid_unique = dict()

        for entry in dqm_data['data']:

            if check_primary_id_is_unique:
                try:
                    primary_id_unique[entry['primaryId']] = primary_id_unique[entry['primaryId']] + 1
                except KeyError:
                    primary_id_unique[entry['primaryId']] = 1

            pmid = '0'
            prefix, identifier, separator = split_identifier(entry['primaryId'])
            if prefix == 'PMID':
                pmid = identifier
            elif prefix in mods or prefix == 'Xenbase':
                if 'crossReferences' in entry:
                    for cross_reference in entry['crossReferences']:
                        prefix_xref, identifier_xref, separator_xref = split_identifier(cross_reference['id'])
                        if prefix_xref == 'PMID':
                            pmid = identifier_xref
            else:
                unknown_prefix.add(prefix)

            if pmid != '0':
                try:
                    pmid_stats[pmid].append(mod)
                except KeyError:
                    pmid_stats[pmid] = [mod]
                if check_pmid_is_unique:
                    try:
                        pmid_unique[pmid] = pmid_unique[pmid] + 1
                    except KeyError:
                        pmid_unique[pmid] = 1
                pmid_references[mod].append(pmid)
            else:
                non_pmid_references[mod].append(entry['primaryId'])

        # output check of a mod's non-unique primaryIds
        if check_primary_id_is_unique:
            for primary_id in primary_id_unique:
                if primary_id_unique[primary_id] > 1:
                    logger.info(f"{mod} primary_id {primary_id} has {primary_id_unique[primary_id]} mentions")
        # output check of a mod's non-unique pmids (different from above because could be crossReferences
        if check_pmid_is_unique:
            for pmid in pmid_unique:
                if pmid_unique[pmid] > 1:
                    logger.info(f"{mod} pmid {pmid} has {pmid_unique[pmid]} mentions")

    # output each mod's count of pmid references
    for mod in pmid_references:
        count = len(pmid_references[mod])
        logger.info(f"{mod} has {count} pmid references")

    # output each mod's count of non-pmid references
    for mod in non_pmid_references:
        count = len(non_pmid_references[mod])
        logger.info(f"{mod} has {count} non-pmid references")

    # output actual reference identifiers that are not pmid
    # for mod in non_pmid_references:
    #     for primary_id in non_pmid_references[mod]:
    #         logger.info(f"{mod} non-pmid {primary_id}")

    # if a reference has an unexpected prefix, give a warning
    for prefix in unknown_prefix:
        logger.info("WARNING: unknown prefix %s", prefix)

    # output set of identifiers that will need XML downloaded
    output_pmid_file = base_path + output_directory + 'inputs/alliance_pmids'
    makedirs(base_path + output_directory + 'inputs', exist_ok=True)

    good_pmids = set()
    bad_pmids = []
    for k in pmid_stats:  # k may be 'PMID:12345', '12345', or junk like '0 paper (i)'
        n = _numeric_pmid_or_none(k)
        if n is None:
            bad_pmids.append(k)
            continue
        good_pmids.add(n)

    with open(output_pmid_file, "w") as pmid_file:
        # for pmid in sorted(pmid_stats.iterkeys(), key=int):	# python 2
        for pmid in sorted(good_pmids):
            pmid_file.write(f"{pmid}\n")
    if bad_pmids:
        logger.info("Skipping %d non-PMID entries in alliance_pmids (examples: %s)", len(bad_pmids), bad_pmids[:5])

    # output pmids and the mods that have them
    output_pmid_mods_file = base_path + output_directory + 'pmids_by_mods'
    with open(output_pmid_mods_file, "w") as pmid_mods_file:
        for identifier in pmid_stats:
            ref_mods_list = pmid_stats[identifier]
            count = len(ref_mods_list)
            ref_mods_str = ", ".join(ref_mods_list)
            pmid_mods_file.write("%s\t%s\t%s\n" % (identifier, count, ref_mods_str))
            # logger.info("pmid %s\t%s\t%s", identifier, count, ref_mods_str)
        pmid_mods_file.close()

    # for primary_id in primary_ids:
    #     logger.info("primary_id %s", primary_id)


def load_mod_resource(mods, resource_to_nlm):  # pragma: no cover
    resource_fields = ['primaryId', 'title', 'isoAbbreviation', 'medlineAbbreviation', 'printISSN', 'onlineISSN']
    resource_to_mod = dict()
    resource_to_mod_issn_nlm = dict()
    # test_issn = '0193-4511'
    # if test_issn in resource_to_nlm:
    # logger.info("BEFORE %s has count %s vals %s", test_issn, len(resource_to_nlm[test_issn]),
    # resource_to_nlm[test_issn])
    for mod in mods:
        resource_to_mod[mod] = dict()
        resource_to_mod_issn_nlm[mod] = dict()
        filename = base_path + 'dqm_data/RESOURCE_' + mod + '.json'
        try:
            with open(filename, 'r') as f:
                dqm_data = json.load(f)
                for entry in dqm_data['data']:
                    primary_id = entry['primaryId']
                    values_to_add = []
                    for field in resource_fields:
                        if field in entry:
                            value = simplify_text_keep_digits(entry[field])
                            values_to_add.append(value)
                    if 'abbreviationSynonyms' in entry:
                        for synonym in entry['abbreviationSynonyms']:
                            value = simplify_text_keep_digits(synonym)
                            values_to_add.append(value)
                    for value in values_to_add:
                        if value in resource_to_mod:
                            if primary_id not in resource_to_mod[mod][value]:
                                resource_to_mod[mod][value].append(primary_id)
                        else:
                            resource_to_mod[mod][value] = [primary_id]
                    if 'crossReferences' in entry:
                        for xref_entry in entry['crossReferences']:
                            # if re.match(r"^ISSN:[0-9]+", xref_id):
                            # if entry['primaryId'] == 'FB:FBmultipub_1740':
                            #     logger.info("id %s xref id %s ", entry['primaryId'], xref_entry['id'])
                            issn_group = re.search(r"^ISSN:(.+)$", xref_entry['id'])
                            if issn_group is not None:
                                issn = issn_group[1]
                                issn = simplify_text_keep_digits(issn)
                                # if entry['primaryId'] == 'FB:FBmultipub_1740':
                                #     logger.info("id %s xref id %s issn %s", entry['primaryId'], xref_entry['id'], issn)
                                if issn in resource_to_nlm:
                                    # if entry['primaryId'] == 'FB:FBmultipub_1740':
                                    #     logger.info("id %s xref id %s issn %s nlm %s", entry['primaryId'], xref_entry['id'], issn, resource_to_nlm[issn])
                                    if len(resource_to_nlm[issn]) == 1:
                                        for value in values_to_add:
                                            resource_to_mod_issn_nlm[mod][value] = resource_to_nlm[issn][0]
                                            # if entry['primaryId'] == 'FB:FBmultipub_1740':
                                            #     logger.info("id %s xref id %s issn %s nlm %s value %s nlm %s mod %s", entry['primaryId'], xref_entry['id'], issn, resource_to_nlm[issn], value,  resource_to_nlm[issn][0], mod)
        except IOError as e:
            logger.warning(e)  # most mods don't have a resource file

    return resource_to_mod, resource_to_mod_issn_nlm


def load_pubmed_resource():  # pragma: no cover
    """

    :return:
    """

    # logger.info("Starting load_pubmed_resource")
    resource_data = dict()
    filename = base_path + 'pubmed_resource_json/resource_pubmed_all.json'
    try:
        f = open(filename)
        resource_data = json.load(f)
        f.close()
    except IOError:
        logger.info("No resource_pubmed_all.json file at %s", filename)
    resource_to_nlm = dict()
    resource_to_nlm_highest = dict()
    resource_nlm_to_title = dict()
    resource_fields = ['primaryId', 'nlm', 'title', 'isoAbbreviation', 'medlineAbbreviation', 'printISSN', 'onlineISSN']
    # ZFIN does not have ISSN in crossReferences, and may have already fixed them for 4.1.0
    for entry in resource_data:
        primary_id = entry['primaryId']
        nlm = entry['nlm']
        title = entry['title']
        resource_nlm_to_title[nlm] = title
        for field in resource_fields:
            if field in entry:
                # value = entry[field].lower()
                value = simplify_text_keep_digits(entry[field])
                # if nlm == '8000640':
                #     logger.info("field %s value %s", field, value)
                # if value == '2985088r':
                #     print("2985088r loaded\n")
                if value in resource_to_nlm:
                    # if value == '2985088r':
                    #     print("already in 2985088r to %s loaded\n" % (value))
                    if primary_id not in resource_to_nlm[value]:
                        resource_to_nlm[value].append(primary_id)
                        if strip_string_to_integer(nlm) > strip_string_to_integer(resource_to_nlm_highest[value]):
                            resource_to_nlm_highest[value] = nlm
                        # if value == '2985088r':
                        #     print("append in 2985088r to %s loaded\n" % (value))
                else:
                    resource_to_nlm[value] = [primary_id]
                    resource_to_nlm_highest[value] = nlm
                    # if value == '2985088r':
                    #     print("orig 2985088r to %s loaded\n" % (value))
    # logger.info("End load_pubmed_resource")

    return resource_to_nlm, resource_to_nlm_highest, resource_nlm_to_title


def load_pmid_multi_mods(output_path):  # pragma: no cover
    """

    :return:
    """

    pmid_multi_mods = dict()
    pmid_multi_mods_file = None
    if output_path:
        pmid_multi_mods_file = base_path + output_path + 'pmids_by_mods'
    else:
        pmid_multi_mods_file = base_path + 'pmids_by_mods'
    with open(pmid_multi_mods_file, 'r') as f:
        for line in f:
            cols = line.split("\t")
            if int(cols[1]) > 1:
                pmid_multi_mods[cols[0]] = cols[1]
        f.close()

    return pmid_multi_mods


COMPARE_IF_DQM_EMPTY = False  # do dqm vs pmid comparison even if dqm has no data, by default skip


def find_resource_abbreviation_not_matched_to_nlm_or_res_mod(resource_not_found: Dict[str, Dict[str, int]],
                                                             report_writer: ReportWriter, base_dir=base_path):  # pragma: no cover
    # output resourceAbbreviations not matched to NLMs or resource MOD IDs to a file for attempt to
    # download from other source
    # with get_pubmed_nlm_resource_unmatched.py
    resource_xml_path = base_dir + 'resource_xml/'
    if not path.exists(resource_xml_path):
        makedirs(resource_xml_path)
    resource_abbreviation_not_found_filename = resource_xml_path + 'resource_abbreviation_not_matched'
    already_reported_res_abbrs = set()
    with open(resource_abbreviation_not_found_filename, "w") as resource_abbreviation_not_found_fh:
        for mod, res_abbr_not_found_count in resource_not_found.items():
            for res_abbr, count in res_abbr_not_found_count.items():
                if res_abbr not in already_reported_res_abbrs:
                    resource_abbreviation_not_found_fh.write(res_abbr + "\n")
                    already_reported_res_abbrs.add(res_abbr)
                report_writer.write(
                    mod=mod, report_type="generic",
                    message="Summary: resourceAbbreviation %s not found %s times.\n" % (res_abbr, count))


def report_unexpected_cross_references(cross_reference_types: Dict[str, Dict[str, list]],
                                       exclude_cross_reference_type, report_writer: ReportWriter):  # pragma: no cover
    for mod, xref_type_xrefs_dict in cross_reference_types.items():
        for xref_type, xref_messages in xref_type_xrefs_dict.items():
            if xref_type.lower() in exclude_cross_reference_type:
                logger.info("unexpected crossReferences mod %s type: %s", mod, xref_type)
                report_writer.write(
                    mod=mod, report_type="generic",
                    message="Warning: unexpected crossReferences type: %s\n" % xref_type)
            else:
                for xref_message in xref_messages:
                    logger.info("unexpected crossReferences mod %s type: %s values: %s", mod, xref_type, xref_message)
                    report_writer.write(
                        mod=mod, report_type="generic",
                        message="Warning: unexpected crossReferences type: %s values: %s\n" % (xref_type, xref_message))


def report_multiple_categories_and_dates_while_merging_multimod(alliance_category_dict, report_writer, pmid,
                                                                date_published_set):  # pragma: no cover
    if len(alliance_category_dict) > 1:
        category_mods = "\t".join([alliance_category + ': ' + ", ".join(mods) for alliance_category, mods in
                                   alliance_category_dict.items()])
        report_writer.write(
            mod="multi", report_type="",
            message=f"Multiple allianceCategory pmid {pmid} alliance categories {category_mods}\n")
    if len(date_published_set) > 1:
        dates_published = "\t".join(date_published_set)
        report_writer.write(
            mod="multi", report_type="",
            message=f"Multiple datePublished pmid {pmid} dates published {dates_published}\n")


AGGREGATE_FIELDS_FOR_MULTIMOD_MERGE = ['keywords', 'MODReferenceTypes', 'tags']
ADDITIONAL_FIELDS_FOR_MULTIMOD_MERGE = ['nlm', 'resource']


def merge_multimod_pubmed_and_dqm_data(unmerged_dqm_data_with_pmid: Dict[str, Dict[str, Reference]],
                                       sanitized_pubmed_multi_mod_data, report_writer):  # pragma: no cover
    for pmid in unmerged_dqm_data_with_pmid:
        date_published_set = set()
        alliance_category_dict = defaultdict(set)
        sanitized_reference = Reference(report_writer=report_writer)
        sanitized_reference.set_xrefs_from_unmerged_data(unmerged_dqm_data_with_pmid[pmid].values())
        for mod, entry in unmerged_dqm_data_with_pmid[pmid].items():
            sanitized_reference['primaryId'] = entry['primaryId']
            for pmid_field in PMID_FIELDS:
                if pmid_field in entry and pmid_field not in sanitized_reference:
                    sanitized_reference[pmid_field] = entry[pmid_field]

            for additional_field in ADDITIONAL_FIELDS_FOR_MULTIMOD_MERGE:
                if additional_field in entry and additional_field not in sanitized_reference:
                    sanitized_reference[additional_field] = entry[additional_field]

            for aggregate_field in AGGREGATE_FIELDS_FOR_MULTIMOD_MERGE:
                if aggregate_field in entry:
                    for value in entry[aggregate_field]:
                        if aggregate_field in sanitized_reference:
                            sanitized_reference[aggregate_field].append(value)
                        else:
                            sanitized_reference[aggregate_field] = [value]

            if 'modCorpusAssociations' not in sanitized_reference:
                sanitized_reference['modCorpusAssociations'] = []
            sanitized_reference['modCorpusAssociations'].append(
                Reference.generate_default_mod_corpus_association_for_dqm_data(mod))

            if 'datePublished' in entry:
                date_published_set.add(entry['datePublished'])

            if 'allianceCategory' in entry:
                sanitized_reference['allianceCategory'] = entry['allianceCategory']
                alliance_category_dict[entry['allianceCategory']].add(mod)

        report_multiple_categories_and_dates_while_merging_multimod(alliance_category_dict, report_writer, pmid,
                                                                    date_published_set)
        sanitized_pubmed_multi_mod_data.append(sanitized_reference)


ALLOWED_MODS = ['RGD', 'MGI', 'SGD', 'FB', 'ZFIN', 'WB', 'XB']


def aggregate_dqm_with_pubmed(input_path, input_mod, output_directory, base_dir=base_path):  # noqa: C901
    # reads agr_schemas's reference.json to check for dqm data that's not accounted for there.
    # outputs sanitized json to sanitized_reference_json/
    # does checks on dqm crossReferences.  if primaryId is not PMID, and a crossReference is PubMed,
    # assigns PMID to primaryId and to authors's referenceId.
    # if any reference's author doesn't have author Rank, assign authorRank based on array order.

    # datePublished is a string, not a proper date field

    logger.info("initializing data structures")
    # RGD should be first in mods list.  if conflicting allianceCategories the later mod gets priority
    if input_mod in ALLOWED_MODS:
        mods = [input_mod]
    else:
        mods = ALLOWED_MODS

    # this has to be loaded, if the mod data is hashed by pmid+mod and sorted for those with
    # multiple mods, there's an out-of-memory crash
    pmid_multi_mods = load_pmid_multi_mods(output_directory)

    # use these two lines to properly load resource data, but it takes a bit of time
    resource_to_nlm_id, resource_to_nlm_highest_id, resource_nlm_id_to_title = load_pubmed_resource()
    resource_to_mod, resource_to_mod_issn_nlm = load_mod_resource(mods, resource_to_nlm_id)
    # use these six lines to more quickly test other things that don't need resource data
    # resource_to_nlm_id = dict()
    # resource_to_nlm_highest_id = dict()
    # resource_nlm_id_to_title = dict()
    # resource_to_mod = dict()
    # for mod in mods:
    #     resource_to_mod[mod] = dict()

    json_storage_path = base_path + output_directory + 'sanitized_reference_json/'
    if not path.exists(json_storage_path):
        makedirs(json_storage_path)

    report_writer = ReportWriter(mod_reports_dir=base_path + output_directory + 'report_files/',
                                 multimod_reports_file_path=base_path + output_directory + 'report_files/multi_mod')

    resource_not_found = defaultdict(lambda: defaultdict(int))
    cross_reference_types = defaultdict(lambda: defaultdict(list))

    logger.info("Aggregating DQM and PubMed data from %s using mods %s", input_path, mods)

    sanitized_pubmed_multi_mod_data = []
    unmerged_dqm_data_with_pmid = defaultdict(dict)  # pubmed data by pmid and mod that needs some fields merged
    for mod in mods:
        sanitized_pubmod_data = []
        sanitized_pubmed_single_mod_data = []
        unexpected_mod_properties = set()
        for reference in load_references_data_from_dqm_json(
                filename=base_dir + input_path + '/REFERENCE_' + mod + '.json', report_writer=report_writer):
            unexpected_mod_properties.update(set(reference.get_list_of_unexpected_mod_properties()))
            reference.delete_blank_fields()
            reference.sanitize_and_sort_entry_into_pubmod_pubmed_or_multi(
                mod, cross_reference_types, resource_to_mod_issn_nlm, resource_to_nlm_id, resource_to_nlm_highest_id,
                resource_to_mod, resource_not_found, sanitized_pubmod_data, pmid_multi_mods,
                unmerged_dqm_data_with_pmid, sanitized_pubmed_single_mod_data, resource_nlm_id_to_title,
                compare_if_dqm_empty=COMPARE_IF_DQM_EMPTY, base_path=base_path)

        logger.info("Generating .json output for mod %s", mod)

        write_sanitized_references_to_json(references=sanitized_pubmod_data, entries_size=50000,
                                           base_file_name=json_storage_path + "REFERENCE_PUBMOD_" + mod)
        write_sanitized_references_to_json(references=sanitized_pubmed_single_mod_data, entries_size=50000,
                                           base_file_name=json_storage_path + "REFERENCE_PUBMED_" + mod)

        for unexpected_mod_property in unexpected_mod_properties:
            logger.info("Warning: Unexpected Mod %s Property %s", mod, unexpected_mod_property)

    logger.info("processing unmerged pubmed_data")

    merge_multimod_pubmed_and_dqm_data(unmerged_dqm_data_with_pmid, sanitized_pubmed_multi_mod_data, report_writer)
    logger.info("outputting sanitized pubmed_data")

    write_sanitized_references_to_json(sanitized_pubmed_multi_mod_data, entries_size=100000,
                                       base_file_name=json_storage_path + "REFERENCE_PUBMED_MULTI")
    report_unexpected_cross_references(cross_reference_types, EXCLUDE_XREF_TYPES, report_writer=report_writer)
    find_resource_abbreviation_not_matched_to_nlm_or_res_mod(resource_not_found, report_writer=report_writer,
                                                             base_dir=base_dir)
    report_writer.close()

# check merging with these pmids and mod with data in dqm_merge/ manually generated files, based on pmids_by_mods
# 27639630        3       SGD, WB, ZFIN
# 27656112        2       SGD, WB


# allianceCategory - single value, check they aren't different for entries with same PMID
# MODReferenceTypes - array of hashes, aggregate the hashes
# tags - array of hashes, aggregate the hashes
# resourceAbbreviation - single value, keep for mod data, try to resolve to journal from PMID


if __name__ == "__main__":
    """
    call main start function
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--generate-pmid-data', action='store_true', help='generate pmid outputs, requires -f')
    parser.add_argument('-f', '--file', action='store', help='take input from REFERENCE files in full path',
                        required=True)
    parser.add_argument('-m', '--mod', action='store', help='which mod, use all for all, requires -f')
    parser.add_argument('-d', '--directory', action='store', help='output directory to generate into, requires -f',
                        default='')

    args = vars(parser.parse_args())
    logger.info("starting parse_dqm_json_reference.py")

    # pipenv run python parse_dqm_json_reference.py -f dqm_sample/ -p
    if args['generate_pmid_data']:
        logger.info("Generating PMID files from DQM data")
        generate_pmid_data(base_path, args['file'], args['directory'], 'all')

    # pipenv run python parse_dqm_json_reference.py -f dqm_sample/ -m WB
    # pipenv run python parse_dqm_json_reference.py -f dqm_data_updates_new/ -m all
    elif args['mod']:
        aggregate_dqm_with_pubmed(args['file'], args['mod'], args['directory'])
    else:
        logger.info("No valid processing for directory passed in.  Use -h for help.")
    logger.info("ending parse_dqm_json_reference.py")
