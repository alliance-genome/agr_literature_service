
import json
import urllib.request

import argparse
import re

from os import environ, path, makedirs
import logging
import logging.config

from helper_file_processing import split_identifier, write_json, clean_up_keywords

from dotenv import load_dotenv

import bs4
import warnings
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


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--generate-pmid-data', action='store_true', help='generate pmid outputs, requires -f')
parser.add_argument('-f', '--file', action='store', help='take input from REFERENCE files in full path')
parser.add_argument('-m', '--mod', action='store', help='which mod, use all for all, requires -f')
parser.add_argument('-c', '--commandline', nargs='*', action='store', help='placeholder for process_single_pmid.py')
# parser.add_argument('-d', '--database', action='store_true', help='take input from database query')
# parser.add_argument('-r', '--restapi', action='store', help='take input from rest api')
# parser.add_argument('-s', '--sample', action='store_true', help='test sample input from hardcoded entries')
# parser.add_argument('-u', '--url', action='store', help='take input from entries in file at url')

args = vars(parser.parse_args())

# base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'
base_path = environ.get('XML_PATH')


# def split_identifier(identifier, ignore_error=False):
#     """
#     Split Identifier
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


def generate_pmid_data(input_path):      # noqa: C901
    """

    output set of PMID identifiers that will need XML downloaded
    output pmids and the mods that have them

    :param input_path:
    :return:
    """

    logger.info("generating pmid sets from dqm data")

    # RGD should be first in mods list.  if conflicting allianceCategories the later mod gets priority
    mods = ['RGD', 'MGI', 'SGD', 'FB', 'ZFIN', 'WB']
    # mods = ['SGD']

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
        filename = base_path + input_path + '/REFERENCE_' + mod + '.json'
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
            elif prefix in mods:
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
                    print("%s primary_id %s has %s mentions" % (mod, primary_id, primary_id_unique[primary_id]))
        # output check of a mod's non-unique pmids (different from above because could be crossReferences
        if check_pmid_is_unique:
            for pmid in pmid_unique:
                if pmid_unique[pmid] > 1:
                    print("%s pmid %s has %s mentions" % (mod, pmid, pmid_unique[pmid]))

    # output each mod's count of pmid references
    for mod in pmid_references:
        count = len(pmid_references[mod])
        print("%s has %s pmid references" % (mod, count))
        # logger.info("%s has %s pmid references", mod, count)

    # output each mod's count of non-pmid references
    for mod in non_pmid_references:
        count = len(non_pmid_references[mod])
        print("%s has %s non-pmid references" % (mod, count))
        # logger.info("%s has %s non-pmid references", mod, count)

    # output actual reference identifiers that are not pmid
    # for mod in non_pmid_references:
    #     for primary_id in non_pmid_references[mod]:
    #         print("%s non-pmid %s" % (mod, primary_id))
    #         # logger.info("%s non-pmid %s", mod, primary_id)

    # if a reference has an unexpected prefix, give a warning
    for prefix in unknown_prefix:
        logger.info("WARNING: unknown prefix %s", prefix)

    # output set of identifiers that will need XML downloaded
    output_pmid_file = base_path + 'inputs/alliance_pmids'
    with open(output_pmid_file, "w") as pmid_file:
        # for pmid in sorted(pmid_stats.iterkeys(), key=int):	# python 2
        for pmid in sorted(pmid_stats, key=int):
            pmid_file.write("%s\n" % (pmid))
        pmid_file.close()

    # output pmids and the mods that have them
    output_pmid_mods_file = base_path + 'pmids_by_mods'
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


def simplify_text_keep_digits(text):
    """

    :param text:
    :return:
    """

    no_html = re.sub('<[^<]+?>', '', str(text))
    stripped = re.sub(r"[^a-zA-Z0-9]+", "", str(no_html))
    clean = stripped.lower()
    return clean


def simplify_text(text):
    """

    :param text:
    :return:
    """

    no_html = re.sub('<[^<]+?>', '', str(text))
    stripped = re.sub(r"[^a-zA-Z]+", "", str(no_html))
    clean = stripped.lower()
    return clean


def compare_dqm_pubmed(fh, pmid, field, dqm_data, pubmed_data):
    """

    :param fh:
    :param pmid:
    :param field:
    :param dqm_data:
    :param pubmed_data:
    :return:
    """

    # to_return = ''
    # logger.info("%s\t%s\t%s\t%s", field, pmid, dqm_data, pubmed_data)
    dqm_clean = simplify_text(dqm_data)
    pubmed_clean = simplify_text(pubmed_data)
    if dqm_clean != pubmed_clean:
        fh.write("dqm and pubmed differ\t%s\t%s\t%s\t%s\n" % (field, pmid, dqm_data, pubmed_data))
        # logger.info("%s\t%s\t%s\t%s", field, pmid, dqm_clean, pubmed_clean)
        # logger.info("%s\t%s\t%s\t%s", field, pmid, dqm_data, pubmed_data)
    # else:
    #     logger.info("%s\t%s\t%s", field, pmid, 'GOOD')


def chunks(list, size):
    """

    :param list:
    :param size:
    :return:
    """

    for i in range(0, len(list), size):
        yield list[i:i + size]


def populate_expected_cross_reference_type():
    # if pages should be stripped from some crossReferences, make this a dict and set some to
    # have or not have, and strip when matched against this
    expected_cross_reference_type = set()
    expected_cross_reference_type.add('PMID:'.lower())
    expected_cross_reference_type.add('PMCID:PMC'.lower())
    expected_cross_reference_type.add('DOI:'.lower())
    expected_cross_reference_type.add('DOI:/S'.lower())
    expected_cross_reference_type.add('DOI:IJIv'.lower())
    expected_cross_reference_type.add('WB:WBPaper'.lower())
    expected_cross_reference_type.add('SGD:S'.lower())
    expected_cross_reference_type.add('RGD:'.lower())
    expected_cross_reference_type.add('MGI:'.lower())
    expected_cross_reference_type.add('ISBN:'.lower())
    expected_cross_reference_type.add('FB:FBrf'.lower())
    expected_cross_reference_type.add('ZFIN:ZDB-PUB-'.lower())

    # when getting pubmed data and merging mod cross references, was excluding these types, but
    # now merging so long as the type does not already exist from pubmed (mods have DOIs not in PubMed)
    pubmed_not_dqm_cross_reference_type = set()
    # pubmed_not_dqm_cross_reference_type.add('PMID:'.lower())
    # pubmed_not_dqm_cross_reference_type.add('PMCID:PMC'.lower())
    # pubmed_not_dqm_cross_reference_type.add('DOI:'.lower())
    # pubmed_not_dqm_cross_reference_type.add('DOI:/S'.lower())
    # pubmed_not_dqm_cross_reference_type.add('DOI:IJIv'.lower())

    exclude_cross_reference_type = set()
    exclude_cross_reference_type.add('WB:WBTransgene'.lower())
    exclude_cross_reference_type.add('WB:WBGene'.lower())
    exclude_cross_reference_type.add('WB:WBVar'.lower())

    return expected_cross_reference_type, exclude_cross_reference_type, pubmed_not_dqm_cross_reference_type


def load_mod_resource(mods, resource_to_nlm):
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
            logger.warning(e)		# most mods don't have a resource file

    return resource_to_mod, resource_to_mod_issn_nlm


def load_pubmed_resource():
    """

    :return:
    """

    # logger.info("Starting load_pubmed_resource")
    filename = base_path + 'pubmed_resource_json/resource_pubmed_all.json'
    f = open(filename)
    resource_data = json.load(f)
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


def strip_string_to_integer(string):
    """

    :param string:
    :return:
    """

    return int("".join(filter(lambda x: x.isdigit(), string)))


def load_pmid_multi_mods():
    """

    :return:
    """

    pmid_multi_mods = dict()
    pmid_multi_mods_file = base_path + 'pmids_by_mods'
    with open(pmid_multi_mods_file, 'r') as f:
        for line in f:
            cols = line.split("\t")
            if int(cols[1]) > 1:
                pmid_multi_mods[cols[0]] = cols[1]
        f.close()

    return pmid_multi_mods


def aggregate_dqm_with_pubmed(input_path, input_mod):      # noqa: C901
    # reads agr_schemas's reference.json to check for dqm data that's not accounted for there.
    # outputs sanitized json to sanitized_reference_json/
    # does checks on dqm crossReferences.  if primaryId is not PMID, and a crossReference is PubMed,
    # assigns PMID to primaryId and to authors's referenceId.
    # if any reference's author doesn't have author Rank, assign authorRank based on array order.
    cross_ref_no_pages_ok_fields = ['DOI', 'PMID', 'PMC', 'PMCID']
    pmid_fields = ['authors', 'volume', 'title', 'pages', 'issueName', 'issueDate', 'datePublished', 'dateArrivedInPubmed', 'dateLastModified', 'abstract', 'pubMedType', 'publisher', 'meshTerms', 'plainLanguageAbstract', 'pubmedAbstractLanguages', 'publicationStatus']
    # single_value_fields = ['volume', 'title', 'pages', 'issueName', 'issueDate', 'datePublished', 'dateArrivedInPubmed', 'dateLastModified', 'abstract', 'pubMedType', 'publisher']
    single_value_fields = ['volume', 'title', 'pages', 'issueName', 'issueDate', 'datePublished', 'dateArrivedInPubmed', 'dateLastModified', 'abstract', 'publisher', 'plainLanguageAbstract', 'pubmedAbstractLanguages', 'publicationStatus']
    replace_value_fields = ['authors', 'pubMedType', 'meshTerms']
    # date_fields = ['issueDate', 'datePublished', 'dateArrivedInPubmed', 'dateLastModified']
    # datePublished is a string, not a proper date field
    date_fields = ['issueDate', 'dateArrivedInPubmed', 'dateLastModified']

    compare_if_dqm_empty = False		# do dqm vs pmid comparison even if dqm has no data, by default skip

    # mods = ['SGD', 'RGD', 'FB', 'WB', 'MGI', 'ZFIN']
    # RGD should be first in mods list.  if conflicting allianceCategories the later mod gets priority
    # mods = ['RGD', 'SGD', 'FB', 'MGI', 'ZFIN', 'WB']
    mods = ['RGD', 'MGI', 'SGD', 'FB', 'ZFIN', 'WB']
    if input_mod in mods:
        mods = [input_mod]

    # this has to be loaded, if the mod data is hashed by pmid+mod and sorted for those with
    # multiple mods, there's an out-of-memory crash
    pmid_multi_mods = load_pmid_multi_mods()

    # use these two lines to properly load resource data, but it takes a bit of time
    resource_to_nlm, resource_to_nlm_highest, resource_nlm_to_title = load_pubmed_resource()
    resource_to_mod, resource_to_mod_issn_nlm = load_mod_resource(mods, resource_to_nlm)
    # use these six lines to more quickly test other things that don't need resource data
    # resource_to_nlm = dict()
    # resource_to_nlm_highest = dict()
    # resource_nlm_to_title = dict()
    # resource_to_mod = dict()
    # for mod in mods:
    #     resource_to_mod[mod] = dict()

    expected_cross_reference_type, exclude_cross_reference_type, pubmed_not_dqm_cross_reference_type = populate_expected_cross_reference_type()

    resource_not_found = dict()
    cross_reference_types = dict()

    json_storage_path = base_path + 'sanitized_reference_json/'
    if not path.exists(json_storage_path):
        makedirs(json_storage_path)

    report_file_path = base_path + 'report_files/'
    if not path.exists(report_file_path):
        makedirs(report_file_path)

    fh_mod_report = dict()
    fh_mod_report_title = dict()
    fh_mod_report_differ = dict()
    # fh_mod_report_xrefs = dict()
    fh_mod_report_resource_unmatched = dict()
    fh_mod_report_reference_no_resource = dict()
    for mod in mods:
        resource_not_found[mod] = dict()
        # cross_reference_types[mod] = set()
        cross_reference_types[mod] = dict()
        filename = report_file_path + mod + '_main'
        filename_title = report_file_path + mod + '_dqm_pubmed_differ_title'
        filename_differ = report_file_path + mod + '_dqm_pubmed_differ_other'
        # filename_xrefs = report_file_path + mod + '_dqm_pubmed_differ_xrefs'
        filename_resource_unmatched = report_file_path + mod + '_resource_unmatched'
        filename_reference_no_resource = report_file_path + mod + '_reference_no_resource'
        fh_mod_report.setdefault(mod, open(filename, 'w'))
        fh_mod_report_title.setdefault(mod, open(filename_title, 'w'))
        fh_mod_report_differ.setdefault(mod, open(filename_differ, 'w'))
        # fh_mod_report_xrefs.setdefault(mod, open(filename_xrefs, 'w'))
        fh_mod_report_resource_unmatched.setdefault(mod, open(filename_resource_unmatched, 'w'))
        fh_mod_report_reference_no_resource.setdefault(mod, open(filename_reference_no_resource, 'w'))

    multi_report_filename = base_path + 'report_files/multi_mod'
    fh_mod_report.setdefault('multi', open(multi_report_filename, 'w'))
    # these are not needed, there are no dqm vs pubmed comparisons for multiple mods
    # multi_report_filename_title = base_path + 'report_files/multi_mod_dqm_pubmed_title_differ'
    # multi_report_filename_differ = base_path + 'report_files/multi_mod_dqm_pubmed_differ'
    # fh_mod_report_title.setdefault('multi', open(multi_report_filename_title, 'w'))
    # fh_mod_report_differ.setdefault('multi', open(multi_report_filename_differ, 'w'))

    logger.info("Aggregating DQM and PubMed data from %s using mods %s", input_path, mods)
    agr_schemas_reference_json_url = 'https://raw.githubusercontent.com/alliance-genome/agr_schemas/master/ingest/resourcesAndReferences/reference.json'
    schema_data = dict()
    with urllib.request.urlopen(agr_schemas_reference_json_url) as url:
        schema_data = json.loads(url.read().decode())
        # print(schema_data)

    # this has been obsoleted by generating from parse_dqm_json_resource.py but leaving in here until a full run works
    # fb have fb ids for resources, but from the resourceAbbreviation and pubmed xml's nlm, we can update
    # fb resource data to primary key off of nlm
    # fb_resource_abbreviation_to_nlm = dict()

    sanitized_pubmed_multi_mod_data = []
    unmerged_pubmed_data = dict()			# pubmed data by pmid and mod that needs some fields merged
    for mod in mods:
        filename = input_path + '/REFERENCE_' + mod + '.json'
        logger.info("Processing %s", filename)
        unexpected_mod_properties = set()
        dqm_data = dict()
        with open(filename, 'r') as f:
            dqm_data = json.load(f)
            f.close()
        entries = dqm_data['data']
        sanitized_pubmod_data = []
        sanitized_pubmed_single_mod_data = []
        for entry in entries:
            is_pubmod = True
            pmid = None
            update_primary_id = False
            primary_id = entry['primaryId']
            orig_primary_id = entry['primaryId']
#             print("primaryId %s" % (entry['primaryId']))
            blank_fields = set()
            for entry_property in entry:
                if entry_property not in schema_data['properties']:
                    unexpected_mod_properties.add(entry_property)
                if entry_property in single_value_fields:
                    if entry[entry_property] == "":
                        blank_fields.add(entry_property)
            too_many_xref_per_type_failure = False
            for entry_field in blank_fields:
                del entry[entry_field]

            # need to process crossReferences once to reassign primaryId if PMID and filter out
            # unexpected crossReferences,
            # then again later to clean up crossReferences that get data from pubmed xml (once the PMID is known)
            if 'crossReferences' in entry:
                expected_cross_references = []
                dqm_xrefs = dict()
                for cross_reference in entry['crossReferences']:
                    prefix, identifier, separator = split_identifier(cross_reference["id"])
                    if prefix not in dqm_xrefs:
                        dqm_xrefs[prefix] = set()
                    dqm_xrefs[prefix].add(identifier)
                    if 'pages' in cross_reference:
                        if len(cross_reference["pages"]) > 1:
                            fh_mod_report[mod].write("mod %s primaryId %s has cross reference identifier %s with multiple web pages %s\n" % (mod, primary_id, cross_reference["id"], cross_reference["pages"]))
                            # logger.info("mod %s primaryId %s has cross reference identifier %s with web pages %s", mod, primary_id, cross_reference["id"], cross_reference["pages"])
                        else:
                            if not re.match(r"^PMID:[0-9]+", orig_primary_id):
                                if cross_reference["pages"][0] == 'PubMed':
                                    xref_id = cross_reference["id"]
                                    if re.match(r"^PMID:[0-9]+", xref_id):
                                        update_primary_id = True
                                        primary_id = xref_id
                                        entry['primaryId'] = xref_id
                    else:
                        if prefix not in cross_ref_no_pages_ok_fields:
                            fh_mod_report[mod].write("mod %s primaryId %s has cross reference identifier %s without web pages\n" % (mod, primary_id, cross_reference["id"]))
                            # logger.debug("mod %s primaryId %s has cross reference %s without pages", mod, primary_id, cross_reference["id"])

                    id = cross_reference['id']
                    cross_ref_type_group = re.search(r"^([^0-9]+)[0-9]", id)
                    if cross_ref_type_group is not None:
                        if cross_ref_type_group[1].lower() not in expected_cross_reference_type:
                            if cross_ref_type_group[1] in cross_reference_types[mod]:
                                cross_reference_types[mod][cross_ref_type_group[1]].append(primary_id + ' ' + id)
                            else:
                                cross_reference_types[mod][cross_ref_type_group[1]] = [primary_id + ' ' + id]
                            # cross_reference_types[mod].add(cross_ref_type_group[1])
                        if cross_ref_type_group[1].lower() not in exclude_cross_reference_type:
                            expected_cross_references.append(cross_reference)
                entry['crossReferences'] = expected_cross_references
                for prefix in dqm_xrefs:
                    if len(dqm_xrefs[prefix]) > 1:
                        too_many_xref_per_type_failure = True
                        fh_mod_report[mod].write("mod %s primaryId %s has too many identifiers for %s %s\n" % (mod, primary_id, prefix, ', '.join(sorted(dqm_xrefs[prefix]))))

            else:
                fh_mod_report[mod].write("mod %s primaryId %s has no cross references\n" % (mod, primary_id))
                # logger.info("mod %s primaryId %s has no cross references", mod, primary_id)

            if too_many_xref_per_type_failure:
                continue

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
                    fh_mod_report[mod].write("Warning: PMID %s does not have PubMed xml, from Mod %s primary_id %s\n" % (pmid, mod, orig_primary_id))
                    # logger.info("Warning: PMID %s does not have PubMed xml, from Mod %s primary_id %s", pmid, mod, orig_primary_id)

            if is_pubmod:
                # print("primaryKey %s is None" % (primary_id))
                if 'authors' in entry:
                    all_authors_have_rank = True
                    for author in entry['authors']:
                        author['correspondingAuthor'] = False
                        author['firstAuthor'] = False
                        if 'authorRank' not in author:
                            all_authors_have_rank = False
                    if all_authors_have_rank is False:
                        authors_with_rank = []
                        for i in range(len(entry['authors'])):
                            author = entry['authors'][i]
                            author['authorRank'] = i + 1
                            authors_with_rank.append(author)
                        entry['authors'] = authors_with_rank
                    if update_primary_id:
                        authors_updated = []
                        for author in entry['authors']:
                            author['referenceId'] = primary_id
                            authors_updated.append(author)
                        entry['authors'] = authors_updated
                if 'crossReferences' in entry:
                    sanitized_cross_references = []
                    for cross_reference in entry['crossReferences']:
                        id = cross_reference['id']
                        prefix, identifier, separator = split_identifier(id)
                        # cross references came from the mod, but some had a pmid (e.g. 24270275) that is no longer at PubMed, so do not add to cross_references
                        if prefix.lower() != 'pmid':
                            sanitized_cross_references.append(cross_reference)
                    entry['crossReferences'] = sanitized_cross_references
                if 'keywords' in entry:
                    entry = clean_up_keywords(mod, entry)
                if 'resourceAbbreviation' in entry:
                    # journal = entry['resourceAbbreviation'].lower()
                    # if journal not in resource_to_nlm:
                    journal_simplified = simplify_text_keep_digits(entry['resourceAbbreviation'])
                    if journal_simplified != '':
                        # logger.info("CHECK mod %s journal_simplified %s", mod, journal_simplified)
                        # highest priority to mod resources from dqm resource file with an issn in crossReferences that maps to a single nlm
                        if journal_simplified in resource_to_mod_issn_nlm[mod]:
                            entry['nlm'] = [resource_to_mod_issn_nlm[mod][journal_simplified]]
                            entry['resource'] = resource_to_mod_issn_nlm[mod][journal_simplified]
                        # next highest priority to resource names that map to an nlm
                        elif journal_simplified in resource_to_nlm:
                            nlm_list = resource_to_nlm[journal_simplified]
                            # a resourceAbbreviation can resolve to multiple NLMs, so we cannot use a list of NLMs to get a single canonical NLM title
                            entry['nlm'] = nlm_list
                            entry['resource'] = 'NLM:' + resource_to_nlm_highest[journal_simplified]
                            if len(nlm_list) > 1:		# e.g. ZFIN:ZDB-PUB-020604-2  FB:FBrf0009739  WB:WBPaper00000557
                                multiple_nlms = ", ".join(nlm_list)
                                fh_mod_report[mod].write("primaryId %s has resourceAbbreviation %s mapping to multiple NLMs %s.\n" % (primary_id, entry['resourceAbbreviation'], multiple_nlms))
                        # next highest priority to resource names that are in the dqm resource submission
                        elif journal_simplified in resource_to_mod[mod]:
                            entry['modResources'] = resource_to_mod[mod][journal_simplified]
                            if len(resource_to_mod[mod][journal_simplified]) > 1:
                                multiple_mod_resources = ", ".join(resource_to_mod[mod][journal_simplified])
                                fh_mod_report[mod].write("primaryId %s has resourceAbbreviation %s mapping to multiple MOD resources %s.\n" % (primary_id, entry['resourceAbbreviation'], multiple_mod_resources))
                            else:
                                entry['resource'] = resource_to_mod[mod][journal_simplified][0]
                        else:
                            fh_mod_report_resource_unmatched[mod].write("primaryId %s has resourceAbbreviation %s not in NLM nor DQM resource file.\n" % (primary_id, entry['resourceAbbreviation']))
                            if entry['resourceAbbreviation'] in resource_not_found[mod]:
                                resource_not_found[mod][entry['resourceAbbreviation']] += 1
                            else:
                                resource_not_found[mod][entry['resourceAbbreviation']] = 1
                else:
                    fh_mod_report_reference_no_resource[mod].write("primaryId %s does not have a resourceAbbreviation.\n" % (primary_id))
            else:
                # pmid_fields = ['authors', 'volume', 'title', 'pages', 'issueName', 'issueDate', 'datePublished', 'dateArrivedInPubmed', 'dateLastModified', 'abstract', 'pubMedType', 'publisher', 'meshTerms']
                for pmid_field in pmid_fields:
                    if pmid_field in single_value_fields:
                        pmid_data = ''
                        dqm_data = ''
                        if pmid_field in pubmed_data:
                            if pmid_field in date_fields:
                                pmid_data = pubmed_data[pmid_field]['date_string']
                            else:
                                pmid_data = pubmed_data[pmid_field]
                        if pmid_field in entry:
                            dqm_data = entry[pmid_field]
                        if dqm_data != '':
                            dqm_data = bs4.BeautifulSoup(dqm_data, "html.parser")
                        # UNCOMMENT to output log of data comparison between dqm and pubmed
                        if (dqm_data != '') or (compare_if_dqm_empty):
                            if pmid_field == 'title':
                                compare_dqm_pubmed(fh_mod_report_title[mod], pmid, pmid_field, dqm_data, pmid_data)
                            else:
                                compare_dqm_pubmed(fh_mod_report_differ[mod], pmid, pmid_field, dqm_data, pmid_data)
                        if pmid_data != '':
                            entry[pmid_field] = pmid_data
                        if pmid_field == 'datePublished':
                            if (pmid_data == '') and (dqm_data != ''):
                                entry[pmid_field] = dqm_data
                    elif pmid_field in replace_value_fields:
                        entry[pmid_field] = []
                        if pmid_field in pubmed_data:
                            # logger.info("PMID %s pmid_field %s data %s", pmid, pmid_field, pubmed_data[pmid_field])
                            entry[pmid_field] = pubmed_data[pmid_field]

                if 'authors' in entry:
                    for author in entry['authors']:
                        author['correspondingAuthor'] = False
                        author['firstAuthor'] = False

                sanitized_cross_references = []
                pubmed_xrefs = dict()
                if 'crossReferences' in pubmed_data:
                    sanitized_cross_references = pubmed_data['crossReferences']
                    for cross_reference in pubmed_data['crossReferences']:
                        id = cross_reference['id']
                        prefix, identifier, separator = split_identifier(id)
                        pubmed_xrefs[prefix] = identifier
                if 'crossReferences' in entry:
                    for cross_reference in entry['crossReferences']:
                        id = cross_reference['id']
                        prefix, identifier, separator = split_identifier(id)
                        if prefix in pubmed_xrefs:
                            if pubmed_xrefs[prefix].lower() != identifier.lower():
                                # fh_mod_report_xrefs[mod].write("primaryId %s has xref %s PubMed has %s%s%s\n" % (primary_id, id, prefix, separator, pubmed_xrefs[prefix]))	# maybe split that out later, but probably not
                                fh_mod_report[mod].write("primaryId %s has xref %s PubMed has %s%s%s\n" % (primary_id, id, prefix, separator, pubmed_xrefs[prefix]))
                        else:
                            sanitized_cross_references.append(cross_reference)
                entry['crossReferences'] = sanitized_cross_references

                if 'nlm' in pubmed_data:
                    nlm = pubmed_data['nlm']
                    entry['nlm'] = ['NLM:' + nlm]
                    entry['resource'] = 'NLM:' + nlm
                    # this has been obsoleted by generating from parse_dqm_json_resource.py but leaving in here until a full run works
                    # if mod == 'FB':
                    #     if 'resourceAbbreviation' in entry:
                    #         fb_resource_abbreviation_to_nlm[entry['resourceAbbreviation']] = nlm
                    if nlm in resource_nlm_to_title:
                        # logger.info("PMID %s has NLM %s setting to title %s", pmid, nlm, resource_nlm_to_title[nlm])
                        entry['resourceAbbreviation'] = resource_nlm_to_title[nlm]
                    nlm_simplified = simplify_text_keep_digits(pubmed_data['nlm'])
                    if nlm_simplified not in resource_to_nlm:
                        fh_mod_report[mod].write("NLM value %s from PMID %s XML does not map to a proper resource.\n" % (pubmed_data['nlm'], pmid))
                else:
                    if 'is_journal' in pubmed_data:
                        fh_mod_report[mod].write("PMID %s does not have an NLM resource.\n" % (pmid))

                if 'keywords' not in entry:
                    entry['keywords'] = []
                else:
                    # e.g. 9882485 25544291 24201188 31188077
                    entry = clean_up_keywords(mod, entry)
                    # remove this after checking it works well
                    # if mod == 'ZFIN':
                    #     if 'keywords' in entry:
                    #         if entry['keywords'][0] == '':
                    #             entry['keywords'] = []
                    #         else:
                    #             zfin_value = entry['keywords'][0]
                    #             zfin_value = str(bs4.BeautifulSoup(zfin_value, "html.parser"))
                    #             comma_count = 0
                    #             semicolon_count = 0
                    #             if ", " in zfin_value:
                    #                 comma_count = zfin_value.count(',')
                    #             if "; " in zfin_value:
                    #                 semicolon_count = zfin_value.count(';')
                    #             if (comma_count == 0) and (semicolon_count == 0):
                    #                 entry['keywords'] = [zfin_value]
                    #             elif comma_count >= semicolon_count:
                    #                 entry['keywords'] = zfin_value.split(", ")
                    #             else:
                    #                 entry['keywords'] = zfin_value.split("; ")
                    # else:
                    #     keywords = []
                    #     for mod_keyword in entry['keywords']:
                    #         mod_keyword = str(bs4.BeautifulSoup(mod_keyword, "html.parser"))
                    #         keywords.append(mod_keyword)
                    #     entry['keywords'] = keywords

                if 'keywords' in pubmed_data:
                    # aggregate for all MODs except ZFIN, which has misformed data and can't fix it.
                    # 19308247 aggregates keywords for WB
                    for mod_keyword in pubmed_data['keywords']:
                        if mod_keyword.upper() not in map(str.upper, entry['keywords']):
                            entry['keywords'].append(mod_keyword)

# # datePublished, keywords, and crossReferences, MODReferenceTypes, tags, allianceCategory, resourceAbbreviation
# # datePublished - pubmed value, if no value use mod's, if multiple mod's different, error
# # resourceAbbreviation - if pmid always use NLM's name
# # keywords - aggregate
# # tags - aggregate
# # MODReferenceTypes - aggregate
# # crossReferences - aggregate and clean up pages
# # allianceCategory - single value, error if there's more than 1 unique value because of different MODs

            if is_pubmod:
                sanitized_pubmod_data.append(entry)
            else:
                if pmid in pmid_multi_mods:
                    # logger.info("MULTIPLE pmid %s mod %s", pmid, mod)
                    if pmid in unmerged_pubmed_data:
                        unmerged_pubmed_data[pmid][mod] = entry
                    else:
                        unmerged_pubmed_data[pmid] = dict()
                        unmerged_pubmed_data[pmid][mod] = entry
                else:
                    sanitized_pubmed_single_mod_data.append(entry)

        logger.info("Generating .json output for mod %s", mod)

        entries_size = 50000
        sanitized_pubmod_list = list(chunks(sanitized_pubmod_data, entries_size))
        for i in range(len(sanitized_pubmod_list)):
            dict_to_output = sanitized_pubmod_list[i]
            json_filename = json_storage_path + 'REFERENCE_PUBMOD_' + mod + '_' + str(i + 1) + '.json'
            write_json(json_filename, dict_to_output)

        sanitized_pubmed_list = list(chunks(sanitized_pubmed_single_mod_data, entries_size))
        for i in range(len(sanitized_pubmed_list)):
            dict_to_output = sanitized_pubmed_list[i]
            json_filename = json_storage_path + 'REFERENCE_PUBMED_' + mod + '_' + str(i + 1) + '.json'
            write_json(json_filename, dict_to_output)

        for unexpected_mod_property in unexpected_mod_properties:
            logger.info("Warning: Unexpected Mod %s Property %s", mod, unexpected_mod_property)

    logger.info("processing unmerged pubmed_data")

    aggregate_fields = ['keywords', 'MODReferenceTypes', 'tags']
    additional_fields = ['nlm', 'resource']

    for pmid in unmerged_pubmed_data:
        # this was when trying to send all mod-pubmed data to a hash, and sort those with muliple mods, but script crashed out of memory
        # if len(unmerged_pubmed_data[pmid]) > 1:
        #     this_mods = ", ".join(unmerged_pubmed_data[pmid])
        #     logger.info("pmid %s length %s", pmid, this_mods)
        # else:
        #     sanitized_pubmod_data.append(entry)

        date_published_set = set()
        alliance_category_dict = dict()
        sanitized_entry = dict()
        cross_references_dict = dict()
        for mod in unmerged_pubmed_data[pmid]:
            entry = unmerged_pubmed_data[pmid][mod]

            sanitized_entry['primaryId'] = entry['primaryId']

            for pmid_field in pmid_fields:
                if pmid_field in entry:
                    if pmid_field not in sanitized_entry:
                        sanitized_entry[pmid_field] = entry[pmid_field]

            for additional_field in additional_fields:
                if additional_field in entry:
                    if additional_field not in sanitized_entry:
                        sanitized_entry[additional_field] = entry[additional_field]

            if 'datePublished' in entry:
                date_published_set.add(entry['datePublished'])

            if 'allianceCategory' in entry:
                sanitized_entry['allianceCategory'] = entry['allianceCategory']
                if not entry['allianceCategory'] in alliance_category_dict:
                    alliance_category_dict[entry['allianceCategory']] = set()
                alliance_category_dict[entry['allianceCategory']].add(mod)

            for aggregate_field in aggregate_fields:
                if aggregate_field in entry:
                    for value in entry[aggregate_field]:
                        if aggregate_field in sanitized_entry:
                            sanitized_entry[aggregate_field].append(value)
                        else:
                            sanitized_entry[aggregate_field] = [value]

            if 'crossReferences' in entry:
                for cross_ref in entry['crossReferences']:
                    id = cross_ref['id']
                    pages = []
                    if 'pages' in cross_ref:
                        pages = cross_ref['pages']
                    cross_references_dict[id] = pages

        for cross_ref_id in cross_references_dict:
            pages = cross_references_dict[cross_ref_id]
            sanitized_cross_ref_dict = dict()
            sanitized_cross_ref_dict["id"] = cross_ref_id
            if len(pages) > 0:
                sanitized_cross_ref_dict["pages"] = pages
            if 'crossReferences' in sanitized_entry:
                sanitized_entry['crossReferences'].append(sanitized_cross_ref_dict)
            else:
                sanitized_entry['crossReferences'] = [sanitized_cross_ref_dict]

        if 'allianceCategory' in sanitized_entry:
            if len(alliance_category_dict) > 1:
                multiple_list = []
                for alliance_category in alliance_category_dict:
                    mods = ", ".join(alliance_category_dict[alliance_category])
                    multiple_list.append(alliance_category + ': ' + mods)
                multiple_alliance_categories = "\t".join(multiple_list)
                # logger.info("MULTIPLE ALLIANCE CATEGORY pmid %s alliance categories %s", pmid, multiple_alliance_categories)
                fh_mod_report['multi'].write("Multiple allianceCategory pmid %s alliance categories %s\n" % (pmid, multiple_alliance_categories))
        if len(date_published_set) > 1:
            dates_published = "\t".join(date_published_set)
            # logger.info("MULTIPLE DATES PUBLISHED pmid %s dates published %s", pmid, dates_published)
            fh_mod_report['multi'].write("Multiple datePublished pmid %s dates published %s\n" % (pmid, dates_published))

        sanitized_pubmed_multi_mod_data.append(sanitized_entry)

    logger.info("outputting sanitized pubmed_data")

    entries_size = 100000
    sanitized_pubmed_list = list(chunks(sanitized_pubmed_multi_mod_data, entries_size))
    for i in range(len(sanitized_pubmed_list)):
        dict_to_output = sanitized_pubmed_list[i]
        json_filename = json_storage_path + 'REFERENCE_PUBMED_MULTI_' + str(i + 1) + '.json'
        write_json(json_filename, dict_to_output)

    resource_abbreviations_not_found = set()
    for mod in resource_not_found:
        for resource_abbrev in resource_not_found[mod]:
            resource_abbreviations_not_found.add(resource_abbrev)
            fh_mod_report[mod].write("Summary: resourceAbbreviation %s not found %s times.\n" % (resource_abbrev, resource_not_found[mod][resource_abbrev]))

    for mod in cross_reference_types:
        # for cross_reference_type in cross_reference_types[mod]:
        #     logger.info("unexpected crossReferences mod %s type: %s", mod, cross_reference_type)
        #     fh_mod_report[mod].write("Warning: unexpected crossReferences type: %s\n" % (cross_reference_type))
        for cross_reference_type in cross_reference_types[mod]:
            if cross_reference_type.lower() in exclude_cross_reference_type:
                logger.info("unexpected crossReferences mod %s type: %s", mod, cross_reference_type)
                fh_mod_report[mod].write("Warning: unexpected crossReferences type: %s\n" % (cross_reference_type))
            else:
                for cross_reference_type_message in cross_reference_types[mod][cross_reference_type]:
                    logger.info("unexpected crossReferences mod %s type: %s values: %s", mod, cross_reference_type, cross_reference_type_message)
                    fh_mod_report[mod].write("Warning: unexpected crossReferences type: %s values: %s\n" % (cross_reference_type, cross_reference_type_message))

    # output resourceAbbreviations not matched to NLMs or resource MOD IDs to a file for attempt to
    # download from other source
    # with get_pubmed_nlm_resource_unmatched.py
    resource_xml_path = base_path + 'resource_xml/'
    if not path.exists(resource_xml_path):
        makedirs(resource_xml_path)
    resource_abbreviation_not_found_filename = resource_xml_path + 'resource_abbreviation_not_matched'
    with open(resource_abbreviation_not_found_filename, "w") as resource_abbreviation_not_found_fh:
        for resource_abbrev in resource_abbreviations_not_found:
            resource_abbreviation_not_found_fh.write(resource_abbrev + "\n")
        resource_abbreviation_not_found_fh.close()

    fh_mod_report['multi'].close()
    # these are not needed, there are no dqm vs pubmed comparisons for multiple mods
    # fh_mod_report_title['multi'].close()
    # fh_mod_report_differ['multi'].close()
    for mod in fh_mod_report:
        fh_mod_report[mod].close()
    for mod in fh_mod_report_title:
        fh_mod_report_title[mod].close()
    for mod in fh_mod_report_differ:
        fh_mod_report_differ[mod].close()
#     for mod in fh_mod_report_xrefs:
#         fh_mod_report_xrefs[mod].close()
    for mod in fh_mod_report_resource_unmatched:
        fh_mod_report_resource_unmatched[mod].close()
    for mod in fh_mod_report_reference_no_resource:
        fh_mod_report_reference_no_resource[mod].close()

    # this has been obsoleted by generating from parse_dqm_json_resource.py but leaving in here until a full run works
    # fb have fb ids for resources, but from the resourceAbbreviation and pubmed xml's nlm, we can update fb resource data to primary key off of nlm
    # json_filename = base_path + 'FB_resourceAbbreviation_to_NLM.json'
    # write_json(json_filename, fb_resource_abbreviation_to_nlm)


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

    logger.info("starting parse_dqm_json_reference.py")

    if args['file']:
        # pipenv run python parse_dqm_json_reference.py -p
        if args['generate_pmid_data']:
            logger.info("Generating PMID files from DQM data")
            generate_pmid_data(args['file'])

        # pipenv run python parse_dqm_json_reference.py -f dqm_sample/ -m WB
        # pipenv run python parse_dqm_json_reference.py -f dqm_data_updates_new/ -m all
        elif args['mod']:
            aggregate_dqm_with_pubmed(args['file'], args['mod'])

        else:
            logger.info("No valid processing for directory passed in.  Use -h for help.")

    elif args['commandline']:
        logger.info("placeholder for process_single_pmid.py")

    else:
        logger.info("No valid processing flag passed in.  Use -h for help.")

    logger.info("ending parse_dqm_json_reference.py")
