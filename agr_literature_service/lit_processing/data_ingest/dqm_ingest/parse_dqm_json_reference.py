import argparse
import json
import logging.config
import os.path
import re
import sys
import urllib.request
import warnings
from collections import defaultdict
from os import environ, makedirs, path
from typing import Dict

import bs4
from dotenv import load_dotenv

from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.dqm_processing_utils import clean_up_keywords
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import write_json
from agr_literature_service.lit_processing.utils.generic_utils import split_identifier
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

init_tmp_dir()

warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

load_dotenv()


class ReportWriter:
    def __init__(self, mod_reports_dir, multimod_reports_file_path):
        self.mod_reports_dir = mod_reports_dir
        self.multimod_reports_file_path = multimod_reports_file_path
        self.report_file_handlers = defaultdict(dict)
        if not path.exists(mod_reports_dir):
            makedirs(mod_reports_dir)

    def get_report_file_name(self, mod, report_type):
        if report_type == "multi":
            return self.multimod_reports_file_path
        else:
            return self.mod_reports_dir + mod + "_" + REPORT_TYPE_FILE_NAME_POSTFIX[report_type]

    def write(self, mod: str, report_type: str, message: str):
        try:
            self.report_file_handlers[report_type][mod].write(message)
        except KeyError:
            self.report_file_handlers[report_type][mod] = open(
                self.get_report_file_name(mod=mod, report_type=report_type), "w")
            self.report_file_handlers[report_type][mod].write(message)

    def close(self):
        for mod_handlers_dict in self.report_file_handlers.values():
            for file_handler in mod_handlers_dict.values():
                file_handler.close()

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
    with open(output_pmid_file, "w") as pmid_file:
        # for pmid in sorted(pmid_stats.iterkeys(), key=int):	# python 2
        for pmid in sorted(pmid_stats, key=int):
            pmid_file.write("%s\n" % (pmid))
        pmid_file.close()

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


def compare_dqm_pubmed(mod, report_type, pmid, field, dqm_data, pubmed_data, report_writer: ReportWriter):

    # to_return = ''
    # logger.info("%s\t%s\t%s\t%s", field, pmid, dqm_data, pubmed_data)
    dqm_clean = simplify_text(dqm_data)
    pubmed_clean = simplify_text(pubmed_data)
    if dqm_clean != pubmed_clean:
        report_writer.write(
            mod=mod, report_type=report_type,
            message="dqm and pubmed differ\t%s\t%s\t%s\t%s\n" % (field, pmid, dqm_data, pubmed_data))
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
    expected_cross_reference_type.add('Xenbase:XB-ART-'.lower())
    expected_cross_reference_type.add('NLM:'.lower())
    expected_cross_reference_type.add('ISSN:'.lower())

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
    exclude_cross_reference_type.add('Xenbase:XB-GENEPAGE-'.lower())

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
            logger.warning(e)  # most mods don't have a resource file

    return resource_to_mod, resource_to_mod_issn_nlm


def load_pubmed_resource():
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


def strip_string_to_integer(string):
    """

    :param string:
    :return:
    """

    return int("".join(filter(lambda x: x.isdigit(), string)))


def load_pmid_multi_mods(output_path):
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


REPORT_TYPE_FILE_NAME_POSTFIX = {
    "generic": "main",
    "title": "dqm_pubmed_differ_title",
    "differ": "dqm_pubmed_differ_other",
    "resource_unmatched": "resource_unmatched",
    "reference_no_resource": "reference_no_resource"
}


CROSS_REF_NO_PAGES_OK_FIELDS = ['DOI', 'PMID', 'PMC', 'PMCID', 'ISBN']


def validate_xref_pages(cross_reference, prefix, mod, primary_id, report_writer: ReportWriter):
    if 'pages' in cross_reference:
        if len(cross_reference["pages"]) > 1:
            report_writer.write(mod=mod, report_type="generic",
                                message="mod %s primaryId %s has cross reference identifier %s with "
                                        "multiple web pages %s\n" % (mod, primary_id, cross_reference["id"],
                                                                     cross_reference["pages"]))
        else:
            return True
    else:
        if prefix not in CROSS_REF_NO_PAGES_OK_FIELDS:
            report_writer.write(mod=mod, report_type="generic",
                                message="mod %s primaryId %s has cross reference identifier %s without "
                                        "web pages\n" % (mod, primary_id, cross_reference["id"]))
    return False


def process_xrefs_and_find_pmid_if_necessary(reference, mod, report_writer: ReportWriter, original_primary_id,
                                             expected_cross_reference_type,
                                             exclude_cross_reference_type,
                                             cross_reference_types: Dict[str, Dict[str, list]]):
    # need to process crossReferences once to reassign primaryId if PMID and filter out
    # unexpected crossReferences,
    # then again later to clean up crossReferences that get data from pubmed xml (once the PMID is known)
    primary_id = original_primary_id
    update_primary_id = False
    too_many_xref_per_type_failure = False
    if 'crossReferences' not in reference:
        report_writer.write(mod=mod, report_type="generic",
                            message="mod %s primaryId %s has no cross references\n" % (mod, primary_id))
    else:
        expected_cross_references = []
        dqm_xrefs = defaultdict(set)
        for cross_reference in reference['crossReferences']:
            prefix, identifier, separator = split_identifier(cross_reference["id"])
            needs_pmid_extraction = validate_xref_pages(cross_reference=cross_reference, prefix=prefix, mod=mod,
                                                        primary_id=primary_id, report_writer=report_writer)
            if needs_pmid_extraction:
                if not re.match(r"^PMID:[0-9]+", original_primary_id) and cross_reference["pages"][0] == 'PubMed' \
                        and re.match(r"^PMID:[0-9]+", cross_reference["id"]):
                    update_primary_id = True
                    reference['primaryId'] = cross_reference["id"]
                    primary_id = cross_reference["id"]

            cross_ref_type_group = re.search(r"^([^0-9]+)[0-9]", cross_reference['id'])
            if cross_ref_type_group is not None:
                if cross_ref_type_group[1].lower() not in expected_cross_reference_type:
                    cross_reference_types[mod][cross_ref_type_group[1]].append(primary_id + ' ' + cross_reference['id'])
                if cross_ref_type_group[1].lower() not in exclude_cross_reference_type:
                    dqm_xrefs[prefix].add(identifier)
                    expected_cross_references.append(cross_reference)
        reference['crossReferences'] = expected_cross_references
        for prefix, identifiers in dqm_xrefs.items():
            if len(identifiers) > 1:
                too_many_xref_per_type_failure = True
                report_writer.write(mod=mod, report_type="generic",
                                    message="mod %s primaryId %s has too many identifiers for %s %s\n" % (
                                        mod, primary_id, prefix, ', '.join(sorted(dqm_xrefs[prefix]))))

    if too_many_xref_per_type_failure:
        return None, None
    else:
        return primary_id, update_primary_id


def get_schema_data_from_alliance():
    agr_schemas_reference_json_url = 'https://raw.githubusercontent.com/alliance-genome/agr_schemas/master/ingest/resourcesAndReferences/reference.json'
    with urllib.request.urlopen(agr_schemas_reference_json_url) as url:
        schema_data = json.loads(url.read().decode())
        schema_data['properties']['mod_corpus_associations'] = 'injected_okay'
    return schema_data


def load_pubmed_data_if_present(primary_id, mod, original_primary_id, report_writer: ReportWriter):
    pmid_group = re.search(r"^PMID:([0-9]+)", primary_id)
    pmid = None
    is_pubmod = True
    pubmed_data = {}
    if pmid_group is not None:
        pmid = pmid_group[1]
        filename = base_path + 'pubmed_json/' + pmid + '.json'
        try:
            with open(filename, 'r') as f:
                pubmed_data = json.load(f)
                is_pubmod = False
        except IOError:
            report_writer.write(mod=mod, report_type="generic",
                                message="Warning: PMID %s does not have PubMed xml, from Mod %s primary_id "
                                        "%s\n" % (pmid, mod, original_primary_id))
    return pubmed_data, is_pubmod, pmid


def set_resource_info_from_abbreviation(entry, mod, primary_id, resource_to_mod_issn_nlm, resource_to_nlm_id,
                                        resource_to_nlm_highest_id, resource_to_mod, resource_not_found,
                                        report_writer: ReportWriter):
    if 'resourceAbbreviation' in entry:
        journal_simplified = simplify_text_keep_digits(entry['resourceAbbreviation'])
        if journal_simplified:
            # logger.info("CHECK mod %s journal_simplified %s", mod, journal_simplified)
            # highest priority to mod resources from dqm resource file with an issn in crossReferences that maps to a single nlm
            if journal_simplified in resource_to_mod_issn_nlm[mod]:
                entry['nlm'] = [resource_to_mod_issn_nlm[mod][journal_simplified]]
                entry['resource'] = resource_to_mod_issn_nlm[mod][journal_simplified]
            # next highest priority to resource names that map to an nlm
            elif journal_simplified in resource_to_nlm_id:
                # a resourceAbbreviation can resolve to multiple NLMs, so we cannot use a list of NLMs to get a single canonical NLM title
                entry['nlm'] = resource_to_nlm_id[journal_simplified]
                entry['resource'] = 'NLM:' + resource_to_nlm_highest_id[journal_simplified]
                if len(resource_to_nlm_id[journal_simplified]) > 1:  # e.g. ZFIN:ZDB-PUB-020604-2  FB:FBrf0009739  WB:WBPaper00000557
                    report_writer.write(
                        mod=mod, report_type="generic",
                        message="primaryId %s has resourceAbbreviation %s mapping to multiple NLMs %s.\n" % (
                            primary_id, entry['resourceAbbreviation'], ", ".join(resource_to_nlm_id[journal_simplified])))
            # next highest priority to resource names that are in the dqm resource submission
            elif journal_simplified in resource_to_mod[mod]:
                entry['modResources'] = resource_to_mod[mod][journal_simplified]
                if len(resource_to_mod[mod][journal_simplified]) > 1:
                    report_writer.write(
                        mod=mod, report_type="generic",
                        message="primaryId %s has resourceAbbreviation %s mapping to multiple MOD "
                                "resources %s.\n" % (primary_id, entry['resourceAbbreviation'],
                                                     ", ".join(resource_to_mod[mod][journal_simplified])))
                else:
                    entry['resource'] = resource_to_mod[mod][journal_simplified][0]
            else:
                report_writer.write(
                    mod=mod, report_type="resource_unmatched",
                    message="primaryId %s has resourceAbbreviation %s not in NLM nor DQM resource "
                            "file.\n" % (primary_id, entry['resourceAbbreviation']))
                resource_not_found[mod][entry['resourceAbbreviation']] += 1
    else:
        report_writer.write(mod=mod, report_type="reference_no_resource",
                            message="primaryId %s does not have a resourceAbbreviation.\n" % primary_id)


def process_pubmod_authors_xrefs_keywords(entry, update_primary_id, primary_id, mod):
    if 'authors' in entry:
        all_authors_have_rank = all(['authorRank' in author for author in entry['authors']])
        for author in entry['authors']:
            author['correspondingAuthor'] = False
            author['firstAuthor'] = False
        if not all_authors_have_rank:
            for idx, _ in enumerate(entry['authors']):
                entry['authors'][idx]['authorRank'] = idx + 1
        if update_primary_id:
            for idx, _ in enumerate(entry['authors']):
                entry['authors'][idx]['referenceId'] = primary_id
    if 'crossReferences' in entry:
        entry['crossReferences'] = [cross_reference for cross_reference in entry['crossReferences'] if
                                    split_identifier(cross_reference['id'])[0].lower() != 'pmid']
    if 'keywords' in entry:
        clean_up_keywords(mod, entry)


REPLACE_VALUE_FIELDS = ['authors', 'pubMedType', 'meshTerms']

SINGLE_VALUE_FIELDS = ['volume', 'title', 'pages', 'issueName', 'datePublished',
                       'dateArrivedInPubmed', 'dateLastModified', 'abstract', 'publisher',
                       'plainLanguageAbstract', 'pubmedAbstractLanguages',
                       'publicationStatus', 'allianceCategory', 'journal']

DATE_FIELDS = ['dateArrivedInPubmed', 'dateLastModified']


PMID_FIELDS = ['authors', 'volume', 'title', 'pages', 'issueName', 'datePublished',
               'dateArrivedInPubmed', 'dateLastModified', 'abstract', 'pubMedType', 'publisher',
               'meshTerms', 'plainLanguageAbstract', 'pubmedAbstractLanguages',
               'publicationStatus', 'allianceCategory', 'journal']


COMPARE_IF_DQM_EMPTY = False  # do dqm vs pmid comparison even if dqm has no data, by default skip


def merge_pubmed_single_value_fields_into_entry(entry, pubmed_data, mod, pmid, report_writer):
    for single_value_field in SINGLE_VALUE_FIELDS:
        pubmed_data_for_field = ""
        dqm_data_for_field = ""
        if single_value_field in pubmed_data:
            if single_value_field in DATE_FIELDS:
                pubmed_data_for_field = pubmed_data[single_value_field]['date_string']
            else:
                pubmed_data_for_field = pubmed_data[single_value_field]
        if single_value_field in entry:
            dqm_data_for_field = entry[single_value_field]
        if dqm_data_for_field != "":
            dqm_data_for_field = str(bs4.BeautifulSoup(dqm_data_for_field, "html.parser"))
        # UNCOMMENT to output log of data comparison between dqm and pubmed
        if dqm_data_for_field != "" or COMPARE_IF_DQM_EMPTY:
            if single_value_field == 'title':
                compare_dqm_pubmed(mod, "title", pmid, single_value_field, dqm_data_for_field, pubmed_data_for_field,
                                   report_writer=report_writer)
            else:
                compare_dqm_pubmed(mod, "differ", pmid, single_value_field, dqm_data_for_field, pubmed_data_for_field,
                                   report_writer=report_writer)
        if pubmed_data_for_field != "":
            entry[single_value_field] = pubmed_data_for_field
        if single_value_field == 'datePublished':
            if pubmed_data_for_field == "" and dqm_data_for_field != "":
                entry[single_value_field] = dqm_data_for_field


def replace_fields_in_dqm_data_with_pubmed_values(entry, pubmed_data):
    for replace_value_field in REPLACE_VALUE_FIELDS:
        # always delete dqm value to be replaced even if the respective pubmed value is empty
        entry[replace_value_field] = []
        if replace_value_field in pubmed_data:
            # logger.info("PMID %s pmid_field %s data %s", pmid, pmid_field, pubmed_data[pmid_field])
            entry[replace_value_field] = pubmed_data[replace_value_field]


def set_additional_author_values_in_dqm_data(entry):
    # needs to happen after "replace_fields_in_dqm_data_with_pubmed_values"
    if 'authors' in entry:
        for author in entry['authors']:
            author['correspondingAuthor'] = False
            author['firstAuthor'] = False


def merge_pubmed_xrefs_into_entry_xrefs(entry, pubmed_data, mod, primary_id, report_writer):
    prefix_xrefs_dict = {}
    if 'crossReferences' in pubmed_data:
        for xref in pubmed_data['crossReferences']:
            prefix, identifier, _ = split_identifier(xref["id"])
            prefix_xrefs_dict[prefix] = (xref, identifier)
    if 'crossReferences' in entry:
        for cross_reference in entry['crossReferences']:
            prefix, identifier, separator = split_identifier(cross_reference['id'])
            if prefix not in prefix_xrefs_dict:
                prefix_xrefs_dict[prefix] = cross_reference, identifier
            else:
                if prefix_xrefs_dict[prefix][1].lower() != identifier.lower():
                    report_writer.write(
                        mod=mod, report_type="generic",
                        message="primaryId %s has xref %s PubMed has %s%s%s\n" % (
                            primary_id, cross_reference['id'], prefix, separator, prefix_xrefs_dict[prefix][1]))

    entry['crossReferences'] = [cross_reference[0] for cross_reference in prefix_xrefs_dict.values()]


def merge_pubmed_nlm_resource_info_into_entry(entry, mod, pmid, pubmed_data, resource_nlm_id_to_title,
                                              resource_to_nlm_id, report_writer: ReportWriter):
    if 'nlm' in pubmed_data:
        nlm_identifier = pubmed_data['nlm']
        entry['nlm'] = ['NLM:' + nlm_identifier]
        entry['resource'] = 'NLM:' + nlm_identifier
        if nlm_identifier in resource_nlm_id_to_title:
            # logger.info("PMID %s has NLM %s setting to title %s", pmid, nlm, resource_nlm_to_title[nlm])
            entry['resourceAbbreviation'] = resource_nlm_id_to_title[nlm_identifier]
        nlm_id_simplified = simplify_text_keep_digits(nlm_identifier)
        if nlm_id_simplified not in resource_to_nlm_id:
            report_writer.write(
                mod=mod, report_type="generic",
                message="NLM value %s from PMID %s XML does not map to a proper resource.\n" % (
                    pubmed_data['nlm'], pmid))
    else:
        if 'is_journal' in pubmed_data:
            report_writer.write(mod=mod, report_type="generic",
                                message="PMID %s does not have an NLM resource.\n" % pmid)


def merge_keywords_from_pubmed_into_entry(entry, pubmed_data, mod):
    if 'keywords' not in entry:
        entry['keywords'] = []
    else:
        # e.g. 9882485 25544291 24201188 31188077
        clean_up_keywords(mod, entry)
    if 'keywords' in pubmed_data:
        # aggregate for all MODs except ZFIN, which has misformed data and can't fix it.
        # 19308247 aggregates keywords for WB
        entry_keywords = {keyword.upper() for keyword in entry['keywords']}
        for pubmed_keyword in pubmed_data['keywords']:
            if pubmed_keyword.upper() not in entry_keywords:
                entry['keywords'].append(pubmed_keyword)


def find_resource_abbreviation_not_matched_to_nlm_or_res_mod(resource_not_found: Dict[str, Dict[str, int]],
                                                             report_writer: ReportWriter, base_dir=base_path):
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
                                       exclude_cross_reference_type, report_writer: ReportWriter):
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


def load_dqm_data_from_json(filename):
    logger.info("Loading %s", filename)
    if os.path.exists(filename):
        return json.load(open(filename, 'r'))
    else:
        logger.info("No file found %s", filename)
        return None


def update_unexpected_mod_properties_and_delete_blank_fields_from_entry(entry, schema_data,
                                                                        unexpected_mod_properties: set):
    blank_fields = set()
    unexpected_mod_properties.update({field for field in entry.keys() if field not in schema_data['properties']})
    for entry_property in entry.keys():
        if entry_property in SINGLE_VALUE_FIELDS and entry[entry_property] == "":
            blank_fields.add(entry_property)
    for entry_field in blank_fields:
        del entry[entry_field]


def process_unmerged_pubmed_data(unmerged_pubmed_data, additional_fields, aggregate_fields,
                                 sanitized_pubmed_multi_mod_data, report_writer):
    for pmid in unmerged_pubmed_data:
        date_published_set = set()
        alliance_category_dict = dict()
        sanitized_entry = dict()
        cross_references_dict = dict()
        mod_corpus_association_dict = dict()
        for mod in unmerged_pubmed_data[pmid]:
            entry = unmerged_pubmed_data[pmid][mod]

            sanitized_entry['primaryId'] = entry['primaryId']

            for pmid_field in PMID_FIELDS:
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

            if 'modCorpusAssociations' in entry:
                for mod_corpus_association in entry['modCorpusAssociations']:
                    id = mod_corpus_association['modAbbreviation']
                    mod_corpus_association_dict[id] = mod_corpus_association
                    # logger.info("mod_corpus_association %s", mod_corpus_association)

            if 'crossReferences' in entry:
                for cross_ref in entry['crossReferences']:
                    id = cross_ref['id']
                    pages = []
                    if 'pages' in cross_ref:
                        pages = cross_ref['pages']
                    cross_references_dict[id] = pages

        for mod_corpus_association_id in mod_corpus_association_dict:
            if 'modCorpusAssociations' in sanitized_entry:
                sanitized_entry['modCorpusAssociations'].append(mod_corpus_association_dict[mod_corpus_association_id])
            else:
                sanitized_entry['modCorpusAssociations'] = [mod_corpus_association_dict[mod_corpus_association_id]]

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
                report_writer.write(
                    mod="multi", report_type="",
                    message="Multiple allianceCategory pmid %s alliance categories %s\n" % (
                        pmid, multiple_alliance_categories))
        if len(date_published_set) > 1:
            dates_published = "\t".join(date_published_set)
            # logger.info("MULTIPLE DATES PUBLISHED pmid %s dates published %s", pmid, dates_published)
            report_writer.write(
                mod="multi", report_type="",
                message="Multiple datePublished pmid %s dates published %s\n" % (pmid, dates_published))

        sanitized_pubmed_multi_mod_data.append(sanitized_entry)


def write_sanitized_multimod_data_to_json(sanitized_pubmed_multi_mod_data, json_storage_path):
    entries_size = 100000
    sanitized_pubmed_list = list(chunks(sanitized_pubmed_multi_mod_data, entries_size))
    for i in range(len(sanitized_pubmed_list)):
        dict_to_output = sanitized_pubmed_list[i]
        json_filename = json_storage_path + 'REFERENCE_PUBMED_MULTI_' + str(i + 1) + '.json'
        write_json(json_filename, dict_to_output)


def aggregate_dqm_with_pubmed(input_path, input_mod, output_directory, base_dir=base_path):  # noqa: C901
    # reads agr_schemas's reference.json to check for dqm data that's not accounted for there.
    # outputs sanitized json to sanitized_reference_json/
    # does checks on dqm crossReferences.  if primaryId is not PMID, and a crossReference is PubMed,
    # assigns PMID to primaryId and to authors's referenceId.
    # if any reference's author doesn't have author Rank, assign authorRank based on array order.
    logger.info("initializing data structures")

    # datePublished is a string, not a proper date field

    # RGD should be first in mods list.  if conflicting allianceCategories the later mod gets priority
    mods = ['RGD', 'MGI', 'SGD', 'FB', 'ZFIN', 'WB', 'XB']
    if input_mod in mods:
        mods = [input_mod]

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

    expected_cross_reference_type, exclude_cross_reference_type, pubmed_not_dqm_cross_reference_type = \
        populate_expected_cross_reference_type()

    json_storage_path = base_path + output_directory + 'sanitized_reference_json/'
    if not path.exists(json_storage_path):
        makedirs(json_storage_path)

    report_writer = ReportWriter(mod_reports_dir=base_path + output_directory + 'report_files/',
                                 multimod_reports_file_path=base_path + output_directory + 'report_files/multi_mod')

    resource_not_found = defaultdict(lambda: defaultdict(int))
    cross_reference_types = defaultdict(lambda: defaultdict(list))

    logger.info("Aggregating DQM and PubMed data from %s using mods %s", input_path, mods)
    schema_data = get_schema_data_from_alliance()

    sanitized_pubmed_multi_mod_data = []
    unmerged_pubmed_data = defaultdict(dict)  # pubmed data by pmid and mod that needs some fields merged
    for mod in mods:
        dqm_data = load_dqm_data_from_json(filename=base_dir + input_path + '/REFERENCE_' + mod + '.json')
        if dqm_data is None:
            continue
        sanitized_pubmod_data = []
        sanitized_pubmed_single_mod_data = []
        unexpected_mod_properties = set()
        for entry in dqm_data['data']:
            orig_primary_id = entry['primaryId']
            update_unexpected_mod_properties_and_delete_blank_fields_from_entry(entry, schema_data,
                                                                                unexpected_mod_properties)
            # inject the mod corpus association data because if it came from that mod dqm file it should have this entry
            entry['modCorpusAssociations'] = [{"modAbbreviation": mod, "modCorpusSortSource": "dqm_files",
                                               "corpus": True}]

            primary_id, update_primary_id = process_xrefs_and_find_pmid_if_necessary(
                reference=entry,
                mod=mod,
                report_writer=report_writer,
                original_primary_id=orig_primary_id,
                expected_cross_reference_type=expected_cross_reference_type,
                exclude_cross_reference_type=exclude_cross_reference_type,
                cross_reference_types=cross_reference_types)
            if not primary_id:
                continue

            pubmed_data, is_pubmod, pmid = load_pubmed_data_if_present(primary_id, mod, orig_primary_id,
                                                                       report_writer=report_writer)

            if is_pubmod:
                process_pubmod_authors_xrefs_keywords(entry, update_primary_id, primary_id, mod)
                set_resource_info_from_abbreviation(entry, mod, primary_id, resource_to_mod_issn_nlm,
                                                    resource_to_nlm_id, resource_to_nlm_highest_id, resource_to_mod,
                                                    resource_not_found, report_writer=report_writer)
                sanitized_pubmod_data.append(entry)
            else:
                # processing pubmed data
                merge_pubmed_single_value_fields_into_entry(entry, pubmed_data, mod, pmid, report_writer)
                replace_fields_in_dqm_data_with_pubmed_values(entry, pubmed_data)
                set_additional_author_values_in_dqm_data(entry)
                merge_pubmed_xrefs_into_entry_xrefs(entry, pubmed_data, mod, primary_id, report_writer)
                merge_pubmed_nlm_resource_info_into_entry(entry, mod, pmid, pubmed_data, resource_nlm_id_to_title,
                                                          resource_to_nlm_id, report_writer)
                merge_keywords_from_pubmed_into_entry(entry, pubmed_data, mod)

                if pmid in pmid_multi_mods.keys():
                    # logger.info("MULTIPLE pmid %s mod %s", pmid, mod)
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

    process_unmerged_pubmed_data(unmerged_pubmed_data, additional_fields, aggregate_fields,
                                 sanitized_pubmed_multi_mod_data, report_writer)
    logger.info("outputting sanitized pubmed_data")

    write_sanitized_multimod_data_to_json(sanitized_pubmed_multi_mod_data, json_storage_path)
    report_unexpected_cross_references(cross_reference_types, exclude_cross_reference_type, report_writer=report_writer)
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

def aggregate_dqm_data(base_dir, input_dir, output_dir, mod, generate_pmid_data_option):
    logger.info("starting parse_dqm_json_reference.py")

    # pipenv run python parse_dqm_json_reference.py -f dqm_sample/ -p
    if generate_pmid_data_option:
        logger.info("Generating PMID files from DQM data")
        generate_pmid_data(base_dir, input_dir, output_dir, 'all')

    # pipenv run python parse_dqm_json_reference.py -f dqm_sample/ -m WB
    # pipenv run python parse_dqm_json_reference.py -f dqm_data_updates_new/ -m all
    elif mod:
        aggregate_dqm_with_pubmed(input_dir, mod, output_dir)

    else:
        logger.info("No valid processing for directory passed in.  Use -h for help.")

    logger.info("ending parse_dqm_json_reference.py")


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
    aggregate_dqm_data(base_path, input_dir=args['file'], output_dir=args['directory'], mod=args['mod'],
                       generate_pmid_data_option=args['generate_pmid_data'])
