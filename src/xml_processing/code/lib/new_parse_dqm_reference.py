"""

"""

import json
import logging
import re
import urllib.request
import warnings
from collections import defaultdict, Counter
import os
import sys
import coloredlogs

import pandas as pd


from .helper_file_processing import clean_up_keywords, split_identifier, write_json

warnings.filterwarnings("ignore", category=UserWarning, module="bs4")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG")



def generate_pmid_data(base_path, output_directory):
    """

    output set of PMID identifiers that will need XML downloaded
    output pmids and the mods that have them

    :param base_path:
    :param output_directory:
    :return:
    """

    logger.info("Generating PMID sets from dqm data")
    dqm_path = os.path.join(base_path, "dqm_data")
    logger.info(f"Input path: {dqm_path}")

    if os.path.isdir(output_directory):
        logger.info(f"Output directory exists: {output_directory}")
    else:
        logger.info(f"Output directory does not exist, creating: {output_directory}")
        os.makedirs(output_directory)

    # RGD should be first in mods list. if conflicting allianceCategories the later mod gets priority
    mods = ["RGD", "MGI", "SGD", "FB", "ZFIN", "WB"]
    mods = ['WB']
    pmid_references = defaultdict(list)
    non_pmid_references = defaultdict(list)
    unknown_prefix = set([])
    pmid_stats = {}

    check_primary_id_is_unique = True
    check_pmid_is_unique = True
    pmid_unique = []

    primary_id_unique = []
    for mod in mods:
        filename = os.path.join(dqm_path, f"REFERENCE/{mod}.json")
        logger.info(f"Loading {mod} data from {filename}")
        try:
            dqm_data = json.load(open(filename))
        except FileNotFoundError:
            logger.error(f"File not found: {filename}")

        primary_id_unique = [entry["primaryId"] for entry in dqm_data["data"]]

        for entry in dqm_data["data"]:
            pmid = "0"
            prefix, identifier, separator = split_identifier(entry["primaryId"])
            if prefix == "PMID":
                pmid = identifier
            elif prefix in mods:
                if "crossReferences" in entry:
                    for cross_reference in entry["crossReferences"]:
                        prefix_xref, identifier_xref, separator_xref = split_identifier(cross_reference["id"])
                        if prefix_xref == "PMID":
                            pmid = identifier_xref
            else:
                unknown_prefix.add(prefix)

            if pmid != "0":
                try:
                    pmid_stats[pmid].append(mod)
                except KeyError:
                    pmid_stats[pmid] = [mod]
                if check_pmid_is_unique:
                    pmid_unique.append(pmid)
                pmid_references[mod].append(pmid)
            else:
                non_pmid_references[mod].append(entry["primaryId"])

        # output check of a mod's non-unique primaryIds

        z = Counter(primary_id_unique)
        non_unique = [x for x in z if z[x] > 1]

        z = Counter(pmid_unique)
        non_unique = [x for x in z if z[x] > 1]

        # print("%s primary_id %s has %s mentions" % (mod, primary_id, primary_id_unique[primary_id]))
        # print("%s pmid %s has %s mentions" % (mod, pmid, pmid_unique[pmid]))

    # output each mod's count of pmid references
    for mod in pmid_references:
        logger.info(f"{mod}: {len(pmid_references[mod])} references")

    # output each mod's count of non-pmid references
    for mod in non_pmid_references:
        logger.info(f"{mod}: {len(non_pmid_references[mod])} non-PMID references")

    # output actual reference identifiers that are not pmid
    # for mod in non_pmid_references:
    #     for primary_id in non_pmid_references[mod]:
    #         print("%s non-pmid %s" % (mod, primary_id))
    #         # logger.info("%s non-pmid %s", mod, primary_id)

    # if a reference has an unexpected prefix, give a warning
    for prefix in unknown_prefix:
        logger.info("WARNING: unknown prefix %s", prefix)

    # output set of identifiers that will need XML downloaded
    output_pmid_file = os.path.join(output_directory + '/alliance_pmids.txt')
    logger.info(f"Writing pmid set to {output_pmid_file}")
    with open(output_pmid_file, "w") as pmid_file:
        for pmid in sorted(pmid_stats, key=int):
            pmid_file.write(f"{pmid}\n")
        pmid_file.close()

    # output pmids and the mods that have them
    output_pmid_mods_file = os.path.join(output_directory + "/pmids_by_mods")
    logger.info(f"Writing pmid-mods to {output_pmid_mods_file}")
    with open(output_pmid_mods_file, "w") as pmid_mods_file:
        for identifier in pmid_stats:
            ref_mods_str = ", ".join(pmid_stats[identifier])
            pmid_mods_file.write(f"{identifier}\t{len(pmid_stats[identifier])}\t{ref_mods_str}\n")
        pmid_mods_file.close()

    # for primary_id in primary_ids:
    #     logger.info("primary_id %s", primary_id)


def load_pmid_multi_mods(result_path):
    """
    Loads the pmid-mods file and returns a dictionary of pmid-mods
    :param result_path:
    :return:
    """

    pmid_multi_mods = {}
    pmid_multi_mods_file = os.path.join(result_path, "pmids_by_mods")
    pmid_file = open(pmid_multi_mods_file).read().splitlines()
    for line in pmid_file:
        cols = line.split("\t")
        if int(cols[1]) > 1:
            pmid_multi_mods[cols[0]] = cols[1]

    logger.info(f"Loaded {len(pmid_multi_mods)} pmids with multiple mods")
    return pmid_multi_mods


def load_pubmed_resource(base_path):
    """
    Loads the pubmed resource file and returns a dictionary of pmid-mods
    :param base_path:
    :return:
    """

    logger.info('Starting load_pubmed_resource')
    resource_data = {}
    filename = os.path.join(base_path, "pubmed_resource_json/resource_pubmed_all.json")
    logger.info(f"Loading {filename}")
    try:
        nlm_df = pd.read_json(filename)
        logger.info(f"Loaded {len(resource_data)} pubmed resources")
    except IOError:
        logger.info(f"No resource_pubmed_all.json file at {filename}")

    nlm_df['onlineISSN'].fillna("NA", inplace=True)
    nlm_df['printISSN'].fillna("NA", inplace=True)
    nlm_df['primaryId'] = nlm_df['primaryId'].str.strip("R")
    print(nlm_pd)
    nlm_pd.to_csv(os.path.join(base_path, "pubmed_resource_json/resource_pubmed_all.csv"), index=False)

    return nlm_df

def simplify_text_keep_digits(text):
    """

    :param text:
    :return:
    """

    no_html = re.sub("<[^<]+?>", "", str(text))
    stripped = re.sub(r"[^a-zA-Z0-9]+", "", str(no_html))
    clean = stripped.lower()

    return clean


def strip_string_to_integer(string):
    """

    :param string:
    :return:
    """

    return int("".join(filter(lambda x: x.isdigit(), string)))


def aggregate_dqm_with_pubmed(base_path, dqm_json_path, output_directory, json_path):
    """
    reads agr_schemas's reference.json to check for dqm data that's not accounted for there.
    outputs sanitized json to sanitized_reference_json/
    does checks on dqm crossReferences.  if primaryId is not PMID, and a crossReference is PubMed,
    assigns PMID to primaryId and to authors's referenceId.
    if any reference's author doesn't have author Rank, assign authorRank based on array order.

    :param input_path:
    :param input_mod:
    :param output_directory:
    :return:
    """

    pmid_fields = ["authors", "volume", "title", "pages", "issueName", "issueDate", "datePublished",
                   "dateArrivedInPubmed", "dateLastModified", "abstract", "pubMedType", "publisher",
                   "meshTerms", "plainLanguageAbstract", "pubmedAbstractLanguages", "publicationStatus"]

    replace_value_fields = ["authors", "pubMedType", "meshTerms"]

    # datePublished is a string, not a proper date field
    date_fields = ["issueDate", "dateArrivedInPubmed", "dateLastModified"]

    # do dqm vs pmid comparison even if dqm has no data, by default skip
    compare_if_dqm_empty = False

    # # RGD should be first in mods list.
    # if conflicting allianceCategories the later mod gets priority
    # mods = ["RGD", "MGI", "SGD", "FB", "ZFIN", "WB"]
    mods = ['WB']

    # this has to be loaded, if the mod data is hashed by pmid+mod and sorted for those with
    # multiple mods, there's an out-of-memory crash
    pmid_multi_mods = load_pmid_multi_mods(output_directory)

    # # # use these two lines to properly load resource data, but it takes a bit of time
    nlm_df = load_pubmed_resource(base_path)

    resource_to_mod, resource_to_mod_issn_nlm = load_mod_resource(mods, resource_to_nlm, dqm_json_path)
    # use these six lines to more quickly test other things that don't need resource data
    resource_to_nlm = {}
    resource_to_nlm_highest = {}
    resource_nlm_to_title = {}
    resource_to_mod = defaultdict(dict)

    # expected_cross_reference_type, exclude_cross_reference_type,
    # pubmed_not_dqm_cross_reference_type = populate_expected_cross_reference_type()

    resource_not_found = defaultdict(dict)

    logger.info(output_directory)
    logger.info(os.path.join(output_directory, "sanitized_reference_json"))
    json_storage_path = os.path.join(output_directory, "sanitized_reference_json")
    logger.info(f"json_storage_path: {json_storage_path}")
    if not os.path.exists(json_storage_path):
        os.makedirs(json_storage_path)
        logger.info(f"Created {json_storage_path}")
    else:
        logger.info(f"{json_storage_path} exists")

    report_file_path = os.path.join(output_directory, "report_files/")
    if not os.path.exists(report_file_path):
        os.makedirs(report_file_path)
        logger.info(f"Created {report_file_path}")
    else:
        logger.info(f"{report_file_path} exists")

    fh_mod_report = {}
    fh_mod_report_title = {}
    fh_mod_report_differ = {}
    # fh_mod_report_xrefs = {}
    fh_mod_report_resource_unmatched = {}
    fh_mod_report_reference_no_resource = {}

    for mod in mods:
        # resource_not_found[mod] = {}
        # # cross_reference_types[mod] = set()
        # cross_reference_types[mod] = {}

        fh_mod_report.setdefault(mod, open(os.path.join(report_file_path, f"{mod}_main"), "w"))
        logger.info(f"Created {os.path.join(report_file_path, f'{mod}_main')}")
        fh_mod_report_title.setdefault(mod, open(os.path.join(report_file_path, f"{mod}_dqm_pubmed_differ_title"), "w"))
        fh_mod_report_differ.setdefault(mod, open(os.path.join(report_file_path, f"{mod}_dqm_pubmed_differ_other"), "w"))
        fh_mod_report_resource_unmatched.setdefault(mod, open(os.path.join(report_file_path, f"{mod}_resource_unmatched"), "w"))
        fh_mod_report_reference_no_resource.setdefault(mod, open(os.path.join(report_file_path, f"{mod}_reference_no_resource"), "w"))
        # filename_xrefs = report_file_path + mod + '_dqm_pubmed_differ_xrefs'
        # fh_mod_report_xrefs.setdefault(mod, open(filename_xrefs, 'w'))

    fh_mod_report.setdefault("multi", open(os.path.join(report_file_path, "multi_mod"), "w"))
    logger.info(f"Aggregating DQM and PubMed data from {dqm_json_path} using mods {', '.join(mods)}")
    agr_schemas_reference_json_url = "https://raw.githubusercontent.com/alliance-genome/agr_schemas/master/ingest/resourcesAndReferences/reference.json"

    # schema_data = {}
    # with urllib.request.urlopen(agr_schemas_reference_json_url) as url:
    #     schema_data = json.loads(url.read().decode())
    #
    # sanitized_pubmed_multi_mod_data = []
    # # pubmed data by pmid and mod that needs some fields merged
    # unmerged_pubmed_data = {}
    # for mod in mods:
    #     filename = dqm_json_path + mod + ".json"
    #     logger.info("Processing %s", filename)
    #
    #     dqm_data = {}
    #     try:
    #         with open(filename) as f:
    #             entries = json.load(f)["data"]
    #     except IOError:
    #         logger.info("No file found for mod %s %s", mod, filename)
    #
    #     process_dqm_entries(entries, schema_data, mod, fh_mod_report, json_path, resource_not_found, resource_to_nlm,
    #                         resource_to_mod_issn_nlm, resource_to_mod, fh_mod_report_resource_unmatched)