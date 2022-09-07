
import json
import logging.config
import re
from os import environ, makedirs, path

from dotenv import load_dotenv

from agr_literature_service.lit_processing.utils.file_processing_utils import (load_pubmed_resource_basic,
                                                                               save_resource_file, split_identifier,
                                                                               write_json)

load_dotenv()

log_file_path = path.join(path.dirname(path.abspath(__file__)), '../../../../logging.conf')
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


def create_storage_path(json_storage_path):
    """

    :return:
    """

    if not path.exists(json_storage_path):
        makedirs(json_storage_path)


def load_mod_resource_to_nlm(mod):
    """

    :param mod:
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
    """
    This mapping via abbreviationSynonyms to mod_to_nlm never matches anything after adding the mapping via online and print issn through crossReferences.  Leaving this here in case future data finds it helps with MOD data that lacks issn information, but has resource abbreviationSynonyms that matches against reference resourceAbbreviation and PubMed XML nlm.
    This takes about a minute to run, compared to the whole script running in a second without this.

    FB have FB ids for resources, but from the references's resourceAbbreviation and pubmed xml's nlm, we can update to match against resources's abbreviationSynonyms

    :param input_path:
    :param mod:
    :return:
    """

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
        # print("primaryId %s" % (entry['primaryId']))

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
                    resource_abbreviation = entry['resourceAbbreviation']
                    if resource_abbreviation in mod_resource_abbreviation_to_nlm:
                        if nlm not in mod_resource_abbreviation_to_nlm[resource_abbreviation]:
                            mod_resource_abbreviation_to_nlm[resource_abbreviation].append(nlm)
                    else:
                        mod_resource_abbreviation_to_nlm[entry['resourceAbbreviation']] = [nlm]

    # fb have fb ids for resources, but from the resourceAbbreviation and pubmed xml's nlm, we can update fb resource data to primary key off of nlm
    # parse_dqm_json_resource takes one second, but generating this takes 49 seconds for FB data, 56s for SGD, 150s for MGI, 15s WB
    # so save to a file for ease of altering this script in the future, being able to load the file with load_mod_resource_to_nlm(mod)
    json_filename = base_path + mod + '_resourceAbbreviation_to_NLM.json'
    write_json(json_filename, mod_resource_abbreviation_to_nlm)

    return mod_resource_abbreviation_to_nlm


def load_mod_resource(json_storage_path, pubmed_by_nlm, nlm_by_issn, mod):      # noqa: C901
    """

    :param json_storage_path:
    :param pubmed_by_nlm:
    :param nlm_by_issn:
    :param mod:
    :return:
    """

    resource_fields_not_in_pubmed = ['titleSynonyms', 'abbreviationSynonyms', 'copyrightDate',
                                     'publisher', 'editorsOrAuthors', 'volumes', 'pages', 'abstractOrSummary']
    base_path = environ.get('XML_PATH')

    # leaving this here in case comment at generate_resource_abbreviation_to_nlm_from_dqm_references helps
    # mod_to_nlm = dict()
    # if mod == 'FB':   # only FB sending abbreviationSynonyms in dqm resource file that can be mapped to resource via NLM
    #     # mod_to_nlm = load_mod_resource_to_nlm(mod)   # for modifying this script without re-generating mod_to_nlm from input_path
    #     input_path = base_path + 'dqm_data/'
    #     mod_to_nlm = generate_resource_abbreviation_to_nlm_from_dqm_references(input_path, mod)

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
                elif 'crossReferences' in entry:
                    for cross_ref in entry['crossReferences']:
                        if 'id' in cross_ref:
                            prefix, identifier, separator = split_identifier(cross_ref['id'])
                            if prefix == 'ISSN':
                                if identifier in nlm_by_issn:
                                    if len(nlm_by_issn[identifier]) == 1:
                                        nlm = nlm_by_issn[identifier][0]
                # leaving this here in case comment at generate_resource_abbreviation_to_nlm_from_dqm_references helps
                # elif 'abbreviationSynonyms' in entry:
                #     for abbreviation in entry['abbreviationSynonyms']:
                #         if abbreviation in mod_to_nlm:
                #             if len(mod_to_nlm) == 1:
                #                 nlm = mod_to_nlm[abbreviation]
                #                 logger.info("found %s in %s", nlm, entry['primaryId'])
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
                                # the zfin primaryId is the nlm without the prefix, check if it already exists before adding for other MOD data
                                zfin_nlm = 'NLM:' + entry['primaryId']
                                if zfin_nlm not in nlm_cross_refs:
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

    base_path = environ.get('XML_PATH', "")
    json_storage_path = base_path + 'sanitized_resource_json/'
    create_storage_path(json_storage_path)

    mods = ['RGD', 'MGI', 'SGD', 'FB', 'ZFIN', 'WB']

    pubmed_by_nlm, nlm_by_issn = load_pubmed_resource_basic()
    for mod in mods:
        pubmed_by_nlm = load_mod_resource(json_storage_path, pubmed_by_nlm, nlm_by_issn, mod)
    save_resource_file(json_storage_path, pubmed_by_nlm, 'NLM')

    logger.info("ending parse_dqm_json_resource.py")

# pipenv run python parse_dqm_json_resource.py
