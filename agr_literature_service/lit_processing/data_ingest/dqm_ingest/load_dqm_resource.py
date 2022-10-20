import json
import logging.config
from os import environ, path

from dotenv import load_dotenv

from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import load_pubmed_resource_basic
from agr_literature_service.lit_processing.utils.generic_utils import split_identifier
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.sort_dqm_json_resource_updates import (
    process_single_resource,
    PROCESSED_NEW,
    PROCESSED_UPDATED,
    PROCESSED_FAILED
)
load_dotenv()
init_tmp_dir()


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../../../../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


def get_nlm_from_xref(entry, nlm_by_issn) -> str:
    nlm = ''
    for cross_ref in entry['crossReferences']:
        if 'id' in cross_ref:
            prefix, identifier, separator = split_identifier(cross_ref['id'])
            if prefix == 'ISSN':
                if identifier in nlm_by_issn:
                    if len(nlm_by_issn[identifier]) == 1:
                        nlm = nlm_by_issn[identifier][0]
    return nlm


def process_nlm(nlm, entry, pubmed_by_nlm):
    resource_fields_not_in_pubmed = ['titleSynonyms', 'abbreviationSynonyms', 'copyrightDate',
                                     'publisher', 'editorsOrAuthors', 'volumes', 'pages', 'abstractOrSummary']
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


def process_entry(db_session, entry, pubmed_by_nlm, nlm_by_issn):
    nlm = ''
    if 'primaryId' in entry:
        primary_id = entry['primaryId']
    if primary_id in pubmed_by_nlm:
        nlm = primary_id
    elif 'crossReferences' in entry:
        nlm = get_nlm_from_xref(entry, nlm_by_issn)
    if nlm != '':
        process_nlm(nlm, entry, pubmed_by_nlm)
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
        # sanitized_data.append(entry)
        update_status, okay, message = process_single_resource(db_session, entry)
        # process_count[update_status] += 1
        if not okay:
            logger.warning(message)
    return update_status, okay, message


def load_mod_resource(db_session, pubmed_by_nlm, nlm_by_issn, mod):
    """

    :param json_storage_path:
    :param pubmed_by_nlm:
    :param nlm_by_issn:
    :param mod:
    :return:
    """

    base_path = environ.get('XML_PATH')

    filename = base_path + 'dqm_data/RESOURCE_' + mod + '.json'
    process_count[0, 0, 0]
    try:
        with open(filename, 'r') as f:
            dqm_data = json.load(f)
            for entry in dqm_data['data']:
                update_status, okay, message = process_entry(db_session, entry, pubmed_by_nlm, nlm_by_issn)
                process_count[update_status] += 1
                if not okay:
                    logger.warning(message)
    except IOError:
        # Some mods have no resources so exception here is okay.
        if mod in ['FB', 'ZFIN']:
            logger.error("Could not open file {filename}.")
    return pubmed_by_nlm, process_count


if __name__ == "__main__":
    """
    call main start function
    """

    logger.info("Starting load_dqm_resource.py")
    db_session = create_postgres_session(False)
    base_path = environ.get('XML_PATH', "")

    mods = ['RGD', 'MGI', 'SGD', 'FB', 'ZFIN', 'WB']

    pubmed_by_nlm, nlm_by_issn = load_pubmed_resource_basic()
    for mod in mods:
        pubmed_by_nlm, process_count = load_mod_resource(db_session, pubmed_by_nlm, nlm_by_issn, mod)
        logger.info(f"{mod}:  New: {process_count[PROCESSED_NEW]}, Updated {process_count[PROCESSED_UPDATED]}. Problems {process_count[PROCESSED_FAILED]}")

    # Process the nlm ones.
    process_count = [0, 0, 0]
    for entry in pubmed_by_nlm:
        update_status, okay, message = process_single_resource(db_session, entry)
        process_count[update_status] += 1
        if not okay:
            logger.warning(message)
    logger.info(f"NLM: New: {process_count[PROCESSED_NEW]}, Updated {process_count[PROCESSED_UPDATED]}. Problems {process_count[PROCESSED_FAILED]}")
    logger.info("ending load_dqm_resource.py")
