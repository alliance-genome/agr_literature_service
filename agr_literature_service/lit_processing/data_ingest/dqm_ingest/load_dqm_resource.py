import json
import logging
from os import environ, path
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from typing import Dict, Tuple

from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import load_pubmed_resource_basic
from agr_literature_service.lit_processing.utils.generic_utils import split_identifier
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.utils.resource_reference_utils import load_xref_data
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_resources_nlm import \
    update_resource_pubmed_nlm
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.dqm_resource_update_utils import (
    process_single_resource,
    PROCESSED_NEW,
    PROCESSED_UPDATED,
    PROCESSED_FAILED
)
from agr_literature_service.api.user import set_global_user_id
load_dotenv()
init_tmp_dir()

process_count = [0, 0, 0]

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_nlm_from_xref(entry: Dict, nlm_by_issn: Dict) -> str:
    """
    Get the nlm vsalue in the entry if it exists

    :param entry: dqm entry in json format
    :param nlm_by_issn: dict of nlm from a issn
    """
    nlm = ''
    for cross_ref in entry['crossReferences']:
        if 'id' in cross_ref:
            prefix, identifier, separator = split_identifier(cross_ref['id'])
            if prefix == 'ISSN':
                if identifier in nlm_by_issn:
                    if len(nlm_by_issn[identifier]) == 1:
                        nlm = nlm_by_issn[identifier][0]
    return nlm


def process_nlm(nlm: str, entry: dict, pubmed_by_nlm: dict) -> None:
    """
    Update the dict pubmed_by_nlm using the entry's data.

    :param nlm: nlm value to process
    :param entry: dqm entry in json format
    :param pubmed_by_nlm: dict of nlm's to entry fields.
    """
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


def process_entry(db_session: Session, entry: dict, pubmed_by_nlm: dict, nlm_by_issn: dict) -> Tuple:
    """
    Process the original dqm json entry.
    First we "sanitize the entry and then process it according
    to wether it has nlm in it or not.

    :param db_session: db connection
    :param entry: dqm entry unaltered in json format
    :param pubmed_by_nlm: pubmed entry by nlm, pubmed_by_nlm processed at the end.
    :param nlm_by_issn: dict to look up nlm vis issn
    """
    nlm = ''
    update_status = PROCESSED_FAILED
    okay = True
    message = ""

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

        update_status, okay, message = process_single_resource(db_session, entry)
        if not okay:
            logger.warning(message)
    return update_status, okay, message


def load_mod_resource(db_session: Session, pubmed_by_nlm: Dict, nlm_by_issn: Dict, mod: str) -> Tuple:
    """

    :param db_session: db connection
    :param pubmed_by_nlm: pubmed entry by nlm, pubmed_by_nlm processed at the end.
    :param nlm_by_issn: dict to look up nlm vis issn
    :param mod: mod to be processed
    :return:
    """

    base_path = environ.get('XML_PATH', '')

    filename = base_path + 'dqm_data/RESOURCE_' + mod + '.json'
    try:
        with open(filename, 'r') as f:
            dqm_data = json.load(f)
            for entry in dqm_data['data']:
                update_status, okay, message = process_entry(db_session, entry, pubmed_by_nlm, nlm_by_issn)
                process_count[update_status] += 1
                if not okay:
                    logger.warning(message)
    except IOError:
        # Some mods have no resources so exception here is okay but give message anyway.
        if mod in ['FB', 'ZFIN']:
            logger.error("Could not open file {filename}.")
    return pubmed_by_nlm, process_count


if __name__ == "__main__":
    """
    call main start function
    """

    db_session = create_postgres_session(False)

    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)

    base_path = environ.get('XML_PATH', "")

    mods = ['FB', 'ZFIN']

    logger.info("Starting PubMed NLM resource update")
    update_resource_pubmed_nlm()

    logger.info("Loading PubMed NLM resource into memory")
    pubmed_by_nlm, nlm_by_issn = load_pubmed_resource_basic()

    logger.info("Loading database resource into memory")
    load_xref_data(db_session, 'resource')

    try:
        for mod in mods:
            pubmed_by_nlm, process_count = load_mod_resource(db_session, pubmed_by_nlm, nlm_by_issn, mod)
            logger.info(f"{mod}:  New: {process_count[PROCESSED_NEW]}, Updated {process_count[PROCESSED_UPDATED]}. Problems {process_count[PROCESSED_FAILED]}")
            process_count[PROCESSED_NEW] = 0
            process_count[PROCESSED_UPDATED] = 0
            process_count[PROCESSED_FAILED] = 0
    except Exception as e:
        mess = f"Error Loading mod resource {mod} with error {e}"
        logger.error(mess)
        print(mess)
        exit(-1)

    # Process the nlm ones.
    for entry_key in pubmed_by_nlm:
        entry = pubmed_by_nlm[entry_key]
        update_status, okay, message = process_single_resource(db_session, entry)
        process_count[update_status] += 1
        if not okay:
            logger.warning(message)
    logger.info(f"NLM: New: {process_count[PROCESSED_NEW]}, Updated {process_count[PROCESSED_UPDATED]}. Problems {process_count[PROCESSED_FAILED]}")

    logger.info("ending load_dqm_resource.py")
