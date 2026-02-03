import json
import logging
import traceback
from os import environ, makedirs, path
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from typing import Dict, Tuple

from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import load_pubmed_resource_basic
from agr_literature_service.lit_processing.utils.generic_utils import split_identifier
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.utils.resource_reference_utils import load_xref_data
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.get_dqm_data import \
    download_dqm_resource_json
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.dqm_resource_update_utils import (
    process_single_resource,
    PROCESSED_NEW,
    PROCESSED_UPDATED,
    PROCESSED_FAILED,
    PROCESSED_NO_CHANGE
)
from agr_literature_service.lit_processing.utils.report_utils import send_report
from agr_literature_service.api.user import set_global_user_id
load_dotenv()
init_tmp_dir()

process_count = [0, 0, 0, 0]  # [NEW, UPDATED, FAILED, NO_CHANGE]

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
        update_status = PROCESSED_NO_CHANGE  # NLM entries are processed later
        missing_prefix_xrefs = []
        xref_conflicts = []
    else:
        if 'primaryId' in entry:
            entry_cross_refs = set()
            if 'crossReferences' in entry:
                for cross_ref in entry['crossReferences']:
                    entry_cross_refs.add(cross_ref['id'])
            if entry['primaryId'] not in entry_cross_refs:
                # Only add primaryId as cross-reference if it has valid prefix:identifier format
                primary_id = entry['primaryId']
                prefix, identifier, _ = split_identifier(primary_id, ignore_error=True)
                if prefix is not None and identifier is not None:
                    entry_cross_refs.add(primary_id)
                    cross_ref = dict()
                    cross_ref['id'] = primary_id
                    if 'crossReferences' in entry:
                        entry['crossReferences'].append(cross_ref)
                    else:
                        entry['crossReferences'] = [cross_ref]

        update_status, okay, message, missing_prefix_xrefs, xref_conflicts = process_single_resource(db_session, entry)
        if not okay:
            logger.warning(message)
    return update_status, okay, message, missing_prefix_xrefs, xref_conflicts


def load_mod_resource(db_session: Session, pubmed_by_nlm: Dict, nlm_by_issn: Dict, mod: str) -> Tuple:
    """

    :param db_session: db connection
    :param pubmed_by_nlm: pubmed entry by nlm, pubmed_by_nlm processed at the end.
    :param nlm_by_issn: dict to look up nlm vis issn
    :param mod: mod to be processed
    :return:
    """

    base_path = environ.get('XML_PATH', '')
    all_missing_prefix_xrefs = []
    all_xref_conflicts = []

    filename = base_path + 'dqm_data/RESOURCE_' + mod + '.json'
    try:
        with open(filename, 'r') as f:
            dqm_data = json.load(f)
            for entry in dqm_data['data']:
                update_status, okay, message, missing_prefix_xrefs, xref_conflicts = process_entry(db_session, entry, pubmed_by_nlm, nlm_by_issn)
                process_count[update_status] += 1
                if missing_prefix_xrefs:
                    all_missing_prefix_xrefs.extend(missing_prefix_xrefs)
                if xref_conflicts:
                    all_xref_conflicts.extend(xref_conflicts)
                if not okay:
                    logger.warning(message)
    except IOError:
        # Some mods have no resources so exception here is okay but give message anyway.
        if mod in ['FB', 'ZFIN']:
            logger.error("Could not open file {filename}.")
    return pubmed_by_nlm, process_count, all_missing_prefix_xrefs, all_xref_conflicts


if __name__ == "__main__":
    """
    call main start function
    """

    db_session = create_postgres_session(False)

    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)

    base_path = environ.get('XML_PATH', "")

    logger.info("Downloading DQM files...")
    download_dqm_resource_json()

    logger.info("Loading PubMed NLM resource into memory...")
    pubmed_by_nlm, nlm_by_issn = load_pubmed_resource_basic()

    logger.info("Loading database resource into memory...")
    load_xref_data(db_session, 'resource')

    mods = ['FB', 'ZFIN']

    # Set up log file path
    report_file_path = ''
    log_url = None
    if environ.get('LOG_PATH'):
        report_file_path = path.join(environ['LOG_PATH'], 'dqm_load/')
    if report_file_path and not path.exists(report_file_path):
        makedirs(report_file_path)
    if environ.get('LOG_URL'):
        log_url = environ['LOG_URL'] + "dqm_load/"

    log_filename = path.join(report_file_path, 'dqm_resource_loading.log') if report_file_path else None
    fh_log = open(log_filename, 'w') if log_filename else None

    mod_results = {}
    all_missing_prefix_xrefs = {}
    all_xref_conflicts = {}
    for mod in mods:
        try:
            pubmed_by_nlm, process_count, missing_prefix_xrefs, xref_conflicts = load_mod_resource(db_session, pubmed_by_nlm, nlm_by_issn, mod)
            mod_results[mod] = {
                'new': process_count[PROCESSED_NEW],
                'updated': process_count[PROCESSED_UPDATED],
                'failed': process_count[PROCESSED_FAILED]
            }
            if missing_prefix_xrefs:
                all_missing_prefix_xrefs[mod] = missing_prefix_xrefs
            if xref_conflicts:
                all_xref_conflicts[mod] = xref_conflicts
            log_msg = f"{mod}:  New: {process_count[PROCESSED_NEW]}, Updated: {process_count[PROCESSED_UPDATED]}, Problems: {process_count[PROCESSED_FAILED]}"
            logger.info(log_msg)
            if fh_log:
                fh_log.write(log_msg + "\n")
                if missing_prefix_xrefs:
                    fh_log.write(f"{mod} cross-references missing prefix: {missing_prefix_xrefs}\n")
                if xref_conflicts:
                    fh_log.write(f"{mod} cross-reference conflicts: {xref_conflicts}\n")
            process_count[PROCESSED_NEW] = 0
            process_count[PROCESSED_UPDATED] = 0
            process_count[PROCESSED_FAILED] = 0
            process_count[PROCESSED_NO_CHANGE] = 0
        except Exception as e:
            tb = traceback.format_exc()
            mess = f"Error Loading mod resource {mod} with error {e}"
            logger.error(mess)
            if fh_log:
                fh_log.write(f"{mess}\n{tb}\n")
            send_report(f"{mod} DQM Resource Loading Failed",
                        f"Error message: {e}<p>Traceback:<br>{tb}")

    # Process the nlm ones.
    try:
        nlm_missing_prefix_xrefs = []
        nlm_xref_conflicts = []
        for entry_key in pubmed_by_nlm:
            entry = pubmed_by_nlm[entry_key]
            update_status, okay, message, missing_prefix_xrefs, xref_conflicts = process_single_resource(db_session, entry)
            process_count[update_status] += 1
            if missing_prefix_xrefs:
                nlm_missing_prefix_xrefs.extend(missing_prefix_xrefs)
            if xref_conflicts:
                nlm_xref_conflicts.extend(xref_conflicts)
            if not okay:
                logger.warning(message)
                if fh_log:
                    fh_log.write(f"NLM Warning: {message}\n")
        if nlm_missing_prefix_xrefs:
            all_missing_prefix_xrefs['NLM'] = nlm_missing_prefix_xrefs
        if nlm_xref_conflicts:
            all_xref_conflicts['NLM'] = nlm_xref_conflicts
        log_msg = f"NLM: New: {process_count[PROCESSED_NEW]}, Updated: {process_count[PROCESSED_UPDATED]}, Problems: {process_count[PROCESSED_FAILED]}"
        logger.info(log_msg)
        if fh_log:
            fh_log.write(log_msg + "\n")
            if nlm_missing_prefix_xrefs:
                fh_log.write(f"NLM cross-references missing prefix: {nlm_missing_prefix_xrefs}\n")
            if nlm_xref_conflicts:
                fh_log.write(f"NLM cross-reference conflicts: {nlm_xref_conflicts}\n")
    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"Error processing NLM resources: {e}")
        if fh_log:
            fh_log.write(f"Error processing NLM resources: {e}\n{tb}\n")

    if fh_log:
        fh_log.close()

    # Send summary report
    email_subject = "DQM Resource Loading Report"
    email_message = "<h3>DQM Resource Loading Report</h3>"
    rows = ""
    for source, counts in mod_results.items():
        rows += f"<tr><td>{source}</td><td>{counts['new']}</td><td>{counts['updated']}</td><td>{counts['failed']}</td></tr>"
    if rows:
        email_message += "<table><thead><tr><th>Source</th><th>New</th><th>Updated</th><th>Problems</th></tr></thead><tbody>"
        email_message += rows
        email_message += "</tbody></table>"

    # Report cross-references missing prefix
    if all_missing_prefix_xrefs:
        email_message += "<p><b>Cross-references missing valid prefix:identifier format:</b></p>"
        for mod, xrefs in all_missing_prefix_xrefs.items():
            email_message += f"<p><b>{mod}:</b> {', '.join(xrefs[:20])}"
            if len(xrefs) > 20:
                email_message += f" ... and {len(xrefs) - 20} more"
            email_message += "</p>"

    # Report cross-reference conflicts (already assigned to another resource)
    if all_xref_conflicts:
        email_message += "<p><b>Cross-reference conflicts (already assigned to another resource):</b></p>"
        for mod, conflicts in all_xref_conflicts.items():
            email_message += f"<p><b>{mod}:</b> {', '.join(conflicts[:20])}"
            if len(conflicts) > 20:
                email_message += f" ... and {len(conflicts) - 20} more"
            email_message += "</p>"

    if log_url:
        email_message += f"<p>Loading log file is available at <a href={log_url}>{log_url}</a></p>"
    elif report_file_path:
        email_message += f"<p>Loading log file is available at {report_file_path}</p>"

    send_report(email_subject, email_message)

    logger.info("ending load_dqm_resource.py")
