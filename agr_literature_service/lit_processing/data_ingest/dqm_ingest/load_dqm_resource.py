# load_dqm_resource.py
import json
import logging
import traceback
from os import environ, makedirs, path
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from typing import Dict, List, Tuple

from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import load_pubmed_resource_basic
from agr_literature_service.lit_processing.utils.generic_utils import split_identifier
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.utils.resource_reference_utils import load_xref_data
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_resources_nlm import \
    update_resource_pubmed_nlm
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.get_dqm_data import \
    download_dqm_resource_json
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.dqm_resource_update_utils import (
    process_single_resource,
    reset_resources_to_update,
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

# Only record true "unsolved" failures in the Problems section (exclude duplicate/shared/conflict-ish cases)
UNSOLVED_FAILURE_KINDS = {"invalid", "db_error", "unknown"}


def _strip_kind_prefix(msg: str) -> str:
    if not msg:
        return msg
    for tag in ("[invalid]", "[db_error]", "[unknown]"):
        if msg.startswith(tag):
            return msg[len(tag):].strip()
    return msg


def _entry_label(entry: Dict) -> str:
    # Keep this minimal and robust
    title = entry.get("title")
    pissn = entry.get("printISSN")
    oissn = entry.get("onlineISSN")

    xrefs = []
    for xr in entry.get("crossReferences", []) or []:
        if isinstance(xr, dict) and xr.get("id"):
            xrefs.append(xr["id"])
    xrefs = xrefs[:5]  # avoid huge email lines

    parts = []
    if title:
        parts.append(f"title={title}")
    if pissn:
        parts.append(f"printISSN={pissn}")
    if oissn:
        parts.append(f"onlineISSN={oissn}")
    if xrefs:
        parts.append(f"xrefs={xrefs}")
    return "; ".join(parts) if parts else "no identifiers available"


def get_nlm_from_xref(entry: Dict, nlm_by_issn: Dict) -> str:
    """
    Get the nlm value in the entry if it exists

    :param entry: dqm entry in json format
    :param nlm_by_issn: dict of nlm from a issn
    """
    nlm = ''
    for cross_ref in entry.get('crossReferences', []):
        if 'id' in cross_ref:
            prefix, identifier, separator = split_identifier(cross_ref['id'], ignore_error=True)
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
        for cross_ref in pubmed_by_nlm[nlm].get('crossReferences', []):
            if 'id' in cross_ref:
                nlm_cross_refs.add(cross_ref['id'])
        if 'crossReferences' in entry:
            for cross_ref in entry['crossReferences']:
                if cross_ref.get('id') and cross_ref['id'] not in nlm_cross_refs:
                    nlm_cross_refs.add(cross_ref['id'])
                    pubmed_by_nlm[nlm].setdefault('crossReferences', []).append(cross_ref)
        if 'primaryId' in entry:
            if entry['primaryId'] not in nlm_cross_refs:
                # the zfin primaryId is the nlm without the prefix, check if it already exists before adding for other MOD data
                zfin_nlm = 'NLM:' + entry['primaryId']
                if zfin_nlm not in nlm_cross_refs:
                    nlm_cross_refs.add(entry['primaryId'])
                    cross_ref = dict()
                    cross_ref['id'] = entry['primaryId']
                    pubmed_by_nlm[nlm].setdefault('crossReferences', []).append(cross_ref)
        # this causes conflicts if different MODs match an NLM and they send different non-pubmed information
        # whichever mod runs last will have the final value
        for field in resource_fields_not_in_pubmed:
            if field in entry:
                pubmed_by_nlm[nlm][field] = entry[field]


def process_entry(db_session: Session, entry: dict, pubmed_by_nlm: dict, nlm_by_issn: dict, mod: str = "", writer_mod: str = "") -> Tuple:
    """
    Process the original dqm json entry.
    First we "sanitize" the entry and then process it according
    to whether it has nlm in it or not.

    Backward compatible behavior:
      - If called with legacy signature (no mod/writer_mod), return:
            (update_status, okay, message)
      - If called with mod/writer_mod, return:
            (update_status, okay, message, field_changes, missing_prefix_xrefs,
             xref_conflicts, xref_additions, failure_kind)

    :return: Tuple of (update_status, okay, message, field_changes, missing_prefix_xrefs,
             xref_conflicts, xref_additions, failure_kind)
    """
    nlm = ''
    update_status = PROCESSED_NO_CHANGE
    okay = True
    message = ""
    field_changes = []
    missing_prefix_xrefs = []
    xref_conflicts = []
    xref_additions = []
    failure_kind = "none"

    primary_id = entry.get('primaryId')  # avoid unbound variable

    if primary_id and primary_id in pubmed_by_nlm:
        nlm = primary_id
    elif 'crossReferences' in entry:
        nlm = get_nlm_from_xref(entry, nlm_by_issn)

    if nlm != '':
        process_nlm(nlm, entry, pubmed_by_nlm)
        update_status = PROCESSED_NO_CHANGE  # NLM entries are processed later
    else:
        # sanitize: only add primaryId as xref if valid prefix:identifier format
        if 'primaryId' in entry:
            entry_cross_refs = set()
            if 'crossReferences' in entry:
                for cross_ref in entry['crossReferences']:
                    if cross_ref.get('id'):
                        entry_cross_refs.add(cross_ref['id'])

            if entry['primaryId'] not in entry_cross_refs:
                primary_id = entry['primaryId']
                prefix, identifier, _ = split_identifier(primary_id, ignore_error=True)
                if prefix is not None and identifier is not None:
                    cross_ref = {'id': primary_id}
                    if 'crossReferences' in entry:
                        entry['crossReferences'].append(cross_ref)
                    else:
                        entry['crossReferences'] = [cross_ref]

        update_status, okay, message, field_changes, missing_prefix_xrefs, xref_conflicts, xref_additions, failure_kind = \
            process_single_resource(db_session, entry, mod=mod, writer_mod=writer_mod)
        if not okay:
            logger.warning(message)

    # Legacy mode for unit tests: return only the first 3 values
    if not mod and not writer_mod:
        return update_status, okay, message

    return update_status, okay, message, field_changes, missing_prefix_xrefs, xref_conflicts, xref_additions, failure_kind


def load_mod_resource(db_session: Session, pubmed_by_nlm: Dict, nlm_by_issn: Dict, mod: str, writer_mod: str) -> Tuple:
    """
    Load and process MOD resource data.

    :return: Tuple of (pubmed_by_nlm, process_count, all_field_changes, all_missing_prefix_xrefs,
             all_xref_conflicts, all_xref_additions, all_problems)
    """

    base_path = environ.get('XML_PATH', '')
    all_field_changes: List[Dict] = []
    all_missing_prefix_xrefs: List[str] = []
    all_xref_conflicts: List[str] = []
    all_xref_additions: List[Dict] = []
    all_problems: List[str] = []

    filename = base_path + 'dqm_data/RESOURCE_' + mod + '.json'
    try:
        with open(filename, 'r') as f:
            dqm_data = json.load(f)
            for entry in dqm_data.get('data', []):
                result = process_entry(db_session, entry, pubmed_by_nlm, nlm_by_issn, mod, writer_mod)
                update_status, okay, message, field_changes, missing_prefix_xrefs, xref_conflicts, xref_additions, failure_kind = result

                process_count[update_status] += 1

                if field_changes:
                    all_field_changes.extend(field_changes)
                if missing_prefix_xrefs:
                    all_missing_prefix_xrefs.extend(missing_prefix_xrefs)
                if xref_conflicts:
                    all_xref_conflicts.extend(xref_conflicts)
                if xref_additions:
                    all_xref_additions.extend(xref_additions)

                # Only record true unsolved failures (exclude duplicates/conflicts/skips)
                if not okay and failure_kind in UNSOLVED_FAILURE_KINDS:
                    pid = entry.get('primaryId')
                    label = pid if pid else _entry_label(entry)
                    clean_message = _strip_kind_prefix(message)
                    all_problems.append(f"{label}: [{failure_kind}] {clean_message}")
                    logger.warning(clean_message)

    except IOError:
        # Some mods have no resources so exception here is okay but give message anyway.
        if mod in ['FB', 'ZFIN']:
            logger.error(f"Could not open file {filename}.")
    return pubmed_by_nlm, process_count, all_field_changes, all_missing_prefix_xrefs, all_xref_conflicts, all_xref_additions, all_problems


if __name__ == "__main__":  # noqa: C901
    """
    call main start function
    """

    db_session = create_postgres_session(False)

    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)

    base_path = environ.get('XML_PATH', "")

    logger.info("Downloading DQM files...")
    download_dqm_resource_json()

    logger.info("Starting PubMed NLM resource update...")
    update_resource_pubmed_nlm()

    logger.info("Loading PubMed NLM resource into memory...")
    pubmed_by_nlm, nlm_by_issn = load_pubmed_resource_basic()

    logger.info("Loading database resource into memory...")
    load_xref_data(db_session, 'resource')

    mods = ['FB', 'ZFIN']
    writer_mod = mods[-1]  # LAST MOD WINS for shared resources

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
    all_mod_field_changes = {}
    all_mod_missing_prefix_xrefs = {}
    all_mod_xref_conflicts = {}
    all_mod_xref_additions = {}
    all_mod_problems = {}

    for mod in mods:
        try:
            # IMPORTANT: reset per-mod run lock (prevents FB vs FB flip-flops within a mod)
            reset_resources_to_update()

            result = load_mod_resource(db_session, pubmed_by_nlm, nlm_by_issn, mod, writer_mod)
            pubmed_by_nlm, process_count, field_changes, missing_prefix_xrefs, xref_conflicts, xref_additions, problems = result
            mod_results[mod] = {
                'new': process_count[PROCESSED_NEW],
                'updated': process_count[PROCESSED_UPDATED],
                'failed': process_count[PROCESSED_FAILED]
            }
            if field_changes:
                all_mod_field_changes[mod] = field_changes
            if missing_prefix_xrefs:
                all_mod_missing_prefix_xrefs[mod] = missing_prefix_xrefs
            if xref_conflicts:
                all_mod_xref_conflicts[mod] = xref_conflicts
            if xref_additions:
                all_mod_xref_additions[mod] = xref_additions
            if problems:
                all_mod_problems[mod] = problems

            log_msg = f"{mod}: New: {process_count[PROCESSED_NEW]}, Updated: {process_count[PROCESSED_UPDATED]}, Problems: {process_count[PROCESSED_FAILED]}"
            logger.info(log_msg)

            # Write to log file
            if fh_log:
                fh_log.write(f"\n{'='*60}\n")
                fh_log.write(f"{mod} Resource Loading Results\n")
                fh_log.write(f"{'='*60}\n")
                fh_log.write(f"{log_msg}\n\n")

                # Log all updated rows (field updates + xref additions), grouped per resource
                grouped = {}
                for change in field_changes or []:
                    key = (change.get('agr', 'N/A'), change.get('primary_id', 'N/A'))
                    grouped.setdefault(key, {'fields': [], 'xrefs': []})
                    grouped[key]['fields'].append(change)
                for add in xref_additions or []:
                    key = (add.get('agr', 'N/A'), add.get('primary_id', 'N/A'))
                    grouped.setdefault(key, {'fields': [], 'xrefs': []})
                    grouped[key]['xrefs'].append(add.get('xref'))

                if grouped:
                    fh_log.write(f"--- {mod} Updated Rows ({len(grouped)} resources changed) ---\n")
                    for (agr, pid), payload in sorted(grouped.items()):
                        fh_log.write(f"\n{agr} ({pid})\n")
                        for ch in payload['fields']:
                            fh_log.write(f"  Field: {ch.get('field')}\n")
                            fh_log.write(f"    Old: {ch.get('old_value')}\n")
                            fh_log.write(f"    New: {ch.get('new_value')}\n")
                        if payload['xrefs']:
                            fh_log.write("  Added crossReferences:\n")
                            for x in payload['xrefs']:
                                fh_log.write(f"    + {x}\n")
                    fh_log.write("\n")

                # Log missing prefix xrefs
                if missing_prefix_xrefs:
                    fh_log.write(f"--- {mod} Cross-references missing valid prefix ({len(missing_prefix_xrefs)}) ---\n")
                    for xref in missing_prefix_xrefs:
                        fh_log.write(f"  {xref}\n")
                    fh_log.write("\n")

                # Log xref conflicts
                if xref_conflicts:
                    fh_log.write(f"--- {mod} Cross-reference conflicts ({len(xref_conflicts)}) ---\n")
                    for conflict in xref_conflicts:
                        fh_log.write(f"  {conflict}\n")
                    fh_log.write("\n")

                # Log true unsolved problems only (excludes duplicates/conflicts)
                if problems:
                    fh_log.write(f"--- {mod} Unsolved Problems ({len(problems)}) ---\n")
                    for problem in problems:
                        fh_log.write(f"  {problem}\n")
                    fh_log.write("\n")

            # Reset counts for next MOD
            process_count[PROCESSED_NEW] = 0
            process_count[PROCESSED_UPDATED] = 0
            process_count[PROCESSED_FAILED] = 0
            process_count[PROCESSED_NO_CHANGE] = 0

        except Exception as e:
            tb = traceback.format_exc()
            mess = f"Error Loading mod resource {mod} with error {e}"
            logger.error(mess)
            if fh_log:
                fh_log.write(f"ERROR: {mess}\n{tb}\n")
            send_report(f"{mod} DQM Resource Loading Failed",
                        f"Error message: {e}<p>Traceback:<br>{tb}")

    # Process the NLM ones (no reporting - NLM data comes from PubMed)
    try:
        for entry_key in pubmed_by_nlm:
            entry = pubmed_by_nlm[entry_key]
            _, okay, message, _, _, _, _, _ = process_single_resource(db_session, entry, mod="", writer_mod="")
            if not okay:
                logger.warning(message)
    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"Error processing NLM resources: {e}")

    if fh_log:
        fh_log.close()

    # Send summary report (FB and ZFIN only, not NLM)
    email_subject = "DQM Resource Loading Report"
    email_message = "<h3>DQM Resource Loading Report</h3>"
    rows = ""
    for source, counts in mod_results.items():
        rows += f"<tr><td>{source}</td><td>{counts['new']}</td><td>{counts['updated']}</td><td>{counts['failed']}</td></tr>"
    if rows:
        email_message += "<table border='1' cellpadding='5' cellspacing='0'>"
        email_message += "<thead><tr><th>Source</th><th>New</th><th>Updated</th><th>Problems</th></tr></thead><tbody>"
        email_message += rows
        email_message += "</tbody></table>"

    # Report cross-references missing prefix
    if all_mod_missing_prefix_xrefs:
        email_message += "<p><b>Cross-references missing valid prefix:identifier format:</b></p>"
        for mod, xrefs in all_mod_missing_prefix_xrefs.items():
            email_message += f"<p><b>{mod}:</b> {', '.join(xrefs[:20])}"
            if len(xrefs) > 20:
                email_message += f" ... and {len(xrefs) - 20} more"
            email_message += "</p>"

    # Report cross-reference conflicts
    if all_mod_xref_conflicts:
        email_message += "<p><b>Cross-reference conflicts (already assigned to another resource):</b></p>"
        for mod, conflicts in all_mod_xref_conflicts.items():
            email_message += f"<p><b>{mod}:</b> {', '.join(conflicts[:20])}"
            if len(conflicts) > 20:
                email_message += f" ... and {len(conflicts) - 20} more"
            email_message += "</p>"

    # Report true unsolved problems only
    if all_mod_problems:
        email_message += "<p><b>Unsolved problems (resources that could not be loaded):</b></p>"
        for mod, problems in all_mod_problems.items():
            cleaned = [p.lstrip(": ").strip() for p in problems[:10]]
            email_message += f"<p><b>{mod}:</b><br>{'<br>'.join(cleaned)}"
            if len(problems) > 10:
                email_message += f"<br>... and {len(problems) - 10} more"
            email_message += "</p>"

    # Add log file link
    if log_url:
        log_file_url = log_url + "dqm_resource_loading.log"
        email_message += f"<p>Loading log file is available at <a href='{log_file_url}'>{log_file_url}</a></p>"
    elif report_file_path:
        email_message += f"<p>Loading log file is available at {report_file_path}dqm_resource_loading.log</p>"

    send_report(email_subject, email_message)

    logger.info("ending load_dqm_resource.py")
