import argparse
import logging
import requests
from datetime import datetime, timezone, timedelta
from typing import Any, Set, List, Dict, Optional, Tuple
from os import environ, path
from dotenv import load_dotenv
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_pubmed_xml import \
    download_pubmed_xml
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml_to_json import generate_json
from agr_literature_service.lit_processing.utils.db_read_utils import (
    retrieve_all_pmids, get_mod_papers, get_pmids_with_obsolete_mod_curie
)
from agr_literature_service.lit_processing.utils.report_utils import send_report
from agr_literature_service.lit_processing.data_ingest.post_reference_to_db import post_references
from agr_literature_service.lit_processing.utils.s3_utils import upload_xml_file_to_s3
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_search_new_references \
    import add_md5sum_to_database
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.sanitize_pubmed_json import \
    sanitize_pubmed_json_list
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir
from agr_literature_service.lit_processing.data_ingest.utils.alliance_paper_utils import (
    associate_papers_with_alliance,
    clean_up_tmp_directories,
    search_pubmed_for_validity,
    update_sgd_corpus_flag_to_true,
    associate_sgd_papers_with_corpus,
    extract_pmids_from_gaf,
    extract_pmids_with_sources_from_gaf,
)
from agr_literature_service.api.schemas import ModCorpusSortSourceType

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()
init_tmp_dir()

FMS_GAF_API_URL = "https://fms.alliancegenome.org/api/datafile/by/GAF?latest=true"

base_path = environ.get("XML_PATH", "")
file_path = base_path + "gaf_data/"
json_path = base_path + "pubmed_json/"
xml_path = base_path + "pubmed_xml/"
log_path = environ.get("LOG_PATH", "")

# Map dataSubType names to MOD abbreviations used in the database
MOD_NAME_MAP = {
    "HUMAN": "AGR",
    "MGI": "MGI",
    "SGD": "SGD",
    "WB": "WB",
    "FB": "FB",
    "ZFIN": "ZFIN",
    "RGD": "RGD",
    "XB": "XB",
}


def load_mod_gaf_papers(force: bool = False, hours: int = 24) -> str:  # pragma: no cover
    """
    Main function to load MOD GAF papers.
    - For HUMAN: Load new papers and associate with AGR MOD
    - For other MODs: Report PMIDs not in MOD corpus

    Args:
        force: If True, process all files regardless of uploadDate
        hours: Number of hours to check for recent uploads (default 24)

    Returns:
        str: Combined message for the Slack report
    """
    db_session = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, script_name)

    clean_up_tmp_directories([file_path, xml_path, json_path])

    # Fetch GAF file list from FMS API
    gaf_files = fetch_gaf_file_list()
    if not gaf_files:
        logger.error("Failed to fetch GAF file list from FMS API")
        return "<b>MOD GAF Paper Loading Report</b><p>Failed to fetch GAF file list from FMS API"

    # Filter files by uploadDate (within specified hours)
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
    files_to_process = []

    for gaf_file in gaf_files:
        upload_date_str = gaf_file.get("uploadDate")
        if not upload_date_str:
            continue

        upload_date = parse_upload_date(upload_date_str)
        if upload_date is None:
            logger.warning(f"Could not parse uploadDate: {upload_date_str}")
            continue

        data_sub_type = gaf_file.get("dataSubType", {}).get("name", "Unknown")

        if force or upload_date >= cutoff_time:
            files_to_process.append(gaf_file)
            logger.info(f"Will process {data_sub_type} GAF (uploaded: {upload_date_str})")
        else:
            logger.info(f"Skipping {data_sub_type} GAF - not updated within {hours} hours "
                        f"(uploaded: {upload_date_str})")

    if not files_to_process:
        logger.info(f"No GAF files updated within the last {hours} hours. Skipping report.")
        db_session.close()
        return ""  # Return empty string to indicate no report needed

    # Sort files: HUMAN first, then alphabetically by dataSubType name
    def sort_key(gaf_file: Dict) -> tuple:
        name = gaf_file.get("dataSubType", {}).get("name", "Unknown")
        return (0 if name == "HUMAN" else 1, name)

    files_to_process.sort(key=sort_key)

    message = "<b>MOD GAF Paper Loading Report</b><p>"
    all_pmids_db = set(retrieve_all_pmids(db_session))

    for gaf_file in files_to_process:
        data_sub_type = gaf_file.get("dataSubType", {}).get("name", "Unknown")
        s3_url = gaf_file.get("s3Url")

        if not s3_url:
            logger.warning(f"No s3Url for {data_sub_type} GAF file")
            continue

        logger.info(f"\nProcessing {data_sub_type} GAF file...")

        if data_sub_type == "HUMAN":
            file_message = process_human_gaf(db_session, s3_url, all_pmids_db)
        else:
            mod_abbr = MOD_NAME_MAP.get(data_sub_type)
            if mod_abbr:
                file_message = process_mod_gaf(db_session, data_sub_type, mod_abbr,
                                               s3_url, all_pmids_db)
            else:
                logger.warning(f"Unknown MOD type: {data_sub_type}")
                file_message = f"<p><b>{data_sub_type}</b>: Unknown MOD type, skipped</p>"

        message += file_message

    db_session.close()
    return message


def fetch_gaf_file_list() -> List[Dict]:
    """
    Fetch the list of GAF files from the FMS API.

    Returns:
        List of GAF file metadata dictionaries
    """
    try:
        response = requests.get(FMS_GAF_API_URL, timeout=60)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching GAF file list: {e}")
        return []


def parse_upload_date(date_str: str) -> Optional[datetime]:
    """
    Parse the uploadDate string from the API response.

    Args:
        date_str: ISO 8601 formatted date string

    Returns:
        datetime object with UTC timezone, or None if parsing fails
    """
    if not date_str:
        return None
    try:
        # Handle various ISO 8601 formats
        if date_str.endswith('Z'):
            date_str = date_str[:-1] + '+00:00'
        dt = datetime.fromisoformat(date_str)
        # Ensure timezone is set (assume UTC if not specified)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        try:
            # Try parsing without timezone
            dt = datetime.strptime(date_str[:19], "%Y-%m-%dT%H:%M:%S")
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None


def process_human_gaf(db_session, s3_url: str, all_pmids_db: Set[str]) -> str:  # pragma: no cover
    """
    Process HUMAN GAF file: load new papers and associate with AGR MOD.

    Args:
        db_session: Database session
        s3_url: URL to download the GAF file
        all_pmids_db: Set of all PMIDs currently in database

    Returns:
        HTML formatted message for the report
    """
    file_name = s3_url.split("/")[-1]
    file_with_path = f"{file_path}{file_name}"

    logger.info(f"Downloading HUMAN GAF from {s3_url}")
    try:
        response = requests.get(s3_url, timeout=300, stream=True)
        response.raise_for_status()
        with open(file_with_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Downloaded {file_name} successfully")
    except requests.RequestException as e:
        logger.error(f"Failed to download HUMAN GAF from {s3_url}: {e}")
        return "<p><b>HUMAN (AGR)</b>: Failed to download GAF file</p>"

    all_pmids = extract_pmids_from_gaf(file_with_path)
    if not all_pmids:
        return "<p><b>HUMAN (AGR)</b>: No PMIDs found in GAF file</p>"

    new_pmids = all_pmids - all_pmids_db
    logger.info(f"HUMAN GAF: {len(all_pmids)} total PMIDs, {len(new_pmids)} new")

    # Associate existing papers with AGR MOD if not in any MOD corpus
    papers_associated = associate_papers_with_alliance(db_session, all_pmids, 'AGR',
                                                       ModCorpusSortSourceType.Gaf)

    pmids_loaded: Set[str] = set()
    if new_pmids:
        # Download PubMed XML for new PMIDs
        download_pubmed_xml(list(new_pmids))
        generate_json(list(new_pmids), [])

        # Sanitize and post references
        inject_object: Dict[str, Any] = {}
        sanitize_pubmed_json_list(new_pmids, [inject_object])

        json_filepath = base_path + 'sanitized_reference_json/REFERENCE_PUBMED_PMID.json'
        post_references(json_filepath)

        # Track successfully loaded PMIDs and upload XML to S3
        for pmid in new_pmids:
            if path.exists(xml_path + pmid + ".xml"):
                pmids_loaded.add(pmid)
                if environ.get('ENV_STATE') and environ['ENV_STATE'] == 'prod':
                    logger.info(f"Uploading XML file to S3 for PMID:{pmid}")
                    upload_xml_file_to_s3(pmid, 'latest')

        add_md5sum_to_database(db_session, None, pmids_loaded)

        # Associate newly loaded papers with AGR MOD
        if pmids_loaded:
            newly_associated = associate_papers_with_alliance(db_session, pmids_loaded, 'AGR',
                                                              ModCorpusSortSourceType.Gaf)
            papers_associated += newly_associated

    # Check for obsolete PMIDs among those not loaded
    obsolete_pmids: Set[str] = set()
    pmids_not_loaded = new_pmids - pmids_loaded
    if pmids_not_loaded:
        api_key = environ.get('NCBI_API_KEY', '')
        obsolete_pmids, valid_pmids = search_pubmed_for_validity(pmids_not_loaded, api_key)

    message = "<p><b>HUMAN (AGR)</b></p>"
    message += "<ul>"
    message += f"<li>Total PMIDs in GAF: {len(all_pmids)}"
    message += f"<li>Already in database: {len(all_pmids) - len(new_pmids)}"
    message += f"<li>New references loaded: {len(pmids_loaded)}"
    message += f"<li>Papers associated with AGR: {papers_associated}"

    if obsolete_pmids:
        message += f"<li>Obsolete PMIDs ({len(obsolete_pmids)}):<br>"
        for pmid in sorted(obsolete_pmids):
            message += f"PMID:{pmid}<br>"

    message += "</ul>"

    return message


def load_sgd_new_papers(db_session, pmids_not_in_db: Set[str]) -> Set[str]:  # pragma: no cover
    """
    Load new papers from PubMed for SGD GAF PMIDs not in database.

    Args:
        db_session: Database session
        pmids_not_in_db: Set of PMIDs not in database

    Returns:
        Set of PMIDs that were successfully loaded
    """
    if not pmids_not_in_db:
        return set()

    pmids_loaded: Set[str] = set()

    # Download PubMed XML for new PMIDs
    logger.info(f"Downloading PubMed XML for {len(pmids_not_in_db)} new SGD papers...")
    download_pubmed_xml(list(pmids_not_in_db))
    generate_json(list(pmids_not_in_db), [])

    # Sanitize and post references
    inject_object: Dict[str, Any] = {}
    sanitize_pubmed_json_list(pmids_not_in_db, [inject_object])

    json_filepath = base_path + 'sanitized_reference_json/REFERENCE_PUBMED_PMID.json'
    post_references(json_filepath)

    # Track successfully loaded PMIDs and upload XML to S3
    for pmid in pmids_not_in_db:
        if path.exists(xml_path + pmid + ".xml"):
            pmids_loaded.add(pmid)
            if environ.get('ENV_STATE') and environ['ENV_STATE'] == 'prod':
                logger.info(f"Uploading XML file to S3 for PMID:{pmid}")
                upload_xml_file_to_s3(pmid, 'latest')

    add_md5sum_to_database(db_session, None, pmids_loaded)

    if pmids_loaded:
        logger.info(f"Successfully loaded {len(pmids_loaded)} new papers for SGD")

    return pmids_loaded


def process_sgd_corpus_updates(db_session, pmids_out_corpus: Set[str],  # pragma: no cover
                               pmids_in_db_not_associated: Set[str],
                               pmids_newly_loaded: Set[str] = None) -> Tuple[int, int, int, Set[str], Set[str], Set[str], Set[str]]:
    """
    Process SGD-specific corpus updates: add missing GAF papers to SGD corpus.

    Args:
        db_session: Database session
        pmids_out_corpus: PMIDs associated but outside corpus
        pmids_in_db_not_associated: PMIDs in DB but not associated with SGD
        pmids_newly_loaded: PMIDs that were just loaded from PubMed

    Returns:
        Tuple of (updated_count, added_count, newly_loaded_added_count,
                  pmids_still_outside, pmids_still_not_associated, pmids_added_from_new,
                  pmids_updated)
    """
    if pmids_newly_loaded is None:
        pmids_newly_loaded = set()

    sgd_updated_count = 0
    sgd_added_count = 0
    sgd_newly_loaded_added_count = 0
    pmids_still_outside_corpus = pmids_out_corpus
    pmids_still_not_associated = pmids_in_db_not_associated
    pmids_added_from_new: Set[str] = set()
    pmids_updated: Set[str] = set()

    # Update papers that are associated but outside corpus to be in corpus
    if pmids_out_corpus:
        sgd_updated_count, pmids_updated = update_sgd_corpus_flag_to_true(
            db_session, pmids_out_corpus
        )
        if sgd_updated_count > 0:
            logger.info(f"Updated {sgd_updated_count} paper(s) to SGD corpus (corpus=True)")
        pmids_still_outside_corpus = pmids_out_corpus - pmids_updated

    # Add papers in DB but not associated with SGD to SGD corpus
    if pmids_in_db_not_associated:
        sgd_added_count, added_pmids = associate_sgd_papers_with_corpus(
            db_session, pmids_in_db_not_associated, ModCorpusSortSourceType.Gaf
        )
        if sgd_added_count > 0:
            logger.info(f"Added {sgd_added_count} paper(s) to SGD corpus")
        pmids_still_not_associated = pmids_in_db_not_associated - added_pmids

    # Associate newly loaded papers with SGD corpus
    if pmids_newly_loaded:
        sgd_newly_loaded_added_count, pmids_added_from_new = associate_sgd_papers_with_corpus(
            db_session, pmids_newly_loaded, ModCorpusSortSourceType.Gaf
        )
        if sgd_newly_loaded_added_count > 0:
            logger.info(f"Added {sgd_newly_loaded_added_count} newly loaded paper(s) to SGD corpus")

    return (sgd_updated_count, sgd_added_count, sgd_newly_loaded_added_count,
            pmids_still_outside_corpus, pmids_still_not_associated, pmids_added_from_new,
            pmids_updated)


def build_mod_gaf_report_message(mod_abbr: str, data_sub_type: str,  # pragma: no cover
                                 all_pmids: Set[str], pmids_in_corpus: Set[str],
                                 pmids_out_corpus: Set[str], pmids_in_db_not_associated: Set[str],
                                 pmids_not_in_db: Set[str], all_pmids_obsolete_mod_curie: Set[str],
                                 sgd_updated_count: int, sgd_added_count: int,
                                 sgd_newly_loaded_count: int,
                                 pmids_still_outside_corpus: Set[str],
                                 pmids_still_not_associated: Set[str],
                                 pmids_not_loaded: Set[str],
                                 pmids_added_existing: Set[str],
                                 pmids_added_new: Set[str],
                                 pmids_updated: Set[str],
                                 format_pmid_func) -> str:
    """
    Build HTML report message for MOD GAF processing.

    Returns:
        HTML formatted message for the report
    """
    message = f"<p><b>{mod_abbr}</b></p>"
    message += "<ul>"
    message += f"<li>Total PMIDs in GAF: {len(all_pmids)}"
    message += f"<li>In {mod_abbr} corpus: {len(pmids_in_corpus)}"

    if data_sub_type == "SGD":
        if sgd_updated_count > 0:
            message += f"<li>Updated to SGD Corpus: {sgd_updated_count}"
        message += f"<li>Associated but outside corpus: {len(pmids_still_outside_corpus)}"
        if sgd_added_count > 0:
            message += f"<li>Added to SGD Corpus (existing in DB): {sgd_added_count}"
        if sgd_newly_loaded_count > 0:
            message += f"<li>New references loaded and added to SGD Corpus: {sgd_newly_loaded_count}"
        message += f"<li>In DB but not associated with {mod_abbr}: {len(pmids_still_not_associated)}"
        if pmids_not_loaded:
            message += f"<li>Not loaded (obsolete or unavailable): {len(pmids_not_loaded)}"
        pmids_out_corpus_for_report = pmids_still_outside_corpus
        pmids_not_associated_for_report = pmids_still_not_associated

        # For SGD: List all papers added to corpus (updated, existing, and newly loaded)
        all_papers_added = pmids_updated | pmids_added_existing | pmids_added_new
        if all_papers_added:
            message += f"<li>Papers added to SGD Corpus ({len(all_papers_added)}):<br>"
            for pmid in sorted(all_papers_added):
                if pmid in pmids_added_new:
                    source_label = " (new)"
                elif pmid in pmids_updated:
                    source_label = " (updated)"
                else:
                    source_label = " (existing)"
                message += f"{format_pmid_func(pmid)}{source_label}<br>"
    else:
        message += f"<li>Associated but outside corpus: {len(pmids_out_corpus)}"
        message += f"<li>In DB but not associated with {mod_abbr}: {len(pmids_in_db_not_associated)}"
        pmids_out_corpus_for_report = pmids_out_corpus
        pmids_not_associated_for_report = pmids_in_db_not_associated

    message += f"<li>Not in database: {len(pmids_not_in_db)}"

    # List PMIDs not in corpus inline in the report (for non-SGD or remaining issues)
    if data_sub_type != "SGD":
        pmids_not_in_corpus = pmids_out_corpus_for_report | pmids_not_associated_for_report | pmids_not_in_db
        if pmids_not_in_corpus:
            message += f"<li>PMIDs not in {mod_abbr} corpus ({len(pmids_not_in_corpus)}):<br>"
            for pmid in sorted(pmids_not_in_corpus):
                message += f"{format_pmid_func(pmid)}<br>"
    else:
        # For SGD: show only remaining issues (papers that couldn't be added)
        pmids_remaining_issues = pmids_out_corpus_for_report | pmids_still_not_associated | pmids_not_loaded
        if pmids_remaining_issues:
            message += f"<li>PMIDs still not in {mod_abbr} corpus ({len(pmids_remaining_issues)}):<br>"
            for pmid in sorted(pmids_remaining_issues):
                message += f"{format_pmid_func(pmid)}<br>"

    # List PMIDs with obsolete MOD curies
    if all_pmids_obsolete_mod_curie:
        message += f"<li>PMIDs with obsolete {mod_abbr} curie ({len(all_pmids_obsolete_mod_curie)}):<br>"
        for pmid in sorted(all_pmids_obsolete_mod_curie):
            message += f"PMID:{pmid}<br>"

    message += "</ul>"
    return message


def process_mod_gaf(db_session, data_sub_type: str, mod_abbr: str,  # pragma: no cover
                    s3_url: str, all_pmids_db: Set[str]) -> str:
    """
    Process MOD GAF file: report PMIDs not in MOD corpus (no loading).

    Args:
        db_session: Database session
        data_sub_type: The data sub type name (e.g., "MGI", "SGD")
        mod_abbr: The MOD abbreviation in the database
        s3_url: URL to download the GAF file
        all_pmids_db: Set of all PMIDs currently in database

    Returns:
        HTML formatted message for the report
    """
    file_name = s3_url.split("/")[-1]
    file_with_path = f"{file_path}{file_name}"

    logger.info(f"Downloading {data_sub_type} GAF from {s3_url}")
    try:
        response = requests.get(s3_url, timeout=300, stream=True)
        response.raise_for_status()
        with open(file_with_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Downloaded {file_name} successfully")
    except requests.RequestException as e:
        logger.error(f"Failed to download {data_sub_type} GAF from {s3_url}: {e}")
        return f"<p><b>{mod_abbr}</b>: Failed to download GAF file</p>"

    all_pmids, pmid_sources = extract_pmids_with_sources_from_gaf(file_with_path)
    if not all_pmids:
        return f"<p><b>{mod_abbr}</b>: No PMIDs found in GAF file</p>"

    # Get MOD corpus papers
    in_corpus_set, out_corpus_set = get_mod_papers(db_session, mod_abbr)

    # Get PMIDs with obsolete MOD curies (invalid MOD reference)
    pmids_with_obsolete_mod_curie = get_pmids_with_obsolete_mod_curie(db_session, mod_abbr)

    pmids_in_corpus = all_pmids & in_corpus_set
    pmids_out_corpus = all_pmids & out_corpus_set
    pmids_not_in_db = all_pmids - all_pmids_db
    pmids_in_db_not_associated = (all_pmids & all_pmids_db) - in_corpus_set - out_corpus_set

    # Filter out PMIDs with obsolete MOD curies from all "not in corpus" categories
    pmids_obsolete_mod_curie_out_corpus = pmids_out_corpus & pmids_with_obsolete_mod_curie
    pmids_obsolete_mod_curie_not_associated = pmids_in_db_not_associated & pmids_with_obsolete_mod_curie

    pmids_out_corpus = pmids_out_corpus - pmids_with_obsolete_mod_curie
    pmids_in_db_not_associated = pmids_in_db_not_associated - pmids_with_obsolete_mod_curie

    # Combine all PMIDs with obsolete MOD curies
    all_pmids_obsolete_mod_curie = pmids_obsolete_mod_curie_out_corpus | pmids_obsolete_mod_curie_not_associated

    logger.info(f"{mod_abbr} GAF: {len(all_pmids)} total, "
                f"{len(pmids_in_corpus)} in corpus, "
                f"{len(pmids_out_corpus)} associated but out of corpus, "
                f"{len(pmids_in_db_not_associated)} in DB not associated, "
                f"{len(pmids_not_in_db)} not in DB, "
                f"{len(all_pmids_obsolete_mod_curie)} with obsolete MOD curie")

    # Check for obsolete PMIDs among those not in database
    obsolete_pmids: Set[str] = set()
    if pmids_not_in_db:
        api_key = environ.get('NCBI_API_KEY', '')
        obsolete_pmids, _ = search_pubmed_for_validity(pmids_not_in_db, api_key)

    # SGD-specific: load new papers and add missing GAF papers to SGD corpus
    sgd_updated_count = 0
    sgd_added_count = 0
    sgd_newly_loaded_count = 0
    pmids_still_outside_corpus = pmids_out_corpus
    pmids_still_not_associated = pmids_in_db_not_associated
    pmids_not_loaded: Set[str] = set()
    pmids_added_existing: Set[str] = set()
    pmids_added_new: Set[str] = set()
    pmids_updated: Set[str] = set()

    if data_sub_type == "SGD":
        # Load new papers from PubMed for SGD (only valid ones, not obsolete)
        pmids_to_load = pmids_not_in_db - obsolete_pmids
        pmids_loaded: Set[str] = set()
        if pmids_to_load:
            pmids_loaded = load_sgd_new_papers(db_session, pmids_to_load)
            pmids_not_loaded = pmids_to_load - pmids_loaded
        # Obsolete PMIDs are also not loaded
        pmids_not_loaded = pmids_not_loaded | obsolete_pmids

        # Track which existing PMIDs will be added (for reporting)
        pmids_added_existing = pmids_in_db_not_associated.copy()

        # Process corpus updates including newly loaded papers
        (sgd_updated_count, sgd_added_count, sgd_newly_loaded_count,
         pmids_still_outside_corpus, pmids_still_not_associated,
         pmids_added_new, pmids_updated) = process_sgd_corpus_updates(
            db_session, pmids_out_corpus, pmids_in_db_not_associated, pmids_loaded
        )

        # Update pmids_added_existing to only include those that were actually added
        # (subtract those still not associated)
        pmids_added_existing = pmids_added_existing - pmids_still_not_associated

    # Helper function to format PMID with source and obsolete label
    def format_pmid_with_source(pmid: str) -> str:
        sources = pmid_sources.get(pmid, set())
        source_str = f" [{', '.join(sorted(sources))}]" if sources else ""
        obsolete_label = " (obsolete)" if pmid in obsolete_pmids else ""
        return f"PMID:{pmid}{source_str}{obsolete_label}"

    # Write log file for MOD (use original sets to show all missing PMIDs, even those added for SGD)
    write_mod_gaf_log_file(data_sub_type, mod_abbr, all_pmids, pmids_in_corpus,
                           pmids_out_corpus, pmids_in_db_not_associated,
                           pmids_not_in_db, all_pmids_obsolete_mod_curie,
                           format_pmid_with_source, sgd_updated_count, sgd_added_count,
                           sgd_newly_loaded_count, pmids_added_existing, pmids_added_new,
                           pmids_updated)

    # Build and return the report message
    return build_mod_gaf_report_message(
        mod_abbr, data_sub_type, all_pmids, pmids_in_corpus,
        pmids_out_corpus, pmids_in_db_not_associated, pmids_not_in_db,
        all_pmids_obsolete_mod_curie, sgd_updated_count, sgd_added_count,
        sgd_newly_loaded_count,
        pmids_still_outside_corpus, pmids_still_not_associated,
        pmids_not_loaded, pmids_added_existing, pmids_added_new,
        pmids_updated, format_pmid_with_source
    )


def _write_log_summary_stats(fw, mod_abbr: str, data_sub_type: str,
                             all_pmids: Set[str], pmids_in_corpus: Set[str],
                             pmids_out_corpus: Set[str], pmids_in_db_not_associated: Set[str],
                             pmids_not_in_db: Set[str], all_pmids_obsolete_mod_curie: Set[str],
                             sgd_updated_count: int, sgd_added_count: int,
                             sgd_newly_loaded_count: int) -> None:
    """Write summary statistics section of the log file."""
    fw.write("Summary:\n")
    fw.write(f"  Total PMIDs in GAF: {len(all_pmids)}\n")
    fw.write(f"  In {mod_abbr} corpus: {len(pmids_in_corpus)}\n")
    if data_sub_type == "SGD":
        _write_sgd_summary_stats(fw, mod_abbr, pmids_out_corpus, pmids_in_db_not_associated,
                                 sgd_updated_count, sgd_added_count, sgd_newly_loaded_count)
    else:
        fw.write(f"  Associated but outside corpus: {len(pmids_out_corpus)}\n")
        fw.write(f"  In DB but not associated with {mod_abbr}: {len(pmids_in_db_not_associated)}\n")
    fw.write(f"  Not in database: {len(pmids_not_in_db)}\n")
    if all_pmids_obsolete_mod_curie:
        fw.write(f"  PMIDs with obsolete {mod_abbr} curie: {len(all_pmids_obsolete_mod_curie)}\n")
    fw.write("\n")


def _write_sgd_summary_stats(fw, mod_abbr: str, pmids_out_corpus: Set[str],
                             pmids_in_db_not_associated: Set[str],
                             sgd_updated_count: int, sgd_added_count: int,
                             sgd_newly_loaded_count: int) -> None:
    """Write SGD-specific summary statistics."""
    if sgd_updated_count > 0:
        fw.write(f"  Updated to SGD Corpus: {sgd_updated_count}\n")
    fw.write(f"  Associated but outside corpus: {len(pmids_out_corpus) - sgd_updated_count}\n")
    if sgd_added_count > 0:
        fw.write(f"  Added to SGD Corpus (existing in DB): {sgd_added_count}\n")
    if sgd_newly_loaded_count > 0:
        fw.write(f"  New references loaded and added to SGD Corpus: {sgd_newly_loaded_count}\n")
    fw.write(f"  In DB but not associated with {mod_abbr}: {len(pmids_in_db_not_associated) - sgd_added_count}\n")


def _write_sgd_papers_added(fw, pmids_updated: Set[str], pmids_added_existing: Set[str],
                            pmids_added_new: Set[str], format_pmid_func) -> None:
    """Write SGD papers added to corpus section."""
    all_papers_added = pmids_updated | pmids_added_existing | pmids_added_new
    if not all_papers_added:
        return
    fw.write(f"Papers added to SGD Corpus ({len(all_papers_added)}):\n")
    fw.write("-" * 40 + "\n")
    for pmid in sorted(all_papers_added):
        source_label = _get_pmid_source_label(pmid, pmids_added_new, pmids_updated)
        fw.write(f"{format_pmid_func(pmid)}{source_label}\n")
    fw.write("\n")


def _get_pmid_source_label(pmid: str, pmids_added_new: Set[str], pmids_updated: Set[str]) -> str:
    """Get the source label for a PMID based on how it was added."""
    if pmid in pmids_added_new:
        return " (new)"
    if pmid in pmids_updated:
        return " (updated)"
    return " (existing)"


def _write_pmids_not_in_corpus(fw, mod_abbr: str, data_sub_type: str,
                               pmids_out_corpus: Set[str], pmids_in_db_not_associated: Set[str],
                               pmids_not_in_db: Set[str], format_pmid_func) -> None:
    """Write PMIDs not in corpus section."""
    pmids_not_in_corpus = pmids_out_corpus | pmids_in_db_not_associated | pmids_not_in_db
    if not pmids_not_in_corpus:
        return
    header = "PMIDs originally not in" if data_sub_type == "SGD" else "PMIDs not in"
    fw.write(f"{header} {mod_abbr} corpus ({len(pmids_not_in_corpus)}):\n")
    fw.write("-" * 40 + "\n")
    for pmid in sorted(pmids_not_in_corpus):
        fw.write(f"{format_pmid_func(pmid)}\n")
    fw.write("\n")


def _write_obsolete_curie_pmids(fw, mod_abbr: str,
                                all_pmids_obsolete_mod_curie: Set[str]) -> None:
    """Write PMIDs with obsolete MOD curie section."""
    if not all_pmids_obsolete_mod_curie:
        return
    fw.write(f"PMIDs with obsolete {mod_abbr} curie ({len(all_pmids_obsolete_mod_curie)}):\n")
    fw.write("-" * 40 + "\n")
    for pmid in sorted(all_pmids_obsolete_mod_curie):
        fw.write(f"PMID:{pmid}\n")
    fw.write("\n")


def write_mod_gaf_log_file(data_sub_type: str, mod_abbr: str,  # pragma: no cover
                           all_pmids: Set[str],
                           pmids_in_corpus: Set[str],
                           pmids_out_corpus: Set[str],
                           pmids_in_db_not_associated: Set[str],
                           pmids_not_in_db: Set[str],
                           all_pmids_obsolete_mod_curie: Set[str],
                           format_pmid_func,
                           sgd_updated_count: int = 0,
                           sgd_added_count: int = 0,
                           sgd_newly_loaded_count: int = 0,
                           pmids_added_existing: Set[str] = None,
                           pmids_added_new: Set[str] = None,
                           pmids_updated: Set[str] = None) -> None:
    """
    Write log file for MOD GAF processing with missing PMIDs and their sources.

    Args:
        data_sub_type: The data sub type name (e.g., "MGI", "SGD")
        mod_abbr: The MOD abbreviation
        all_pmids: All PMIDs extracted from GAF file
        pmids_in_corpus: PMIDs already in MOD corpus
        pmids_out_corpus: PMIDs associated but outside corpus (original, before SGD updates)
        pmids_in_db_not_associated: PMIDs in DB but not associated with MOD (original, before SGD adds)
        pmids_not_in_db: PMIDs not in database
        all_pmids_obsolete_mod_curie: PMIDs with obsolete MOD curie
        format_pmid_func: Function to format PMID with source/label
        sgd_updated_count: Number of papers updated to SGD corpus (SGD only)
        sgd_added_count: Number of papers added to SGD corpus from existing DB (SGD only)
        sgd_newly_loaded_count: Number of newly loaded papers added to SGD corpus (SGD only)
        pmids_added_existing: Set of PMIDs added from existing DB (SGD only)
        pmids_added_new: Set of PMIDs loaded from PubMed and added (SGD only)
        pmids_updated: Set of PMIDs updated from corpus=False/NULL to True (SGD only)
    """
    pmids_added_existing = pmids_added_existing or set()
    pmids_added_new = pmids_added_new or set()
    pmids_updated = pmids_updated or set()

    if not log_path:
        return

    logfile_name = f"gaf_{data_sub_type.lower()}.log"
    with open(log_path + logfile_name, "w") as fw:
        fw.write(f"{mod_abbr} GAF Processing Report\n")
        fw.write("=" * 50 + "\n\n")

        _write_log_summary_stats(fw, mod_abbr, data_sub_type, all_pmids, pmids_in_corpus,
                                 pmids_out_corpus, pmids_in_db_not_associated, pmids_not_in_db,
                                 all_pmids_obsolete_mod_curie, sgd_updated_count, sgd_added_count,
                                 sgd_newly_loaded_count)

        if data_sub_type == "SGD":
            _write_sgd_papers_added(fw, pmids_updated, pmids_added_existing,
                                    pmids_added_new, format_pmid_func)

        _write_pmids_not_in_corpus(fw, mod_abbr, data_sub_type, pmids_out_corpus,
                                   pmids_in_db_not_associated, pmids_not_in_db, format_pmid_func)

        _write_obsolete_curie_pmids(fw, mod_abbr, all_pmids_obsolete_mod_curie)


def send_slack_report(message: str):
    """Send the report to Slack."""
    email_subject = "MOD GAF Paper Loading Report"
    send_report(email_subject, message)


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Load MOD GAF papers - HUMAN to AGR corpus, report others"
    )
    parser.add_argument(
        '-n', '--no-slack',
        action='store_true',
        help="Do not send Slack report (for testing)"
    )
    parser.add_argument(
        '-f', '--force',
        action='store_true',
        help="Process all files regardless of uploadDate"
    )
    parser.add_argument(
        '--hours',
        type=int,
        default=24,
        help="Number of hours to check for recent uploads (default: 24)"
    )
    args = parser.parse_args()

    message = load_mod_gaf_papers(force=args.force, hours=args.hours)

    if not message:
        logger.info("No report to send (no GAF files updated).")
    elif not args.no_slack:
        send_slack_report(message)
    else:
        logger.info("Slack report disabled. Message content:")
        logger.info(message)
