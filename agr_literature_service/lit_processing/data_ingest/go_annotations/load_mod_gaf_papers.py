import argparse
import logging
import requests
import gzip
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
)
from agr_literature_service.api.models import ModCorpusAssociationModel, ModModel
from agr_literature_service.api.schemas import ModCorpusSortSourceType
from sqlalchemy import text

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


def process_sgd_corpus_updates(db_session, pmids_out_corpus: Set[str],  # pragma: no cover
                               pmids_in_db_not_associated: Set[str]) -> Tuple[int, int, Set[str], Set[str]]:
    """
    Process SGD-specific corpus updates: add missing GAF papers to SGD corpus.

    Args:
        db_session: Database session
        pmids_out_corpus: PMIDs associated but outside corpus
        pmids_in_db_not_associated: PMIDs in DB but not associated with SGD

    Returns:
        Tuple of (updated_count, added_count, pmids_still_outside, pmids_still_not_associated)
    """
    sgd_updated_count = 0
    sgd_added_count = 0
    pmids_still_outside_corpus = pmids_out_corpus
    pmids_still_not_associated = pmids_in_db_not_associated

    # Update papers that are associated but outside corpus to be in corpus
    if pmids_out_corpus:
        sgd_updated_count, updated_pmids = update_sgd_corpus_flag_to_true(
            db_session, pmids_out_corpus
        )
        if sgd_updated_count > 0:
            logger.info(f"Updated {sgd_updated_count} paper(s) to SGD corpus (corpus=True)")
        pmids_still_outside_corpus = pmids_out_corpus - updated_pmids

    # Add papers in DB but not associated with SGD to SGD corpus
    if pmids_in_db_not_associated:
        sgd_added_count, added_pmids = associate_sgd_gaf_papers_with_corpus(
            db_session, pmids_in_db_not_associated
        )
        if sgd_added_count > 0:
            logger.info(f"Added {sgd_added_count} paper(s) to SGD corpus")
        pmids_still_not_associated = pmids_in_db_not_associated - added_pmids

    return sgd_updated_count, sgd_added_count, pmids_still_outside_corpus, pmids_still_not_associated


def build_mod_gaf_report_message(mod_abbr: str, data_sub_type: str,  # pragma: no cover
                                 all_pmids: Set[str], pmids_in_corpus: Set[str],
                                 pmids_out_corpus: Set[str], pmids_in_db_not_associated: Set[str],
                                 pmids_not_in_db: Set[str], all_pmids_obsolete_mod_curie: Set[str],
                                 sgd_updated_count: int, sgd_added_count: int,
                                 pmids_still_outside_corpus: Set[str],
                                 pmids_still_not_associated: Set[str],
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
            message += f"<li>Added to SGD Corpus: {sgd_added_count}"
        message += f"<li>In DB but not associated with {mod_abbr}: {len(pmids_still_not_associated)}"
        pmids_out_corpus_for_report = pmids_still_outside_corpus
        pmids_not_associated_for_report = pmids_still_not_associated
    else:
        message += f"<li>Associated but outside corpus: {len(pmids_out_corpus)}"
        message += f"<li>In DB but not associated with {mod_abbr}: {len(pmids_in_db_not_associated)}"
        pmids_out_corpus_for_report = pmids_out_corpus
        pmids_not_associated_for_report = pmids_in_db_not_associated

    message += f"<li>Not in database: {len(pmids_not_in_db)}"

    # List PMIDs not in corpus inline in the report
    pmids_not_in_corpus = pmids_out_corpus_for_report | pmids_not_associated_for_report | pmids_not_in_db
    if pmids_not_in_corpus:
        message += f"<li>PMIDs not in {mod_abbr} corpus ({len(pmids_not_in_corpus)}):<br>"
        for pmid in sorted(pmids_not_in_corpus):
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

    # SGD-specific: add missing GAF papers to SGD corpus
    sgd_updated_count = 0
    sgd_added_count = 0
    pmids_still_outside_corpus = pmids_out_corpus
    pmids_still_not_associated = pmids_in_db_not_associated
    if data_sub_type == "SGD":
        (sgd_updated_count, sgd_added_count,
         pmids_still_outside_corpus, pmids_still_not_associated) = process_sgd_corpus_updates(
            db_session, pmids_out_corpus, pmids_in_db_not_associated
        )

    # Check for obsolete PMIDs among those not in database
    obsolete_pmids: Set[str] = set()
    if pmids_not_in_db:
        api_key = environ.get('NCBI_API_KEY', '')
        obsolete_pmids, _ = search_pubmed_for_validity(pmids_not_in_db, api_key)

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
                           format_pmid_with_source, sgd_updated_count, sgd_added_count)

    # Build and return the report message
    return build_mod_gaf_report_message(
        mod_abbr, data_sub_type, all_pmids, pmids_in_corpus,
        pmids_out_corpus, pmids_in_db_not_associated, pmids_not_in_db,
        all_pmids_obsolete_mod_curie, sgd_updated_count, sgd_added_count,
        pmids_still_outside_corpus, pmids_still_not_associated,
        format_pmid_with_source
    )


def write_mod_gaf_log_file(data_sub_type: str, mod_abbr: str,  # pragma: no cover
                           all_pmids: Set[str],
                           pmids_in_corpus: Set[str],
                           pmids_out_corpus: Set[str],
                           pmids_in_db_not_associated: Set[str],
                           pmids_not_in_db: Set[str],
                           all_pmids_obsolete_mod_curie: Set[str],
                           format_pmid_func,
                           sgd_updated_count: int = 0,
                           sgd_added_count: int = 0) -> None:
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
        sgd_added_count: Number of papers added to SGD corpus (SGD only)
    """
    if not log_path:
        return

    logfile_name = f"gaf_{data_sub_type.lower()}.log"
    with open(log_path + logfile_name, "w") as fw:
        fw.write(f"{mod_abbr} GAF Processing Report\n")
        fw.write("=" * 50 + "\n\n")

        # Summary statistics
        fw.write("Summary:\n")
        fw.write(f"  Total PMIDs in GAF: {len(all_pmids)}\n")
        fw.write(f"  In {mod_abbr} corpus: {len(pmids_in_corpus)}\n")
        if data_sub_type == "SGD":
            if sgd_updated_count > 0:
                fw.write(f"  Updated to SGD Corpus: {sgd_updated_count}\n")
            fw.write(f"  Associated but outside corpus: {len(pmids_out_corpus) - sgd_updated_count}\n")
            if sgd_added_count > 0:
                fw.write(f"  Added to SGD Corpus: {sgd_added_count}\n")
            fw.write(f"  In DB but not associated with {mod_abbr}: {len(pmids_in_db_not_associated) - sgd_added_count}\n")
        else:
            fw.write(f"  Associated but outside corpus: {len(pmids_out_corpus)}\n")
            fw.write(f"  In DB but not associated with {mod_abbr}: {len(pmids_in_db_not_associated)}\n")
        fw.write(f"  Not in database: {len(pmids_not_in_db)}\n")
        if all_pmids_obsolete_mod_curie:
            fw.write(f"  PMIDs with obsolete {mod_abbr} curie: {len(all_pmids_obsolete_mod_curie)}\n")
        fw.write("\n")

        # List all missing PMIDs (not in corpus) with their sources
        pmids_not_in_corpus = pmids_out_corpus | pmids_in_db_not_associated | pmids_not_in_db
        if pmids_not_in_corpus:
            if data_sub_type == "SGD":
                fw.write(f"PMIDs not in {mod_abbr} corpus - added to corpus ({len(pmids_not_in_corpus)}):\n")
            else:
                fw.write(f"PMIDs not in {mod_abbr} corpus ({len(pmids_not_in_corpus)}):\n")
            fw.write("-" * 40 + "\n")
            for pmid in sorted(pmids_not_in_corpus):
                fw.write(f"{format_pmid_func(pmid)}\n")
            fw.write("\n")

        # PMIDs with obsolete MOD curie
        if all_pmids_obsolete_mod_curie:
            fw.write(f"PMIDs with obsolete {mod_abbr} curie ({len(all_pmids_obsolete_mod_curie)}):\n")
            fw.write("-" * 40 + "\n")
            for pmid in sorted(all_pmids_obsolete_mod_curie):
                fw.write(f"PMID:{pmid}\n")
            fw.write("\n")


def extract_pmids_from_gaf(file_with_path: str) -> Set[str]:
    """
    Extract all unique PMIDs from a GAF file.

    Args:
        file_with_path: Path to the GAF file (gzipped)

    Returns:
        Set of PMIDs (without PMID: prefix)
    """
    pmids, _ = extract_pmids_with_sources_from_gaf(file_with_path)
    return pmids


def extract_pmids_with_sources_from_gaf(file_with_path: str) -> Tuple[Set[str], Dict[str, Set[str]]]:
    """
    Extract all unique PMIDs and their annotation sources from a GAF file.

    Args:
        file_with_path: Path to the GAF file (gzipped or plain text)

    Returns:
        Tuple of (Set of PMIDs, Dict mapping PMID to set of sources)
    """
    all_pmids: Set[str] = set()
    pmid_sources: Dict[str, Set[str]] = {}

    try:
        # Handle both gzipped and plain text files
        if file_with_path.endswith('.gz'):
            f = gzip.open(file_with_path, "rt")
        else:
            f = open(file_with_path, "r")

        with f:
            for line in f:
                # Skip comment lines
                if line.startswith("!"):
                    continue

                parts = line.strip().split("\t")
                if len(parts) < 15:
                    continue

                # Column 6 (index 5) contains the DB:Reference field
                # Column 15 (index 14) contains the assigned_by field
                ref_col = parts[5]
                source = parts[14] if len(parts) > 14 else "Unknown"
                refs = ref_col.split("|")

                for ref in refs:
                    ref = ref.strip()
                    if ref.startswith("PMID:"):
                        pmid = ref.replace("PMID:", "")
                        if pmid.isdigit():
                            all_pmids.add(pmid)
                            if pmid not in pmid_sources:
                                pmid_sources[pmid] = set()
                            pmid_sources[pmid].add(source)
    except Exception as e:
        logger.error(f"Error reading GAF file {file_with_path}: {e}")

    return all_pmids, pmid_sources


def update_sgd_corpus_flag_to_true(db_session, pmids: Set[str]) -> Tuple[int, Set[str]]:  # pragma: no cover
    """
    Update SGD GAF papers that are associated but outside corpus
    to be inside the corpus (corpus=True).

    Args:
        db_session: Database session
        pmids: Set of PMIDs to update

    Returns:
        Tuple of (count of papers updated, set of PMIDs that were updated)
    """
    if not pmids:
        return 0, set()

    sgd_mod = db_session.query(ModModel).filter(
        ModModel.abbreviation == 'SGD'
    ).first()
    if not sgd_mod:
        logger.warning("SGD MOD not found in database")
        return 0, set()

    sgd_mod_id = sgd_mod.mod_id

    # Build parameterized query for PMIDs
    pmid_curies = [f"PMID:{pmid}" for pmid in pmids]

    # Get reference_ids for PMIDs using parameterized query
    query = text(
        "SELECT cr.curie, cr.reference_id "
        "FROM cross_reference cr "
        "WHERE cr.curie = ANY(:pmid_curies) "
        "AND cr.is_obsolete = False"
    )
    rows = db_session.execute(query, {"pmid_curies": pmid_curies}).fetchall()

    pmid_to_ref_id = {row[0].replace('PMID:', ''): row[1] for row in rows}
    reference_ids_in_db = set(pmid_to_ref_id.values())

    if not reference_ids_in_db:
        return 0, set()

    ref_ids_list = list(reference_ids_in_db)

    # Build reverse mapping: ref_id -> pmid
    ref_id_to_pmid = {v: k for k, v in pmid_to_ref_id.items()}

    # Update mod_corpus_association records where corpus=False to corpus=True
    # Use RETURNING to get the reference_ids that were actually updated
    update_query = text(
        "UPDATE mod_corpus_association "
        "SET corpus = True "
        "WHERE reference_id = ANY(:ref_ids) "
        "AND mod_id = :sgd_mod_id "
        "AND corpus = False "
        "RETURNING reference_id"
    )
    result = db_session.execute(
        update_query,
        {
            "ref_ids": ref_ids_list,
            "sgd_mod_id": sgd_mod_id
        }
    )
    updated_ref_ids = {row[0] for row in result.fetchall()}
    count = len(updated_ref_ids)

    # Map back to PMIDs
    updated_pmids = {ref_id_to_pmid[ref_id] for ref_id in updated_ref_ids if ref_id in ref_id_to_pmid}

    if count > 0:
        db_session.commit()

    return count, updated_pmids


def associate_sgd_gaf_papers_with_corpus(db_session, pmids: Set[str]) -> Tuple[int, Set[str]]:  # pragma: no cover
    """
    Associate SGD GAF papers with the SGD corpus.
    Only associates papers that do NOT already have a mod_corpus_association
    with SGD.

    Args:
        db_session: Database session
        pmids: Set of PMIDs to associate

    Returns:
        Tuple of (count of papers associated, set of PMIDs that were added)
    """
    if not pmids:
        return 0, set()

    sgd_mod = db_session.query(ModModel).filter(
        ModModel.abbreviation == 'SGD'
    ).first()
    if not sgd_mod:
        logger.warning("SGD MOD not found in database")
        return 0, set()

    sgd_mod_id = sgd_mod.mod_id

    # Build parameterized query for PMIDs
    pmid_curies = [f"PMID:{pmid}" for pmid in pmids]

    # Get reference_ids for PMIDs using parameterized query
    query = text(
        "SELECT cr.curie, cr.reference_id "
        "FROM cross_reference cr "
        "WHERE cr.curie = ANY(:pmid_curies) "
        "AND cr.is_obsolete = False"
    )
    rows = db_session.execute(query, {"pmid_curies": pmid_curies}).fetchall()

    pmid_to_ref_id = {row[0].replace('PMID:', ''): row[1] for row in rows}
    reference_ids_in_db = set(pmid_to_ref_id.values())

    if not reference_ids_in_db:
        return 0, set()

    ref_ids_list = list(reference_ids_in_db)

    # Get reference_ids that already have an association with SGD
    refs_already_associated_query = text(
        "SELECT DISTINCT reference_id FROM mod_corpus_association "
        "WHERE reference_id = ANY(:ref_ids) "
        "AND mod_id = :sgd_mod_id"
    )
    refs_already_associated = db_session.execute(
        refs_already_associated_query,
        {"ref_ids": ref_ids_list, "sgd_mod_id": sgd_mod_id}
    ).fetchall()

    already_associated = {row[0] for row in refs_already_associated}

    # Build reverse mapping: ref_id -> pmid
    ref_id_to_pmid = {v: k for k, v in pmid_to_ref_id.items()}

    # Add mod_corpus_association for papers not yet associated with SGD
    count = 0
    pmids_added = set()
    for ref_id in reference_ids_in_db:
        if ref_id not in already_associated:
            mca = ModCorpusAssociationModel(
                reference_id=ref_id,
                mod_id=sgd_mod_id,
                corpus=True,
                mod_corpus_sort_source=ModCorpusSortSourceType.Gaf
            )
            db_session.add(mca)
            count += 1
            if ref_id in ref_id_to_pmid:
                pmids_added.add(ref_id_to_pmid[ref_id])

    if count > 0:
        db_session.commit()
        logger.info(f"Associated {count} SGD GAF paper(s) with SGD corpus")

    return count, pmids_added


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
