import argparse
import logging
import requests
import gzip
import shutil
from datetime import datetime, timezone, timedelta
from typing import Set, List, Dict, Optional
from os import environ, makedirs, path
from dotenv import load_dotenv
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import \
    download_file
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_pubmed_xml import \
    download_pubmed_xml
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml_to_json import generate_json
from agr_literature_service.lit_processing.utils.db_read_utils import retrieve_all_pmids, get_mod_papers
from agr_literature_service.api.models import ModCorpusAssociationModel, ModModel
from agr_literature_service.api.schemas import ModCorpusSortSourceType
from sqlalchemy import text
from agr_literature_service.lit_processing.utils.report_utils import send_report
from agr_literature_service.lit_processing.data_ingest.post_reference_to_db import post_references
from agr_literature_service.lit_processing.utils.s3_utils import upload_xml_file_to_s3
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_search_new_references \
    import add_md5sum_to_database
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.sanitize_pubmed_json import \
    sanitize_pubmed_json_list
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()
init_tmp_dir()

FMS_GAF_API_URL = "https://fms.alliancegenome.org/api/datafile/by/GAF?latest=true"
pubmed_efetch_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
pubmed_search_url = f"{pubmed_efetch_url}?api_key={environ.get('NCBI_API_KEY', '')}&db=pubmed&id="

base_path = environ.get("XML_PATH", "")
file_path = base_path + "gaf_data/"
json_path = base_path + "pubmed_json/"
xml_path = base_path + "pubmed_xml/"
log_path = environ.get("LOG_PATH", "")
log_url = environ.get("LOG_URL", "")

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


def load_mod_gaf_papers(force: bool = False, hours: int = 24) -> str:
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

    clean_up_tmp_directories()

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
        message = "<b>MOD GAF Paper Loading Report</b><p>"
        message += f"<p>No GAF files updated within the last {hours} hours.</p>"
        db_session.close()
        return message

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
        datetime object or None if parsing fails
    """
    try:
        # Handle various ISO 8601 formats
        if date_str.endswith('Z'):
            date_str = date_str[:-1] + '+00:00'
        return datetime.fromisoformat(date_str)
    except ValueError:
        try:
            # Try parsing without timezone
            dt = datetime.strptime(date_str[:19], "%Y-%m-%dT%H:%M:%S")
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None


def process_human_gaf(db_session, s3_url: str, all_pmids_db: Set[str]) -> str:
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
    download_file(s3_url, file_with_path)

    all_pmids = extract_pmids_from_gaf(file_with_path)
    if not all_pmids:
        return "<p><b>HUMAN (AGR)</b>: No PMIDs found in GAF file</p>"

    new_pmids = all_pmids - all_pmids_db
    logger.info(f"HUMAN GAF: {len(all_pmids)} total PMIDs, {len(new_pmids)} new")

    # Associate existing papers with AGR MOD if not in any MOD corpus
    papers_associated = associate_human_papers_with_alliance(db_session, all_pmids)

    pmids_loaded: Set[str] = set()
    if new_pmids:
        # Download PubMed XML for new PMIDs
        download_pubmed_xml(list(new_pmids))
        generate_json(list(new_pmids), [])

        # Sanitize and post references
        inject_object = {}
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
            newly_associated = associate_human_papers_with_alliance(db_session, pmids_loaded)
            papers_associated += newly_associated

    message = "<p><b>HUMAN (AGR)</b></p>"
    message += "<ul>"
    message += f"<li>Total PMIDs in GAF: {len(all_pmids)}"
    message += f"<li>Already in database: {len(all_pmids) - len(new_pmids)}"
    message += f"<li>New references loaded: {len(pmids_loaded)}"
    message += f"<li>Papers associated with AGR: {papers_associated}"
    message += "</ul>"

    return message


def process_mod_gaf(db_session, data_sub_type: str, mod_abbr: str,
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
    download_file(s3_url, file_with_path)

    all_pmids = extract_pmids_from_gaf(file_with_path)
    if not all_pmids:
        return f"<p><b>{data_sub_type} ({mod_abbr})</b>: No PMIDs found in GAF file</p>"

    # Get MOD corpus papers
    in_corpus_set, out_corpus_set = get_mod_papers(db_session, mod_abbr)

    pmids_in_corpus = all_pmids & in_corpus_set
    pmids_out_corpus = all_pmids & out_corpus_set
    pmids_not_in_db = all_pmids - all_pmids_db
    pmids_in_db_not_associated = (all_pmids & all_pmids_db) - in_corpus_set - out_corpus_set

    logger.info(f"{data_sub_type} GAF: {len(all_pmids)} total, "
                f"{len(pmids_in_corpus)} in corpus, "
                f"{len(pmids_out_corpus)} associated but out of corpus, "
                f"{len(pmids_in_db_not_associated)} in DB not associated, "
                f"{len(pmids_not_in_db)} not in DB")

    message = f"<p><b>{data_sub_type} ({mod_abbr})</b></p>"
    message += "<ul>"
    message += f"<li>Total PMIDs in GAF: {len(all_pmids)}"
    message += f"<li>In {mod_abbr} corpus: {len(pmids_in_corpus)}"
    message += f"<li>Associated but outside corpus: {len(pmids_out_corpus)}"
    message += f"<li>In DB but not associated with {mod_abbr}: {len(pmids_in_db_not_associated)}"
    message += f"<li>Not in database: {len(pmids_not_in_db)}"

    # Write log file for PMIDs not in corpus
    pmids_not_in_corpus = pmids_out_corpus | pmids_in_db_not_associated | pmids_not_in_db
    if pmids_not_in_corpus:
        logfile_name = f"gaf_{data_sub_type.lower()}_not_in_corpus.log"
        with open(log_path + logfile_name, "w") as fw:
            fw.write(f"PMIDs from {data_sub_type} GAF not in {mod_abbr} corpus:\n\n")
            if pmids_out_corpus:
                fw.write(f"Associated but outside corpus ({len(pmids_out_corpus)}):\n")
                for pmid in sorted(pmids_out_corpus):
                    fw.write(f"PMID:{pmid}\n")
                fw.write("\n")
            if pmids_in_db_not_associated:
                fw.write(f"In DB but not associated with {mod_abbr} ({len(pmids_in_db_not_associated)}):\n")
                for pmid in sorted(pmids_in_db_not_associated):
                    fw.write(f"PMID:{pmid}\n")
                fw.write("\n")
            if pmids_not_in_db:
                fw.write(f"Not in database ({len(pmids_not_in_db)}):\n")
                for pmid in sorted(pmids_not_in_db):
                    fw.write(f"PMID:{pmid}\n")

        if log_url:
            log_file = log_url + logfile_name
            message += f"<li><a href='{log_file}'>PMIDs not in corpus log</a>"

    message += "</ul>"
    return message


def extract_pmids_from_gaf(file_with_path: str) -> Set[str]:
    """
    Extract all unique PMIDs from a GAF file.

    Args:
        file_with_path: Path to the GAF file (gzipped)

    Returns:
        Set of PMIDs (without PMID: prefix)
    """
    all_pmids: Set[str] = set()

    try:
        with gzip.open(file_with_path, "rt") as f:
            for line in f:
                # Skip comment lines
                if line.startswith("!"):
                    continue

                parts = line.strip().split("\t")
                if len(parts) < 6:
                    continue

                # Column 6 (index 5) contains the DB:Reference field
                ref_col = parts[5]
                refs = ref_col.split("|")

                for ref in refs:
                    ref = ref.strip()
                    if ref.startswith("PMID:"):
                        pmid = ref.replace("PMID:", "")
                        if pmid.isdigit():
                            all_pmids.add(pmid)
    except Exception as e:
        logger.error(f"Error reading GAF file {file_with_path}: {e}")

    return all_pmids


def associate_human_papers_with_alliance(db_session, all_pmids: Set[str]) -> int:
    """
    Associate HUMAN GAF papers with the 'AGR' MOD.
    Only associate papers that do NOT already have a mod_corpus_association
    with corpus=True for any MOD.

    Args:
        db_session: Database session
        all_pmids: Set of PMIDs to associate

    Returns:
        Number of papers associated with AGR MOD
    """
    alliance_mod = db_session.query(ModModel).filter(
        ModModel.abbreviation == 'AGR'
    ).first()
    if not alliance_mod:
        logger.warning("AGR MOD not found in database")
        return 0

    alliance_mod_id = alliance_mod.mod_id

    if not all_pmids:
        return 0

    pmid_curies = [f"PMID:{pmid}" for pmid in all_pmids]

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
        return 0

    ref_ids_list = list(reference_ids_in_db)

    refs_to_exclude_query = text(
        "SELECT DISTINCT reference_id FROM mod_corpus_association "
        "WHERE reference_id = ANY(:ref_ids) "
        "AND (corpus = True OR mod_id = :alliance_mod_id)"
    )
    refs_to_exclude = db_session.execute(
        refs_to_exclude_query,
        {"ref_ids": ref_ids_list, "alliance_mod_id": alliance_mod_id}
    ).fetchall()

    already_excluded = {row[0] for row in refs_to_exclude}

    count = 0
    for ref_id in reference_ids_in_db:
        if ref_id not in already_excluded:
            mca = ModCorpusAssociationModel(
                reference_id=ref_id,
                mod_id=alliance_mod_id,
                corpus=True,
                mod_corpus_sort_source=ModCorpusSortSourceType.Automated_alliance
            )
            db_session.add(mca)
            count += 1

    if count > 0:
        db_session.commit()
        logger.info(f"Associated {count} HUMAN GAF paper(s) with AGR MOD")

    return count


def clean_up_tmp_directories():
    """Clean up and recreate temporary directories."""
    try:
        if path.exists(file_path):
            shutil.rmtree(file_path)
        if path.exists(xml_path):
            shutil.rmtree(xml_path)
        if path.exists(json_path):
            shutil.rmtree(json_path)
    except OSError as e:
        logger.info(f"Error deleting old gaf/xml/json files: {e.strerror}")

    makedirs(file_path, exist_ok=True)
    makedirs(xml_path, exist_ok=True)
    makedirs(json_path, exist_ok=True)


def send_slack_report(message: str):
    """Send the report to Slack."""
    email_subject = "MOD GAF Paper Loading Report"
    send_report(email_subject, message)


if __name__ == "__main__":
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

    if not args.no_slack:
        send_slack_report(message)
    else:
        logger.info("Slack report disabled. Message content:")
        logger.info(message)
