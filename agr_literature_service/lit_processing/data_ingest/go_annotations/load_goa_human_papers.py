import argparse
import logging
import requests
import gzip
import shutil
from typing import Set, Tuple
from os import environ, makedirs, path
from dotenv import load_dotenv
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import \
    download_file
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_pubmed_xml import \
    download_pubmed_xml
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml_to_json import generate_json
from agr_literature_service.lit_processing.utils.db_read_utils import retrieve_all_pmids
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

GOA_HUMAN_URL = "https://current.geneontology.org/annotations/goa_human.gaf.gz"
pubmed_efetch_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
pubmed_search_url = f"{pubmed_efetch_url}?api_key={environ.get('NCBI_API_KEY', '')}&db=pubmed&id="

base_path = environ.get("XML_PATH", "")
file_path = base_path + "goa_data/"
json_path = base_path + "pubmed_json/"
xml_path = base_path + "pubmed_xml/"
log_path = environ.get("LOG_PATH", "")
log_url = environ.get("LOG_URL", "")


def load_goa_human_papers() -> str:
    """
    Main function to load GOA human papers into the database.
    Downloads the goa_human.gaf.gz file, extracts PMIDs, loads new papers,
    and associates them with the AGR MOD.

    Returns:
        str: Message for the Slack report
    """
    db_session = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, script_name)

    clean_up_tmp_directories()

    message = ""
    obsolete_pmids: Set[str] = set()

    # Download and extract PMIDs from GOA human GAF file
    file_name, all_pmids = extract_pmids_from_goa_human()

    if len(all_pmids) == 0:
        logger.info("No PMIDs found in GOA human file")
        message = "<b>GOA Human Paper Loading Report</b><p>"
        message += "<ul><li>No PMIDs found in GOA human file</ul>"
        return message

    logger.info(f"Found {len(all_pmids)} unique PMIDs in GOA human file")

    # Get existing PMIDs from database
    all_pmids_db = retrieve_all_pmids(db_session)
    new_pmids = all_pmids - set(all_pmids_db)

    logger.info(f"New PMIDs to load: {len(new_pmids)}")

    # Associate existing papers with AGR MOD if not in any MOD corpus
    papers_associated = associate_goa_human_papers_with_alliance(db_session, all_pmids)
    logger.info(f"Papers associated with AGR MOD: {papers_associated}")

    pmids_loaded: Set[str] = set()
    if len(new_pmids) > 0:
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
        if len(pmids_loaded) > 0:
            newly_associated = associate_goa_human_papers_with_alliance(db_session, pmids_loaded)
            papers_associated += newly_associated

    # Compose the report message
    message = compose_report_message(
        db_session, file_name, all_pmids, pmids_loaded,
        papers_associated, obsolete_pmids
    )

    db_session.close()
    return message


def extract_pmids_from_goa_human() -> Tuple[str, Set[str]]:
    """
    Download the GOA human GAF file and extract all unique PMIDs.

    Returns:
        Tuple containing the file name and set of PMIDs
    """
    file_name = "goa_human.gaf.gz"
    file_with_path = f"{file_path}{file_name}"

    logger.info(f"Downloading GOA human file from {GOA_HUMAN_URL}")
    download_file(GOA_HUMAN_URL, file_with_path)

    all_pmids: Set[str] = set()

    with gzip.open(file_with_path, "rt") as f:
        for line in f:
            # Skip comment lines
            if line.startswith("!"):
                continue

            parts = line.strip().split("\t")
            if len(parts) < 6:
                continue

            # Column 6 (index 5) contains the DB:Reference field
            # Format: DB:Reference(|DB:Reference) - can have multiple refs separated by |
            ref_col = parts[5]
            refs = ref_col.split("|")

            for ref in refs:
                ref = ref.strip()
                if ref.startswith("PMID:"):
                    pmid = ref.replace("PMID:", "")
                    if pmid.isdigit():
                        all_pmids.add(pmid)

    logger.info(f"Extracted {len(all_pmids)} unique PMIDs from {file_name}")
    return file_name, all_pmids


def associate_goa_human_papers_with_alliance(db_session, all_pmids: Set[str]) -> int:
    """
    Associate GOA human papers with the 'AGR' MOD.
    Only associate papers that do NOT already have a mod_corpus_association
    with corpus=True for any MOD. This ensures we only add papers to 'AGR'
    that are not already in another MOD's corpus.

    Args:
        db_session: Database session
        all_pmids: Set of PMIDs to associate

    Returns:
        Number of papers associated with AGR MOD
    """
    # Get AGR mod_id
    alliance_mod = db_session.query(ModModel).filter(
        ModModel.abbreviation == 'AGR'
    ).first()
    if not alliance_mod:
        logger.warning("AGR MOD not found in database")
        return 0

    alliance_mod_id = alliance_mod.mod_id

    if not all_pmids:
        return 0

    # Build parameterized query for PMIDs
    pmid_curies = [f"PMID:{pmid}" for pmid in all_pmids]

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
        return 0

    ref_ids_list = list(reference_ids_in_db)

    # Get reference_ids that already have corpus=True for any MOD
    # OR already have an association with the AGR MOD (to avoid unique constraint violation)
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

    # Add mod_corpus_association for papers not yet in any MOD's corpus
    # and not already associated with AGR MOD
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
        logger.info(f"Associated {count} GOA human paper(s) with AGR MOD")

    return count


def compose_report_message(db_session, file_name: str, all_pmids: Set[str],
                           pmids_loaded: Set[str], papers_associated: int,
                           obsolete_pmids: Set[str]) -> str:
    """
    Compose the Slack report message with statistics.

    Args:
        db_session: Database session
        file_name: Name of the downloaded file
        all_pmids: All PMIDs extracted from the GAF file
        pmids_loaded: PMIDs that were successfully loaded
        papers_associated: Number of papers associated with AGR MOD
        obsolete_pmids: Set of obsolete PMIDs

    Returns:
        HTML formatted message for the Slack report
    """
    all_pmids_db = retrieve_all_pmids(db_session)
    pmids_in_db = all_pmids & set(all_pmids_db)
    pmids_not_in_db = all_pmids - set(all_pmids_db)

    # Check for obsolete PMIDs among those not loaded
    if pmids_not_in_db - pmids_loaded:
        obsolete, valid = search_pubmed(pmids_not_in_db - pmids_loaded)
        obsolete_pmids.update(obsolete)

    message = "<b>GOA Human Paper Loading Report</b><p>"
    message += f"<p>Source: {GOA_HUMAN_URL}</p>"
    message += "<ul>"
    message += f"<li>Total unique PMIDs in GAF file: {len(all_pmids)}"
    message += f"<li>PMIDs already in database: {len(pmids_in_db) - len(pmids_loaded)}"
    message += f"<li>New references loaded: {len(pmids_loaded)}"
    message += f"<li>Papers associated with AGR MOD: {papers_associated}"

    if obsolete_pmids:
        message += f"<li>Obsolete PMIDs: {len(obsolete_pmids)}"
        # Write obsolete PMIDs to log file
        logfile_name = "goa_human_obsolete_pmids.log"
        with open(log_path + logfile_name, "w") as fw:
            fw.write("Obsolete PMIDs:\n\n")
            for pmid in sorted(obsolete_pmids):
                fw.write(f"PMID:{pmid}\n")
        if log_url:
            log_file = log_url + logfile_name
            message += f"<li><a href='{log_file}'>Obsolete PMIDs log</a>"

    # Check for valid PMIDs that were not loaded (possible errors)
    valid_not_loaded = (pmids_not_in_db - pmids_loaded) - obsolete_pmids
    if valid_not_loaded:
        message += f"<li>Valid PMIDs not loaded (possible errors): {len(valid_not_loaded)}"
        if len(valid_not_loaded) <= 10:
            message += f" - {', '.join(sorted(valid_not_loaded))}"

    message += "</ul>"

    logger.info(f"Report: Total={len(all_pmids)}, InDB={len(pmids_in_db)}, "
                f"Loaded={len(pmids_loaded)}, Associated={papers_associated}")

    return message


def search_pubmed(pmids: Set[str]) -> Tuple[Set[str], Set[str]]:
    """
    Check if PMIDs are valid or obsolete in PubMed.

    Args:
        pmids: Set of PMIDs to check

    Returns:
        Tuple of (obsolete_pmids, valid_pmids)
    """
    obsolete_pmids: Set[str] = set()
    valid_pmids: Set[str] = set()

    for pmid in pmids:
        url = f"{pubmed_search_url}{pmid}"
        try:
            response = requests.get(url, timeout=30)
            content = response.text.replace("\n", "")
            if "<PubmedArticleSet></PubmedArticleSet>" in content:
                obsolete_pmids.add(pmid)
            else:
                valid_pmids.add(pmid)
        except requests.RequestException as e:
            logger.warning(f"Error checking PMID {pmid}: {e}")
            valid_pmids.add(pmid)  # Assume valid if we can't check

    return obsolete_pmids, valid_pmids


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
        logger.info(f"Error deleting old goa/xml/json files: {e.strerror}")

    makedirs(file_path, exist_ok=True)
    makedirs(xml_path, exist_ok=True)
    makedirs(json_path, exist_ok=True)


def send_slack_report(message: str):
    """Send the report to Slack."""
    email_subject = "GOA Human Paper Loading Report"
    send_report(email_subject, message)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load GOA human papers into ABC database and associate with AGR MOD"
    )
    parser.add_argument(
        '-n', '--no-slack',
        action='store_true',
        help="Do not send Slack report (for testing)"
    )
    args = parser.parse_args()

    message = load_goa_human_papers()

    if not args.no_slack:
        send_slack_report(message)
    else:
        logger.info("Slack report disabled. Message content:")
        logger.info(message)
