import json
import logging
import shutil
import time
import requests
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from sqlalchemy import text
from os import path, environ, makedirs, listdir, remove
from dotenv import load_dotenv
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ReferenceModel
from agr_literature_service.lit_processing.utils.s3_utils import upload_file_to_s3
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.md5sum_utils import \
    get_md5sum
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import \
    download_file, gunzip_file, gzip_file, download_pmc_package_from_s3
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.load_pmc_metadata import \
    load_ref_file_metadata_into_db
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_identify_main_pdfs import \
    identify_main_pdfs

load_dotenv()

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_bucket = 'agr-literature'
# Legacy FTP URL - deprecated August 2026
# pmcRootUrl = 'https://ftp.ncbi.nlm.nih.gov/pub/pmc/'
dataDir = 'data/'
pmcFileDir = 'pubmed_pmc_download/'
suppl_file_uploaded = dataDir + "pmc_oa_files_uploaded.txt"
batch_size = 250

# EuroPMC API settings
EUROPEPMC_API_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
EUROPEPMC_BATCH_SIZE = 100
EUROPEPMC_MAX_PAGE_SIZE = 1000
OA_CACHE_FILE = dataDir + "europepmc_oa_cache.json"


# -----------------------------
# EuroPMC OA Status Functions
# -----------------------------
def load_oa_cache(cache_path: str) -> Dict[str, dict]:
    """Load OA metadata cache from JSON file."""
    if not path.exists(cache_path):
        return {}
    try:
        with open(cache_path, 'r') as f:
            return json.load(f)
    except Exception:
        return {}


def save_oa_cache(cache_path: str, cache: Dict[str, dict]) -> None:
    """Save OA metadata cache to JSON file."""
    Path(cache_path).parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, 'w') as f:
        json.dump(cache, f, indent=2, sort_keys=True)


def normalize_pmcid(pmcid: str) -> str:
    """Normalize 'PMCID:PMC123' or 'PMC123' to 'PMC123' (uppercase)."""
    s = pmcid.strip().upper()
    if s.startswith("PMCID:"):
        s = s.split(":", 1)[1]
    if not s.startswith("PMC"):
        s = f"PMC{s}"
    return s


def fetch_oa_metadata_batch(pmcids: List[str], session: requests.Session,
                            timeout: int = 60) -> Dict[str, dict]:
    """
    Fetch OA metadata from EuroPMC API for a batch of PMCIDs.

    Returns dict mapping PMCID -> {hit, is_open_access, has_pdf, license}
    """
    if not pmcids:
        return {}

    effective_page_size = min(len(pmcids), EUROPEPMC_MAX_PAGE_SIZE)
    or_query = " OR ".join([f"PMCID:{p}" for p in pmcids])
    query = f"({or_query})"

    params = {
        "query": query,
        "resultType": "core",
        "format": "json",
        "pageSize": effective_page_size
    }

    try:
        r = session.get(EUROPEPMC_API_URL, params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json()

        result_list = data.get("resultList", {})
        results = result_list.get("result", []) if result_list else []

        out: Dict[str, dict] = {}
        for res in results:
            if not isinstance(res, dict):
                continue
            pmcid = (res.get("pmcid") or "").upper()
            if not pmcid:
                continue

            out[pmcid] = {
                "hit": True,
                "is_open_access": res.get("isOpenAccess") == "Y",
                "has_pdf": res.get("hasPDF") == "Y",
                "license": res.get("license"),
            }

        return out

    except Exception as e:
        logger.warning(f"EuroPMC API batch fetch failed: {type(e).__name__}: {e}")
        return {}


def fetch_oa_status_for_pmcids(pmcids: List[Tuple[str, str]],
                               cache: Dict[str, dict]) -> Dict[str, dict]:
    """
    Fetch OA status for list of (pmid, pmcid) tuples using EuroPMC API.
    Uses and updates the cache. Returns updated cache.

    Args:
        pmcids: List of (pmid, pmcid) tuples
        cache: Existing OA metadata cache

    Returns:
        Updated cache dict
    """
    # Normalize and find missing PMCIDs
    unique_pmcids = set()
    for (_, pmcid) in pmcids:
        unique_pmcids.add(normalize_pmcid(pmcid))

    missing = [p for p in unique_pmcids if p not in cache]

    if not missing:
        logger.info(f"OA cache hit: all {len(unique_pmcids)} PMCIDs found in cache")
        return cache

    logger.info(f"OA cache: {len(unique_pmcids)} unique PMCIDs, {len(missing)} missing, fetching from EuroPMC...")

    session = requests.Session()
    session.headers.update({"User-Agent": "agr-pmc-download/1.0"})

    fetched = 0
    for i in range(0, len(missing), EUROPEPMC_BATCH_SIZE):
        batch = missing[i:i + EUROPEPMC_BATCH_SIZE]
        batch_meta = fetch_oa_metadata_batch(batch, session)

        # Update cache with results
        for pmcid, meta in batch_meta.items():
            cache[pmcid] = meta

        # Mark PMCIDs not returned as non-OA
        for pmcid in batch:
            if pmcid not in batch_meta:
                cache[pmcid] = {
                    "hit": False,
                    "is_open_access": False,
                    "has_pdf": False,
                    "license": None,
                }

        fetched += len(batch)
        if fetched % 500 == 0 or fetched == len(missing):
            logger.info(f"Fetched OA metadata: {fetched}/{len(missing)}")

        # Rate limiting
        time.sleep(0.1)

    return cache


def is_open_access(cache: Dict[str, dict], pmcid: str) -> bool:
    """Check if a PMCID is Open Access (does not require PDF, includes suppl files)."""
    pmcid = normalize_pmcid(pmcid)
    entry = cache.get(pmcid, {})
    return entry.get("is_open_access", False)


def download_pmc_files(mapping_file=None):  # pragma: no cover
    """
    Download PMC Open Access packages for papers in the corpus.

    Uses EuroPMC API to check Open Access status before downloading.
    Downloads from AWS S3 bucket pmc-oa-opendata for OA papers only.
    License info is extracted from S3 JSON metadata.
    """
    logger.info("Retrieving PMID/PMCID list for papers that do not have PMC package downloaded...")

    (pmcids_for_pmc_loading, pmids_for_license_loading) = get_pmids_and_pmcids()

    if not pmcids_for_pmc_loading:
        logger.info("No PMCIDs to process.")
        return

    logger.info(f"Found {len(pmcids_for_pmc_loading)} papers to check for PMC packages")

    # Load OA cache and fetch OA status from EuroPMC API
    makedirs(dataDir, exist_ok=True)
    oa_cache = load_oa_cache(OA_CACHE_FILE)
    oa_cache = fetch_oa_status_for_pmcids(pmcids_for_pmc_loading, oa_cache)
    save_oa_cache(OA_CACHE_FILE, oa_cache)

    # Filter to only Open Access papers with PDFs
    oa_pmcids = []
    non_oa_count = 0
    for (pmid, pmcid) in pmcids_for_pmc_loading:
        if is_open_access(oa_cache, pmcid):
            oa_pmcids.append((pmid, pmcid))
        else:
            non_oa_count += 1

    logger.info(f"OA filter: {len(oa_pmcids)} Open Access papers, {non_oa_count} non-OA skipped")

    if not oa_pmcids:
        logger.info("No Open Access papers to download.")
        return

    logger.info("Downloading PMC OA packages from S3...")

    # Downloads packages and extracts license info from S3 JSON metadata
    # Only downloading OA papers (pre-filtered via EuroPMC API)
    pmid_to_license = download_packages_from_s3(oa_pmcids, oa_cache)

    logger.info("Uploading the files to s3...")

    upload_suppl_files_to_s3()

    logger.info("Loading the metadata into database...")

    load_ref_file_metadata_into_db()

    logger.info("Identifying main PDF files in the database...")

    identify_main_pdfs(True)

    if pmid_to_license:
        logger.info(f"Loading license information for {len(pmid_to_license)} papers into database...")
        load_license_into_db(pmids_for_license_loading, pmid_to_license)


def upload_suppl_files_to_s3():  # pragma: no cover

    files_uploaded = {}
    fw = None

    if path.exists(suppl_file_uploaded):
        f = open(suppl_file_uploaded)
        for line in f:
            pmid = line.split("\t")[0]
            files_uploaded[pmid] = 1
        f.close()
        fw = open(suppl_file_uploaded, "a")
    else:
        fw = open(suppl_file_uploaded, "w")

    # download_PMC/9971735/PMC2132911
    # eg, under download_PMC/
    for file_dir in listdir(pmcFileDir):
        pmid = file_dir.strip()
        if pmid in files_uploaded:
            continue
        pmid_dir = path.join(pmcFileDir, pmid)
        # eg, under download_PMC/9971735/
        for pmcid in listdir(pmid_dir):
            sub_dir = path.join(pmcFileDir, pmid, pmcid)
            # eg, under download_PMC/9971735/PMC2132911/
            for file_name in listdir(sub_dir):
                file_with_path = path.join(sub_dir, file_name)
                if not path.exists(file_with_path):
                    continue
                md5sum = get_md5sum(file_with_path)
                gzip_file_with_path = None
                if file_with_path.endswith('.gz'):
                    gzip_file_with_path = file_with_path
                else:
                    gzip_file_with_path = gzip_file(file_with_path)
                if gzip_file_with_path is None:
                    continue
                status = upload_suppl_file_to_s3(gzip_file_with_path, md5sum)
                if status is True:
                    fw.write(pmid + "\t" + pmcid + "\t" + file_name + "\t" + md5sum + "\n")
    fw.close()


def upload_suppl_file_to_s3(gzip_file_with_path, md5sum):  # pragma: no cover

    if environ.get('ENV_STATE') is None or environ.get('ENV_STATE') == 'test':
        return

    s3_file_path = "/reference/documents/"

    storage = None
    if environ.get('ENV_STATE') == 'prod':
        s3_file_path = 'prod' + s3_file_path
        storage = 'GLACIER_IR'
    else:
        s3_file_path = 'develop' + s3_file_path
        storage = 'STANDARD'
    s3_file_path = s3_file_path + md5sum[0] + "/" + md5sum[1] + \
        "/" + md5sum[2] + "/" + md5sum[3] + "/"
    s3_file_location = s3_file_path + md5sum + ".gz"

    logger.info("Uploading " + gzip_file_with_path.split("/")[-1] + " to AGR s3: " + s3_file_location)

    status = upload_file_to_s3(gzip_file_with_path, s3_bucket, s3_file_location, storage)

    return status


def unpack_packages():  # pragma: no cover

    i = 0
    for file_name in listdir(pmcFileDir):
        i += 1
        pmid = file_name.replace(".tar.gz", "")
        pmid_path = path.join(pmcFileDir, pmid)
        if not path.exists(pmid_path):
            logger.info(str(i) + ": unpacking " + file_name)
            file_with_path = path.join(pmcFileDir, file_name)
            status = gunzip_file(file_with_path, pmcFileDir + pmid + "/")
            if status:
                remove(file_with_path)


def download_packages_from_s3(pmcids, oa_cache: Dict[str, dict]):  # pragma: no cover
    """
    Download PMC packages from AWS S3 bucket pmc-oa-opendata.
    Only called with pre-filtered OA papers (via EuroPMC API).

    Args:
        pmcids: List of tuples (pmid, pmcid) to download (pre-filtered for OA)
        oa_cache: OA metadata cache from EuroPMC API

    Returns:
        Dict mapping PMID to license_code
    """
    s3_download_count = 0
    s3_not_found_count = 0
    pmid_to_license = {}

    for (pmid, pmcid) in pmcids:
        # Check if already downloaded
        pmid_dir = path.join(pmcFileDir, pmid)
        if path.exists(pmid_dir) and listdir(pmid_dir):
            # Get license from OA cache
            norm_pmcid = normalize_pmcid(pmcid)
            cache_entry = oa_cache.get(norm_pmcid, {})
            license_code = cache_entry.get('license')
            if license_code and (license_code.startswith('CC') or license_code == 'cc0'):
                pmid_to_license[pmid] = license_code
            continue

        logger.info(f"PMID:{pmid} PMCID:{pmcid} - Downloading from S3 (pmc-oa-opendata)...")
        makedirs(pmid_dir, exist_ok=True)

        # Download from S3
        success = download_pmc_package_from_s3(pmcid, pmid_dir)

        if success:
            s3_download_count += 1
            logger.info(f"PMID:{pmid} PMCID:{pmcid} - S3 download successful")

            # Get license from OA cache (EuroPMC API)
            norm_pmcid = normalize_pmcid(pmcid)
            cache_entry = oa_cache.get(norm_pmcid, {})
            license_code = cache_entry.get('license')
            if license_code and (license_code.startswith('CC') or license_code.lower() == 'cc0'):
                pmid_to_license[pmid] = license_code
                logger.info(f"PMID:{pmid} - License: {license_code}")
        else:
            s3_not_found_count += 1
            logger.warning(f"PMID:{pmid} PMCID:{pmcid} - Not found in S3 (may not be migrated yet)")
            # Remove empty directory
            if path.exists(pmid_dir) and not listdir(pmid_dir):
                shutil.rmtree(pmid_dir)

    # Summary
    logger.info(f"Download summary: {s3_download_count} downloaded from S3, {s3_not_found_count} not found in S3")

    return pmid_to_license


def download_packages(pmids, pmid_to_oa_url):  # pragma: no cover
    """
    Legacy function - downloads PMC packages from FTP.
    Deprecated: Use download_packages_from_s3 instead.
    """
    logger.warning("download_packages using FTP is deprecated. Use download_packages_from_s3.")
    # Keep for backward compatibility with annual_pmc_package_update.py
    pmcRootUrl = 'https://ftp.ncbi.nlm.nih.gov/pub/pmc/'
    for pmid in pmids:
        pmc_file = pmcFileDir + pmid + '.tar.gz'
        if path.exists(pmc_file):
            continue
        if pmid in pmid_to_oa_url:
            pmc_url = pmcRootUrl + pmid_to_oa_url[pmid]
            logger.info("PMID:" + pmid + " " + pmc_url)
            download_file(pmc_url, pmc_file)


def get_pmids_and_pmcids():  # pragma: no cover
    """
    Get PMIDs and PMCIDs for papers that need PMC package downloads.

    Returns:
        Tuple of (pmcids_for_pmc_loading, pmids_for_license_loading)
        - pmcids_for_pmc_loading: List of (pmid, pmcid) tuples
        - pmids_for_license_loading: List of (pmid, reference_id) tuples
    """
    db_session = create_postgres_session(False)

    rows = db_session.execute(text("SELECT distinct rf.reference_id "
                                   "FROM referencefile rf, referencefile_mod rfm "
                                   "WHERE rfm.mod_id is null "
                                   "AND rf.referencefile_id = rfm.referencefile_id ")).fetchall()

    reference_ids_with_PMC = set()
    for x in rows:
        reference_ids_with_PMC.add(x[0])

    rows = db_session.execute(text("SELECT reference_id "
                                   "FROM reference "
                                   "WHERE copyright_license_id is not null")).fetchall()

    reference_ids_with_license = set()
    for x in rows:
        reference_ids_with_license.add(x[0])

    pmcids_for_pmc_loading = []
    pmids_for_license_loading = []

    limit = 5000
    loop_count = 200000
    for index in range(loop_count):
        offset = index * limit
        logger.info(f"offset={offset} Retrieving pmids and pmcids...")
        rows = db_session.execute(text(f"SELECT cr.reference_id, cr.curie as pmid, cr2.curie as pmcid "
                                       f"FROM cross_reference cr, mod_corpus_association mca, "
                                       f"cross_reference cr2 "
                                       f"WHERE cr.curie_prefix = 'PMID' "
                                       f"AND cr.is_obsolete is False "
                                       f"AND cr.reference_id = cr2.reference_id "
                                       f"AND cr2.curie_prefix = 'PMCID' "
                                       f"AND cr2.is_obsolete is False "
                                       f"AND cr.reference_id = mca.reference_id "
                                       f"AND mca.corpus is True "
                                       f"order by cr.reference_id "
                                       f"limit {limit} "
                                       f"offset {offset}")).mappings().fetchall()
        if len(rows) == 0:
            break

        for x in rows:
            pmid = x["pmid"].replace("PMID:", "")
            pmcid = x["pmcid"].replace("PMCID:", "")
            if x["reference_id"] not in reference_ids_with_PMC:
                if (pmid, pmcid) not in pmcids_for_pmc_loading:
                    pmcids_for_pmc_loading.append((pmid, pmcid))
            if x["reference_id"] not in reference_ids_with_license:
                if (pmid, x["reference_id"]) not in pmids_for_license_loading:
                    pmids_for_license_loading.append((pmid, x["reference_id"]))

    db_session.close()

    return (pmcids_for_pmc_loading, pmids_for_license_loading)


def get_pmids():  # pragma: no cover
    """
    Legacy function for backward compatibility.
    Returns PMIDs only (not PMCIDs).
    """
    pmcids_for_pmc_loading, pmids_for_license_loading = get_pmids_and_pmcids()
    pmids_for_pmc_loading = [pmid for (pmid, pmcid) in pmcids_for_pmc_loading]
    return (pmids_for_pmc_loading, pmids_for_license_loading)


def load_license_into_db(pmids_with_ref_ids, pmid_to_license):

    db_session = create_postgres_session(False)

    rows = db_session.execute(text("SELECT copyright_license_id, name FROM copyright_license")).fetchall()
    license_to_id = {}
    for x in rows:
        license_to_id[x[1]] = x[0]

    i = 0
    for (pmid, reference_id) in pmids_with_ref_ids:
        if pmid in pmid_to_license:
            license = pmid_to_license[pmid]
            if license in license_to_id:
                license_id = license_to_id[license]
                x = db_session.query(ReferenceModel).filter_by(reference_id=reference_id).one_or_none()
                if x:
                    try:
                        x.copyright_license_id = license_id
                        db_session.add(x)
                        i += 1
                        if i % batch_size == 0:
                            db_session.commit()
                        logger.info("PMID:" + pmid + " adding license_id to reference table for reference_id = " + str(reference_id))
                    except Exception as e:
                        logger.info("PMID:" + pmid + " an error occurred when adding license_id to reference table for reference_id = " + str(reference_id) + ". error = " + str(e))

    db_session.commit()
    db_session.close()


def get_pmid_to_pmc_url_mapping(mapping_file):  # pragma: no cover

    # File,Article Citation,Accession ID,Last Updated (YYYY-MM-DD HH:MM:SS),PMID,License
    # oa_package/08/e0/PMC13900.tar.gz,Breast Cancer Res. 2001 Nov 2; 3(1):55-60,PMC13900,2019-11-05 11:56:12,11250746,NO-CC CODE

    pmid_to_oa_url = {}
    pmid_to_license = {}
    f = open(mapping_file)
    for line in f:
        if line.startswith('File,'):
            continue
        pieces = line.strip().split(',')
        if pieces[4] and pieces[4].isdigit():
            pmid_to_oa_url[pieces[4]] = pieces[0]
            if len(pieces) < 6:
                continue
            if pieces[5].startswith("CC BY") or pieces[5] == 'CC0':
                pmid_to_license[pieces[4]] = pieces[5]
    f.close()

    return (pmid_to_oa_url, pmid_to_license)


def create_tmp_dirs():  # pragma: no cover

    if path.exists(dataDir):
        shutil.rmtree(dataDir)
    makedirs(dataDir)
    if path.exists(pmcFileDir):
        shutil.rmtree(pmcFileDir)
    makedirs(pmcFileDir)


if __name__ == "__main__":

    create_tmp_dirs()

    # PMC FTP service deprecated August 2026
    # Now using AWS S3 bucket pmc-oa-opendata directly
    download_pmc_files()
