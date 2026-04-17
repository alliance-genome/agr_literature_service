import json
import logging
import shutil
import time
import requests
from pathlib import Path
from typing import Dict, List, Tuple
from sqlalchemy import text
from os import path, environ, makedirs, listdir, remove
from dotenv import load_dotenv
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ReferenceModel
from agr_literature_service.lit_processing.utils.s3_utils import upload_file_to_s3
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.md5sum_utils import \
    get_md5sum
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import \
    download_file, gunzip_file, gzip_file, download_pmc_package_from_s3, normalize_pmcid
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.load_pmc_metadata import \
    load_ref_file_metadata_into_db

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
OA_CACHE_TTL_DAYS = 30  # Re-check cached entries older than this


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
    try:
        Path(cache_path).parent.mkdir(parents=True, exist_ok=True)
        with open(cache_path, 'w') as f:
            json.dump(cache, f, indent=2, sort_keys=True)
    except OSError as e:
        logger.warning(f"Failed to save OA cache to {cache_path}: {e}")


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
        cached_at = time.time()
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
                "cached_at": cached_at,
            }

        return out

    except Exception as e:
        logger.warning(f"EuroPMC API batch fetch failed: {type(e).__name__}: {e}")
        return {}


def _is_cache_entry_stale(entry: dict) -> bool:
    """Check if a cache entry is older than TTL."""
    cached_at = entry.get("cached_at")
    if cached_at is None:
        # Legacy entry without timestamp, consider stale
        return True
    age_days = (time.time() - cached_at) / (60 * 60 * 24)
    return age_days > OA_CACHE_TTL_DAYS


def fetch_oa_status_for_pmcids(pmcids: List[Tuple[str, str]],
                               cache: Dict[str, dict]) -> Dict[str, dict]:
    """
    Fetch OA status for list of (pmid, pmcid) tuples using EuroPMC API.
    Uses and updates the cache. Returns updated cache.
    Entries older than OA_CACHE_TTL_DAYS are re-fetched.

    Args:
        pmcids: List of (pmid, pmcid) tuples
        cache: Existing OA metadata cache

    Returns:
        Updated cache dict
    """
    # Normalize and find missing/stale PMCIDs
    unique_pmcids = set()
    for (_, pmcid) in pmcids:
        unique_pmcids.add(normalize_pmcid(pmcid))

    missing = []
    stale = 0
    for p in unique_pmcids:
        if p not in cache:
            missing.append(p)
        elif _is_cache_entry_stale(cache[p]):
            missing.append(p)
            stale += 1

    if not missing:
        logger.info(f"OA cache hit: all {len(unique_pmcids)} PMCIDs found in cache")
        return cache

    if stale > 0:
        logger.info(f"OA cache: {len(unique_pmcids)} unique PMCIDs, {len(missing)} to fetch ({stale} stale)")
    else:
        logger.info(f"OA cache: {len(unique_pmcids)} unique PMCIDs, {len(missing)} missing, fetching from EuroPMC...")

    session = requests.Session()
    session.headers.update({"User-Agent": "agr-pmc-download/1.0"})

    fetched = 0
    cached_at = time.time()
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
                    "cached_at": cached_at,
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


def download_pmc_files():  # pragma: no cover
    """
    Download PMC Open Access packages for papers in the corpus.

    Uses EuroPMC API to check Open Access status before downloading.
    Downloads from AWS S3 bucket pmc-oa-opendata for OA papers only.
    License info is extracted from S3 JSON metadata.

    Note: File classification uses XML root-name matching (determine_file_class)
    which works for most cases. For edge cases where the XML root name doesn't
    match the PDF root name, run identify_main_pdfs.py separately for cleanup.
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

    # Filter to only Open Access papers
    oa_pmcids = []
    non_oa_count = 0
    for (pmid, pmcid) in pmcids_for_pmc_loading:
        if is_open_access(oa_cache, pmcid):
            oa_pmcids.append((pmid, pmcid))
        else:
            non_oa_count += 1

    logger.info(f"OA filter: {len(oa_pmcids)} Open Access papers, {non_oa_count} non-OA skipped")
    logger.info(f"Total papers to download from S3: {len(oa_pmcids)} (have PMCID, no PMC package downloaded so far, is_open_access=True)")

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

    # Count total PMIDs to process for progress logging
    all_pmids = [d for d in listdir(pmcFileDir) if d.strip() not in files_uploaded]
    total_pmids = len(all_pmids)
    logger.info(f"Total PMIDs to upload to S3: {total_pmids}")

    upload_count = 0
    error_count = 0
    skip_count = 0

    # download_PMC/9971735/PMC2132911
    # eg, under download_PMC/
    for idx, file_dir in enumerate(all_pmids, 1):
        pmid = file_dir.strip()
        pmid_dir = path.join(pmcFileDir, pmid)

        try:
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
                        skip_count += 1
                        continue
                    status = upload_suppl_file_to_s3(gzip_file_with_path, md5sum)
                    if status is True:
                        fw.write(pmid + "\t" + pmcid + "\t" + file_name + "\t" + md5sum + "\n")
                        upload_count += 1
        except Exception as e:
            error_count += 1
            logger.error(f"[{idx}/{total_pmids}] PMID:{pmid} - Error uploading: {type(e).__name__}: {e}")
            continue

        # Progress logging every 500 PMIDs
        if idx % 500 == 0:
            logger.info(f"Upload progress: {idx}/{total_pmids} PMIDs processed, {upload_count} files uploaded, {error_count} errors")

    fw.close()
    logger.info(f"Upload summary: {upload_count} files uploaded, {skip_count} skipped, {error_count} errors")


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
    s3_error_count = 0
    pmid_to_license = {}
    total = len(pmcids)

    for idx, (pmid, pmcid) in enumerate(pmcids, 1):
        # Check if already downloaded
        pmid_dir = path.join(pmcFileDir, pmid)
        if path.exists(pmid_dir) and listdir(pmid_dir):
            # Get license from OA cache
            norm_pmcid = normalize_pmcid(pmcid)
            cache_entry = oa_cache.get(norm_pmcid, {})
            license_code = cache_entry.get('license')
            if license_code and (license_code.upper().startswith('CC') or license_code.upper() == 'CC0'):
                pmid_to_license[pmid] = license_code
            continue

        try:
            logger.info(f"[{idx}/{total}] PMID:{pmid} PMCID:{pmcid} - Downloading from S3 (pmc-oa-opendata)...")
            makedirs(pmid_dir, exist_ok=True)

            # Download from S3
            success = download_pmc_package_from_s3(pmcid, pmid_dir)

            if success:
                s3_download_count += 1
                logger.info(f"[{idx}/{total}] PMID:{pmid} PMCID:{pmcid} - S3 download successful")

                # Get license from OA cache (EuroPMC API)
                norm_pmcid = normalize_pmcid(pmcid)
                cache_entry = oa_cache.get(norm_pmcid, {})
                license_code = cache_entry.get('license')
                if license_code and (license_code.upper().startswith('CC') or license_code.upper() == 'CC0'):
                    pmid_to_license[pmid] = license_code
                    logger.info(f"PMID:{pmid} - License: {license_code}")
            else:
                s3_not_found_count += 1
                logger.warning(f"[{idx}/{total}] PMID:{pmid} PMCID:{pmcid} - Not found in S3 (may not be migrated yet)")
                # Remove empty directory
                if path.exists(pmid_dir) and not listdir(pmid_dir):
                    shutil.rmtree(pmid_dir)
        except Exception as e:
            s3_error_count += 1
            logger.error(f"[{idx}/{total}] PMID:{pmid} PMCID:{pmcid} - Error downloading: {type(e).__name__}: {e}")
            # Remove empty directory on error
            if path.exists(pmid_dir) and not listdir(pmid_dir):
                shutil.rmtree(pmid_dir)
            continue

        # Progress logging every 500 papers
        if idx % 500 == 0:
            logger.info(f"Progress: {idx}/{total} processed, {s3_download_count} downloaded, {s3_not_found_count} not found, {s3_error_count} errors")

    # Summary
    logger.info(f"Download summary: {s3_download_count} downloaded from S3, {s3_not_found_count} not found in S3, {s3_error_count} errors")

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
    # Use sets for O(1) membership checks instead of O(n) list scans
    pmcids_for_pmc_loading_set = set()
    pmids_for_license_loading_set = set()

    limit = 5000
    loop_count = 200000
    for index in range(loop_count):
        offset = index * limit
        logger.info(f"offset={offset} Retrieving pmids and pmcids...")
        rows = db_session.execute(text(f"SELECT cr.reference_id, cr.curie as pmid, cr2.curie as pmcid "
                                       f"FROM cross_reference cr, mod_corpus_association mca, "
                                       f"cross_reference cr2, mod m "
                                       f"WHERE cr.curie_prefix = 'PMID' "
                                       f"AND cr.is_obsolete is False "
                                       f"AND cr.reference_id = cr2.reference_id "
                                       f"AND cr2.curie_prefix = 'PMCID' "
                                       f"AND cr2.is_obsolete is False "
                                       f"AND cr.reference_id = mca.reference_id "
                                       f"AND mca.corpus is True "
                                       f"AND mca.mod_id = m.mod_id "
                                       f"AND m.abbreviation != 'AGR' "
                                       f"order by cr.reference_id "
                                       f"limit {limit} "
                                       f"offset {offset}")).mappings().fetchall()
        if len(rows) == 0:
            break

        for x in rows:
            pmid = x["pmid"].replace("PMID:", "")
            pmcid = x["pmcid"].replace("PMCID:", "")
            if x["reference_id"] not in reference_ids_with_PMC:
                if (pmid, pmcid) not in pmcids_for_pmc_loading_set:
                    pmcids_for_pmc_loading.append((pmid, pmcid))
                    pmcids_for_pmc_loading_set.add((pmid, pmcid))
            if x["reference_id"] not in reference_ids_with_license:
                if (pmid, x["reference_id"]) not in pmids_for_license_loading_set:
                    pmids_for_license_loading.append((pmid, x["reference_id"]))
                    pmids_for_license_loading_set.add((pmid, x["reference_id"]))

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
