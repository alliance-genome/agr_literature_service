import logging
import shutil
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


def download_pmc_files(mapping_file=None):  # pragma: no cover
    """
    Download PMC Open Access packages for papers in the corpus.

    Uses AWS S3 bucket pmc-oa-opendata (new method, August 2026+).
    Falls back to FTP for older PMCIDs not yet migrated to S3.

    The mapping_file parameter is used for FTP fallback if provided.
    """
    logger.info("Retrieving PMID/PMCID list for papers that do not have PMC package downloaded...")

    (pmcids_for_pmc_loading, pmids_for_license_loading) = get_pmids_and_pmcids()

    # Load FTP mapping for fallback (older PMCIDs not in S3) and license info
    pmid_to_oa_url = None
    pmid_to_license = {}
    if mapping_file and path.exists(mapping_file):
        logger.info("Loading FTP mapping and license info from oa_file_list.csv...")
        (pmid_to_oa_url, pmid_to_license) = get_pmid_to_pmc_url_mapping(mapping_file)
    else:
        # Download the mapping file for fallback and license info
        logger.info("Downloading oa_file_list.csv for FTP fallback and license info...")
        oafile_ftp = 'ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.csv'
        mapping_file = dataDir + "oa_file_list.csv"
        try:
            download_file(oafile_ftp, mapping_file)
            (pmid_to_oa_url, pmid_to_license) = get_pmid_to_pmc_url_mapping(mapping_file)
        except Exception as e:
            logger.warning(f"Could not download oa_file_list.csv for fallback: {e}")
            logger.warning("Will only use S3 - older PMCIDs may fail, license loading skipped")

    logger.info("Downloading PMC OA packages (S3 primary, FTP fallback)...")

    download_packages_from_s3(pmcids_for_pmc_loading, pmid_to_oa_url)

    logger.info("Uploading the files to s3...")

    upload_suppl_files_to_s3()

    logger.info("Loading the metadata into database...")

    load_ref_file_metadata_into_db()

    logger.info("Identifying main PDF files in the database...")

    identify_main_pdfs(True)

    if pmid_to_license:
        logger.info("Loading license information into database...")
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


def download_packages_from_s3(pmcids, pmid_to_oa_url=None):  # pragma: no cover
    """
    Download PMC packages from AWS S3 bucket pmc-oa-opendata.
    Falls back to FTP for older PMCIDs not yet in S3.

    Args:
        pmcids: List of tuples (pmid, pmcid) to download
        pmid_to_oa_url: Optional dict mapping PMID to FTP path for fallback
    """
    pmcRootUrl = 'https://ftp.ncbi.nlm.nih.gov/pub/pmc/'
    ftp_fallback_count = 0

    for (pmid, pmcid) in pmcids:
        # Check if already downloaded
        pmid_dir = path.join(pmcFileDir, pmid)
        if path.exists(pmid_dir) and listdir(pmid_dir):
            continue

        logger.info(f"PMID:{pmid} PMCID:{pmcid} - Downloading from S3...")
        makedirs(pmid_dir, exist_ok=True)

        # Try S3 first
        success = download_pmc_package_from_s3(pmcid, pmid_dir)

        if not success:
            # Fall back to FTP if S3 doesn't have the package
            if pmid_to_oa_url and pmid in pmid_to_oa_url:
                logger.info(f"PMID:{pmid} - Falling back to FTP download...")
                pmc_file = pmcFileDir + pmid + '.tar.gz'
                pmc_url = pmcRootUrl + pmid_to_oa_url[pmid]
                try:
                    download_file(pmc_url, pmc_file)
                    # Unpack the tar.gz
                    gunzip_file(pmc_file, pmid_dir + "/")
                    if path.exists(pmc_file):
                        remove(pmc_file)
                    ftp_fallback_count += 1
                    logger.info(f"PMID:{pmid} - FTP download successful")
                except Exception as e:
                    logger.warning(f"PMID:{pmid} - FTP fallback failed: {e}")
            else:
                logger.warning(f"Failed to download PMC package for PMID:{pmid} PMCID:{pmcid} (not in S3, no FTP fallback)")

    if ftp_fallback_count > 0:
        logger.info(f"Used FTP fallback for {ftp_fallback_count} packages (older PMCIDs not yet in S3)")


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
