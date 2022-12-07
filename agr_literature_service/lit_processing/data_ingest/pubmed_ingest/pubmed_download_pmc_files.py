import logging
import shutil
from os import path, environ, makedirs, listdir, remove
from dotenv import load_dotenv
from agr_literature_service.lit_processing.utils.db_read_utils import \
    get_pmid_list_without_pmc_package
from agr_literature_service.lit_processing.utils.s3_utils import upload_file_to_s3
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.md5sum_utils import \
    get_md5sum
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import \
    download_file, gunzip_file, gzip_file
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.load_pmc_metadata import \
    load_ref_file_metadata_into_db
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_identify_main_pdfs import \
    identify_main_pdfs

load_dotenv()

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_bucket = 'agr-literature'
pmcRootUrl = 'https://ftp.ncbi.nlm.nih.gov/pub/pmc/'
dataDir = 'data/'
pmcFileDir = 'pubmed_pmc_download/'
suppl_file_uploaded = dataDir + "pmc_oa_files_uploaded.txt"


def download_pmc_files(mapping_file):  # pragma: no cover

    logger.info("Reading oa_file_list.csv mapping file...")

    pmid_to_oa_url = get_pmid_to_pmc_url_mapping(mapping_file)

    logger.info("Retrieving pmid list for papers that do not have PMC package downloaded...")

    pmids = get_pmid_list_without_pmc_package(['SGD', 'WB', 'ZFIN', 'FB'])

    logger.info("Downloading PMC OA packages...")

    download_packages(pmids, pmid_to_oa_url)

    logger.info("Unpacking PMC OA packages...")

    unpack_packages()

    logger.info("Uploading the files to s3...")

    upload_suppl_files_to_s3()

    logger.info("Loading the metadata into database...")

    load_ref_file_metadata_into_db()

    logger.info("Identifying main PDF files in the database...")

    identify_main_pdfs()


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


def download_packages(pmids, pmid_to_oa_url):  # pragma: no cover

    for pmid in pmids:
        pmc_file = pmcFileDir + pmid + '.tar.gz'
        if path.exists(pmc_file):
            continue
        if pmid in pmid_to_oa_url:
            pmc_url = pmcRootUrl + pmid_to_oa_url[pmid]
            logger.info("PMID:" + pmid + " " + pmc_url)
            download_file(pmc_url, pmc_file)


def get_pmid_to_pmc_url_mapping(mapping_file):  # pragma: no cover

    # File,Article Citation,Accession ID,Last Updated (YYYY-MM-DD HH:MM:SS),PMID,License
    # oa_package/08/e0/PMC13900.tar.gz,Breast Cancer Res. 2001 Nov 2; 3(1):55-60,PMC13900,2019-11-05 11:56:12,11250746,NO-CC CODE

    pmid_to_oa_url = {}
    f = open(mapping_file)
    for line in f:
        if line.startswith('File,'):
            continue
        pieces = line.split(',')
        if pieces[4] and pieces[4].isdigit():
            pmid_to_oa_url[pieces[4]] = pieces[0]
    f.close()

    return pmid_to_oa_url


def create_tmp_dirs():  # pragma: no cover

    if path.exists(dataDir):
        shutil.rmtree(dataDir)
    makedirs(dataDir)
    if path.exists(pmcFileDir):
        shutil.rmtree(pmcFileDir)
    makedirs(pmcFileDir)


if __name__ == "__main__":

    create_tmp_dirs()

    oafile_ftp = 'ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.csv'
    mapping_file = dataDir + "oa_file_list.csv"

    download_file(oafile_ftp, mapping_file)

    download_pmc_files(mapping_file)
