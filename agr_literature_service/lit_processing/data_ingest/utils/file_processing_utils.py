import json
import logging
import tarfile
import gzip
import shutil
from typing import Optional
from urllib import request
import datetime
from os import environ, path, listdir, remove, makedirs
from os.path import join as path_join, basename
import html
import re

import boto3  # type: ignore
from botocore import UNSIGNED  # type: ignore
from botocore.config import Config  # type: ignore
from botocore.exceptions import ClientError  # type: ignore

from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

init_tmp_dir()

base_path = environ.get('XML_PATH')

logger = logging.getLogger(__name__)


def load_pubmed_resource_basic():
    """

    :return:
    """

    filename = base_path + 'pubmed_resource_json/resource_pubmed_all.json'
    f = open(filename)
    resource_data = json.load(f)
    pubmed_by_nlm = dict()
    nlm_by_issn = dict()
    for entry in resource_data:
        # primary_id = entry['primaryId']
        nlm = entry['nlm']
        pubmed_by_nlm[nlm] = entry
        if 'printISSN' in entry:
            pissn = entry['printISSN']
            if pissn in nlm_by_issn:
                if nlm not in nlm_by_issn[pissn]:
                    nlm_by_issn[pissn].append(nlm)
            else:
                nlm_by_issn[pissn] = [nlm]
        if 'onlineISSN' in entry:
            oissn = entry['onlineISSN']
            if oissn in nlm_by_issn:
                if nlm not in nlm_by_issn[oissn]:
                    nlm_by_issn[oissn].append(nlm)
            else:
                nlm_by_issn[oissn] = [nlm]
    return pubmed_by_nlm, nlm_by_issn


def save_resource_file(json_storage_path, pubmed_by_nlm, datatype):
    """

    :param json_storage_path:
    :param pubmed_by_nlm:
    :return:
    """

    pubmed_data = dict()
    pubmed_data['data'] = []
    for nlm in pubmed_by_nlm:
        pubmed_data['data'].append(pubmed_by_nlm[nlm])
    json_filename = json_storage_path + 'RESOURCE_' + datatype + '.json'
    write_json(json_filename, pubmed_data)


def write_json(json_filename, dict_to_output):
    """

    :param json_filename:
    :param dict_to_output:
    :return:
    """

    with open(json_filename, "w") as json_file:
        # not sure how to logger from imported function without breaking logger in main function
        # logger.info("Generating JSON for %s", json_filename)
        json_data = json.dumps(dict_to_output, indent=4, sort_keys=True)
        json_file.write(json_data)
        json_file.close()


def chunks(list, size):
    for i in range(0, len(list), size):
        yield list[i:i + size]


def download_file(url, file, *, required=False):
    """
    Download file from URL to local path.

    If required=True, raise ExcludeListUnavailableError on failure.
    Otherwise, log and continue.
    """
    try:
        logger.info("Downloading %s", url)
        req = request.urlopen(url)
        data = req.read()
        with open(file, "wb") as fh:
            fh.write(data)
        return True

    except Exception as e:
        logger.error(
            "Error downloading file %s from %s. Error=%s",
            file, url, str(e)
        )

        if required:
            raise

        return False


def download_s3_file(bucket, key, file):
    """
    Download a file from S3 bucket using IAM instance credentials.

    :param bucket: S3 bucket name
    :param key: S3 object key (path within bucket)
    :param file: Local file path to save to
    :return: True on success, False on failure
    """
    try:
        logger.info(f"Downloading s3://{bucket}/{key}")
        s3 = boto3.client('s3')
        s3.download_file(bucket, key, file)
        return True
    except ClientError as e:
        logger.error(f"Error downloading S3 file: {bucket}/{key}. Error={str(e)}")
        return False


def gunzip_file(file_with_path, to_file_dir):

    try:
        file = tarfile.open(file_with_path)
        file.extractall(to_file_dir)
        return True
    except Exception as e:
        logger.error(e)
        return False


def gzip_file(file_with_path):

    gzip_file_with_path = None
    try:
        gzip_file_with_path = file_with_path + ".gz"
        with open(file_with_path, 'rb') as f_in, gzip.open(gzip_file_with_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    except Exception as e:
        logging.error(e)
        gzip_file_with_path = None

    return gzip_file_with_path


def remove_old_files(dir_path, days_old):

    now = datetime.datetime.now()

    # iterate over all files in the directory
    for filename in listdir(dir_path):
        file_path = path.join(dir_path, filename)
        if path.isfile(file_path):
            file_mod_time = datetime.datetime.fromtimestamp(path.getmtime(file_path))
            delta = now - file_mod_time
            if delta.days > days_old:
                remove(file_path)


def classify_pmc_file(file_name, file_extension):

    """
    image_related_file_extensions = [
        'jpg', 'jpeg', 'gif', 'tif', 'tiff', 'png',
        'eps', 'ai', 'bmp', 'svg', 'webp', 'emf'
    ]
    """
    image_related_file_extensions = ['jpg', 'jpeg', 'gif', 'tif', 'tiff', 'png']
    if file_extension.lower() == "nxml":
        return "nXML"
    if "thumb" in file_name.lower() and file_extension.lower() in image_related_file_extensions:
        return "thumbnail"
    # if "fig" in file_name.lower() and file_extension.lower() in image_related_file_extensions:
    if file_extension.lower() in image_related_file_extensions:
        return "figure"
    return "supplement"


class ExcludeListUnavailableError(RuntimeError):
    """Raised when a required remote exclude PMID list cannot be retrieved."""
    def __init__(self, mod: str, url: str, error: str):
        super().__init__(
            f"[{mod}] Required exclude PMID list is unavailable.\n"
            f"URL: {url}\n"
            f"Error: {error}"
        )
        self.mod = mod
        self.url = url
        self.error = error


def get_pmids_from_exclude_list(mod=None):
    """
    Return a set of PMIDs (no 'PMID:' prefix) to exclude.

    For WB and XB:
      - the remote exclude list is REQUIRED
      - failure to retrieve it raises ExcludeListUnavailableError
      - local cached files are NOT used as fallback
    """

    data_path = path.join(
        path.dirname(path.dirname(path.abspath(__file__))),
        "pubmed_ingest",
        "data_for_pubmed_processing",
    )

    exclude_pmid_file = None

    # Global exclude list (non-fatal)
    if mod is None:
        exclude_pmid_file = path.join(data_path, "pmids_to_excude.txt")

    else:
        mod_false_positive_file = {
            "WB": "WB_false_positive_pmids.txt",
            "XB": "XB_false_positive_pmids.txt",
            "ZFIN": "ZFIN_false_positive_pmids.txt",
        }
        mod_to_fp_pmids_url = {
            "WB": "https://caltech-curation.textpressolab.com/files/pub/agr_upload/pap_papers/rejected_pmids",
            "XB": "https://ftp.xenbase.org/pub/DataExchange/AGR/XB_false_positive_pmids.txt",
        }

        if mod in mod_false_positive_file:
            exclude_pmid_file = path.join(
                data_path, mod_false_positive_file[mod]
            )

            # WB / XB: remote file is REQUIRED
            if mod in mod_to_fp_pmids_url:
                fp_url = mod_to_fp_pmids_url[mod]
                try:
                    download_file(fp_url, exclude_pmid_file, required=True)
                except Exception as e:
                    raise ExcludeListUnavailableError(
                        mod=mod,
                        url=fp_url,
                        error=str(e),
                    ) from e

    exclude_pmids = set()
    if exclude_pmid_file and path.exists(exclude_pmid_file):
        with open(exclude_pmid_file, "r", encoding="utf-8") as infile_fh:
            exclude_pmids = {
                line.strip().replace("PMID:", "")
                for line in infile_fh
                if line.strip()
            }

    return exclude_pmids


def remove_surrogates(text):
    """
    to deal with error like the following:
    'utf-8' codec can't encode characters in position 163848625-163848626:
    surrogates not allowed
    """

    if text:
        return re.sub(r'[\uD800-\uDFFF]', '', text)
    return text


def escape_special_characters(text):

    if text:
        text = remove_surrogates(text)

        # Unescape HTML entities
        text = html.unescape(text)

        ## escape any newline, carriage return, and tab
        if "\n" in text or "\r" in text or "\t" in text:
            text = text.replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')

        ## escape double quote
        # text = text.replace('"', '\\"')

    return text


# PMC Open Access S3 utilities
# As of August 2026, PMC FTP service is deprecated in favor of AWS S3
# See: https://ncbiinsights.ncbi.nlm.nih.gov/2026/02/12/pmc-article-dataset-distribution-services/

PMC_OA_S3_BUCKET = "pmc-oa-opendata"

# Cached S3 client for PMC OA bucket (anonymous access)
_pmc_oa_s3_client = None


def get_pmc_oa_s3_client():
    """
    Get an S3 client configured for anonymous access to the PMC Open Access bucket.
    Client is cached as a module-level singleton for performance.

    Returns:
        boto3 S3 client configured with anonymous credentials
    """
    global _pmc_oa_s3_client
    if _pmc_oa_s3_client is None:
        _pmc_oa_s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    return _pmc_oa_s3_client


def list_pmc_package_versions(pmcid: str) -> list:
    """
    List all available versions of a PMC package in S3.

    Args:
        pmcid: PMC ID (e.g., 'PMC10009402' or '10009402')

    Returns:
        List of version prefixes (e.g., ['PMC10009402.1/', 'PMC10009402.2/'])
    """
    if not pmcid.startswith('PMC'):
        pmcid = f'PMC{pmcid}'

    s3_client = get_pmc_oa_s3_client()

    try:
        response = s3_client.list_objects_v2(
            Bucket=PMC_OA_S3_BUCKET,
            Prefix=f"{pmcid}.",
            Delimiter='/'
        )
        prefixes = [p['Prefix'] for p in response.get('CommonPrefixes', [])]
        return sorted(prefixes)
    except ClientError as e:
        logger.error(f"Error listing PMC package versions for {pmcid}: {e}")
        return []


def get_latest_pmc_version(pmcid: str) -> Optional[str]:
    """
    Get the latest version prefix for a PMC package.

    Args:
        pmcid: PMC ID (e.g., 'PMC10009402' or '10009402')

    Returns:
        Latest version prefix (e.g., 'PMC10009402.2') or None if not found
    """
    versions = list_pmc_package_versions(pmcid)
    if versions:
        # Return the highest version number (remove trailing /)
        return versions[-1].rstrip('/')
    return None


def list_pmc_package_files(pmcid: str, version: str = None) -> list:
    """
    List all files in a PMC package.

    Args:
        pmcid: PMC ID (e.g., 'PMC10009402')
        version: Specific version (e.g., '1') or None for latest

    Returns:
        List of file keys in the package
    """
    if not pmcid.startswith('PMC'):
        pmcid = f'PMC{pmcid}'

    if version:
        prefix = f"{pmcid}.{version}/"
    else:
        latest = get_latest_pmc_version(pmcid)
        if not latest:
            return []
        prefix = f"{latest}/"

    s3_client = get_pmc_oa_s3_client()

    try:
        # Use paginator to handle >1000 files
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=PMC_OA_S3_BUCKET, Prefix=prefix)
        files = []
        for page in pages:
            for obj in page.get('Contents', []):
                files.append(obj['Key'])
        return files
    except ClientError as e:
        logger.error(f"Error listing PMC package files for {pmcid}: {e}")
        return []


def download_pmc_package_from_s3(pmcid: str, dest_dir: str, version: str = None) -> bool:
    """
    Download all files from a PMC package to a local directory.

    Args:
        pmcid: PMC ID (e.g., 'PMC10009402' or '10009402')
        dest_dir: Local directory to download files to
        version: Specific version or None for latest

    Returns:
        True if successful, False otherwise
    """
    if not pmcid.startswith('PMC'):
        pmcid = f'PMC{pmcid}'

    if not version:
        # Check if package exists
        if not get_latest_pmc_version(pmcid):
            logger.warning(f"No PMC package found for {pmcid}")
            return False

    s3_client = get_pmc_oa_s3_client()
    files = list_pmc_package_files(pmcid, version)

    if not files:
        logger.warning(f"No files found for {pmcid}")
        return False

    # Create destination directory
    package_dir = path_join(dest_dir, pmcid)
    makedirs(package_dir, exist_ok=True)

    try:
        for file_key in files:
            file_name = basename(file_key)
            local_path = path_join(package_dir, file_name)
            logger.info(f"Downloading {file_key} to {local_path}")
            s3_client.download_file(PMC_OA_S3_BUCKET, file_key, local_path)
        return True
    except ClientError as e:
        logger.error(f"Error downloading PMC package {pmcid}: {e}")
        return False


def download_pmc_file_from_s3(pmcid: str, file_name: str, dest_path: str,
                              version: str = None) -> bool:
    """
    Download a specific file from a PMC package.

    Args:
        pmcid: PMC ID (e.g., 'PMC10009402')
        file_name: Name of the file to download (e.g., 'PMC10009402.1.pdf')
        dest_path: Full local path to save the file
        version: Specific version or None for latest

    Returns:
        True if successful, False otherwise
    """
    if not pmcid.startswith('PMC'):
        pmcid = f'PMC{pmcid}'

    if version:
        prefix = f"{pmcid}.{version}"
    else:
        latest = get_latest_pmc_version(pmcid)
        if not latest:
            logger.warning(f"No PMC package found for {pmcid}")
            return False
        prefix = latest

    s3_client = get_pmc_oa_s3_client()
    file_key = f"{prefix}/{file_name}"

    try:
        s3_client.download_file(PMC_OA_S3_BUCKET, file_key, dest_path)
        return True
    except ClientError as e:
        logger.error(f"Error downloading {file_key}: {e}")
        return False


def get_pmc_package_metadata(pmcid: str, version: str = None) -> dict:
    """
    Get metadata JSON for a PMC package.

    Args:
        pmcid: PMC ID (e.g., 'PMC10009402')
        version: Specific version or None for latest

    Returns:
        Metadata dict or empty dict if not found
    """
    if not pmcid.startswith('PMC'):
        pmcid = f'PMC{pmcid}'

    if version:
        prefix = f"{pmcid}.{version}"
    else:
        latest = get_latest_pmc_version(pmcid)
        if not latest:
            return {}
        prefix = latest

    s3_client = get_pmc_oa_s3_client()
    json_key = f"{prefix}/{prefix}.json"

    try:
        response = s3_client.get_object(Bucket=PMC_OA_S3_BUCKET, Key=json_key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except ClientError as e:
        logger.debug(f"No metadata found for {pmcid}: {e}")
        return {}
