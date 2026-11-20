import gzip
import zipfile
import shutil
import logging
from os import environ, makedirs, path, remove, rename

from dotenv import load_dotenv
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import \
    download_url_to_file, download_s3_file

load_dotenv()
init_tmp_dir()

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

base_path = environ.get('XML_PATH', "")
dqm_json_path = base_path + 'dqm_data/'

if not path.exists(dqm_json_path):
    makedirs(dqm_json_path, exist_ok=True)

# MGI's server drops the connection mid-transfer now and then (SCRUM-6846);
# a later attempt usually gets the whole file.
DQM_DOWNLOAD_ATTEMPTS = 3


def download_dqm_json():  # pragma: no cover

    download_dqm_reference_json()
    download_dqm_resource_json()


def download_dqm_reference_json():  # pragma: no cover

    mod_to_reference_url = {
        "WB": "https://caltech-curation.textpressolab.com/files/pub/agr_upload/pap_papers/agr_wb_literature.json",
        "ZFIN": "https://zfin.org/downloads/ZFIN_1.0.1.4_Reference.json",
        "XB": "https://ftp.xenbase.org/pub/DataExchange/AGR/XB_REFERENCE.json.zip",
        "MGI": "http://www.informatics.jax.org/downloads/alliance/reference.json.gz",
        "RGD": "https://download.rgd.mcw.edu/data_release/agr/REFERENCE_RGD.json"
    }

    # FlyBase uses S3 for secure cross-account data transfer
    mod_to_reference_s3 = {
        "FB": {"bucket": "flybase-alliance-data", "key": "pub-exports/FB_reference.json.gz"}
    }

    failed_mods = {}
    for mod in mod_to_reference_url:
        logger.info("Download REFERENCE json file for " + mod)
        error = download_dqm_file(mod, mod_to_reference_url[mod], "REFERENCE")
        if error:
            failed_mods[mod] = error

    for mod in mod_to_reference_s3:
        logger.info("Download REFERENCE json file for " + mod + " from S3")
        error = download_dqm_s3_file(mod, mod_to_reference_s3[mod], "REFERENCE")
        if error:
            failed_mods[mod] = error

    if failed_mods:
        logger.error("REFERENCE DQM download FAILED for: " + ", ".join(failed_mods))
    return failed_mods


def download_dqm_resource_json():  # pragma: no cover

    mod_to_resource_url = {
        "ZFIN": "https://zfin.org/downloads/ZFIN_1.0.1.4_Resource.json"
    }

    # FlyBase uses S3 for secure cross-account data transfer
    mod_to_resource_s3 = {
        "FB": {"bucket": "flybase-alliance-data", "key": "pub-exports/FB_resource.json.gz"}
    }

    failed_mods = {}
    for mod in mod_to_resource_url:
        logger.info("Download RESOURCE json file for " + mod)
        error = download_dqm_file(mod, mod_to_resource_url[mod], "RESOURCE")
        if error:
            failed_mods[mod] = error

    for mod in mod_to_resource_s3:
        logger.info("Download RESOURCE json file for " + mod + " from S3")
        error = download_dqm_s3_file(mod, mod_to_resource_s3[mod], "RESOURCE")
        if error:
            failed_mods[mod] = error

    if failed_mods:
        logger.error("RESOURCE DQM download FAILED for: " + ", ".join(failed_mods))
    return failed_mods


def download_dqm_file(mod, url, datatype):  # pragma: no cover
    """Download one MOD's DQM file, returning None on success and the error
    message on failure. On ANY failure the target json file is removed (if a
    previous run left one), so a stale download can never masquerade as a
    fresh one, and the error is returned for the caller to report: a swallowed
    log line alone let a broken XB zip silently skip the load for days."""

    json_file = dqm_json_path + datatype + '_' + mod + '.json'
    error = None

    try:
        if url.endswith('.gz'):
            gzip_json_file = json_file + ".gz"
            logger.info("Downloading %s", url)
            download_url_to_file(url, gzip_json_file, attempts=DQM_DOWNLOAD_ATTEMPTS)
            with gzip.open(gzip_json_file, 'rb') as f_in:
                with open(json_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            remove(gzip_json_file)
        elif url.endswith('.zip'):
            zip_json_file = json_file + ".zip"
            logger.info("Downloading %s", url)
            download_url_to_file(url, zip_json_file, attempts=DQM_DOWNLOAD_ATTEMPTS)
            # Take the single .json member whatever it is named: the inner
            # name is the submitting MOD's choice and has changed before
            # (XB: filepart_abc_meta_data_merged.json -> XB_REFERENCE.json),
            # which silently broke the load when it was hardcoded here.
            # __MACOSX/._*.json resource-fork entries are not real members.
            with zipfile.ZipFile(zip_json_file, 'r') as zip_ref:
                json_members = [m for m in zip_ref.namelist()
                                if m.lower().endswith('.json')
                                and not m.startswith('__MACOSX/')]
                if len(json_members) != 1:
                    raise ValueError(
                        f"{mod}: expected exactly one .json member in {url}, "
                        f"found {json_members or zip_ref.namelist()}")
                zip_ref.extract(json_members[0], dqm_json_path)
            orig_json_file = path.join(dqm_json_path, json_members[0])
            if path.abspath(orig_json_file) != path.abspath(json_file):
                rename(orig_json_file, json_file)
            remove(zip_json_file)
        else:
            logger.info("Downloading %s", url)
            download_url_to_file(url, json_file, attempts=DQM_DOWNLOAD_ATTEMPTS)
    except Exception as e:
        error = f"{type(e).__name__}: {e}"
        logger.error("%s DQM %s download/unpack failed for %s: %s", mod, datatype, url, error)

    if error and path.exists(json_file):
        remove(json_file)
    return error


def download_dqm_s3_file(mod, s3_config, datatype):  # pragma: no cover
    """
    Download a DQM file from S3 bucket. Returns None on success; on failure
    any stale target json file is removed and the error message is returned
    (see download_dqm_file).

    :param mod: MOD abbreviation (e.g., 'FB')
    :param s3_config: Dict with 'bucket' and 'key' for S3 location
    :param datatype: Type of data (e.g., 'REFERENCE', 'RESOURCE')
    """
    json_file = dqm_json_path + datatype + '_' + mod + '.json'
    bucket = s3_config['bucket']
    key = s3_config['key']
    error = None
    s3_url = f"s3://{bucket}/{key}"

    if key.endswith('.gz'):
        gzip_json_file = json_file + ".gz"
        if download_s3_file(bucket, key, gzip_json_file):
            try:
                with gzip.open(gzip_json_file, 'rb') as f_in:
                    with open(json_file, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                remove(gzip_json_file)
            except Exception as e:
                error = f"{type(e).__name__}: {e}"
                logger.error("%s DQM %s unpack failed for %s: %s", mod, datatype, s3_url, error)
        else:
            error = f"download failed for {s3_url}"
    elif not download_s3_file(bucket, key, json_file):
        error = f"download failed for {s3_url}"

    if error and path.exists(json_file):
        remove(json_file)
    return error


if __name__ == "__main__":

    download_dqm_json()
