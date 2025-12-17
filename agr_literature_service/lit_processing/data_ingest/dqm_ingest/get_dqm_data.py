import gzip
import zipfile
import shutil
import logging
from os import environ, makedirs, path, remove, rename

from dotenv import load_dotenv
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import \
    download_file, download_s3_file

load_dotenv()
init_tmp_dir()

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

base_path = environ.get('XML_PATH', "")
dqm_json_path = base_path + 'dqm_data/'

if not path.exists(dqm_json_path):
    makedirs(dqm_json_path)


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

    for mod in mod_to_reference_url:
        logger.info("Download REFERENCE json file for " + mod)
        download_dqm_file(mod, mod_to_reference_url[mod], "REFERENCE")

    for mod in mod_to_reference_s3:
        logger.info("Download REFERENCE json file for " + mod + " from S3")
        download_dqm_s3_file(mod, mod_to_reference_s3[mod], "REFERENCE")


def download_dqm_resource_json():  # pragma: no cover

    mod_to_resource_url = {
        "ZFIN": "https://zfin.org/downloads/ZFIN_1.0.1.4_Resource.json"
    }

    # FlyBase uses S3 for secure cross-account data transfer
    mod_to_resource_s3 = {
        "FB": {"bucket": "flybase-alliance-data", "key": "pub-exports/FB_resource.json.gz"}
    }

    for mod in mod_to_resource_url:
        logger.info("Download RESOURCE json file for " + mod)
        download_dqm_file(mod, mod_to_resource_url[mod], "RESOURCE")

    for mod in mod_to_resource_s3:
        logger.info("Download RESOURCE json file for " + mod + " from S3")
        download_dqm_s3_file(mod, mod_to_resource_s3[mod], "RESOURCE")


def download_dqm_file(mod, url, datatype):  # pragma: no cover

    json_file = dqm_json_path + datatype + '_' + mod + '.json'

    if url.endswith('.gz'):
        gzip_json_file = json_file + ".gz"
        download_file(url, gzip_json_file)
        try:
            with gzip.open(gzip_json_file, 'rb') as f_in:
                with open(json_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            remove(gzip_json_file)
        except Exception as e:
            logger.error(e)
    elif url.endswith('.zip'):
        zip_json_file = json_file + ".zip"
        download_file(url, zip_json_file)
        try:
            with zipfile.ZipFile(zip_json_file, 'r') as zip_ref:
                zip_ref.extractall(dqm_json_path)
            orig_json_file = dqm_json_path + "filepart_abc_meta_data_merged.json"
            rename(orig_json_file, json_file)
            remove(zip_json_file)
        except Exception as e:
            logger.error(e)
    else:
        download_file(url, json_file)


def download_dqm_s3_file(mod, s3_config, datatype):  # pragma: no cover
    """
    Download a DQM file from S3 bucket.

    :param mod: MOD abbreviation (e.g., 'FB')
    :param s3_config: Dict with 'bucket' and 'key' for S3 location
    :param datatype: Type of data (e.g., 'REFERENCE', 'RESOURCE')
    """
    json_file = dqm_json_path + datatype + '_' + mod + '.json'
    bucket = s3_config['bucket']
    key = s3_config['key']

    if key.endswith('.gz'):
        gzip_json_file = json_file + ".gz"
        download_s3_file(bucket, key, gzip_json_file)
        try:
            with gzip.open(gzip_json_file, 'rb') as f_in:
                with open(json_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            remove(gzip_json_file)
        except Exception as e:
            logger.error(e)
    else:
        download_s3_file(bucket, key, json_file)


if __name__ == "__main__":

    download_dqm_json()
