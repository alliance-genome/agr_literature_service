import gzip
import zipfile
import shutil
import logging
from os import environ, makedirs, path, remove, rename

from dotenv import load_dotenv
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import \
    download_file

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
        "SGD": "https://sgd-prod-upload.s3.us-west-2.amazonaws.com/latest/REFERENCE_SGD.json",
        "WB": "https://caltech-curation.textpressolab.com/files/pub/agr_upload/pap_papers/agr_wb_literature.json",
        "ZFIN": "https://zfin.org/downloads/ZFIN_1.0.1.4_Reference.json",
        "FB": "https://s3ftp.flybase.org/flybase/associated_files/alliance/FB_reference.json.gz",
        "XB": "https://ftp.xenbase.org/pub/DataExchange/AGR/XB_REFERENCE.json.zip",
        "MGI": "http://www.informatics.jax.org/downloads/alliance/reference.json.gz",
        "RGD": "https://download.rgd.mcw.edu/data_release/agr/REFERENCE_RGD.json"
    }

    for mod in mod_to_reference_url:
        logger.info("Download REFERENCE json file for " + mod)
        download_dqm_file(mod, mod_to_reference_url[mod], "REFERENCE")


def download_dqm_resource_json():  # pragma: no cover

    mod_to_resource_url = {
        "ZFIN": "https://zfin.org/downloads/ZFIN_1.0.1.4_Resource.json",
        "FB": "https://s3ftp.flybase.org/flybase/associated_files/alliance/FB_resource.json.gz"
    }

    for mod in mod_to_resource_url:
        logger.info("Download RESOURCE json file for " + mod)
        download_dqm_file(mod, mod_to_resource_url[mod], "RESOURCE")


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


if __name__ == "__main__":

    download_dqm_json()
