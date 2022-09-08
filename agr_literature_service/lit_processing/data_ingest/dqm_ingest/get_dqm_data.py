
import sys
import gzip
import hashlib
import io
import json
import logging
import logging.config
import os
import urllib.request
from os import environ, makedirs, path

from dotenv import load_dotenv
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

load_dotenv()

init_tmp_dir()

# get_dqm_data.py downloads DQM MOD JSON from FMS and uncompresses. compares md5sum to current file to prevent downloading if it's the same
# pipenv run python get_dqm_data.py


logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format= '%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

base_path = environ.get('XML_PATH', "")
fms_release = environ.get('ALLIANCE_FMS_RELEASE', '')
storage_path = base_path + 'dqm_data/'


def get_md5_sum_from_path(filename):
    """

    :param filename:
    :return:
    """

    if os.path.exists(filename):
        return md5_update_from_file(filename, hashlib.md5()).hexdigest()
    else:
        return 'no file found'


def md5_update_from_file(filename, hash):
    """

    :param filename:
    :param hash:
    :return:
    """

    with open(str(filename), "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash.update(chunk)
    return hash


def download_dqm_json():
    """

    :return:
    """

    if not path.exists(storage_path):
        makedirs(storage_path)

    mods = ['SGD', 'RGD', 'FB', 'XB', 'WB', 'MGI', 'ZFIN']
    datatypes = ['REFERENCE', 'REF-EXCHANGE', 'RESOURCE']
#     mods = ['XB']
#     datatypes = ['RESOURCE']
#     mods = ['WB', 'FB']
#     datatypes = ['REFERENCE']
    for datatype in datatypes:
        for mod in mods:
            url = 'https://fms.alliancegenome.org/api/datafile/by/' + fms_release + '/' + datatype + '/' + mod + '?latest=true'
            response = urllib.request.urlopen(url)
            data = json.loads(response.read())
#             data = ''
#             with urllib.request.urlopen(url) as url_data:
#                 file_data = url_data.read().decode('utf-8')
#                 data = json.loads(file_data)
            if len(data) < 1:
                continue
            if 's3Url' not in data[0]:
                continue
            file_url = data[0]['s3Url']
            md5sum_fms = data[0]['md5Sum']
            outfile_path_decompressed = storage_path + datatype + '_' + mod + '.json'

            md5sum_local = get_md5_sum_from_path(outfile_path_decompressed)
            logger.debug(outfile_path_decompressed + ' has md5sum ' + md5sum_local)

            if md5sum_local != md5sum_fms:
                logger.info("downloading %s to %s", file_url, outfile_path_decompressed)

                response = urllib.request.urlopen(file_url)
                compressed_file = io.BytesIO(response.read())
                decompressed_file = gzip.GzipFile(fileobj=compressed_file)

                with open(outfile_path_decompressed, 'wb') as outfile:
                    outfile.write(decompressed_file.read())

# if wanting to keep copies of compressed files to save space (md5sum check wouldn't work against fms)
#                 outfile_path_compressed = storage_path + datatype + '_' + mod + '.json.gz'
#                 with open(outfile_path_compressed, 'wb') as outfile:
#                     outfile.write(compressed_file.read())


if __name__ == "__main__":
    """
    call main start function
    """

    download_dqm_json()
