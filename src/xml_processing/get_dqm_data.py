
import urllib.request
import json

import io
import gzip
import hashlib

import os
from os import path
import logging
import logging.config


# get_dqm_data.py downloads DQM MOD JSON from FMS and uncompresses. compares md5sum to current file to prevent downloading if it's the same
# pipenv run python get_dqm_data.py


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


# todo: save this in an env variable
base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'
storage_path = base_path + 'dqm_data/'


def get_md5_sum_from_path(filename):
    if os.path.exists(filename):
        return md5_update_from_file(filename, hashlib.md5()).hexdigest()
    else:
        return 'no file found'


def md5_update_from_file(filename, hash):
    with open(str(filename), "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash.update(chunk)
    return hash


def download_dqm_json():
    mods = ['SGD', 'RGD', 'FB', 'WB', 'MGI', 'ZFIN']
    datatypes = ['REFERENCE', 'REF-EXCHANGE', 'RESOURCE']
#     mods = ['WB']
#     datatypes = ['RESOURCE']
#     mods = ['WB', 'FB']
#     datatypes = ['REFERENCE']
    release = '4.0.0'
    for datatype in datatypes:
        for mod in mods:
            url = 'https://fms.alliancegenome.org/api/datafile/by/' + release + '/' + datatype + '/' + mod + '?latest=true'
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
    """ call main start function """
    download_dqm_json()
