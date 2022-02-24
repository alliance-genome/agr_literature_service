"""
get_dqm_data
============

downloads DQM MOD JSON from FMS and uncompresses.
compares md5sum to current file to prevent downloading if it's the same
# pipenv run python get_dqm_data.py

"""

import gzip
import hashlib
import io
import json
import logging.config
import os
import urllib.request
from os import environ, makedirs, path

import coloredlogs
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG")

# contants that need to be modified on new MODs or new release
MODS = ["SGD", "RGD", "FB", "WB", "MGI", "ZFIN"]
RELEASE = "4.1.0"
DATATYPES = ["REFERENCE", "REF-EXCHANGE", "RESOURCE"]


def get_md5_sum_from_path(filename):
    """

    :param filename:
    :return:
    """

    if os.path.exists(filename):
        return md5_update_from_file(filename, hashlib.md5()).hexdigest()
    else:
        return "no file found"


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


def download_dqm_json(base_path):
    """
    for testing use this
    mods = ['WB']
    DATATYPES = ['RESOURCE']
    mods = ['WB', 'FB']
    DATATYPES = ['REFERENCE']

    :return:
    """

    storage_path = os.path.join(base_path, "dqm_data/")
    if not path.exists(storage_path):
        logger.info(f"Creating DQM data  {storage_path}")
        makedirs(storage_path)
    else:
        logger.info(f"DQM data {storage_path} already exists")

    for datatype in DATATYPES:
        datatype_path = os.path.join(storage_path, datatype)
        if not os.path.isdir(datatype_path):
            makedirs(datatype_path)
        for mod in MODS:
            url = 'https://fms.alliancegenome.org/api/datafile/by/' + RELEASE + '/' + datatype + '/' + mod + '?latest=true'
            logger.info(f"Processing {url}")
            response = urllib.request.urlopen(url)
            try:
                data = json.loads(response.read())
                outfile_path_decompressed = os.path.join(datatype_path, mod + ".json")

                if not os.path.exists(outfile_path_decompressed):
                    md5sum_local = get_md5_sum_from_path(outfile_path_decompressed)
                    logger.debug(outfile_path_decompressed + " has md5sum " + md5sum_local)

                    if md5sum_local != data[0]["md5Sum"]:
                        logger.info(f"Downloading {data[0]['s3Url']} to {outfile_path_decompressed}")
                        response = urllib.request.urlopen(data[0]["s3Url"])
                        compressed_file = io.BytesIO(response.read())
                        decompressed_file = gzip.GzipFile(fileobj=compressed_file)

                        with open(outfile_path_decompressed, "wb") as outfile:
                            outfile.write(decompressed_file.read())
                else:
                    logger.info(f"{outfile_path_decompressed} already exists")
            except Exception as e:
                logger.error(f"Error processing {url}")
                logger.error(e)


if __name__ == "__main__":
    """
    redirects to download_dqm_json
    """

    base_path = os.getcwd()
    download_dqm_json(base_path)
