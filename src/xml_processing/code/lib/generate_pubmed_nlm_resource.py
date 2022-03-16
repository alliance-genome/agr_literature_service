"""
generate_pubmed_nlm_resources.py
================================

# generate from local file and do not upload to s3
# pipenv run python generate_pubmed_nlm_resource.py -l
#
# generate from url and do not upload to s3
# pipenv run python generate_pubmed_nlm_resource.py -u
#
# generate from url and upload to s3
# pipenv run python generate_pubmed_nlm_resource.py -u -s

# https://ftp.ncbi.nih.gov/pubmed/J_Medline.txt

"""


import json
import logging
import os
import re
import urllib

import boto3
import click
import coloredlogs
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG")


def populate_nlm_info(file_data):
    """

    :param file_data:
    :return:
    """

    nlm_info = []
    logger.info("Generating NLM data from file")
    entries = file_data.split(
        "\n--------------------------------------------------------\n"
    )

    for entry in entries:
        nlm = ""
        try:
            nlm = re.search("NlmId: (.+)", entry).group(1)
            data_dict = {"primaryId": "NLM:" + nlm, "nlm": nlm, "crossReferences": [{"id": f"NLM: + {nlm}"}]}
            if re.search("JournalTitle: (.+)", entry):
                data_dict["title"] = re.search("JournalTitle: (.+)", entry).group(1)
            if re.search("IsoAbbr: (.+)", entry):
                data_dict["isoAbbreviation"] = re.search("IsoAbbr: (.+)", entry).group(1)
            if re.search("MedAbbr: (.+)", entry):
                data_dict["medlineAbbreviation"] = re.search("MedAbbr: (.+)", entry).group(1)
            if re.search(r"ISSN \(Print\): (.+)", entry):
                data_dict["printISSN"] = re.search(r"ISSN \(Print\): (.+)", entry).group(1)
            if re.search(r"ISSN \(Online\): (.+)", entry):
                data_dict["onlineISSN"] = re.search(r"ISSN \(Online\): (.+)", entry).group(1)

            nlm_info.append(data_dict)
        except AttributeError:
            logger.info("No NlmId found")

    return nlm_info


def upload_file_to_s3(file_name, bucket, object_name=None):
    """
    Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client("s3")
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        if response is not None:
            logger.info("boto 3 uploaded response: %s", response)
        else:
            logger.info("uploaded to s3 %s %s", bucket, file_name)
    except ClientError as e:
        logging.error(e)
        return False

    return True


def generate_json(base_path, nlm_info, upload_to_s3):
    """

    to remove an uploaded file
    aws s3 rm s3://agr-literature/develop/resource/metadata/resource_pubmed_all.json

    :param nlm_info:
    :param upload_to_s3:
    :return:
    """

    storage_path = os.path.join(base_path + "/pubmed_resource_json/")

    logger.info("Generating JSON from NLM data and saving to outfile")
    json_data = json.dumps(nlm_info, indent=4, sort_keys=True)

    if not os.path.exists(storage_path):
        os.makedirs(storage_path)

    filename = "resource_pubmed_all.json"
    output_json_file = os.path.join(storage_path, filename)
    logger.info(f"Writing to {output_json_file}")
    with open(output_json_file, "w") as json_file:
        json_file.write(json_data)
        json_file.close()

    if upload_to_s3:
        s3_bucket = "agr-literature"
        s3_filename = "develop/resource/metadata/" + filename
        logger.info(f"Uploading {filename} to s3://{s3_bucket}/{s3_filename}")
        upload_file_to_s3(output_json_file, s3_bucket, s3_filename)


def populate_from_url():
    """

    :return:
    """

    url_medline = "https://ftp.ncbi.nih.gov/pubmed/J_Medline.txt"
    print(url_medline)
    with urllib.request.urlopen(url_medline) as url:
        file_data = url.read().decode("utf-8")
        return file_data


def populate_from_local_file(base_path):
    """

    :param base_path:
    :return:
    """

    logger.info(f"Reading NLM file from {base_path}/J_Medline.txt")
    filename = os.path.join(base_path, "J_Medline.txt")
    if not os.path.exists(filename):
        logger.info(f"Journal file {filename} does not exist")
        return "journal info file not found"
    else:
        nlm_info = populate_nlm_info(open(filename).read())
        generate_json(base_path, nlm_info, False)
        return "success"


@click.command()
@click.option("-L", "--input-localfile", "local", help="take input from local file", required=False, default=False)
@click.option("-u", "--input-url", "url", help="take input from url", required=False, default=False)
@click.option("-s", "--upload-s3", "s3", help="upload json to s3", required=False, default=False)
def run_tasks(local, url, s3):
    """

    :param local:
    :param url:
    :param s3:
    :return:
    """

    file_data = ""
    if url:
        file_data = populate_from_url()
        logger.info("Processing input from url")
    elif local:
        file_data = populate_from_local_file()
        logger.info("Processing input from local file")
    else:
        file_data = populate_from_url()
        logger.info("Processing input from url")

    if s3:
        logger.info("Upload file to s3")

    nlm_info = populate_nlm_info(file_data)
    generate_json(nlm_info, s3)


if __name__ == "__main__":
    """
    call main start function
    """

    run_tasks()
