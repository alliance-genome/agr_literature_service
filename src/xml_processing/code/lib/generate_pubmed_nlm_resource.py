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


# todo: save this in an env variable
# root_path = '/home/azurebrd/git/agr_literature_service_demo/'
# base_path = root_path + 'src/xml_processing/'
base_path = os.environ.get("XML_PATH", "")
storage_path = base_path + "pubmed_resource_json/"


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

    #     counter = 0
    for entry in entries:
        # counter = counter + 1
        # if counter > 5:
        #     continue
        nlm = ""
        if re.search("NlmId: (.+)", entry):
            nlm_group = re.search("NlmId: (.+)", entry)
            nlm = nlm_group.group(1)
        if nlm:
            data_dict = {"primaryId": "NLM:" + nlm, "nlm": nlm, "crossReferences": [{"id": "NLM:" + nlm}]}
            if re.search("JournalTitle: (.+)", entry):
                title_group = re.search("JournalTitle: (.+)", entry)
                title = title_group.group(1)
                data_dict["title"] = title
            if re.search("IsoAbbr: (.+)", entry):
                iso_abbreviation_group = re.search("IsoAbbr: (.+)", entry)
                iso_abbreviation = iso_abbreviation_group.group(1)
                data_dict["isoAbbreviation"] = iso_abbreviation
            if re.search("MedAbbr: (.+)", entry):
                medline_abbreviation_group = re.search("MedAbbr: (.+)", entry)
                medline_abbreviation = medline_abbreviation_group.group(1)
                data_dict["medlineAbbreviation"] = medline_abbreviation
            if re.search(r"ISSN \(Print\): (.+)", entry):
                print_issn_group = re.search(r"ISSN \(Print\): (.+)", entry)
                print_issn = print_issn_group.group(1)
                data_dict["printISSN"] = print_issn
            if re.search(r"ISSN \(Online\): (.+)", entry):
                online_issn_group = re.search(r"ISSN \(Online\): (.+)", entry)
                online_issn = online_issn_group.group(1)
                data_dict["onlineISSN"] = online_issn

            nlm_info.append(data_dict)

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


def generate_json(nlm_info, upload_to_s3):
    """

    to remove an uploaded file
    aws s3 rm s3://agr-literature/develop/resource/metadata/resource_pubmed_all.json

    :param nlm_info:
    :param upload_to_s3:
    :return:
    """

    logger.info("Generating JSON from NLM data and saving to outfile")
    json_data = json.dumps(nlm_info, indent=4, sort_keys=True)

    if not os.path.exists(storage_path):
        os.makedirs(storage_path)

    # Write the json data to output json file
    # UNCOMMENT TO write to json directory
    filename = "resource_pubmed_all.json"
    output_json_file = storage_path + filename
    with open(output_json_file, "w") as json_file:
        json_file.write(json_data)
        json_file.close()

    if upload_to_s3:
        s3_bucket = "agr-literature"
        s3_filename = "develop/resource/metadata/" + filename
        # UNCOMMENT TO upload to aws bucket
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


def populate_from_local_file():
    """

    :return:
    """

    filename = base_path + "J_Medline.txt"
    if not os.path.exists(filename):
        return "journal info file not found"
    else:
        return open(filename).read()


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
