import json

# generate from local file and do not upload to s3
# pipenv run python generate_pubmed_nlm_resource.py -l
#
# generate from url and do not upload to s3
# pipenv run python generate_pubmed_nlm_resource.py -u
#
# generate from url and upload to s3
# pipenv run python generate_pubmed_nlm_resource.py -u -s

# https://ftp.ncbi.nih.gov/pubmed/J_Medline.txt


import re
import urllib

from os import environ, path, makedirs
import logging
import logging.config

import argparse
import boto3
from botocore.exceptions import ClientError

from dotenv import load_dotenv

load_dotenv()


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')

parser = argparse.ArgumentParser()
parser.add_argument('-l', '--input-localfile', action='store_true', help='take input from local file')
parser.add_argument('-u', '--input-url', action='store_true', help='take input from url')
parser.add_argument('-s', '--upload-s3', action='store_true', help='upload json to s3')
args = vars(parser.parse_args())


# todo: save this in an env variable
# root_path = '/home/azurebrd/git/agr_literature_service_demo/'
# base_path = root_path + 'src/xml_processing/'
base_path = environ.get('XML_PATH', "")
storage_path = base_path + 'pubmed_resource_json/'


def populate_nlm_info(file_data):
    """

    :param file_data:
    :return:
    """

    nlm_info = []
    logger.info("Generating NLM data from file")
    entries = file_data.split('\n--------------------------------------------------------\n')

#     counter = 0
    for entry in entries:
        # counter = counter + 1
        # if counter > 5:
        #     continue
        nlm = ''
        if re.search("NlmId: (.+)", entry):
            nlm_group = re.search("NlmId: (.+)", entry)
            nlm = nlm_group.group(1)
        if not nlm:
            # print "skip"
            continue
        data_dict = {}
        data_dict['primaryId'] = 'NLM:' + nlm
        data_dict['nlm'] = nlm
        data_dict['crossReferences'] = [{'id': 'NLM:' + nlm}]
        if re.search("JournalTitle: (.+)", entry):
            title_group = re.search("JournalTitle: (.+)", entry)
            title = title_group.group(1)
            data_dict['title'] = title
        if re.search("IsoAbbr: (.+)", entry):
            iso_abbreviation_group = re.search("IsoAbbr: (.+)", entry)
            iso_abbreviation = iso_abbreviation_group.group(1)
            data_dict['isoAbbreviation'] = iso_abbreviation
        if re.search("MedAbbr: (.+)", entry):
            medline_abbreviation_group = re.search("MedAbbr: (.+)", entry)
            medline_abbreviation = medline_abbreviation_group.group(1)
            data_dict['medlineAbbreviation'] = medline_abbreviation
        if re.search(r"ISSN \(Print\): (.+)", entry):
            print_issn_group = re.search(r"ISSN \(Print\): (.+)", entry)
            print_issn = print_issn_group.group(1)
            data_dict['printISSN'] = print_issn
        if re.search(r"ISSN \(Online\): (.+)", entry):
            online_issn_group = re.search(r"ISSN \(Online\): (.+)", entry)
            online_issn = online_issn_group.group(1)
            data_dict['onlineISSN'] = online_issn

#             print nlm
#             data_dict['nlm'] = nlm
        nlm_info.append(data_dict)
#         print entry
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
    s3_client = boto3.client('s3')
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
    logger.info("Generating JSON from NLM data and saving to outfile")
    json_data = json.dumps(nlm_info, indent=4, sort_keys=True)

    if not path.exists(storage_path):
        makedirs(storage_path)

# Write the json data to output json file
# UNCOMMENT TO write to json directory
    filename = 'resource_pubmed_all.json'
    output_json_file = storage_path + filename
    with open(output_json_file, "w") as json_file:
        json_file.write(json_data)
        json_file.close()

    if upload_to_s3:
        s3_bucket = 'agr-literature'
        s3_filename = 'develop/resource/metadata/' + filename
# UNCOMMENT TO upload to aws bucket
        upload_file_to_s3(output_json_file, s3_bucket, s3_filename)

# to remove an uploaded file
# aws s3 rm s3://agr-literature/develop/resource/metadata/resource_pubmed_all.json


def populate_from_url():
    """

    :return:
    """

    url_medline = "https://ftp.ncbi.nih.gov/pubmed/J_Medline.txt"
    print(url_medline)
    with urllib.request.urlopen(url_medline) as url:
        file_data = url.read().decode('utf-8')
        return file_data


def populate_from_local_file():
    """

    :return:
    """

    filename = base_path + 'J_Medline.txt'
    with open(filename) as txt_file:
        if not path.exists(filename):
            return "journal info file not found"
        file_data = txt_file.read()
        txt_file.close()
        return file_data


if __name__ == "__main__":
    """
    call main start function
    """

    file_data = ''
    upload_to_s3 = False
    if args['input_url']:
        file_data = populate_from_url()
        logger.info("Processing input from url")
    elif args['input_localfile']:
        file_data = populate_from_local_file()
        logger.info("Processing input from local file")
    else:
        file_data = populate_from_url()
        logger.info("Processing input from url")

    if args['upload_s3']:
        upload_to_s3 = True
        logger.info("Upload file to s3")

    nlm_info = populate_nlm_info(file_data)
    generate_json(nlm_info, upload_to_s3)
