import logging
import tarfile
import boto3
from botocore.exceptions import ClientError
from os import environ, path
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

suppl_file_urls_file = "data/PMC_OA_file_urls.txt"
suppl_file_downloaded = "data/PMC_OA_files_downloaded.txt"

download_dir = "download_PMC/"

SGD_AWS_ACCESS_KEY_ID = environ.get('SGD_AWS_ACCESS_KEY_ID', '')
SGD_AWS_SECRET_ACCESS_KEY = environ.get('SGD_AWS_SECRET_ACCESS_KEY', '')


def download_suppl_files_from_sgd():

    s3_client_from = boto3.client('s3',
                                  aws_access_key_id=SGD_AWS_ACCESS_KEY_ID,
                                  aws_secret_access_key=SGD_AWS_SECRET_ACCESS_KEY)

    files_downloaded = {}
    fw = None

    if path.exists(suppl_file_downloaded):
        f = open(suppl_file_downloaded)
        for line in f:
            files_downloaded[line.strip()] = 1
        f.close()
        fw = open(suppl_file_downloaded, "a")
    else:
        fw = open(suppl_file_downloaded, "w")

    f = open(suppl_file_urls_file)
    for line in f:
        s3_url = line.strip()
        file_name = s3_url.split("/")[-1]
        if file_name in files_downloaded:
            continue
        pmid = s3_url.split("/")[-1].replace(".tar.gz", "")
        file_with_path = download_file_from_s3(s3_client_from, s3_url)
        if file_with_path is False:
            continue
        gunzip_file(file_with_path, pmid)
        fw.write(file_name + "\n")

    f.close()
    fw.close()


def gunzip_file(file_with_path, pmid):

    try:
        file = tarfile.open(file_with_path)
        file.extractall(download_dir + pmid + "/")
    except Exception as e:
        logger.error(e)


def download_file_from_s3(s3_client, s3_url):

    s3_url = s3_url.replace("https://", '')
    s3_bucket = s3_url.split('/')[0]
    s3_file_location = s3_url.replace(s3_bucket + "/", '')

    s3_bucket = s3_bucket.replace(".s3.amazonaws.com", '')

    file_name = s3_file_location.split('/')[-1]
    file_with_path = download_dir + file_name

    logger.info("Downloading " + file_name + " from SGD s3: " + s3_file_location)

    try:
        s3_client.download_file(s3_bucket,
                                s3_file_location,
                                file_with_path)
    except ClientError as e:
        logging.error(e)
        return False

    return file_with_path


if __name__ == "__main__":

    download_suppl_files_from_sgd()
