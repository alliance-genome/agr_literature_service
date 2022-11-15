import logging
import gzip
import shutil
import boto3
from botocore.exceptions import ClientError
from os import environ, path, remove
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.md5sum_utils import \
    get_md5sum
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

pdf_urls_file = "data/pdf_file_urls.txt"
pdf_md5sum_file = "data/pdf_md5sum.txt"
download_dir = "download/"

SGD_AWS_ACCESS_KEY_ID = environ.get('SGD_AWS_ACCESS_KEY_ID', '')
SGD_AWS_SECRET_ACCESS_KEY = environ.get('SGD_AWS_SECRET_ACCESS_KEY', '')
S3_BUCKET = "agr-literature"
AWS_ACCESS_KEY_ID = environ.get('AWS_ACCESS_KEY_ID', '')
AWS_SECRET_ACCESS_KEY = environ.get('AWS_SECRET_ACCESS_KEY', '')


def transfer_pdfs():

    s3_client_from = boto3.client('s3',
                                  aws_access_key_id=SGD_AWS_ACCESS_KEY_ID,
                                  aws_secret_access_key=SGD_AWS_SECRET_ACCESS_KEY)

    s3_client_to = boto3.client('s3',
                                aws_access_key_id=AWS_ACCESS_KEY_ID,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    pdf_uploaded = {}
    fw = None

    if path.exists(pdf_md5sum_file):
        f = open(pdf_md5sum_file)
        for line in f:
            pieces = line.split("\t")
            pmid = pieces[0]
            pdf_uploaded[pmid] = 1
        f.close()
        fw = open(pdf_md5sum_file, "a")
    else:
        fw = open(pdf_md5sum_file, "w")

    f = open(pdf_urls_file)
    for line in f:
        s3_url = line.strip()
        pmid = s3_url.split("/")[-1].replace(".pdf", "")
        if pmid in pdf_uploaded:
            continue
        file_with_path = download_file_from_s3(s3_client_from, s3_url)
        if file_with_path is False:
            continue
        md5sum = get_md5sum(file_with_path)
        gzip_file_with_path = gzip_file(file_with_path)
        status = upload_file_to_s3(s3_client_to, gzip_file_with_path, md5sum)
        if status is True:
            fw.write(pmid + "\t" + md5sum + "\n")
            remove(gzip_file_with_path)
            remove(file_with_path)

    f.close()
    fw.close()


def gzip_file(file_with_path):

    gzip_file_with_path = file_with_path + ".gz"
    with open(file_with_path, 'rb') as f_in, gzip.open(gzip_file_with_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

    return gzip_file_with_path


def upload_file_to_s3(s3_client_to, gzip_file_with_path, md5sum):

    if environ.get('ENV_STATE') is None or environ.get('ENV_STATE') == 'test':
        return

    s3_file_path = "/reference/documents/"

    if environ.get('ENV_STATE') == 'prod':
        s3_file_path = 'prod' + s3_file_path
    else:
        s3_file_path = 'develop' + s3_file_path
    s3_file_path = s3_file_path + md5sum[0] + "/" + md5sum[1] + \
        "/" + md5sum[2] + "/" + md5sum[3] + "/"
    s3_file_location = s3_file_path + md5sum + ".gz"

    logger.info("Uploading " + gzip_file_with_path.split("/")[-1] + " to AGR s3: " + s3_file_location)

    try:
        response = s3_client_to.upload_file(gzip_file_with_path,
                                            S3_BUCKET,
                                            s3_file_location,
                                            ExtraArgs={'StorageClass': "STANDARD"})
        if response is not None:
            logger.info("boto 3 uploaded response: %s", response)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def download_file_from_s3(s3_client, s3_url):

    s3_url = s3_url.replace("s3://", '')
    s3_bucket = s3_url.split('/')[0]
    s3_file_location = s3_url.replace(s3_bucket + "/", '')

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

    transfer_pdfs()
