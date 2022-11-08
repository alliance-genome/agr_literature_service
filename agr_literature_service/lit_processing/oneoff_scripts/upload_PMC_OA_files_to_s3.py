import logging
import gzip
import shutil
import boto3
from botocore.exceptions import ClientError
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.utils.md5sum_utils import \
    get_md5sum
from os import environ, path, listdir
# remove
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET = "agr-literature"
AWS_ACCESS_KEY_ID = environ.get('AWS_ACCESS_KEY_ID', '')
AWS_SECRET_ACCESS_KEY = environ.get('AWS_SECRET_ACCESS_KEY', '')

suppl_file_uploaded = "data/PMC_OA_files_uploaded.txt"
download_dir = "download_PMC/"


def upload_suppl_files():

    s3_client_to = boto3.client('s3',
                                aws_access_key_id=AWS_ACCESS_KEY_ID,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    files_uploaded = {}
    fw = None

    if path.exists(suppl_file_uploaded):
        f = open(suppl_file_uploaded)
        for line in f:
            pmid = line.split("\t")[0]
            files_uploaded[pmid] = 1
        f.close()
        fw = open(suppl_file_uploaded, "a")
    else:
        fw = open(suppl_file_uploaded, "w")

    # download_PMC/9971735/PMC2132911
    # eg, under download_PMC/
    for file_dir in listdir(download_dir):
        pmid = file_dir.strip()
        if pmid in files_uploaded:
            continue
        pmid_dir = path.join(download_dir, pmid)
        # eg, under download_PMC/9971735/
        for pmcid in listdir(pmid_dir):
            sub_dir = path.join(download_dir, pmid, pmcid)
            # eg, under download_PMC/9971735/PMC2132911/
            for file_name in listdir(sub_dir):
                file_with_path = path.join(sub_dir, file_name)
                md5sum = get_md5sum(file_with_path)
                gzip_file_with_path = None
                if file_with_path.endswith('.gz'):
                    gzip_file_with_path = file_with_path
                else:
                    gzip_file_with_path = gzip_file(file_with_path)
                if gzip_file_with_path is None:
                    continue
                status = upload_file_to_s3(s3_client_to, gzip_file_with_path, md5sum)
                if status is True:
                    fw.write(pmid + "\t" + pmcid + "\t" + file_name + "\t" + md5sum + "\n")
                    # remove(gzip_file_with_path)
                    # remove(file_with_path)
    fw.close()


def gzip_file(file_with_path):

    gzip_file_with_path = None
    try:
        gzip_file_with_path = file_with_path + ".gz"
        with open(file_with_path, 'rb') as f_in, gzip.open(gzip_file_with_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    except Exception as e:
        logging.error(e)
        gzip_file_with_path = None

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


if __name__ == "__main__":

    upload_suppl_files()
