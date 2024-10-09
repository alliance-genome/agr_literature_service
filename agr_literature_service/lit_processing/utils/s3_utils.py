# needs AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in environment.


from os import environ, path
import sys
import logging
import logging.config
import boto3  # type: ignore
from botocore.exceptions import ClientError  # type: ignore

from dotenv import load_dotenv
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

load_dotenv()
init_tmp_dir()

logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format= '%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
logging.getLogger("s3transfer.utils").setLevel(logging.WARNING)
logging.getLogger("s3transfer.tasks").setLevel(logging.WARNING)
logging.getLogger("s3transfer.futures").setLevel(logging.WARNING)


def file_exist_from_s3(bucketname, s3_file_location):
    """

    :param bucketname:
    :param s3_file_location:
    :return: True/False
    """

    s3_client = boto3.client('s3')
    try:
        s3_client.head_object(Bucket=bucketname, Key=s3_file_location)
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            logging.info("file does not exists")
            return False
        else:
            logging.error(e)
            return False
    return True


def download_file_from_s3(filepath, bucketname, s3_file_location):
    """

    :param filepath:
    :param bucketname:
    :param s3_file_location:
    :return:
    """

    s3_client = boto3.client('s3')
    try:
        response = s3_client.download_file(bucketname, s3_file_location, filepath)
        if response is not None:
            logger.info("boto 3 downloaded response: %s", response)
    except ClientError as e:
        logging.error(e)
        return False

    return True


def upload_file_to_s3(filepath, bucketname, s3_file_location, storage_class='STANDARD'):
    """

    :param filepath: local path to filename to upload
    :param bucketname: s3 bucket to upload to
    :param s3_file_location: s3 object name
    :param storage_class: s3 storage class, STANDARD for default, GLACIER_IR for glacier instand retrieval
    :return: True if file was uploaded, else False
    """

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(filepath, bucketname, s3_file_location, ExtraArgs={'StorageClass': storage_class})
        if response is not None:
            logger.info("boto 3 uploaded response: %s", response)
    except ClientError as e:
        logging.error(e)
        return False

    return True


def upload_xml_file_to_s3(pmid, subDir=None):
    base_path = environ.get('XML_PATH')
    env_state = environ.get('ENV_STATE', 'develop')
    if env_state == 'build':
        env_state = 'develop'
    if env_state != 'test':
        bucketname = 'agr-literature'
        xml_filename = pmid + '.xml'
        local_file_location = base_path + 'pubmed_xml/' + xml_filename
        if not path.exists(local_file_location):
            return
        if subDir is None:
            s3_file_location = env_state + '/reference/metadata/pubmed/xml/original/' + xml_filename
        else:
            # eg subDir = 'latest'
            s3_file_location = env_state + '/reference/metadata/pubmed/xml/' + subDir + '/' + xml_filename
        upload_file_to_s3(local_file_location, bucketname, s3_file_location, 'GLACIER_IR')
