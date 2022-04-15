
# needs AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in environment.


from os import environ
# from os import path
import sys
import logging
import logging.config
import boto3
from botocore.exceptions import ClientError

from dotenv import load_dotenv

load_dotenv()

# log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
# logging.config.fileConfig(log_file_path)
# logger = logging.getLogger('literature logger')
# logging.getLogger("s3transfer.utils").setLevel(logging.WARNING)
# logging.getLogger("s3transfer.tasks").setLevel(logging.WARNING)
# logging.getLogger("s3transfer.futures").setLevel(logging.WARNING)

logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format= '%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
logging.getLogger("s3transfer.utils").setLevel(logging.WARNING)
logging.getLogger("s3transfer.tasks").setLevel(logging.WARNING)
logging.getLogger("s3transfer.futures").setLevel(logging.WARNING)


def upload_file_to_s3(filepath, bucketname, s3_file_location):
    """

    :param filepath:
    :param bucketname:
    :param s3_file_location:
    :return:
    """

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(filepath, bucketname, s3_file_location)
        if response is not None:
            logger.info("boto 3 uploaded response: %s", response)
        # else:
        #     logger.info("uploaded to s3 %s %s", bucketname, filepath)
    except ClientError as e:
        logging.error(e)
        return False

    return True


def upload_xml_file_to_s3(pmid):
    base_path = environ.get('XML_PATH')
    env_state = environ.get('ENV_STATE', 'develop')
    if env_state == 'build':
        env_state = 'develop'
    bucketname = 'agr-literature'
    xml_filename = pmid + '.xml'
    local_file_location = base_path + 'pubmed_xml/' + xml_filename
    s3_file_location = env_state + '/reference/metadata/pubmed/xml/original/' + xml_filename
    upload_file_to_s3(local_file_location, bucketname, s3_file_location)
