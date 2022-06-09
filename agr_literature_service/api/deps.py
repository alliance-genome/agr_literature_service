import boto3
from botocore.client import BaseClient

from literature.config import config


def s3_auth() -> BaseClient:
    """

    :return:
    """

    s3 = boto3.client(service_name='s3', aws_access_key_id=config.AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
                      )

    return s3
