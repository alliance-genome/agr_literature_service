import io
from botocore.exceptions import ClientError
from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder

def download_file_from_bucket(s3_client, bucket, folder, object_name=None):
    """Upload a file to an S3 bucket
    :param bucket: Bucket to upload to
    :param folder: Folder to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else file
    """
    # download the file
    try:
        object = s3_client.get_object(Bucket=bucket,
                                      Key=f'{folder}/{object_name}')
        response = object['Body']
        return response
    except ClientError as e:
        print(e)
        raise HTTPException(status_code=e.response['Error']['Code'],
                            detail=jsonable_encoder(e))
