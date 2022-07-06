from botocore.exceptions import ClientError
from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from os import environ, getcwd
from agr_literature_service.lit_processing.helper_s3 import download_file_from_s3
from fastapi.responses import FileResponse


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


def get_json_file_from_s3(mod):

    env_state = environ.get('ENV_STATE', 'develop')
    if env_state == 'build':
        env_state = 'develop'
    bucketname = environ.get('BUCKET_NAME', 'agr-literature')
    json_file = 'reference_' + mod + '.json'
    s3_file_location = env_state + '/reference/dumps/latest/' + json_file

    try:
        # not sure where to put the file downloaded from s3 in the docker container
        # so just put it under current directory
        # should we set a place outside docker container to store these files?
        # so we can easily clean the directoty up (by cronjob or other method)
        download_file_from_s3(json_file, bucketname, s3_file_location)
    except Exception as e:
        raise HTTPException(status_code=e.response['Error']['Code'],
                            detail=f"Error occurred when retrieving the json file from s3 {s3_file_location}")

    file_path = getcwd() + "/" + json_file
    return FileResponse(path=file_path, filename=json_file, media_type='application/json')
