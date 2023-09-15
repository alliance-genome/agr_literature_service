from botocore.exceptions import ClientError


def delete_file_in_bucket(s3_client, bucket, folder, object_name=None):
    """Delete a file in a S3 bucket
    :param bucket: Bucket to upload to
    :param folder: Folder to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    # Below commented out as file_obj does not exist
    # if object_name is None:
    #     object_name = file_obj

    # Delete the file
    objectKey = f"{folder}/{object_name}"
    try:
        s3_client.delete_object(Bucket=bucket,
                                Key=objectKey)
    except ClientError as e:
        print(f"Error while attempting to delete '{objectKey}' from S3 bucket {bucket}.")
        print(e)
        return False

    return True
