import logging
import string
import boto3
# from botocore.exceptions import ClientError
from os import environ
from typing import Set, Dict
from sqlalchemy import text

# remove
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.utils.report_utils import send_report
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET = "agr-literature"
AWS_ACCESS_KEY_ID = environ.get('AWS_ACCESS_KEY_ID', '')
AWS_SECRET_ACCESS_KEY = environ.get('AWS_SECRET_ACCESS_KEY', '')


def compare_s3_files():

    db_md5sum = set()    # type: Set
    s3_md5sum = set()    # type: Set
    s3_md5sum_dict = dict()  # type: Dict

    db_session = create_postgres_session(False)
    rs = db_session.execute(text("SELECT md5sum FROM referencefile"))
    rows = rs.fetchall()
    for x in rows:
        db_md5sum.add(x[0])

    # chars is array of : 0-9 + a-f
    chars = list(string.hexdigits[:16])

    # smaller test set
    # base_prefix = 'prod/reference/documents/0/0/'
    # s3_client = boto3.client('s3',
    #                          aws_access_key_id=AWS_ACCESS_KEY_ID,
    #                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    # for char1 in chars:
    #     for char2 in chars:
    #         prefix = base_prefix + char1 + '/' + char2 + '/'
    #         objects = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    #         if 'Contents' in objects:
    #             for obj in objects['Contents']:
    #                 s3_md5sum.add(obj['Key'].replace(prefix, '').replace('.gz', ''))

    # 3054497 in prod db referencefile
    # 3072357 in prod s3.  takes 32 minutes to read
    base_prefix = 'prod/reference/documents/'
    s3_client = boto3.client('s3',
                             aws_access_key_id=AWS_ACCESS_KEY_ID,
                             aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    for char1 in chars:
        for char2 in chars:
            for char3 in chars:
                for char4 in chars:
                    prefix = base_prefix + char1 + '/' + char2 + '/' + char3 + '/' + char4 + '/'
                    objects = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
                    if 'Contents' in objects:
                        for obj in objects['Contents']:
                            name = obj['Key'].replace(prefix, '').replace('.gz', '')
                            s3_md5sum.add(name)
                            s3_md5sum_dict[name] = {'size': obj['Size'], 'date': obj['LastModified']}

    email_message = ''
    for md5sum in s3_md5sum:
        if md5sum not in db_md5sum:
            email_message = email_message + "date " + s3_md5sum_dict[md5sum]['date'] + " : " + md5sum + " in s3 not in db, size " + s3_md5sum_dict[md5sum]['size'] + "<br/>"
            # print(f"date {s3_md5sum_dict[md5sum]['date']} : {md5sum} in s3 not in db, size {s3_md5sum_dict[md5sum]['size']}")

    for md5sum in db_md5sum:
        if md5sum not in s3_md5sum:
            email_message = email_message + md5sum + " in db not in s3<br/>"
            # print(f"{md5sum} in db not in s3")

    email_subject = 's3 files differ from rdsprod literature'
    if email_message != '':
        send_report(email_subject, email_message)

# this might list everything, but there's too much, takes too long
#     s3r = boto3.resource('s3',
#                          aws_access_key_id=AWS_ACCESS_KEY_ID,
#                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
#     bucket = s3r.Bucket(S3_BUCKET)
#     files_in_bucket = list(bucket.objects.all())
#     for obj in files_in_bucket:
#         print(obj)

# this lists only 1000
#     s3_client = boto3.client('s3',
#                              aws_access_key_id=AWS_ACCESS_KEY_ID,
#                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
#     objects = s3_client.list_objects_v2(Bucket=S3_BUCKET)
#     for obj in objects['Contents']:
#         print(obj['Key'])


if __name__ == "__main__":

    compare_s3_files()
