import argparse
import logging
import json
from os import environ, makedirs, path, rename, remove
from dotenv import load_dotenv
from datetime import datetime, date
from agr_literature_service.lit_processing.utils.s3_utils import upload_file_to_s3, download_file_from_s3, delete_file_from_s3, file_exist_from_s3
from datetime import datetime, timedelta
import os
# init_tmp_dir()

logging.basicConfig(format='%(message)s')
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

load_dotenv()

s3_bucket = 'agr-literature'
sub_bucket = 'prod/database_dump/'
latest_bucket = sub_bucket + 'latest/'
lastweek_bucket = sub_bucket + 'last_week/'
monthly_bucket = sub_bucket + 'monthly_archive/'
ondemand_bucket = sub_bucket + 'ondemand/'

def dump_database(dump_type ="ondemand"):  # noqa: C901
    database = environ.get('PSQL_DATABASE', "")
    host = environ.get('PSQL_HOST', "")
    username = environ.get('PSQL_USERNAME', "")
    password = environ.get('PSQL_PASSWORD', "")
    port = environ.get('PSQL_PORT', "")
    # file_name_date_7 = database + "_" + str(date.today() - timedelta(days=7)) + "_.sql"
    file_name_date_7 = database + "_" + str(date.today()) + "_.sql"
    file_name = database + "_" + str(date.today()) + "_.sql"
    cmd = "PGPASSWORD=" + password + " pg_dump -Fc --clean -h " + host + " -p " + port + " -U " + username + " " + database + "  > " + file_name
    log.info(cmd)
    os.system(cmd)
    if dump_type == 'cron':
        s3_filename = lastweek_bucket + file_name
        upload_file_to_s3(file_name, s3_bucket, s3_filename)
        # delete local file after upload to s3
        try:
            os.remove(file_name)
        except OSError:
            log.info("fail to delete local file")
            return False

        # put the file older than 7 days into monthly if exists
        file_date_7_exist = file_exist_from_s3(file_name_date_7)
        if file_date_7_exist:
            s3_filename_day7 = lastweek_bucket + file_name_date_7
            file_name_downloaded = database + "_" + str(date.today()) + "_downloaded_.sql"
            log.info(s3_filename_day7)
            downloaded = download_file_from_s3(file_name_downloaded, s3_bucket, s3_filename_day7)
            if downloaded:
                s3_filename_monthly = monthly_bucket + file_name_downloaded
                upload_file_to_s3(file_name_downloaded, s3_bucket, s3_filename_monthly, 'DEEP_ARCHIVE')
                delete_file_from_s3(s3_bucket, s3_filename_day7)
                try:
                    os.remove(file_name_downloaded)
                except OSError:
                    return False
    return True


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--type', action='store', type=str, help="either cron or ondemand dump",
                        choices=['cron', 'ondemand'], required=True)
    args = vars(parser.parse_args())
    dump_database(args['type'])