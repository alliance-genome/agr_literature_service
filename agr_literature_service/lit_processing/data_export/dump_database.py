import argparse
import logging
from os import environ
from dotenv import load_dotenv
from agr_literature_service.lit_processing.utils.s3_utils import upload_file_to_s3, delete_file_from_s3, file_exist_from_s3
from datetime import timedelta, date
import os

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


def dump_database(dump_type="ondemand"):  # noqa: C901
    env_state = environ.get('ENV_STATE', "")
    if dump_type == 'cron' and env_state != 'prod':
        log.info("dump for production database only")
        return False
    database = environ.get('PSQL_DATABASE', "")
    host = environ.get('PSQL_HOST', "")
    username = environ.get('PSQL_USERNAME', "")
    password = environ.get('PSQL_PASSWORD', "")
    port = environ.get('PSQL_PORT', "")
    file_name_date_7 = database + "_" + str(date.today() - timedelta(days=7)) + ".sql.gz"
    file_name = database + "_" + str(date.today()) + ".sql.gz"
    cmd = "PGPASSWORD=" + password + " pg_dump -Fc --clean -h " + host + " -p " + port + " -U " + username + " " + database + "  | gzip > " + file_name
    log.info(cmd)
    os.system(cmd)
    if dump_type == 'cron':
        s3_filename = lastweek_bucket + file_name
        upload_file_to_s3(file_name, s3_bucket, s3_filename)
        ## upload file to monthly bucket if it is first day of the month
        todayDate = date.today()
        if todayDate.day == 1:
            s3_filename_monthly = monthly_bucket + file_name
            upload_file_to_s3(file_name, s3_bucket, s3_filename_monthly, 'GLACIER_IR')
        # delete local file after upload to s3
        try:
            os.remove(file_name)
        except OSError:
            log.info("fail to delete local file")
            return False

        ## delete day7 file from lastweek folder
        s3_filename_day7 = lastweek_bucket + file_name_date_7
        file_date_7_exist = file_exist_from_s3(s3_bucket, s3_filename_day7)
        if file_date_7_exist:
            delete_file_from_s3(s3_bucket, s3_filename_day7)

    return True


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--type', action='store', type=str, help="either cron or ondemand dump",
                        choices=['cron', 'ondemand'], required=True)
    args = vars(parser.parse_args())
    dump_database(args['type'])
