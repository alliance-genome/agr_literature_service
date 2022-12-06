from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ReferencefileModel
import logging
import sys


logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format='%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',
                    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def populate_data():
    # these are the correct values as validated by Kimberly on https://agr-jira.atlassian.net/browse/SCRUM-2165
    md5_fileclass = {
        "001caa5aec863a7e06bfa3106749d57c": "main",
        "001e47e27f62db3e4af9a1340454be8a": "main",
        "00271f9923ded3a0673778cb44a8df4d": "main",
        "08fa4e2991ccef8cebdacf054a1d814c": "main",
        "14a7b45c77172f5b30410c080f888d17": "main",
        "14ea07ae75de4dc0d82a761ace26013c": "main",
        "16c74945cbfcfca3a90ca505001f3028": "main",
        "1c1256fcc5ea4d952c7cfeeaacd6e09b": "main",
        "1fb363f66425d6a033f59da99965f599": "main",
        "23aa2175ed40227db24891c0f12c09c6": "supplement",
        "3f21b63ea99bff0b98ff838556d314f5": "main",
        "4451a6c237d5f689b0356b9691fd964f": "supplement",
        "485d08840cd5f93939081da0f5fc0420": "main",
        "4c3e7a5c4fc6b6aaa85a6b28d106b8b2": "main",
        "4db7a0e5c9667bc3d968a18beb29d6f7": "supplement",
        "570fa7e4f5d36252dd0854dc3497fb5f": "main",
        "59fdfbe84de25c2b21b270b24acbe310": "main",
        "5f6124a044449ec5600879bfc9977a97": "main",
        "7b41c2c2d825378e676dfd2febf82422": "main",
        "85a15562d6442b91e0f6764a64c87d46": "main",
        "8d3567cc04ee9abcfdd149ccbe40a677": "main",
        "91cd211fe411764c5e9da5ca60720f60": "main",
        "9419d5efc84a54de1a48d475b7b90436": "main",
        "a016502642828c36272f4becf9977556": "main",
        "a1987c0f0d760988bfac566af508ed9f": "main",
        "ab4fb8790cc893fe7e7573e1b5215b60": "main",
        "b23ca51c5b759a3ed64d56e158fcd199": "supplement",
        "b59b189f48b05b3a98b165523326216b": "main",
        "b63a9d1eeea2ec4cad7d2b0d5ed7bcb7": "main",
        "b6968c4a004e65eaa2a8f15ab052e9f5": "main",
        "b81e0ebb36187b1bca57ecfdb54424b1": "main",
        "b9827377a9ad1e6404d2f15976386dbc": "main",
        "bb84ef50c2531f69b91f7d70bf10d1f7": "supplement",
        "d648077f9d9292a18a0974875029400d": "main",
        "dbda61400f83688395edf5a068960541": "main",
        "e86ca92c3707be36af5210f6cec3db3b": "supplement",
        "ede5ff297fb732e8e3806507d861175b": "supplement"
    }
    return md5_fileclass


def find_data_mappings(db_session):
    md5_fileclass = populate_data()
    md5s = "','".join(md5_fileclass.keys())

    db_md5_fileclass = {}
    logger.info(f"{md5s}")
    db_result = db_session.execute(f"SELECT md5sum, file_class FROM public.referencefile WHERE md5sum IN ('{md5s}')")
    rows = db_result.fetchall()
    for row in rows:
        db_md5_fileclass[row[0]] = row[1]
    logger.info(f"{rows}")
    logger.info("md5\tcorrect\tdatabase\t")
    for md5, fileclass in md5_fileclass.items():
        if fileclass == db_md5_fileclass[md5]:
            logger.info(f"{md5}\t{fileclass}\t{db_md5_fileclass[md5]}\tis okay")
        else:
            logger.info(f"{md5}\t{fileclass}\t{db_md5_fileclass[md5]}\tis different")
            x = db_session.query(ReferencefileModel).filter_by(md5sum=md5).one_or_none()
            if x is None:
                return
            logger.info(f"UPDATE\t{x.md5sum} from {x.file_class} to {fileclass}")
            x.file_class = fileclass
    db_session.commit()


if __name__ == "__main__":
    db_session = create_postgres_session(False)
    find_data_mappings(db_session)
    db_session.close()
