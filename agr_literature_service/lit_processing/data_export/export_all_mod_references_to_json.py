import logging
import time
from os import environ, makedirs, path
from dotenv import load_dotenv
import shutil

from agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json import \
    dump_data
from agr_literature_service.lit_processing.utils.db_read_utils import get_mod_abbreviations

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def dump_all_data():

    for mod in get_mod_abbreviations():
        logger.info("Dumping json data for " + mod)
        try:
            dump_data(mod=mod, email=None, ondemand=False)
        except Exception as e:
            logger.info("Error occurred when dumping json data for " + mod + ": " + str(e))
        time.sleep(5)
    # dump papers for all mods
    # dump_data(mod=None, email=None, ondemand=False)
    # When pytest runs the code, it automatically sets PYTEST_CURRENT_TEST in os.environ
    if "PYTEST_CURRENT_TEST" not in environ:
        cleanup_temp_directory()


def cleanup_temp_directory():  # pragma: no cover
    load_dotenv()
    base_path = environ.get('XML_PATH', "")
    json_data_path = base_path + "json_data/"
    try:
        if path.exists(json_data_path):
            shutil.rmtree(json_data_path)
    except OSError as e:
        logger.info("Error deleting old json reference files: %s" % (e.strerror))
    makedirs(json_data_path)


if __name__ == "__main__":

    dump_all_data()
