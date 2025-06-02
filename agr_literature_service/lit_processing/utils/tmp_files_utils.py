import shutil
import logging
from os import environ, makedirs, path

logger = logging.getLogger(__name__)


def init_tmp_dir():
    base_path = environ.get('XML_PATH', "")
    makedirs(base_path, exist_ok=True)


def cleanup_temp_directory(tmp_dir):
    base_path = environ.get("XML_PATH", "")
    tmp_data_path = path.join(base_path, tmp_dir)
    try:
        if path.exists(tmp_data_path):
            shutil.rmtree(tmp_data_path)
    except OSError as e:
        logger.info("Error deleting tmp files under %s: %s", tmp_dir, e.strerror)

    # Recreate the directory (and any missing parents)
    makedirs(tmp_data_path, exist_ok=True)
