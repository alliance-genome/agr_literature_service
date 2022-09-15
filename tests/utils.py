import shutil
from os import environ


def cleanup_tmp_files():
    base_path = environ.get('XML_PATH')
    shutil.rmtree(base_path)
