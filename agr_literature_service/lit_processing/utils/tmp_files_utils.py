from os import environ, makedirs


def init_tmp_dir():
    base_path = environ.get('XML_PATH', "")
    makedirs(base_path, exist_ok=True)
