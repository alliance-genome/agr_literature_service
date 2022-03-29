# from os import environ
# import json
# import requests
import argparse
import logging
import logging.config
from os import path

from helper_file_processing import generate_cross_references_file

# pipenv run python get_datatypes_cross_references.py -d resource
# pipenv run python get_datatypes_cross_references.py -d reference

# about 1 minute 13 seconds to generate file with cross_references and is_obsolete
# about 45 seconds to generate file when it only had cross_references without is_obsolete
# generate reference_curie_to_xref file mapping alliance reference curies to cross_references identifiers from database


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('get_datatypes_cross_references')


if __name__ == "__main__":
    """
    This script generates bulk cross_reference data from the API and database.
    4 seconds for resource
    88 seconds for reference

    call main start function
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--datatype', action='store', help='take input from RESOURCE files in full path')

    args = vars(parser.parse_args())

    logger.info("starting get_datatypes_cross_references.py")

    if args['datatype']:
        generate_cross_references_file(args['datatype'])

    else:
        logger.info("No flag passed in.  Use -h for help.")

    logger.info("ending get_datatypes_cross_references.py")
