
import logging
import logging.config
import sys
from os import environ

from generate_pubmed_nlm_resource import (populate_from_url, populate_nlm_info,
                                          generate_json)
from helper_file_processing import load_pubmed_resource_basic
from parse_dqm_json_resource import (save_resource_file, create_storage_path)
from helper_sqlalchemy import sqlalchemy_load_ref_xref
from post_resource_to_api import post_resources


logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format= '%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def update_resource_pubmed_nlm():
    """
    download J_Medline file, convert to json, compare to existing resources, post new ones to api and database
    """

    upload_to_s3 = True
    file_data = populate_from_url()
    nlm_info = populate_nlm_info(file_data)
    generate_json(nlm_info, upload_to_s3)
    pubmed_by_nlm, nlm_by_issn = load_pubmed_resource_basic()

    xref_ref, ref_xref_valid, ref_xref_obsolete = sqlalchemy_load_ref_xref('resource')

    resources_to_create = dict()

    for nlm in pubmed_by_nlm:
        if 'NLM' in xref_ref and nlm in xref_ref['NLM'] and xref_ref['NLM'][nlm] is not None:
            logger.info(f"{nlm} already {xref_ref['NLM'][nlm]}")
        else:
            logger.info(f"create {nlm}")
            resources_to_create[nlm] = pubmed_by_nlm[nlm]

    base_path = environ.get('XML_PATH', "")
    json_storage_path = base_path + 'sanitized_resource_json/'
    create_storage_path(json_storage_path)
    save_resource_file(json_storage_path, resources_to_create, 'NLM')
    post_resources('sanitized_resource_json', 'NLM')


if __name__ == "__main__":
    """
    process nlm updates from medline to database
    """

    logger.info("start update_resource_pubmed_nlm")
    update_resource_pubmed_nlm()
    logger.info("end update_resource_pubmed_nlm")
