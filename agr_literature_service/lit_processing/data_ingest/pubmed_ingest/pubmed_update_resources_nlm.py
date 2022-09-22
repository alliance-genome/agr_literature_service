import logging.config
import sys
from os import environ, makedirs, path

from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import load_pubmed_resource_basic
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.generate_pubmed_nlm_resource import (populate_from_url, populate_nlm_info,
                                                                                                          generate_json)
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils import save_resource_file
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import sqlalchemy_load_ref_xref
from agr_literature_service.lit_processing.data_ingest.post_resource_to_db import post_resources
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format= '%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

init_tmp_dir()


def update_resource_pubmed_nlm(set_user=None):
    """
    download J_Medline file, convert to json, compare to existing resources, post new ones to api and database
    """

    if set_user:
        db_session = create_postgres_session(False)
        scriptNm = path.basename(__file__).replace(".py", "")
        set_global_user_id(db_session, scriptNm)
        db_session.close()

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
    if not path.exists(json_storage_path):
        makedirs(json_storage_path)
    save_resource_file(json_storage_path, resources_to_create, 'NLM')
    post_resources('sanitized_resource_json', 'NLM')


if __name__ == "__main__":
    """
    process nlm updates from medline to database
    """

    logger.info("start update_resource_pubmed_nlm")
    set_user = 1
    update_resource_pubmed_nlm(set_user)
    logger.info("end update_resource_pubmed_nlm")
