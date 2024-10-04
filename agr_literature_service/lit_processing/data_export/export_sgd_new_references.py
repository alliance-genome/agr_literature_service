import argparse
import logging
from sqlalchemy import text
from os import environ, makedirs, path
from dotenv import load_dotenv
from datetime import datetime, date

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.utils.db_read_utils import get_journal_by_resource_id,\
    get_all_reference_relation_data, get_mod_corpus_association_data_for_ref_ids, \
    get_cross_reference_data_for_ref_ids, get_author_data_for_ref_ids, \
    get_mesh_term_data_for_ref_ids, get_mod_reference_type_data_for_ref_ids
from agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json import \
    generate_json_file, upload_json_file_to_s3, get_meta_data, get_reference_col_names, \
    generate_json_data
from agr_literature_service.lit_processing.utils.report_utils import send_data_export_report
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

init_tmp_dir()

logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()

s3_bucket = 'agr-literature'
sub_bucket = 'develop/reference/dumps/'
latest_bucket = sub_bucket + 'latest/'
recent_bucket = sub_bucket + 'recent/'
monthly_bucket = sub_bucket + 'monthly/'
ondemand_bucket = sub_bucket + 'ondemand/'

mod = 'SGD'


def dump_data(email, ondemand, ui_root_url=None):  # noqa: C901  pragma: no cover

    json_file = "reference_new" + "_" + mod
    datestamp = str(date.today()).replace("-", "")
    if ondemand:
        # 2022-06-23 18:27:28.150889 => 20220623T182728
        datestamp = str(datetime.now()).replace('-', '').replace(':', '').replace(' ', 'T').split('.')[0]
        json_file = json_file + "_" + datestamp
    json_file = json_file + ".json"

    base_path = environ.get('XML_PATH', "")
    json_path = base_path + "json_data/"
    if not path.exists(json_path):
        makedirs(json_path)

    db_session = create_postgres_session(False)

    logger.info("Getting reference_relation data from the database...")

    reference_id_to_reference_relation_data = get_all_reference_relation_data(db_session)

    logger.info("Getting journal data from the database...")

    resource_id_to_journal = get_journal_by_resource_id(db_session)

    db_session.close()

    logger.info("Getting data from Reference table and generating json file...")
    try:
        status = get_reference_data_and_generate_json(
            mod, reference_id_to_reference_relation_data,
            resource_id_to_journal, json_path + json_file,
            datestamp)
        if not status:
            logger.info("No new papers!")
            return

    except Exception as e:
        error_msg = "Error occurred when retrieving data from Reference data and generating json file: " + str(e)
        logger.info(error_msg)
        if ondemand:
            send_data_export_report("ERROR", email, mod, error_msg)
        return

    logger.info("Uploading json file to s3...")
    filename = None
    try:
        filename = upload_json_file_to_s3(json_path, json_file, datestamp, ondemand)
    except Exception as e:
        error_msg = "Error occurred when uploading json file to s3: " + str(e)
        logger.info(error_msg)
        if ondemand:
            send_data_export_report("ERROR", email, mod, error_msg)
        return

    if ondemand:
        logger.info("Sending email...")
        ui_url = str(ui_root_url) + filename
        email_message = "The file " + filename + " is ready for <a href=" + ui_url + ">download</a>"
        send_data_export_report("SUCCESS", email, mod, email_message)

    logger.info("DONE!")


def get_reference_data_and_generate_json(mod, reference_id_to_reference_relation_data, resource_id_to_journal, json_file_with_path, datestamp):  # pragma: no cover

    metaData = get_meta_data(mod, datestamp)

    data = []

    db_session = create_postgres_session(False)

    rs = db_session.execute(text("SELECT mod_id FROM mod where abbreviation = '" + mod + "'"))
    rows = rs.fetchall()
    mod_id = rows[0][0]

    refColNmList = ", ".join(get_reference_col_names())
    rs = db_session.execute(text(f"SELECT {refColNmList} "
                                 f"FROM reference "
                                 f"WHERE reference_id IN "
                                 f"(select reference_id from mod_corpus_association "
                                 f"where mod_id = {mod_id} and corpus is True) "
                                 f"AND reference_id IN "
                                 f"(select reference_id from cross_reference "
                                 f"where curie_prefix = 'SGD' and curie like 'SGD:S1%' "
                                 f"and is_obsolete is False) "
                                 f"order by reference_id"))

    rows = rs.fetchall()

    if len(rows) == 0:
        return False

    reference_id_list = []
    for x in rows:
        reference_id_list.append(x[0])

    ref_ids = ", ".join([str(x) for x in reference_id_list])

    reference_id_to_xrefs = get_cross_reference_data_for_ref_ids(db_session, ref_ids)
    reference_id_to_authors = get_author_data_for_ref_ids(db_session, ref_ids)
    reference_id_to_mesh_terms = get_mesh_term_data_for_ref_ids(db_session, ref_ids)
    reference_id_to_mod_corpus_data = get_mod_corpus_association_data_for_ref_ids(db_session, ref_ids)
    reference_id_to_mod_reference_types = get_mod_reference_type_data_for_ref_ids(db_session, ref_ids)

    generate_json_data(rows, reference_id_to_xrefs, reference_id_to_authors, reference_id_to_reference_relation_data, reference_id_to_mod_reference_types, reference_id_to_mesh_terms, reference_id_to_mod_corpus_data, resource_id_to_journal, data)

    generate_json_file(metaData, data, json_file_with_path)

    db_session.close()

    return True


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    """
    parser.add_argument('-m', '--mod', action='store', type=str, help='MOD to dump',
                        choices=['SGD', 'WB', 'FB', 'ZFIN', 'MGI', 'RGD', 'XB'], required=True)
    """
    parser.add_argument('-e', '--email', action='store', type=str, help="Email address to send file")
    parser.add_argument('-o', '--ondemand', action='store_true', help="by curator's request")

    args = vars(parser.parse_args())
    dump_data(args['email'], args['ondemand'])
