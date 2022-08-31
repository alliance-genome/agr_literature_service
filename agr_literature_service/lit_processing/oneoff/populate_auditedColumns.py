from agr_literature_service.lit_processing.helper_sqlalchemy import create_postgres_engine
import logging
import sys

logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format= '%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def populate_audited_columns():

    engine = create_postgres_engine(False)
    db_connection = engine.connect()

    # table author data 5002229 version 5117199
    # table editor data 3595 version 3595
    # table mod_corpus_association data 943117 version 63329 - something went wrong that we don't have _version data
    # table mod data 8 version 8
    # table reference data 904936 version 1746920
    # table resource data 43025 version 43025

    # audited_tables = ['author', 'editor', 'mod_corpus_association', 'mod', 'reference', 'resource', 'topic_entity_tag', 'topic_entity_tag_prop', 'workflow_tag']
    audited_tables_with_data = ['author', 'editor', 'mod_corpus_association', 'mod', 'reference', 'resource']

    version_count = {}
    data_count = {}
    for table_name in audited_tables_with_data:
        # logger.info(f"looking at {table_name}")
        rs = db_connection.execute(f"SELECT COUNT(*) FROM {table_name}_version")
        rows = rs.fetchall()
        # if rows[0][0] > 0:
        #     logger.info(f"table {table_name} has {rows[0][0]} entries")
        version_count[table_name] = rows[0][0]
        rs = db_connection.execute(f"SELECT COUNT(*) FROM {table_name}")
        rows = rs.fetchall()
        data_count[table_name] = rows[0][0]
        logger.info(f"table {table_name} data {data_count[table_name]} version {version_count[table_name]}")

    # juancarlos set the mods
    uid_juancarlos = '00u1ctzvjgMpk87Qm5d7'
    rs = db_connection.execute(f"UPDATE mod SET created_by = '{uid_juancarlos}' WHERE created_by IS NULL")
    rs = db_connection.execute(f"UPDATE mod SET updated_by = '{uid_juancarlos}' WHERE updated_by IS NULL")

    # ceri set some cross_reference entries, but we don't track audited object for them
    # ceri created some mod_corpus_association entries with these transaction ids  911180, 911184, 946252, 1025540, 1025541
    # these correspond to these mod_corpus_association_id [888330, 888331, 898738, 936132, 936694]
    # SELECT * from mod_corpus_association_version where mod_corpus_association_id IN (888330, 888331, 898738, 936132, 936694) ORDER BY mod_corpus_association_id
    # they are all operation_type 1 (update).  all transactions on mod_corpus_association_version are 1, none are 0 (create) nor 2 (delete).
    # the date_created date_updated all seem tied to the API time, not to the transaction time.
    # some mod_corpus_association_id from Ceri's changes have other changes, but those are not attributed to Ceri.  probably easiest to set everyone to okta cid.  should we try to fix the date_created / date_updated ?
    # uid_ceri = '00u1mhf3mf28xjpPt5d7'

    # this code finds which mod_corpus_association_id have been modified at some point by Ceri
    # mca_ids = []
    # rs = db_connection.execute(f"SELECT mod_corpus_association_id FROM mod_corpus_association_version WHERE transaction_id IN (911180, 911184, 946252, 1025540, 1025541)")
    # rows = rs.fetchall()
    # for x in rows:
    #     mca_ids.append(x[0])
    # logger.info(mca_ids)

    # date processing
    # these are fine
    # mca only has updates.  date created/updated probably from time api started.
    # author has date created + date updated
    # editor has date created + date updated
    # reference has date created + date updated

    # resource has date created only.  but there's exactly 9 timestamps for date_created for all 43025 resources
    resource_dates = []
    rs = db_connection.execute("SELECT DISTINCT(date_created) FROM public.resource WHERE date_updated IS NULL")
    rows = rs.fetchall()
    for x in rows:
        resource_dates.append(x[0])
    for res_date in resource_dates:
        logger.info(f"UPDATE public.resource SET date_updated = '{res_date}' WHERE date_updated IS NULL AND date_created = '{res_date}'")
        rs = db_connection.execute(f"UPDATE public.resource SET date_updated = '{res_date}' WHERE date_updated IS NULL AND date_created = '{res_date}'")

    # user processing
    okta_client_id = '0oa1cs2ineBqEFiD85d7'
    audited_tables_need_user = ['author', 'editor', 'mod_corpus_association', 'reference', 'resource']
    for table_name in audited_tables_need_user:
        logger.info(f"updating user for {table_name}")
        # rs = db_connection.execute(f"SELECT COUNT(*) FROM {table_name} WHERE updated_by IS NOT NULL")
        # rows = rs.fetchall()
        # if rows[0][0] > 0:
        #     logger.info(f"table {table_name} has {rows[0][0]} updated_by entries")
        # rs = db_connection.execute(f"SELECT COUNT(*) FROM {table_name} WHERE created_by IS NOT NULL")
        # rows = rs.fetchall()
        # if rows[0][0] > 0:
        #     logger.info(f"table {table_name} has {rows[0][0]} created_by entries")
        logger.info(f"UPDATE {table_name} SET created_by = '{okta_client_id}' WHERE created_by IS NULL")
        rs = db_connection.execute(f"UPDATE {table_name} SET created_by = '{okta_client_id}' WHERE created_by IS NULL")
        logger.info(f"UPDATE {table_name} SET updated_by = '{okta_client_id}' WHERE updated_by IS NULL")
        rs = db_connection.execute(f"UPDATE {table_name} SET updated_by = '{okta_client_id}' WHERE updated_by IS NULL")


if __name__ == "__main__":

    populate_audited_columns()
