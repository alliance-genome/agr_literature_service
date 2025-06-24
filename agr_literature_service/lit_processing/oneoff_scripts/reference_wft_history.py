from sqlalchemy_continuum import Operation
import argparse
from sqlalchemy import text
from agr_literature_service.api.crud.ateam_db_helpers import atp_get_name

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session

# print(f"INSERT -> {Operation.INSERT}")
# print(f"UPDATE -> {Operation.UPDATE}")
# print(f"DELETE -> {Operation.DELETE}")


def dump_data(db_session, reference_id):
    """Get version data for this reference"""

    # First dump out the current status
    wft = 0
    created = 1
    updated = 2
    sql = f"""select workflow_tag_id, date_created, date_updated from workflow_tag where reference_id =  {reference_id}"""
    rs = db_session.execute(text(sql))
    print("Current values are:-")
    print("created\tupdated\tatp")
    for row in rs:
        print(f"{row[created]}\t{row[updated]} {atp_get_name(row[wft])}")

    print("\n\nData from version table")
    wft = 3
    wft_mod = 4
    created = 5
    updated = 6
    rfid = 7
    sql = """SELECT transaction_id, operation_type, end_transaction_id, workflow_tag_id, workflow_tag_id_mod, date_created, date_updated, reference_workflow_tag_id
              FROM workflow_tag_version
                WHERE reference_id = '{}'
                ORDER BY transaction_id
         """.format(reference_id)

    rs = db_session.execute(text(sql))
    for row in rs:
        op = '?'
        if row[1] == Operation.INSERT:
            op = "Insert"
        elif row[1] == Operation.UPDATE:
            op = "Update"
        elif row[1] == Operation.DELETE:
            op = "Delete"
        print(f"rwfid={row[rfid]}\tmodified={row[wft_mod]}\top={op}\t{row[created]}\t{row[updated]}\t{atp_get_name(row[wft])}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-r', '--reference_id', help='reference_id', type=str, required=True)
    args = parser.parse_args()
    db = create_postgres_session(False)
    dump_data(db, args.reference_id)
