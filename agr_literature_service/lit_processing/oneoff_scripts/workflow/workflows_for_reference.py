"""
Get transitions from table and print.

i.e.
   python3 table_to_human_readable_transitions.py > new_filename

"""
import logging
import argparse
from sqlalchemy import text
from sqlalchemy.orm import Session
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_engine, \
    create_postgres_session
from agr_literature_service.api.crud.ateam_db_helpers import atp_get_name


logger = logging.getLogger(__name__)

helptext = r"example: python3 workflows_for_reference -m FB"
parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, description=helptext)
parser.add_argument('-m', '--mod_abbr', help='specific mod', type=str, required=True, default=None)
parser.add_argument('-r', '--reference', help='reference to display', type=str, required=True, default=None)
args = parser.parse_args()

def print_data(db: Session, reference, mod_abbr):  # noqa
    try:
        query = f"""
        select wt.workflow_tag_id, wt.date_created
          from reference r, mod m, workflow_tag wt
          where wt.reference_id = r.reference_id
                and wt.mod_id = m.mod_id
                and wt.reference_id = r.reference_id
                and m.abbreviation = '{mod_abbr}'
                and r.curie = '{reference}'
                order by date_created desc"""
        wts = db.execute(text(query)).mappings().fetchall()
        for wt in wts:
            print(f"{wt['date_created']}\t{atp_get_name(wt['workflow_tag_id'])}")

    except Exception as e:
        logger.error(e)
        print(f"Error: {e}")
        exit(-1)


if __name__ == "__main__":
    engine = create_postgres_engine(False)
    db_connection = engine.connect()
    db_session: Session = create_postgres_session(False)

    print_data(db_session, args.reference, args.mod_abbr)
