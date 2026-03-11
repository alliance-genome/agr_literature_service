"""
Get transitions from table and print.

i.e.
   python3 table_to_file.py > new_filename

"""
import logging
import argparse
from sqlalchemy import text
from sqlalchemy.orm import Session
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_engine, \
    create_postgres_session
from agr_literature_service.api.crud.ateam_db_helpers import atp_get_name


logger = logging.getLogger(__name__)
mod_ids = {}
mod_abbrs = {}

helptext = r"example: python3 table_to_file -m FB"
parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, description=helptext)
parser.add_argument('-m', '--mod_abbr', help='list transition for a specific mod', type=str, required=False, default=None)
args = parser.parse_args()


def load_mod_abbr(db, debug):
    global mod_ids, mod_abbrs
    try:
        mod_results = db.execute(text("select abbreviation, mod_id from mod"))
        mods = mod_results.fetchall()
        if debug:
            print(f"DEBUG: mods {mods}")
        for mod_abbr, mod_id in mods:
            if debug:
                print(f"DEBUG: mod abbr {mod_abbr}")
            if mod_abbr not in ['GO', 'alliance']:
                mod_ids[mod_abbr] = mod_id
                mod_abbrs[mod_id] = mod_abbr
    except Exception as e:
        print('load_mod_abbr Error: ' + str(type(e)))


def get_transitions(db: Session, mod_abbr):  # noqa
    global mod_abbrs
    try:
        query = r"""
        select workflow_transition_id, mod_id, transition_from, transition_to, requirements, transition_type, actions, condition
          from workflow_transition"""
        if mod_abbr:
            query += f" where mod_abbreviation = '{mod_abbr}'"
        trans = db.execute(text(query)).mappings().fetchall()
        start = '{'
        end = '}'
        for tran in trans:
            print(f"""
        {start}'id': "{tran['workflow_transition_id']}",
               'mod': "{mod_abbrs[tran['mod_id']]}",
               'atpfrom':  "{tran['transition_from']}",
               'atpto': "{tran['transition_to']}",
               'from': "{atp_get_name(tran['transition_from'])}",
               'to': "{atp_get_name(tran['transition_to'])}",
               'requirements': "{tran['requirements']}",
               'actions': "{tran['actions']}",
               'condition': "{tran['condition']}",
               'transition_type': "{tran['transition_type']}"{end},""")
    except Exception as e:
        logger.error(e)
        exit(-1)


if __name__ == "__main__":
    engine = create_postgres_engine(False)
    db_connection = engine.connect()
    db_session: Session = create_postgres_session(False)

    load_mod_abbr(db_session, False)
    get_transitions(db_session, args.mod_abbr)
