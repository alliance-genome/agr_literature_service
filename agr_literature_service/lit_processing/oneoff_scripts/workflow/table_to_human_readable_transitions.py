"""
Get transitions from table and print.

i.e.
   python3 table_to_human_readable_transitions.py > new_filename

"""
import argparse
import logging
from sqlalchemy import text
from sqlalchemy.orm import Session
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_engine, \
    create_postgres_session
from agr_literature_service.api.crud.ateam_db_helpers import atp_get_name


logger = logging.getLogger(__name__)

mod_ids = {}
mod_abbrs = {}

helptext = r"example: python3 table_to_human_readable_transition -c -m FB"
parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, description=helptext)
# parser.add_argument('-h', '--help', help='run and see', default=False, type=bool, required=False)
parser.add_argument('-c', '--comma_seperated', help='comma seperated output', type=bool, default=False, required=False)
parser.add_argument('-m', '--mod_abbr', help='list transition for a specific mod', type=str, required=False, default="")
parser.add_argument('-d', '--debug', help='print bebug messages', type=bool, required=False, default=False)
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
            if mod_abbr != 'GO':
                mod_ids[mod_abbr] = mod_id
                mod_abbrs[mod_id] = mod_abbr
    except Exception as e:
        print('load_mod_abbr Error: ' + str(type(e)))


def print_transitions(db: Session, comma_format, mod_only: str, debug: bool):  # noqa
    global mod_abbrs

    try:
        query = r"""
        select workflow_transition_id, mod_id, transition_from, transition_to, requirements, transition_type, actions, condition
          from workflow_transition"""
        if mod_only:
            query += f" where mod_id = '{mod_ids[mod_only]}'"
        trans = db.execute(text(query)).mappings().fetchall()
        start = '{'
        end = '}'
        if debug:
            print(f"DEBUG: trans {trans}")
        for tran in trans:
            if debug:
                print(f"DEBUG: tran: {tran}")
            if comma_format:
                print(f"'{mod_abbrs[tran['mod_id']]}', '{atp_get_name(tran['transition_from'])}', '{atp_get_name(tran['transition_to'])}', ",
                      f"'{tran['requirements']}', '{tran['actions']}', '{tran['condition']}'")
            else:
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
        print(f"Error: {e}")
        exit(-1)


if __name__ == "__main__":
    engine = create_postgres_engine(False)
    db_connection = engine.connect()
    db_session: Session = create_postgres_session(False)

    load_mod_abbr(db_session, args.debug)
    print_transitions(db_session, args.comma_seperated, args.mod_abbr, args.debug)
