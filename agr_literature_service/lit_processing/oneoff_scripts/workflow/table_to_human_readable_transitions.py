"""
Get transitions from table and print.

i.e.
   python3 table_to_file.py > new_filename

"""
import json
from fastapi_okta.okta_utils import get_authentication_token
import urllib.request
import argparse
import logging
from fastapi import HTTPException
from urllib.error import HTTPError

from starlette import status
from sqlalchemy import text
from sqlalchemy.orm import Session
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_engine, \
    create_postgres_session


logger = logging.getLogger(__name__)
name_to_atp = {}
atp_to_name = {}
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


def get_name_to_atp_and_children(token, curie='ATP:0000177'):
    """
    Add data to atp_to_name and name_to_atp dictionaries.
    From the top curie given go down all children and store the data.
    """
    global name_to_atp
    global atp_to_name

    ateam_api = f"https://beta-curation.alliancegenome.org/api/atpterm/{curie}/children"
    try:
        request = urllib.request.Request(url=ateam_api)
        request.add_header("Authorization", f"Bearer {token}")
        request.add_header("Content-type", "application/json")
        request.add_header("Accept", "application/json")
    except Exception as e:
        logger.error(f"Exception setting up request:get_nme_to_atp: {e}")
        return []
    try:
        with urllib.request.urlopen(request) as response:
            resp = response.read().decode("utf8")
            resp_obj = json.loads(resp)
            for bob in resp_obj:
                for jane in resp_obj[bob]:
                    name_to_atp[jane['name']] = jane['curie']
                    atp_to_name[jane['curie']] = jane['name']
                    get_name_to_atp_and_children(token=token, curie=jane['curie'])
    except HTTPError:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Error from A-team API")


def print_transitions(db: Session, comma_format, mod_only: str, debug: bool):  # noqa
    global atp_to_name
    global mod_abbrs

    try:
        query = r"""
        select mod_id as mod_id, transition_from, transition_to, requirements, transition_type, actions, condition
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
                print(f"'{mod_abbrs[tran['mod_id']]}', '{atp_to_name[tran['transition_from']]}', '{atp_to_name[tran['transition_to']]}', ",
                      f"'{tran['requirements']}', '{tran['actions']}', '{tran['condition']}'")
            else:
                print(f"""
        {start}'mod': "{mod_abbrs[tran['mod_id']]}",
               'from': "{atp_to_name[tran['transition_from']]}",
               'to': "{atp_to_name[tran['transition_to']]}",
               'requirements': "{tran['requirements']}",
               'actions': "{tran['actions']}",
               'condition': "{tran['condition']}",
               'transition_type': "{tran['transition_type']}"{end},""")
    except Exception as e:
        logger.error(e)
        print(f"Error: {e}")
        exit(-1)


if __name__ == "__main__":
    auth_token = get_authentication_token()
    if not auth_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)
    get_name_to_atp_and_children(token=auth_token)
    engine = create_postgres_engine(False)
    db_connection = engine.connect()
    db_session: Session = create_postgres_session(False)

    load_mod_abbr(db_session, args.debug)
    print_transitions(db_session, args.comma_seperated, args.mod_abbr, args.debug)
