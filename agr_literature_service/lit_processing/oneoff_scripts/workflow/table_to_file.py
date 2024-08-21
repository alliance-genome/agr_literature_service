"""
Get transitions from table and print.

i.e.
   python3 table_to_file.py > new_filename

"""
import json
from fastapi_okta.okta_utils import get_authentication_token
import urllib.request
# import argparse
import logging
from fastapi import HTTPException
from urllib.error import HTTPError
from starlette import status
from sqlalchemy.orm import Session
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_engine, \
    create_postgres_session


logger = logging.getLogger(__name__)
name_to_atp = {}
atp_to_name = {}
mod_ids = {}
mod_abbrs = {}


def load_mod_abbr(db):
    global mod_ids, mod_abbrs
    try:
        mod_results = db.execute("select abbreviation, mod_id from mod")
        mods = mod_results.fetchall()
        for mod in mods:
            if mod["abbreviation"] != 'GO':
                mod_ids[mod["abbreviation"]] = mod["mod_id"]
                mod_abbrs[mod["mod_id"]] = mod["abbreviation"]
    except Exception as e:
        print('Error: ' + str(type(e)))


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


def get_transitions(db: Session, debug: bool = False):  # noqa
    global atp_to_name
    global mod_abbrs

    try:
        query = r"""
        select mod_id, transition_from, transition_to, requirements, transition_type, actions, condition
          from workflow_transition;"""
        trans_results = db.execute(query)
        trans = trans_results.fetchall()
        start = '{'
        end = '}'
        for tran in trans:
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
        exit(-1)


if __name__ == "__main__":
    auth_token = get_authentication_token()
    if not auth_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)
    get_name_to_atp_and_children(token=auth_token)
    engine = create_postgres_engine(False)
    db_connection = engine.connect()
    db_session: Session = create_postgres_session(False)

    load_mod_abbr(db_session)
    get_transitions(db_session)
