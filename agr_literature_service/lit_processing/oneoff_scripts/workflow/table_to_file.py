"""
Add transitions.

i.e.
   python3 -d transitions_add.py -f file_upload -v 1

   data files (methods that return the data to insert into the table) are in the
   directory 'data'.
   If new transitions are added then update existing files or create a new file and
   import it here (See # Data files) and add an elif (see # Add new data files here with appropriate elif)

    get_name_to_atp_and_children method maybe useful outside the workflow module,
    so we may want to move this into the api at some point.
"""
import json
from fastapi_okta.okta_utils import get_authentication_token
import urllib.request
import argparse
import logging
from fastapi import HTTPException
from urllib.error import HTTPError
from starlette import status
from sqlalchemy.orm import Session
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_engine, \
    create_postgres_session

from agr_literature_service.api.models import WorkflowTransitionModel

# Data files
from agr_literature_service.lit_processing.oneoff_scripts.workflow.data.file_upload import get_data as file_upload
from agr_literature_service.lit_processing.oneoff_scripts.workflow.data.classification import get_data as classifications

logger = logging.getLogger(__name__)
name_to_atp = {}
atp_to_name = {}
mod_ids = {}
mod_abbrs = {}

helptext = "--filename file1 -debug"
parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, description=helptext)
parser.add_argument('-f', '--filename', help='Filename to be processed.', type=str, required=True)
parser.add_argument('-v', '--verbose', help='Print a lot of info during run.', default=False, type=bool, required=False)
args = parser.parse_args()


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


def get_name_to_atp_and_children(token, debug, curie='ATP:0000177'):
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
                    if debug:
                        print(f"\t {jane['curie']} - {jane['name']}")
                    name_to_atp[jane['name']] = jane['curie']
                    atp_to_name[jane['curie']] = jane['name']
                    get_name_to_atp_and_children(token=token, curie=jane['curie'], debug=debug)
    except HTTPError:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Error from A-team API")


def get_transitions(db: Session, filename: str, debug: bool = False):  # noqa
    global atp_to_name
    global mod_abbrs

    try:
        query = r"""
        select mod_id, transition_from, transition_to, requirements, transition_type 
          from workflow_transition;"""
        trans_results = db.execute(query)
        trans = trans_results.fetchall()
        start = '{'
        end= '}'
        for tran in trans:
            print (f"""
        {start}'mod': "{mod_abbrs[tran['mod_id']]}",
               'from': "{atp_to_name[tran['transition_from']]}",
               'to': "{atp_to_name[tran['transition_to']]}",
               'requirements': "{tran['requirements']}",
               'transition_type': "{tran['transition_type']}"{end},""")
    except Exception as e:
        logger.error(e)
        exit(-1)


if __name__ == "__main__":
    auth_token = get_authentication_token()
    if not auth_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)
    get_name_to_atp_and_children(token=auth_token, debug=args.verbose)
    engine = create_postgres_engine(False)
    db_connection = engine.connect()
    db_session: Session = create_postgres_session(False)

    load_mod_abbr(db_session)
    get_transitions(db_session, args.filename, args.verbose)
