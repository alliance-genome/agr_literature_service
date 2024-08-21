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
from agr_literature_service.lit_processing.oneoff_scripts.workflow.data.text_conversion import get_data as text_conversions
from agr_literature_service.lit_processing.oneoff_scripts.workflow.data.stage import get_data as stage


logger = logging.getLogger(__name__)
name_to_atp = {}
atp_to_name = {}
mod_ids = {}

helptext = "--filename file1 -debug"
parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, description=helptext)
parser.add_argument('-f', '--filename', help='Filename to be processed.', type=str, required=True)
parser.add_argument('-v', '--verbose', help='Print a lot of info during run.', default=False, type=bool, required=False)
args = parser.parse_args()


def load_mod_abbr(db):
    global mod_ids
    try:
        mod_results = db.execute("select abbreviation, mod_id from mod")
        mods = mod_results.fetchall()
        for mod in mods:
            if mod["abbreviation"] != 'GO':
                mod_ids[mod["abbreviation"]] = mod["mod_id"]
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


def add_transitions(db: Session, filename: str, debug: bool = False):  # noqa
    global name_to_atp
    global mod_ids

    # Add new data files here with appropriate elif
    if filename == "file_upload":
        data_to_add = file_upload(name_to_atp)
    elif filename == "classifications":
        data_to_add = classifications(name_to_atp)
    elif filename == "text_conversions":
        data_to_add = text_conversions(name_to_atp)
    elif filename == "stage":
        data_to_add = stage(name_to_atp)
    else:
        print(f"Unknown filename {filename}")
        return
    for transition in data_to_add:
        mod_list: list = []
        if 'mod' in transition:
            if transition['mod'] == 'ALL':
                mod_list = list(mod_ids.keys())
            elif transition['mod'].startswith("NOT_"):
                for mod in mod_ids.keys():
                    if mod != transition['mod'][4:]:
                        mod_list.append(mod)
            else:
                mod_list.append(transition['mod'])

        for mod_abbr in mod_list:
            try:
                trans_from = name_to_atp[transition['from']]
            except KeyError:
                print(f"ERROR: {transition['from']} is not found in name_to_atp")
                exit(-1)
            try:
                trans_to = name_to_atp[transition['to']]
            except KeyError:
                print(f"ERROR: {transition['to']} is not found in name_to_atp")
                exit(-1)
            wft = db.query(WorkflowTransitionModel).\
                filter(WorkflowTransitionModel.mod_id == mod_ids[mod_abbr],
                       WorkflowTransitionModel.transition_from == trans_from,
                       WorkflowTransitionModel.transition_to == trans_to).one_or_none()
            if wft:
                if debug:
                    print(f"Transition found, will update actions and condition only for {wft}")
            if not wft:
                if debug:
                    print(f"Adding new wft {wft}")
                wft = WorkflowTransitionModel(mod_id=mod_ids[mod_abbr],
                                              transition_from=trans_from,
                                              transition_to=trans_to)
                db.add(wft)
            if 'actions' in transition:
                wft.actions = transition['actions']
            else:
                wft.actions = []
            if 'condition' in transition:
                wft.condition = transition['condition']
            else:
                wft.condition = None
            if 'requirements' in transition and transition['requirements'] != "None":
                wft.requirements = transition['requirements']
            else:
                wft.requirements = None
            if 'transition_type' in transition:
                wft.transition_type = transition['transition_type']
            else:
                wft.transition_type = "any"
    db.commit()
    if debug:
        wfts = db.query(WorkflowTransitionModel).all()
        for wft in wfts:
            print(f"Final: {wft}")


if __name__ == "__main__":
    auth_token = get_authentication_token()
    if not auth_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)
    get_name_to_atp_and_children(token=auth_token, debug=args.verbose)
    engine = create_postgres_engine(False)
    db_connection = engine.connect()
    db_session: Session = create_postgres_session(False)

    load_mod_abbr(db_session)
    add_transitions(db_session, args.filename, args.verbose)
