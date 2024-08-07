"""
Add subtask transition.
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

from agr_literature_service.lit_processing.oneoff_scripts.workflow.data.file_upload import get_data as file_upload
logger = logging.getLogger(__name__)
name_to_atp = {}
atp_to_name = {}
mod_ids = {}

helptext = "--filename ."
parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, description=helptext)
parser.add_argument('-f', '--filename', help='Filename to be processed', type=str, required=True)
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
                    print(f"\t {jane['curie']} - {jane['name']}")
                    name_to_atp[jane['name']] = jane['curie']
                    atp_to_name[jane['curie']] = jane['name']
                    get_name_to_atp_and_children(token=token, curie=jane['curie'])
    except HTTPError:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Error from A-team API")


def add_transitions(db: Session, filename: str):
    global name_to_atp
    global mod_ids

    if filename == "file_upload":
        data_to_add = file_upload(name_to_atp)
    else:
        return
    for transition in data_to_add:
        mod_list: list = []
        if 'mod' in transition:
            if transition['mod'] == 'ALL':
                mod_list = list(mod_ids.keys())
                print(mod_list)
            elif transition['mod'].startswith("NOT_"):
                for mod in mod_ids.keys():
                    print(f" {type(mod)} {mod}")
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
            print(f"mod_abbreviation: {mod_abbr} -> mod_id {mod_ids[mod_abbr]}")
            wft = db.query(WorkflowTransitionModel).\
                filter(WorkflowTransitionModel.mod_id == mod_ids[mod_abbr],
                       WorkflowTransitionModel.transition_from == trans_from,
                       WorkflowTransitionModel.transition_to == trans_to).one_or_none()
            if not wft:
                wft = WorkflowTransitionModel(mod_id=mod_ids[mod_abbr],
                                              transition_from=trans_from,
                                              transition_to=trans_to)
                db.add(wft)
            if 'action' in transition:
                wft.actions = transition['action']
            if 'condition' in transition:
                wft.condition = transition['condition']
            print(f"pre commit: {wft}")
    db.commit()
    wfts = db.query(WorkflowTransitionModel).all()
    for wft in wfts:
        print(f"Final: {wft}")


if __name__ == "__main__":
    auth_token = get_authentication_token()
    if not auth_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)
    get_name_to_atp_and_children(token=auth_token)
    engine = create_postgres_engine(False)
    db_connection = engine.connect()
    db_session: Session = create_postgres_session(False)

    load_mod_abbr(db_session)
    add_transitions(db_session, args.filename)
