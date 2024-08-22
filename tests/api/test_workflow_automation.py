# Testing automation of workflow progression and jobs.
# Because we do not look up ATP from the ateam for these as they are already
# coded in the transition table we can add fake ATP values here to make it more
# readable.
#
# So we are going to mimic "ATP:main_needed" which has 3 subtasks "ATP:task1_needed",
# "ATP:task2_needed" and "ATP:task3_needed".
# For testing, we will have task3 fail the condition for being set.
# This is covered by the actions and will be
# "proceed_on_value::category::thesis::ATP:task1_needed,
# proceed_on_value::category::thesis::ATP:task2_needed,
# proceed_on_value::category::failure::ATP:task3_needed"
#
# At this point we want to check that "ATP:main_needed" is no longer there.
# "ATP:main_in_progress" should now be set.
# "ATP:task1_needed", "ATP:task2_needed" should be set but not "ATP:task3_needed".
#
# Call api end point job_started for each task.
# Check "XXX needed" no longer there and "XXX in_progress" is now set for these.
# For both tasks call api end point start_job.
# Check this is true
#
# Call end point successful_job for task1.
# Call end point failed_job for task2.
# Check these are correct and old ones are removed.
# Check main is now set to failed too.
#
# from collections import namedtuple

# import pytest
# from sqlalchemy import and_
from starlette.testclient import TestClient
from fastapi import status
from unittest.mock import patch

from agr_literature_service.api.main import app
from agr_literature_service.api.models import WorkflowTagModel, ReferenceModel, WorkflowTransitionModel, ModModel
# from agr_literature_service.api.crud.workflow_tag_crud import (
# get_workflow_process_from_tag,
# get_workflow_tags_from_process)
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from .test_reference import test_reference # noqa
from .test_mod import test_mod # noqa
# from .test_workflow_tag import test_workflow_tag
from agr_literature_service.api.crud import workflow_tag_crud

test_reference2 = test_reference


def get_tags_mock(workflow_tag_atp_id: str):
    print(f"***** Mocking get_parents name = {workflow_tag_atp_id}")
    if workflow_tag_atp_id == 'ATP:fileupload':
        return ['ATP:0000141', 'ATP:fileuploadinprogress', 'ATP:fileuploadcomplete', 'ATP:fileuploadfailed']


# TestWFTData = namedtuple('TestWFTData', ['response'])
def get_process_mock(workflow_tag_atp_id: str):
    # MUST start with ATP:0000003 for this to work
    print(f"***** Mocking get_ancestors name = {workflow_tag_atp_id}")
    if workflow_tag_atp_id == 'ATP:0000141':  # file upload needed
        return 'ATP:fileupload'
    elif workflow_tag_atp_id == 'ATP:fileuploadinprogress':
        return 'ATP:fileupload'
    elif workflow_tag_atp_id == 'ATP:fileuploadcomplete':
        return 'ATP:fileupload'
    elif workflow_tag_atp_id == 'ATP:fileuploadfailed':
        return 'ATP:fileupload'
    elif workflow_tag_atp_id == 'ATP:task2_failed':
        return 'ATP:task2_needed'
    elif workflow_tag_atp_id == 'ATP:fileupload':
        return ['ATP:ont1']
    else:
        print("returning NOTHING!!")
        return []


def get_descendants_mock(name):
    # MUST start with ATP:0000003 for this to work
    print(f"***** Mocking get_ancestors name = {name}")
    if name == 'ATP:0000177':
        return ['ATP:0000172', 'ATP:0000140', 'ATP:0000165', 'ATP:0000161']
    elif name == 'ATP:0000172':
        return ['ATP:0000175', 'ATP:0000174', 'ATP:0000173', 'ATP:0000178']
    elif name == 'ATP:0000140':
        return ['ATP:0000141', 'ATP:0000135', 'ATP:0000139', 'ATP:0000134']
    elif name == 'ATP:0000165':
        return ['ATP:0000168', 'ATP:0000167', 'ATP:0000170', 'ATP:0000171', 'ATP:0000169', 'ATP:0000166']
    elif name == 'ATP:0000161':
        return ['ATP:0000164', 'ATP:0000163', 'ATP:0000162']
    elif name == 'ATP:fileupload':
        return ['ATP:0000141', 'ATP:fileuploadinprogress', 'ATP:fileuploadcomplete', 'ATP:fileuploadfailed']
    else:
        print("returning NOTHING!!")
        return []


def workflow_automation_init(db):  # noqa
    print("workflow_automation_init")
    populate_test_mods()
    test_data = [
        # [transition_from, transition_to, actions, condition]
        # ATP:0000141 is file upload needed and hard coded in
        ["ATP:top", "ATP:0000141", [], None],
        ["ATP:0000141", "ATP:fileuploadinprogress", [], 'on_start'],
        ["ATP:fileuploadinprogress",
         "ATP:fileuploadcomplete",
         ["proceed_on_value::category::thesis::ATP:task1_needed",
          "proceed_on_value::category::thesis::ATP:task2_needed",
          "proceed_on_value::category::failure::ATP:task3_needed"],
         'on_success'],
        ["ATP:fileuploadinprogress", "ATP:fileuploadfailed", [], 'on_failed'],
        ["ATP:needed", "ATP:task1_needed", None, "task1_job"],
        ["ATP:needed", "ATP:task2_needed", None, "task2_job"],
        ["ATP:needed", "ATP:task3_needed", None, "task3_job"],
        ["ATP:task1_needed", "ATP:task1_in_progress", None, "on_start"],
        ["ATP:task1_in_progress", "ATP:task1_successful", None, "on_success"],
        ["ATP:task1_in_progress", "ATP:task1_failed", None, "on_failed"],

        ["ATP:task2_needed", "ATP:task2_in_progress", None, "on_start"],
        ["ATP:task2_in_progress", "ATP:task2_successful", None, "on_success"],
        ["ATP:task2_in_progress", "ATP:task2_failed", None, "on_failed"]
    ]
    mods = db.query(ModModel).all()
    for mod in mods:
        print(f"BOB: {mod.mod_id}, {mod.abbreviation}")
    for data in test_data:
        for mod in mods:
            # print(data)
            db.add(WorkflowTransitionModel(mod_id=mod.mod_id,
                                           transition_from=data[0],
                                           transition_to=data[1],
                                           actions=data[2],
                                           condition=data[3]))
    db.commit()
    return


class TestWorkflowTagAutomation:
    @patch("agr_literature_service.api.crud.workflow_tag_crud.get_workflow_process_from_tag", get_process_mock)
    @patch("agr_literature_service.api.crud.workflow_tag_crud.get_descendants", get_descendants_mock)
    @patch("agr_literature_service.api.crud.workflow_tag_crud.get_workflow_tags_from_process", get_tags_mock)
    def test_transition_actions(self, db, auth_headers, test_mod, test_reference):  # noqa
        print("test_transition_actions")
        mod = db.query(ModModel).filter(ModModel.abbreviation == test_mod.new_mod_abbreviation).one()
        # workflow_automation_init(db, mod.mod_id)
        reference = db.query(ReferenceModel).filter(ReferenceModel.curie == test_reference.new_ref_curie).one()
        workflow_automation_init(db)
        print(f"BOB1: {mod}")

        # sworkflow_tag_crud.transition_to_workflow_status(
        #    db,
        #    curie_or_reference_id=reference.curie,
        #    mod_abbreviation=mod.abbreviation,
        #    new_workflow_tag_atp_id="ATP:0000141",
        #    transition_type='automated')

        with TestClient(app) as client:
            # Set initial workflow tag to "ATP:0000141" , hard coded so allowed
            print(f"BOB2: {reference}")

            transition_req = {
                "curie_or_reference_id": reference.curie,
                "mod_abbreviation": mod.abbreviation,
                "new_workflow_tag_atp_id": "ATP:0000141",
                "transition_type": 'automated'
            }
            print(f"BOB3: {transition_req}")
            response = client.post(url="/workflow_tag/transition_to_workflow_status", json=transition_req,
                                  headers=auth_headers)
            print(response.content)
            print(response.text)
            print(response.reason)
            assert response.status_code == status.HTTP_200_OK

            # Test actions by transitioning from "ATP:0000141" to "ATP:fileuploadinprogress"
            print(f"BOB4: {reference}")
            transition_req = {
                "curie_or_reference_id": reference.curie,
                "mod_abbreviation": mod.abbreviation,
                "new_workflow_tag_atp_id": "ATP:fileuploadinprogress",
                "transition_type": 'automated'
            }
            print(f"BOB: {transition_req}")
            response = client.post(url="/workflow_tag/transition_to_workflow_status", json=transition_req,
                                   headers=auth_headers)
            print(response.content)
            print(response.text)
            print(response.reason)
            assert response.status_code == status.HTTP_200_OK

            transition_req = {
                "curie_or_reference_id": reference.curie,
                "mod_abbreviation": mod.abbreviation,
                "new_workflow_tag_atp_id": "ATP:fileuploadcomplete"
            }
            response = client.post(url="/workflow_tag/transition_to_workflow_status", json=transition_req,
                                   headers=auth_headers)
            print(response.content)
            print(response.text)
            print(response.reason)
            assert response.status_code == status.HTTP_200_OK
            # So we should have "ATP:main_needed", "ATP:task1_needed"," ATP:task2_needed"
            # all set for this mod and reference
            wft = {}
            for atp in ["ATP:task1_needed", "ATP:task2_needed"]:
                print(f"atp = {atp}")
                wft[atp] = db.query(WorkflowTagModel).\
                    filter(WorkflowTagModel.workflow_tag_id == atp,
                           WorkflowTagModel.reference_id == reference.reference_id,
                           WorkflowTagModel.mod_id == mod.mod_id).one()
                assert wft[atp].reference_workflow_tag_id

            # Check the get_jobs url
            response = client.get(url="/workflow_tag/jobs/task1_job",
                                  headers=auth_headers)
            print(response.content)
            print(response.text)
            print(response.reason)
            results = response.json()
            assert response.status_code == status.HTTP_200_OK
            results = response.json()

            # test jobs returning duplicates
            # we should have only 1 response here.
            assert len(results) == 1

            # TODO: on a task we need to set the main one to in_progress too
            #       Code should check actions for the atp code it is and set
            #       the main one from needed to in progress
            #       Similarly for success of all subtasks or failure of any.
            for atp in ["ATP:task1_needed", "ATP:task2_needed"]:
                print(f"atp = {wft[atp]}")
                response = client.post(url=f"/workflow_tag/job/started/{wft[atp].reference_workflow_tag_id}",
                                       headers=auth_headers)
                assert response.status_code == status.HTTP_200_OK

                test_id = db.query(WorkflowTagModel). \
                    filter(WorkflowTagModel.workflow_tag_id == atp,
                           WorkflowTagModel.reference_id == reference.reference_id,
                           WorkflowTagModel.mod_id == mod.mod_id).one_or_none()
                assert test_id is None

            for atp in ["ATP:task1_in_progress", "ATP:task2_in_progress"]:
                test_id = db.query(WorkflowTagModel). \
                    filter(WorkflowTagModel.workflow_tag_id == atp,
                           WorkflowTagModel.reference_id == reference.reference_id,
                           WorkflowTagModel.mod_id == mod.mod_id).one_or_none()
                assert test_id

            # set task1 and task2 to successful.
            for atp in ["ATP:task1_in_progress", "ATP:task2_in_progress"]:
                # id used for jobs should still be the same
                old_atp = atp.replace("in_progress", "needed")
                print(f"ref = {wft[old_atp].reference_workflow_tag_id}")
                response = client.post(url=f"/workflow_tag/job/success/{wft[old_atp].reference_workflow_tag_id}",
                                       headers=auth_headers)
                assert response.status_code == status.HTTP_200_OK

            # When we know the hierarchy we can add main back in testing
            # for atp in ["ATP:task1_successful", "ATP:task2_successful", "ATP:main_successful"]:
            for atp in ["ATP:task1_successful", "ATP:task2_successful"]:
                test_id = db.query(WorkflowTagModel).\
                    filter(WorkflowTagModel.workflow_tag_id == atp,
                           WorkflowTagModel.reference_id == reference.reference_id,
                           WorkflowTagModel.mod_id == mod.mod_id).one_or_none()
                print(f"atp test {atp}")
                assert test_id

    @patch("agr_literature_service.api.crud.workflow_tag_crud.get_workflow_process_from_tag", get_process_mock)
    @patch("agr_literature_service.api.crud.workflow_tag_crud.get_descendants", get_descendants_mock)
    def test_transition_work_failed(self, db, auth_headers, test_mod, test_reference):  # noqa
        print("test_transition_actions")
        with TestClient(app) as client:
            mod = db.query(ModModel).filter(ModModel.abbreviation == test_mod.new_mod_abbreviation).one()
            # reference = db.query(ReferenceModel).filter(ReferenceModel.curie == test_reference.new_ref_curie).one()
            workflow_automation_init(db)

            reference = db.query(ReferenceModel).filter(ReferenceModel.curie == test_reference.new_ref_curie).one()

            # Set initial workflow tag to "ATP:XXXX_in_progress"
            atp_to_ref_wft_id = {}
            for atp in ["ATP:main_in_progress", "ATP:task1_in_progress", "ATP:task2_in_progress"]:
                new_wft = {"reference_curie": reference.curie,
                           "mod_abbreviation": mod.abbreviation,
                           "workflow_tag_id": atp,
                           }
                print(F"new wft: {new_wft} {client}")
                response = client.post(url="/workflow_tag/", json=new_wft, headers=auth_headers)
                print("post wft create")
                assert response.status_code == status.HTTP_201_CREATED
                atp_to_ref_wft_id[atp] = response.json()
                print(F"new wft: {atp} {atp_to_ref_wft_id[atp]}")
                assert atp_to_ref_wft_id[atp]

            # set task1 to success BUT task2 to failure
            response = client.post(url=f"/workflow_tag/job/success/{atp_to_ref_wft_id['ATP:task1_in_progress']}",
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK

            response = client.post(url=f"/workflow_tag/job/failed/{atp_to_ref_wft_id['ATP:task2_in_progress']}",
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK

            # we should have "ATP:task1_successful", "ATP:task2_failed" and "ATP:main_failed"
            # TODO: add "ATP:main_failed", as this needs to be covered BUT not codeded yet
            #       as we may need the hierarchy
            for atp in ["ATP:task1_successful", "ATP:task2_failed"]:

                test_id = db.query(WorkflowTagModel). \
                    filter(WorkflowTagModel.workflow_tag_id == atp,
                           WorkflowTagModel.reference_id == reference.reference_id,
                           WorkflowTagModel.mod_id == mod.mod_id).one_or_none()
                assert test_id

            # make sure originals have gone
            # Ditto here "ATP:main_in_progress",
            for atp in ["ATP:task1_in_progress", "ATP:task2_in_progress"]:
                test_id = db.query(WorkflowTagModel). \
                    filter(WorkflowTagModel.workflow_tag_id == atp,
                           WorkflowTagModel.reference_id == reference.reference_id,
                           WorkflowTagModel.mod_id == mod.mod_id).one_or_none()
                assert test_id is None

    @patch("agr_literature_service.api.crud.workflow_tag_crud.get_workflow_process_from_tag", get_process_mock)
    @patch("agr_literature_service.api.crud.workflow_tag_crud.get_descendants", get_descendants_mock)
    def test_bad_transitions(self, db, auth_headers, test_mod, test_reference):  # noqa
        print("test_bad_transitions")
        with TestClient(app) as client:
            mod = db.query(ModModel).filter(ModModel.abbreviation == test_mod.new_mod_abbreviation).one()
            workflow_automation_init(db)

            # Bad new workflow ?
            transition_req = {
                "curie_or_reference_id": test_reference.new_ref_curie,
                "mod_abbreviation": mod.abbreviation,
                "new_workflow_tag_atp_id": "ATP:MadeUp"
            }
            response = client.post(url="/workflow_tag/transition_to_workflow_status", json=transition_req,
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
            assert response.json().get("detail") == "process_atp_id ATP:MadeUp has NO process."

            # Bad mod abbreviation
            transition_req = {
                "curie_or_reference_id": test_reference.new_ref_curie,
                "mod_abbreviation": "BadMod",
                "new_workflow_tag_atp_id": "ATP:main_needed"
            }
            response = client.post(url="/workflow_tag/transition_to_workflow_status", json=transition_req,
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND
            assert response.json().get("detail") == 'Mod abbreviation BadMod does not exist'

        # Bad mod abbreviation
        transition_req = {
            "curie_or_reference_id": "MadeUpCurie",
            "mod_abbreviation": mod.abbreviation,
            "new_workflow_tag_atp_id": "ATP:main_needed"
        }
        response = client.post(url="/workflow_tag/transition_to_workflow_status", json=transition_req,
                               headers=auth_headers)
        assert response.status_code == status.HTTP_404_NOT_FOUND

        # Now do transition NOT in the transition table.
        transition_req = {
            "curie_or_reference_id": test_reference.new_ref_curie,
            "mod_abbreviation": mod.abbreviation,
            "new_workflow_tag_atp_id": "ATP:task2_failed"
        }
        response = client.post(url="/workflow_tag/transition_to_workflow_status", json=transition_req,
                               headers=auth_headers)
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        assert response.json().get("detail") == 'Transition to ATP:task2_failed not allowed as not initial state.'
