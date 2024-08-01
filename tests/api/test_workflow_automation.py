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
from collections import namedtuple

import pytest
# from sqlalchemy import and_
from starlette.testclient import TestClient
from fastapi import status
from unittest.mock import patch

from agr_literature_service.api.main import app
from agr_literature_service.api.models import WorkflowTagModel, ReferenceModel, WorkflowTransitionModel, ModModel
# from agr_literature_service.api.crud.workflow_tag_crud import (
# get_workflow_process_from_tag,
# get_workflow_tags_from_process)
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from .test_reference import test_reference # noqa
from .test_mod import test_mod # noqa
from .test_workflow_tag import test_workflow_tag

test_reference2 = test_reference


# TestWFTData = namedtuple('TestWFTData', ['response'])
def get_process_mock(workflow_tag_atp_id: str):
    # MUST start with ATP:0000003 for this to work
    print(f"***** Mocking get_ancestors name = {workflow_tag_atp_id}")
    if workflow_tag_atp_id == 'ATP:main_needed':
        return 'ATP:ont1'
    else:
        print("returning NOTHING!!")
        return []


def workflow_automation_init(db, mod_id):
    print("workflow_automation_init")
    test_data = [
        # [transition_from, transition_to, actions, condition]
        ["ATP:ont1", "ATP:main_needed", ["proceed_on_value::category::thesis::ATP:task1_needed",
                                         "proceed_on_value::category::thesis::ATP:task2_needed",
                                         "proceed_on_value::category::failure::ATP:task3_needed"], None],
        ["ATP:main_needed", "ATP:main_in_progress", None, "on_start_job"],
        ["ATP:main_in_progress", "ATP:main_failed", None, "on_failed"],
        ["ATP:main_in_progress", "ATP:main_successful", None, "on_success"],

        ["ATP:task1_needed", "ATP:task1_in_progress", None, "on_start_job"],
        ["ATP:task1_in_progress", "ATP:task1_successful", None, "on_success"],
        ["ATP:task1_in_progress", "ATP:task1_failed", None, "on_failed"],

        ["ATP:task2_needed", "ATP:task2_in_progress", None, "on_start_job"],
        ["ATP:task2_in_progress", "ATP:task2_successful", None, "on_success"],
        ["ATP:task2_in_progress", "ATP:task2_failed", None, "on_failed"]
    ]
    # mod = db.query(ModModel).filter(ModModel.mod_id == mod_id).one()
    ids = []
    for data in test_data:
        print(data)
        db.add(WorkflowTransitionModel(mod_id=mod_id,
                                       transition_from=data[0],
                                       transition_to=data[1],
                                       actions=data[2],
                                       condition=data[3]))
        db.commit()
        bob: WorkflowTransitionModel = db.query(WorkflowTransitionModel).filter(
            WorkflowTransitionModel.mod_id == mod_id,
            WorkflowTransitionModel.transition_from == data[0],
            WorkflowTransitionModel.transition_to == data[1]).one()
        ids.append(bob.workflow_transition_id)
    db.commit()
    print(ids)
    print("data added for transitions")
    return ids[0]


class TestWorkflowTagAutomation:
    @patch("agr_literature_service.api.crud.workflow_tag_crud.get_workflow_process_from_tag", get_process_mock)
    def transition_actions(self, db, auth_headers, test_mod, test_reference):  # noqa
        print("test_transition_actions")
        mod = db.query(ModModel).filter(ModModel.abbreviation == test_mod.new_mod_abbreviation).one()
        # workflow_automation_init(db, mod.mod_id)
        reference = db.query(ReferenceModel).filter(ReferenceModel.curie == test_reference.new_ref_curie).one()
        workflow_automation_init(db, mod.mod_id)
        # Set initial workflow tag to "ATP:initial"
        with TestClient(app) as client:
            # Test actions by transitioning from "ATP:initial" to "ATP:main_needed"
            transition_req = {
                "curie_or_reference_id": reference.curie,
                "mod_abbreviation": mod.abbreviation,
                "new_workflow_tag_atp_id": "ATP:main_needed"
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
            for atp in ["ATP:main_needed", "ATP:task1_needed", "ATP:task2_needed"]:
                wft[atp] = db.query(WorkflowTagModel).\
                    filter(WorkflowTagModel.workflow_tag_id == atp,
                           WorkflowTagModel.reference_id == reference.reference_id,
                           WorkflowTagModel.mod_id == mod.mod_id).one()
                assert wft[atp].reference_workflow_tag_id

            # task1 and task2, pretend we are a job and we are starting it so we want
            # to let the api know it is now in progress

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

            # for atp in ["ATP:task1_in_progress", "ATP:task2_in_progress"]:
            #     response = client.post(url=f"/workflow_tag/job/started/{wft[atp]}",
            #                            headers=auth_headers)
            #     assert response.status_code == status.HTTP_200_OK
            #
            #     test_id = db.query(WorkflowTagModel). \
            #         filter(WorkflowTagModel.workflow_tag_id == atp,
            #                WorkflowTagModel.reference_id == reference.reference_id,
            #                WorkflowTagModel.mod_id == mod.mod_id).one_or_none()
            #     assert test_id

            # on starting sub tasks the main one should be set to in_progress too.
            test_id = db.query(WorkflowTagModel).\
                filter(WorkflowTagModel.workflow_tag_id == "ATP:main_needed",
                       WorkflowTagModel.reference_id == reference.reference_id,
                       WorkflowTagModel.mod_id == mod.mod_id).one_or_none()
            assert not test_id

            # make sure the needed has gone
            test_id = db.query(WorkflowTagModel).\
                filter(WorkflowTagModel.workflow_tag_id == "ATP:main_in_progress",
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
    def test_transition_work_failed(self, db, auth_headers, test_mod, test_reference):  # noqa
        print("test_transition_actions")
        with TestClient(app) as client:
            mod = db.query(ModModel).filter(ModModel.abbreviation == test_mod.new_mod_abbreviation).one()
            # workflow_automation_init(db, mod.mod_id)
            reference = db.query(ReferenceModel).filter(ReferenceModel.curie == test_reference.new_ref_curie).one()
            workflow_automation_init(db, mod.mod_id)

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
            for atp in [ "ATP:task1_in_progress", "ATP:task2_in_progress"]:
                test_id = db.query(WorkflowTagModel). \
                    filter(WorkflowTagModel.workflow_tag_id == atp,
                           WorkflowTagModel.reference_id == reference.reference_id,
                           WorkflowTagModel.mod_id == mod.mod_id).one_or_none()
                assert test_id is None
