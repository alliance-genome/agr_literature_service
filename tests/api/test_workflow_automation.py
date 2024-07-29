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
# from unittest.mock import patch

from agr_literature_service.api.main import app
from agr_literature_service.api.models import WorkflowTagModel, ReferenceModel, WorkflowTransitionModel, ModModel
# from agr_literature_service.api.crud.workflow_tag_crud import (
# get_workflow_process_from_tag,
# get_workflow_tags_from_process)
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from .test_reference import test_reference # noqa
from .test_mod import test_mod # noqa


def workflow_automation_init(db_session, mod_id):
    test_data = [
        # [transition_from, transition_to, actions, condition]
        ["ATP:initial", "ATP:main_needed", "proceed_on_value::category::thesis::ATP:task1_needed,proceed_on_value::category::thesis::ATP:task2_needed,proceed_on_value::category::failure::ATP:task3_needed", None],
        ["ATP:main_needed", "ATP:main_in_progress", None, "on_start_job"],
        ["ATP:main_in_progress", "ATP:main_failed", None, "on_failure"],
        ["ATP:main_in_progress", "ATP:main_successful", None, "on_success"],

        ["ATP:task1_needed", "ATP:task1_in_progress", None, "on_start_job"],
        ["ATP:task1_in_progress", "ATP:task1_successful", None, "on_success"],
        ["ATP:task1_in_progress", "ATP:task1_failed", None, "on_failure"],

        ["ATP:task2_needed", "ATP:task2_in_progress", None, "on_start_job"],
        ["ATP:task2_in_progress", "ATP:task2_successful", None, "on_success"],
        ["ATP:task2_in_progress", "ATP:task2_failed", None, "on_failure"]
    ]
    for data in test_data:
        db_session.add(WorkflowTransitionModel(mod_id=mod_id,
                                               transition_from=data[0],
                                               transition_to=data[1],
                                               actions=data[2],
                                               condition=data[3]))


class TestWorkflowTagAutomation:

    def test_transition_actions(self, db, test_mod, test_reference,  # noqa
                                                                           auth_headers):  # noqa
        mod = db.query(ModModel).filter(ModModel.abbreviation == test_mod.new_mod_abbreviation).one()
        workflow_automation_init(db, mod.mod_id)
        reference = db.query(ReferenceModel).filter(ReferenceModel.curie == test_reference.new_ref_curie).one()

        # Set initial workflow tag to "ATP:initial"
        with TestClient(app) as client:
            new_wt = {"reference_curie": reference.curie,
                      "mod_abbreviation": mod.abbreviation,
                      "workflow_tag_id": "ATP:initial",
                      }
            response = client.post(url="/workflow_tag/", json=new_wt, headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK

            # Test actions by transitioning from "ATP:initial" to "ATP:main_needed"
            transition_req = {
                "curie_or_reference_id": reference.curie,
                "mod_abbreviation": mod.abbreviation,
                "new_workflow_tag_atp_id": "ATP:main_needed"
            }
            response = client.post(url="/workflow_tag/transition_to_workflow_status", json=transition_req,
                                   headers=auth_headers)
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
            for atp in ["ATP:main_needed", "ATP:task1_needed", " ATP:task2_needed"]:
                response = client.post(url=f"/workflow_tag/job/started/{wft[atp]}",
                                       headers=auth_headers)
                assert response.status_code == status.HTTP_200_OK

                test_id = db.query(WorkflowTagModel). \
                    filter(WorkflowTagModel.workflow_tag_id == atp,
                           WorkflowTagModel.reference_id == reference.reference_id,
                           WorkflowTagModel.mod_id == mod.mod_id).one_or_none()
                assert test_id is None

            for atp in ["ATP:main_in_progress", "ATP:task1_in_progress", "ATP:task2_in_progress"]:
                response = client.post(url=f"/workflow_tag/job/started/{wft[atp]}",
                                       headers=auth_headers)
                assert response.status_code == status.HTTP_200_OK

                test_id = db.query(WorkflowTagModel). \
                    filter(WorkflowTagModel.workflow_tag_id == atp,
                           WorkflowTagModel.reference_id == reference.reference_id,
                           WorkflowTagModel.mod_id == mod.mod_id).one_or_none()
                assert test_id

            # set task1 and task2 to successful.
            for atp in ["ATP:task1_in_progress", "ATP:task2_in_progress"]:
                response = client.post(url=f"/workflow_tag/job/success/{wft[atp]}",
                                       headers=auth_headers)
                assert response.status_code == status.HTTP_200_OK

            for atp in ["ATP:main_in_progress", "ATP:task1_in_progress", "ATP:task2_in_progress"]:
                response = client.post(url=f"/workflow_tag/job/started/{wft[atp]}",
                                       headers=auth_headers)
                assert response.status_code == status.HTTP_200_OK

                test_id = db.query(WorkflowTagModel). \
                    filter(WorkflowTagModel.workflow_tag_id == atp,
                           WorkflowTagModel.reference_id == reference.reference_id,
                           WorkflowTagModel.mod_id == mod.mod_id).one_or_none()
                assert test_id is None

            for atp in ["ATP:main_successful", "ATP:task1_successful", "ATP:task2_successful"]:
                response = client.post(url=f"/workflow_tag/job/started/{wft[atp]}",
                                       headers=auth_headers)
                assert response.status_code == status.HTTP_200_OK

                test_id = db.query(WorkflowTagModel). \
                    filter(WorkflowTagModel.workflow_tag_id == atp,
                           WorkflowTagModel.reference_id == reference.reference_id,
                           WorkflowTagModel.mod_id == mod.mod_id).one_or_none()
                assert test_id

    def test_transition_job_failure(self, db, test_mod, test_reference,  # noqa
                                                                           auth_headers):  # noqa
        mod = db.query(ModModel).filter(ModModel.abbreviation == test_mod.new_mod_abbreviation).one()
        workflow_automation_init(db, mod.mod_id)
        reference = db.query(ReferenceModel).filter(ReferenceModel.curie == test_reference.new_ref_curie).one()

        with TestClient(app) as client:
            # Set initial workflow tag to "ATP:XXXX_in_progress"
            for atp in ["ATP:main_in_progress", "ATP:task1_in_progress", "ATP:task2_in_progress"]:
                new_wft = {"reference_curie": reference.curie,
                           "mod_abbreviation": mod.abbreviation,
                           "workflow_tag_id": atp,
                           }
                response = client.post(url="/workflow_tag/", json=new_wft, headers=auth_headers)
                assert response.status_code == status.HTTP_200_OK
                new_wft[atp] = response.json()
                assert new_wft[atp]

            # set task1 to success BUT task2 to failure
            response = client.post(url=f"/workflow_tag/job/success/{new_wft['ATP:task1_in_progress']}",
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK

            response = client.post(url=f"/workflow_tag/job/failed/{new_wft['ATP:task2_in_progress']}",
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK

            # we should have "ATP:task1_successful", "ATP:task2_failed" and "ATP:main_failed"
            for atp in ["ATP:main_failed", "ATP:task1_successful", "ATP:task2_failed"]:

                test_id = db.query(WorkflowTagModel). \
                    filter(WorkflowTagModel.workflow_tag_id == atp,
                           WorkflowTagModel.reference_id == reference.reference_id,
                           WorkflowTagModel.mod_id == mod.mod_id).one_or_none()
                assert test_id

            # make sure originals have gone
            for atp in ["ATP:main_in_progress", "ATP:task1_in_progress", "ATP:task2_in_progress"]:
                test_id = db.query(WorkflowTagModel). \
                    filter(WorkflowTagModel.workflow_tag_id == atp,
                           WorkflowTagModel.reference_id == reference.reference_id,
                           WorkflowTagModel.mod_id == mod.mod_id).one_or_none()
                assert test_id is None
