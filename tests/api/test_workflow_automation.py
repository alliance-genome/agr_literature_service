# Testing automation of workflow progression and jobs.
# Because we do not look up ATP from the ateam for these as they are already
# coded in the transition table we can add fake ATP values here to make it more
# readable.
#
# So we are going to mimic "ATP:0000166" which has 3 subtasks "ATP:task1_needed",
# "ATP:task2_needed" and "ATP:task3_needed".
# For testing, we will have task3 fail the condition for being set.
# This is covered by the actions and will be
# "proceed_on_value::category::thesis::ATP:task1_needed,
# proceed_on_value::category::thesis::ATP:task2_needed,
# proceed_on_value::category::failure::ATP:task3_needed"
#
# At this point we want to check that "ATP:0000166" is no longer there.
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
from starlette.testclient import TestClient
from fastapi import status
from unittest.mock import patch

from agr_literature_service.api.main import app
from agr_literature_service.api.models import (
    WorkflowTagModel,
    ReferenceModel,
    WorkflowTransitionModel,
    ModModel,
    ModReferencetypeAssociationModel,
    ReferencetypeModel
)
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from ..fixtures import db # noqa
from ..fixtures import load_name_to_atp_and_relationships_mock # noqa
from .fixtures import auth_headers # noqa
from .test_reference import test_reference # noqa
from .test_mod import test_mod # noqa
from agr_literature_service.api.crud import workflow_tag_crud  # noqa
from agr_literature_service.api.crud.workflow_transition_actions.first_pass_curation import (
    set_first_pass_curation_tbd)
from agr_literature_service.api.crud.workflow_transition_actions.subtask_process import sub_task_complete
from agr_literature_service.api.crud.ateam_db_helpers import set_globals

test_reference2 = test_reference


def _get_or_create_fb_mod(db_session):
    mod = db_session.query(ModModel).filter(ModModel.abbreviation == 'FB').one_or_none()
    if mod is None:
        mod = ModModel(abbreviation='FB', short_name='FB', full_name='FlyBase')
        db_session.add(mod)
        db_session.commit()
    return mod


def mock_load_name_to_atp_and_relationships():
    print("*** LOCAL TWA mock_load_name_to_atp_and_relationships ***")
    workflow_children = {
        'ATP:0000177': ['ATP:0000172', 'ATP:0000140', 'ATP:0000165', 'ATP:0000161'],
        'ATP:0000172': ['ATP:0000175', 'ATP:0000174', 'ATP:0000173', 'ATP:0000178'],
        'ATP:0000140': ['ATP:0000141', 'ATP:0000135', 'ATP:0000139', 'ATP:0000134'],
        'ATP:0000161': ['ATP:0000164', 'ATP:0000163', 'ATP:0000162'],
        'ATP:fileupload': ['ATP:0000141', 'ATP:fileuploadinprogress', 'ATP:fileuploadcomplete', 'ATP:fileuploadfailed'],
        'ATP:0000165': ['ATP:0000166', 'ATP:0000178', 'ATP:0000189', 'ATP:0000169'],
        'ATP:0000166': ['ATP:task1_needed', 'ATP:task2_needed', 'ATP:task3_needed'],
        'ATP:0000178': ['ATP:task1_in_progress', 'ATP:task2_in_progress', 'ATP:task3_in_progress'],
        'ATP:0000189': ['ATP:task1_failed', 'ATP:task2_failed', 'ATP:task3_failed'],
        'ATP:0000169': ['ATP:task1_complete', 'ATP:task2_complete', 'ATP:task3_complete'],

        # Leaf nodes (no children) - needed to prevent API calls during tests
        'ATP:0000175': [], 'ATP:0000174': [], 'ATP:0000173': [],
        'ATP:0000141': [], 'ATP:0000135': [], 'ATP:0000139': [], 'ATP:0000134': [],
        'ATP:0000164': [], 'ATP:0000163': [], 'ATP:0000162': [],
        'ATP:fileuploadinprogress': [], 'ATP:fileuploadcomplete': [], 'ATP:fileuploadfailed': [],
        'ATP:task1_needed': [], 'ATP:task2_needed': [], 'ATP:task3_needed': [],
        'ATP:task1_in_progress': [], 'ATP:task2_in_progress': [], 'ATP:task3_in_progress': [],
        'ATP:task1_failed': [], 'ATP:task2_failed': [], 'ATP:task3_failed': [],
        'ATP:task1_complete': [], 'ATP:task2_complete': [], 'ATP:task3_complete': []
    }
    workflow_parent = {}
    atp_to_name = {}
    name_to_atp = {"reference classification needed": "ATP:0000166"}
    for atp in workflow_children.keys():
        atp_to_name[atp] = atp
        name_to_atp[atp] = atp
        for atp2 in workflow_children[atp]:
            workflow_parent[atp2] = atp
            name_to_atp[atp2] = atp2
            atp_to_name[atp2] = atp2
    atp_to_name["ATP:0000166"] = "reference classification needed"
    set_globals(atp_to_name, name_to_atp, workflow_children, workflow_parent)


def mock_get_jobs_to_run(name: str, mod_abbreviation: str): # noqa
    results = {'reference classification': ['ATP:0000166',
                                            'ATP:task1_needed',
                                            'ATP:task2_needed'],
               'ATP:task3_needed': ['ATP:task3_needed'],
               'ATP:NEW': ['ATP:NEW']
               }
    return results[name]


def workflow_automation_init(db):  # noqa
    print("workflow_automation_init")
    test_data = [
        # [transition_from, transition_to, actions, condition]
        # ATP:0000141 is file upload needed and hard coded in
        ["ATP:top", "ATP:0000141", [], None],
        ["ATP:0000141", "ATP:fileuploadinprogress", [], 'on_start'],
        ["ATP:fileuploadinprogress",
         "ATP:fileuploadcomplete",
         ["proceed_on_value::category::thesis::reference classification",
          "proceed_on_value::category::failure::ATP:task3_needed",
          "proceed_on_value::reference_type::Experimental::ATP:NEW"],
         'on_success'],
        ["ATP:fileuploadinprogress", "ATP:fileuploadfailed", [], 'on_failed'],
        ["ATP:needed", "ATP:task1_needed", None, "task1_job"],
        ["ATP:needed", "ATP:task2_needed", None, "task2_job"],
        ["ATP:needed", "ATP:task3_needed", None, "task3_job"],

        ["ATP:task1_needed", "ATP:task1_in_progress", ["sub_task_in_progress::reference classification"], "on_start"],
        ["ATP:task1_in_progress", "ATP:task1_successful", ["sub_task_complete::reference classification"], "on_success"],
        ["ATP:task1_failed", "ATP:task1_failed", ["sub_task_failed::reference classification"], "on_failed"],
        ["ATP:task1_in_progress", "ATP:task1_needed", ["sub_task_retry::reference classification"], "on_retry"],
        ["ATP:task1_failed", "ATP:task1_needed", ["sub_task_retry::reference classification"], "on_retry"],

        ["ATP:task2_needed", "ATP:task2_in_progress", ["sub_task_in_progress::reference classification"], "on_start"],
        ["ATP:task2_in_progress", "ATP:task2_successful", ["sub_task_complete::reference classification"], "on_success"],
        ["ATP:task2_in_progress", "ATP:task2_failed", ["sub_task_failed::reference classification"], "on_failed"],
        ["ATP:task2_in_progress", "ATP:task2_needed", ["sub_task_retry::reference classification"], "on_retry"],
        ["ATP:task2_failed", "ATP:task2_needed", ["sub_task_retry::reference classification"], "on_retry"]
    ]
    mods = db.query(ModModel).all()

    for data in test_data:
        for mod in mods:
            db.add(WorkflowTransitionModel(mod_id=mod.mod_id,
                                           transition_from=data[0],
                                           transition_to=data[1],
                                           actions=data[2],
                                           condition=data[3]))
    db.commit()
    return


class TestWorkflowTagAutomation:
    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
           mock_load_name_to_atp_and_relationships)
    @patch("agr_literature_service.api.crud.workflow_transition_actions.proceed_on_value.get_workflow_tags_for_mod", mock_get_jobs_to_run)
    def test_transition_actions(self, db, auth_headers, test_mod, test_reference):  # noqa
        print("test_transition_actions")
        mod = db.query(ModModel).filter(ModModel.abbreviation == test_mod.new_mod_abbreviation).one()
        reference = db.query(ReferenceModel).filter(ReferenceModel.curie == test_reference.new_ref_curie).one()
        workflow_automation_init(db)

        with TestClient(app) as client:
            mock_load_name_to_atp_and_relationships()
            populate_test_mods()
            response = client.get(url="/workflow_tag/get_name/ATP:fileupload", headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK

            # Set initial workflow tag to "ATP:0000141" , hard coded so allowed
            ref_type = ReferencetypeModel(label="Experimental")
            db.add(ref_type)
            db.commit()
            ref_type = db.query(ReferencetypeModel).filter(ReferencetypeModel.label == "Experimental").one()
            mod_ref_type = ModReferencetypeAssociationModel(referencetype_id=ref_type.referencetype_id, mod_id=mod.mod_id, display_order=1)
            db.add(mod_ref_type)
            db.commit()
            new_mod_ref_type = {
                "reference_curie": reference.curie,
                "reference_type": "Experimental",
                "mod_abbreviation": mod.abbreviation
            }
            client.post(url="/reference/mod_reference_type/", json=new_mod_ref_type, headers=auth_headers)

            transition_req = {
                "curie_or_reference_id": reference.curie,
                "mod_abbreviation": mod.abbreviation,
                "new_workflow_tag_atp_id": "ATP:0000141",
                "transition_type": 'automated'
            }
            response = client.post(url="/workflow_tag/transition_to_workflow_status", json=transition_req,
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK

            # Test actions by transitioning from "ATP:0000141" to "ATP:fileuploadinprogress"
            transition_req = {
                "curie_or_reference_id": reference.curie,
                "mod_abbreviation": mod.abbreviation,
                "new_workflow_tag_atp_id": "ATP:fileuploadinprogress",
                "transition_type": 'automated'
            }
            response = client.post(url="/workflow_tag/transition_to_workflow_status", json=transition_req,
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK

            transition_req = {
                "curie_or_reference_id": reference.curie,
                "mod_abbreviation": mod.abbreviation,
                "new_workflow_tag_atp_id": "ATP:fileuploadcomplete"
            }
            response = client.post(url="/workflow_tag/transition_to_workflow_status", json=transition_req,
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            # So we should have "ATP:0000166", "ATP:task1_needed"," ATP:task2_needed"
            # all set for this mod and reference
            wft = {}
            for atp in ["ATP:0000166", "ATP:task1_needed", "ATP:task2_needed", "ATP:NEW"]:
                wft[atp] = db.query(WorkflowTagModel).\
                    filter(WorkflowTagModel.workflow_tag_id == atp,
                           WorkflowTagModel.reference_id == reference.reference_id,
                           WorkflowTagModel.mod_id == mod.mod_id).one()
                assert wft[atp].reference_workflow_tag_id

            # Check the get_jobs url
            response = client.get(url="/workflow_tag/jobs/task1_job",
                                  headers=auth_headers)
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
                response = client.post(url=f"/workflow_tag/job/started/{wft[atp].reference_workflow_tag_id}",
                                       headers=auth_headers)
                assert response.status_code == status.HTTP_200_OK

                test_id = db.query(WorkflowTagModel). \
                    filter(WorkflowTagModel.workflow_tag_id == atp,
                           WorkflowTagModel.reference_id == reference.reference_id,
                           WorkflowTagModel.mod_id == mod.mod_id).one_or_none()
                assert test_id is None

            for atp in ["ATP:0000178", "ATP:task1_in_progress", "ATP:task2_in_progress"]:
                test_id = db.query(WorkflowTagModel). \
                    filter(WorkflowTagModel.workflow_tag_id == atp,
                           WorkflowTagModel.reference_id == reference.reference_id,
                           WorkflowTagModel.mod_id == mod.mod_id).one_or_none()
                assert test_id

            # set task1 and task2 to successful.
            for atp in ["ATP:task1_in_progress", "ATP:task2_in_progress"]:
                # id used for jobs should still be the same
                old_atp = atp.replace("in_progress", "needed")
                response = client.post(url=f"/workflow_tag/job/success/{wft[old_atp].reference_workflow_tag_id}",
                                       headers=auth_headers)
                assert response.status_code == status.HTTP_200_OK

            # When we know the hierarchy we can add main back in testing
            # for atp in ["ATP:task1_successful", "ATP:task2_successful", "ATP:main_successful"]:
            for atp in ["ATP:0000169", "ATP:task1_successful", "ATP:task2_successful"]:
                test_id = db.query(WorkflowTagModel).\
                    filter(WorkflowTagModel.workflow_tag_id == atp,
                           WorkflowTagModel.reference_id == reference.reference_id,
                           WorkflowTagModel.mod_id == mod.mod_id).one_or_none()
                assert test_id

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
           mock_load_name_to_atp_and_relationships)
    def test_transition_work_failed(self, db, auth_headers, test_mod, test_reference):  # noqa
        with TestClient(app) as client:
            populate_test_mods()
            mock_load_name_to_atp_and_relationships()
            mod = db.query(ModModel).filter(ModModel.abbreviation == test_mod.new_mod_abbreviation).one()
            workflow_automation_init(db)

            reference = db.query(ReferenceModel).filter(ReferenceModel.curie == test_reference.new_ref_curie).one()

            # Set initial workflow tag to "ATP:XXXX_in_progress"
            atp_to_ref_wft_id = {}
            for atp in ["ATP:0000178", "ATP:task1_in_progress", "ATP:task2_in_progress"]:
                new_wft = {"reference_curie": reference.curie,
                           "mod_abbreviation": mod.abbreviation,
                           "workflow_tag_id": atp,
                           }
                response = client.post(url="/workflow_tag/", json=new_wft, headers=auth_headers)
                assert response.status_code == status.HTTP_201_CREATED
                atp_to_ref_wft_id[atp] = response.json()
                assert atp_to_ref_wft_id[atp]

            # set task1 to success BUT task2 to failure
            response = client.post(url=f"/workflow_tag/job/success/{atp_to_ref_wft_id['ATP:task1_in_progress']}",
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK

            response = client.post(url=f"/workflow_tag/job/failed/{atp_to_ref_wft_id['ATP:task2_in_progress']}",
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK

            # we should have "ATP:task1_successful", "ATP:task2_failed" and "ATP:0000189"
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

            # test main which should be failed
            test_id = db.query(WorkflowTagModel). \
                filter(WorkflowTagModel.workflow_tag_id == 'ATP:0000189',
                       WorkflowTagModel.reference_id == reference.reference_id,
                       WorkflowTagModel.mod_id == mod.mod_id).one_or_none()
            assert test_id

            # set the job to retry for task2
            response = client.post(url=f"/workflow_tag/job/retry/{atp_to_ref_wft_id['ATP:task2_in_progress']}",
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK

            test_id = db.query(WorkflowTagModel). \
                filter(WorkflowTagModel.workflow_tag_id == 'ATP:task2_needed',
                       WorkflowTagModel.reference_id == reference.reference_id,
                       WorkflowTagModel.mod_id == mod.mod_id).one_or_none()
            assert test_id

            # test main which should now be in_progress
            test_id = db.query(WorkflowTagModel). \
                filter(WorkflowTagModel.workflow_tag_id == 'ATP:0000178',
                       WorkflowTagModel.reference_id == reference.reference_id,
                       WorkflowTagModel.mod_id == mod.mod_id).one_or_none()
            assert test_id

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
           mock_load_name_to_atp_and_relationships)

    def test_bad_transitions(self, db, auth_headers, test_mod, test_reference):  # noqa
        with TestClient(app) as client:
            mock_load_name_to_atp_and_relationships()
            populate_test_mods()

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
                "new_workflow_tag_atp_id": "ATP:0000166"
            }
            response = client.post(url="/workflow_tag/transition_to_workflow_status", json=transition_req,
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND
            assert response.json().get("detail") == 'Mod abbreviation BadMod does not exist'

            # Bad curie
            transition_req = {
                "curie_or_reference_id": "MadeUpCurie",
                "mod_abbreviation": mod.abbreviation,
                "new_workflow_tag_atp_id": "ATP:0000166"
            }
            response = client.post(url="/workflow_tag/transition_to_workflow_status", json=transition_req,
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND
            # Now do transition NOT in the transition table.
            transition_req = {
                "curie_or_reference_id": test_reference.new_ref_curie,
                "mod_abbreviation": mod.abbreviation,
                "new_workflow_tag_atp_id": "ATP:fileuploadcomplete"
            }
            response = client.post(url="/workflow_tag/transition_to_workflow_status", json=transition_req,
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
            assert response.json().get("detail") == 'Transition to ATP:fileuploadcomplete not allowed as not initial state.'

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
           load_name_to_atp_and_relationships_mock)
    def test_first_pass_curation_manual_transition(self, db, auth_headers, test_mod, test_reference):  # noqa
        """First pass curation transitions are manual_only: an automated attempt is
        rejected, but a manual transition between two states succeeds (SCRUM-5478)."""
        load_name_to_atp_and_relationships_mock()
        mod = db.query(ModModel).filter(ModModel.abbreviation == test_mod.new_mod_abbreviation).one()
        reference = db.query(ReferenceModel).filter(ReferenceModel.curie == test_reference.new_ref_curie).one()
        # first pass curation needed -> first pass curation in progress (manual only)
        db.add(WorkflowTransitionModel(mod=mod, transition_from='ATP:0000331', transition_to='ATP:0000332',
                                       transition_type='manual_only'))
        db.add(WorkflowTagModel(reference=reference, mod=mod, workflow_tag_id='ATP:0000331'))
        db.commit()
        with TestClient(app) as client:
            # an automated transition must be rejected for a manual_only row
            automated_req = {
                "curie_or_reference_id": reference.curie,
                "mod_abbreviation": mod.abbreviation,
                "new_workflow_tag_atp_id": "ATP:0000332",
                "transition_type": "automated",
            }
            response = client.post(url="/workflow_tag/transition_to_workflow_status", json=automated_req,
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            # a manual transition succeeds
            manual_req = {
                "curie_or_reference_id": reference.curie,
                "mod_abbreviation": mod.abbreviation,
                "new_workflow_tag_atp_id": "ATP:0000332",
                "transition_type": "manual",
            }
            response = client.post(url="/workflow_tag/transition_to_workflow_status", json=manual_req,
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert db.query(WorkflowTagModel).filter(
                WorkflowTagModel.reference_id == reference.reference_id,
                WorkflowTagModel.mod_id == mod.mod_id,
                WorkflowTagModel.workflow_tag_id == 'ATP:0000332'
            ).first()

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
           load_name_to_atp_and_relationships_mock)
    def test_first_pass_curation_tbd_set_on_entity_extraction_rollup(self, db, auth_headers, test_reference):  # noqa
        """When the last extraction subtask completes and entity extraction rolls up to
        complete (via sub_task_complete), and reference + curation classification are
        already complete, first pass curation TBD (ATP:0000371) is seeded for FB."""
        load_name_to_atp_and_relationships_mock()
        fb = _get_or_create_fb_mod(db)
        reference = db.query(ReferenceModel).filter(ReferenceModel.curie == test_reference.new_ref_curie).one()
        # main entity extraction in progress + the other two prerequisite completes
        db.add(WorkflowTagModel(reference=reference, mod=fb, workflow_tag_id='ATP:0000190'))
        db.add(WorkflowTagModel(reference=reference, mod=fb, workflow_tag_id='ATP:0000169'))
        db.add(WorkflowTagModel(reference=reference, mod=fb, workflow_tag_id='ATP:0000312'))
        subtask = WorkflowTagModel(reference=reference, mod=fb, workflow_tag_id='ATP:0000214')
        db.add(subtask)
        db.commit()

        sub_task_complete(db, subtask, ['entity extraction'])
        db.commit()

        # main entity extraction rolled up to complete
        assert db.query(WorkflowTagModel).filter(
            WorkflowTagModel.reference_id == reference.reference_id,
            WorkflowTagModel.mod_id == fb.mod_id,
            WorkflowTagModel.workflow_tag_id == 'ATP:0000174'
        ).first()
        # and first pass curation TBD was seeded exactly once
        assert db.query(WorkflowTagModel).filter(
            WorkflowTagModel.reference_id == reference.reference_id,
            WorkflowTagModel.mod_id == fb.mod_id,
            WorkflowTagModel.workflow_tag_id == 'ATP:0000371'
        ).count() == 1

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
           load_name_to_atp_and_relationships_mock)
    def test_first_pass_curation_tbd_not_set_when_incomplete(self, db, auth_headers, test_reference):  # noqa
        """TBD is NOT seeded when only two of the three prerequisites are complete, even
        after entity extraction rolls up to complete."""
        load_name_to_atp_and_relationships_mock()
        fb = _get_or_create_fb_mod(db)
        reference = db.query(ReferenceModel).filter(ReferenceModel.curie == test_reference.new_ref_curie).one()
        # curation classification complete (ATP:0000312) is missing
        db.add(WorkflowTagModel(reference=reference, mod=fb, workflow_tag_id='ATP:0000190'))
        db.add(WorkflowTagModel(reference=reference, mod=fb, workflow_tag_id='ATP:0000169'))
        subtask = WorkflowTagModel(reference=reference, mod=fb, workflow_tag_id='ATP:0000214')
        db.add(subtask)
        db.commit()

        sub_task_complete(db, subtask, ['entity extraction'])
        db.commit()

        assert db.query(WorkflowTagModel).filter(
            WorkflowTagModel.reference_id == reference.reference_id,
            WorkflowTagModel.mod_id == fb.mod_id,
            WorkflowTagModel.workflow_tag_id == 'ATP:0000174'
        ).first()  # entity extraction still rolled up to complete
        assert db.query(WorkflowTagModel).filter(
            WorkflowTagModel.reference_id == reference.reference_id,
            WorkflowTagModel.mod_id == fb.mod_id,
            WorkflowTagModel.workflow_tag_id == 'ATP:0000371'
        ).first() is None

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
           load_name_to_atp_and_relationships_mock)
    def test_first_pass_curation_tbd_not_set_for_non_fb(self, db, auth_headers, test_mod, test_reference):  # noqa
        """First pass curation is FlyBase-only: a non-FB MOD never gets a TBD tag even when
        all three prerequisites are complete and entity extraction rolls up to complete."""
        load_name_to_atp_and_relationships_mock()
        mod = db.query(ModModel).filter(ModModel.abbreviation == test_mod.new_mod_abbreviation).one()
        reference = db.query(ReferenceModel).filter(ReferenceModel.curie == test_reference.new_ref_curie).one()
        db.add(WorkflowTagModel(reference=reference, mod=mod, workflow_tag_id='ATP:0000190'))
        db.add(WorkflowTagModel(reference=reference, mod=mod, workflow_tag_id='ATP:0000169'))
        db.add(WorkflowTagModel(reference=reference, mod=mod, workflow_tag_id='ATP:0000312'))
        subtask = WorkflowTagModel(reference=reference, mod=mod, workflow_tag_id='ATP:0000214')
        db.add(subtask)
        db.commit()

        sub_task_complete(db, subtask, ['entity extraction'])
        db.commit()

        assert db.query(WorkflowTagModel).filter(
            WorkflowTagModel.reference_id == reference.reference_id,
            WorkflowTagModel.mod_id == mod.mod_id,
            WorkflowTagModel.workflow_tag_id == 'ATP:0000174'
        ).first()  # rolls up to complete regardless of MOD
        assert db.query(WorkflowTagModel).filter(
            WorkflowTagModel.reference_id == reference.reference_id,
            WorkflowTagModel.mod_id == mod.mod_id,
            WorkflowTagModel.workflow_tag_id == 'ATP:0000371'
        ).first() is None

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
           load_name_to_atp_and_relationships_mock)
    def test_first_pass_curation_tbd_idempotent(self, db, auth_headers, test_reference):  # noqa
        """The action is idempotent: repeated calls add no duplicate, because the seeded
        TBD tag is itself a first pass curation tag and trips the idempotency guard."""
        load_name_to_atp_and_relationships_mock()
        fb = _get_or_create_fb_mod(db)
        reference = db.query(ReferenceModel).filter(ReferenceModel.curie == test_reference.new_ref_curie).one()
        for atp in ('ATP:0000174', 'ATP:0000169', 'ATP:0000312'):
            db.add(WorkflowTagModel(reference=reference, mod=fb, workflow_tag_id=atp))
        db.commit()
        trigger = db.query(WorkflowTagModel).filter(
            WorkflowTagModel.reference_id == reference.reference_id,
            WorkflowTagModel.mod_id == fb.mod_id,
            WorkflowTagModel.workflow_tag_id == 'ATP:0000312'
        ).one()

        set_first_pass_curation_tbd(db, trigger, [])
        db.commit()
        set_first_pass_curation_tbd(db, trigger, [])
        db.commit()
        assert db.query(WorkflowTagModel).filter(
            WorkflowTagModel.reference_id == reference.reference_id,
            WorkflowTagModel.mod_id == fb.mod_id,
            WorkflowTagModel.workflow_tag_id == 'ATP:0000371'
        ).count() == 1
