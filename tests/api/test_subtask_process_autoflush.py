"""Sub task actions see the sub task's new state with autoflush off (SCRUM-6828).

The lit_processing sessions (create_postgres_session) have autoflush off. When the
weekly check_wft_in_progress cron retried a failed FB gene extraction, job_change_atp_code
set the sub task back to needed in memory, but sub_task_retry's query for failed sub tasks
still read "failed" from the database, so the main entity extraction went back to failed.
The retried job then completed and the main task stayed failed for good: 211 FB papers on
prod. The tests run job_change_atp_code the way the cron does, with autoflush off.
"""
from unittest.mock import patch

from agr_literature_service.api.crud.workflow_tag_crud import job_change_atp_code
from agr_literature_service.api.models import ModModel, ReferenceModel, WorkflowTagModel
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa
from .test_mod import test_mod  # noqa
from .test_reference import test_reference  # noqa
from .test_workflow_automation import mock_load_name_to_atp_and_relationships, workflow_automation_init

MAIN_IN_PROGRESS = "ATP:0000178"
MAIN_FAILED = "ATP:0000189"
MAIN_COMPLETE = "ATP:0000169"


def _setup(db, test_mod, test_reference, main, **sub_tasks):  # noqa
    mock_load_name_to_atp_and_relationships()
    workflow_automation_init(db)
    mod = db.query(ModModel).filter(ModModel.abbreviation == test_mod.new_mod_abbreviation).one()
    reference = db.query(ReferenceModel).filter(ReferenceModel.curie == test_reference.new_ref_curie).one()
    db.add(WorkflowTagModel(reference=reference, mod=mod, workflow_tag_id=main))
    tags = {}
    for name, workflow_tag_id in sub_tasks.items():
        tags[name] = WorkflowTagModel(reference=reference, mod=mod, workflow_tag_id=workflow_tag_id)
        db.add(tags[name])
    db.commit()
    db.autoflush = False  # as in create_postgres_session
    return reference, mod, tags


def _main(db, reference, mod):  # noqa
    return db.query(WorkflowTagModel).filter(
        WorkflowTagModel.reference_id == reference.reference_id,
        WorkflowTagModel.mod_id == mod.mod_id,
        WorkflowTagModel.workflow_tag_id.in_([MAIN_IN_PROGRESS, MAIN_FAILED, MAIN_COMPLETE])).one().workflow_tag_id


@patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
       mock_load_name_to_atp_and_relationships)
class TestSubTasksWithAutoflushOff:

    def test_retrying_the_only_failed_sub_task_clears_the_main_failure(self, db, test_mod, test_reference):  # noqa
        reference, mod, tags = _setup(db, test_mod, test_reference, MAIN_FAILED, task1="ATP:task1_failed")
        try:
            job_change_atp_code(db, tags["task1"].reference_workflow_tag_id, "on_retry")
            assert tags["task1"].workflow_tag_id == "ATP:task1_needed"
            assert _main(db, reference, mod) == MAIN_IN_PROGRESS
        finally:
            db.autoflush = True

    def test_main_stays_failed_while_another_sub_task_is_failed(self, db, test_mod, test_reference):  # noqa
        reference, mod, tags = _setup(db, test_mod, test_reference, MAIN_FAILED,
                                      task1="ATP:task1_failed", task2="ATP:task2_failed")
        try:
            job_change_atp_code(db, tags["task1"].reference_workflow_tag_id, "on_retry")
            assert _main(db, reference, mod) == MAIN_FAILED
        finally:
            db.autoflush = True

    def test_completing_the_last_sub_task_completes_the_main_task(self, db, test_mod, test_reference):  # noqa
        reference, mod, tags = _setup(db, test_mod, test_reference, MAIN_IN_PROGRESS,
                                      task1="ATP:task1_in_progress")
        try:
            job_change_atp_code(db, tags["task1"].reference_workflow_tag_id, "on_success")
            assert tags["task1"].workflow_tag_id == "ATP:task1_successful"
            assert _main(db, reference, mod) == MAIN_COMPLETE
        finally:
            db.autoflush = True

    def test_main_is_not_completed_while_another_sub_task_runs(self, db, test_mod, test_reference):  # noqa
        reference, mod, tags = _setup(db, test_mod, test_reference, MAIN_IN_PROGRESS,
                                      task1="ATP:task1_in_progress", task2="ATP:task2_in_progress")
        try:
            job_change_atp_code(db, tags["task1"].reference_workflow_tag_id, "on_success")
            assert _main(db, reference, mod) == MAIN_IN_PROGRESS
        finally:
            db.autoflush = True
