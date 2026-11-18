from datetime import datetime

import pytest
from sqlalchemy import event

import agr_literature_service.api.resource_descriptor_cache as _rdc
from agr_literature_service.api.models import ModModel, WorkflowTransitionModel

FILE_UPLOAD_PROCESS = "ATP:0000140"
# the states a paper's file upload workflow may start in: file needed (corpus entry), file
# upload in progress and files uploaded (a file uploaded before any file upload tag)
FILE_UPLOAD_FIRST_STATES = ("ATP:0000141", "ATP:0000139", "ATP:0000134")


@pytest.fixture(autouse=True)
def _isolate_resource_descriptor_cache(monkeypatch):
    """Keep the process-global descriptor cache hermetic: never call A-team in
    tests, and reset between tests. Tests that need descriptors call
    resource_descriptor_cache._seed([...])."""
    _rdc._reset()
    monkeypatch.setattr(_rdc, "_fetch", lambda: [])
    yield
    _rdc._reset()


def _add_file_upload_first_state_transitions(mapper, connection, mod):
    connection.execute(WorkflowTransitionModel.__table__.insert(), [
        {"mod_id": mod.mod_id, "transition_from": FILE_UPLOAD_PROCESS, "transition_to": state,
         "transition_type": "any", "date_created": datetime.utcnow()}
        for state in FILE_UPLOAD_FIRST_STATES])


@pytest.fixture(autouse=True)
def _file_upload_first_state_transitions_for_every_mod():
    """Every MOD inserted through the ORM gets the file upload -> first state rows, as every
    production MOD has (SCRUM-6845): corpus entry ("file needed") and a file upload on a paper
    without a file upload tag go through transition_to_workflow_status, which refuses a first
    state without a row from the process term. A test about a MOD without them deletes them."""
    event.listen(ModModel, "after_insert", _add_file_upload_first_state_transitions)
    yield
    event.remove(ModModel, "after_insert", _add_file_upload_first_state_transitions)
