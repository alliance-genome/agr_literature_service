from datetime import datetime

import pytest
from sqlalchemy import event

import agr_literature_service.api.resource_descriptor_cache as _rdc
from agr_literature_service.api.models import ModModel, WorkflowTransitionModel

FILE_UPLOAD_PROCESS = "ATP:0000140"
FILE_NEEDED = "ATP:0000141"


@pytest.fixture(autouse=True)
def _isolate_resource_descriptor_cache(monkeypatch):
    """Keep the process-global descriptor cache hermetic: never call A-team in
    tests, and reset between tests. Tests that need descriptors call
    resource_descriptor_cache._seed([...])."""
    _rdc._reset()
    monkeypatch.setattr(_rdc, "_fetch", lambda: [])
    yield
    _rdc._reset()


def _add_corpus_entry_transition(mapper, connection, mod):
    connection.execute(WorkflowTransitionModel.__table__.insert().values(
        mod_id=mod.mod_id, transition_from=FILE_UPLOAD_PROCESS, transition_to=FILE_NEEDED,
        transition_type="any", date_created=datetime.utcnow()))


@pytest.fixture(autouse=True)
def _corpus_entry_transition_for_every_mod():
    """Every MOD inserted through the ORM gets the file upload -> file needed row, as every
    production MOD has (SCRUM-6845): corpus entry sets "file needed" through
    transition_to_workflow_status, which refuses a first state without a row from the
    process term. A test about a MOD without that row deletes it."""
    event.listen(ModModel, "after_insert", _add_corpus_entry_transition)
    yield
    event.remove(ModModel, "after_insert", _add_corpus_entry_transition)
