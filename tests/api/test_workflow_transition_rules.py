"""
SCRUM-6845: every workflow tag change goes through a row of the transition table.

- A paper's first state in a process needs a row from the process term to that state.
- A transition to the process term itself removes the paper's tag, and needs a row too.
- Rows of type 'action' are job-poll connectors and are never fired.
- transition_to_workflow_status sets curation_tag/note, can skip actions (internal callers)
  and can flush instead of commit.
"""
from unittest.mock import patch

import pytest
from fastapi import HTTPException, status
from starlette.testclient import TestClient

from agr_literature_service.api.crud import topic_entity_tag_crud as tet_crud
from agr_literature_service.api.crud import workflow_tag_crud as wtc
from agr_literature_service.api.main import app
from agr_literature_service.api.models import ModModel, ReferenceModel, WorkflowTagModel, WorkflowTransitionModel
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from ..fixtures import load_name_to_atp_and_relationships_mock
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa

ATP_MOCK = "agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships"
MANUAL_INDEXING = "ATP:0000273"
MI_NEEDED = "ATP:0000274"
MI_COMPLETE = "ATP:0000275"
MI_WONT = "ATP:0000343"
FILE_UPLOAD = "ATP:0000140"
FILE_NEEDED = "ATP:0000141"
TEXT_CONVERSION_NEEDED = "ATP:0000162"
CONVERTED_TO_TEXT = "ATP:0000163"
TEXT_CONVERSION_IN_PROGRESS = "ATP:0000198"
REFERENCE_CURIE = "AGRKB:101000300001"


@pytest.fixture
def wb_ref(db):  # noqa
    load_name_to_atp_and_relationships_mock()
    populate_test_mods()
    mod = db.query(ModModel).filter(ModModel.abbreviation == "WB").one()
    ref = ReferenceModel(curie=REFERENCE_CURIE, category="research_article")
    db.add(ref)
    db.commit()
    db.refresh(ref)
    return db, mod, ref


def _row(db, mod, transition_from, transition_to, **kwargs):  # noqa
    db.add(WorkflowTransitionModel(mod_id=mod.mod_id, transition_from=transition_from,
                                   transition_to=transition_to, **kwargs))
    db.commit()


def _tag(db, ref, mod, workflow_tag_id):  # noqa
    tag = WorkflowTagModel(reference_id=ref.reference_id, mod_id=mod.mod_id, workflow_tag_id=workflow_tag_id)
    db.add(tag)
    db.commit()
    db.refresh(tag)
    return tag


def _tags(db, ref, mod):  # noqa
    db.expire_all()
    return sorted(t.workflow_tag_id for t in db.query(WorkflowTagModel).filter(
        WorkflowTagModel.reference_id == ref.reference_id, WorkflowTagModel.mod_id == mod.mod_id).all())


@patch(ATP_MOCK, load_name_to_atp_and_relationships_mock)
class TestFirstState:

    def test_first_state_needs_a_row_from_the_process_term(self, wb_ref):  # noqa
        db, mod, ref = wb_ref  # noqa
        with pytest.raises(HTTPException) as exc:
            wtc.transition_to_workflow_status(db, REFERENCE_CURIE, "WB", MI_NEEDED)
        assert exc.value.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "no row from ATP:0000273" in exc.value.detail
        assert _tags(db, ref, mod) == []

        _row(db, mod, MANUAL_INDEXING, MI_NEEDED)
        wtc.transition_to_workflow_status(db, REFERENCE_CURIE, "WB", MI_NEEDED)
        assert _tags(db, ref, mod) == [MI_NEEDED]

    def test_no_state_is_seeded_before_the_first_one(self, wb_ref):  # noqa
        """The old code added "manual indexing needed" silently before any other first state."""
        db, mod, ref = wb_ref  # noqa
        _row(db, mod, MANUAL_INDEXING, MI_WONT)
        _row(db, mod, MI_NEEDED, MI_WONT)
        wtc.transition_to_workflow_status(db, REFERENCE_CURIE, "WB", MI_WONT)
        assert _tags(db, ref, mod) == [MI_WONT]

    def test_file_needed_is_not_special(self, wb_ref):  # noqa
        """Corpus entry's "file needed" comes from the file upload -> file needed row that every
        MOD has (tests/conftest.py adds it); without the row it is refused like any state."""
        db, mod, ref = wb_ref  # noqa
        wtc.transition_to_workflow_status(db, REFERENCE_CURIE, "WB", FILE_NEEDED)
        assert _tags(db, ref, mod) == [FILE_NEEDED]

        db.query(WorkflowTagModel).delete()
        db.query(WorkflowTransitionModel).filter(WorkflowTransitionModel.transition_from == FILE_UPLOAD).delete()
        db.commit()
        with pytest.raises(HTTPException):
            wtc.transition_to_workflow_status(db, REFERENCE_CURIE, "WB", FILE_NEEDED)
        assert _tags(db, ref, mod) == []

    def test_a_state_held_by_the_paper_uses_the_row_from_that_state(self, wb_ref):  # noqa
        db, mod, ref = wb_ref  # noqa
        _row(db, mod, MANUAL_INDEXING, MI_COMPLETE)  # only a first-state row: not usable from 274
        _tag(db, ref, mod, MI_NEEDED)
        with pytest.raises(HTTPException) as exc:
            wtc.transition_to_workflow_status(db, REFERENCE_CURIE, "WB", MI_COMPLETE)
        assert "from ATP:0000274 to ATP:0000275" in exc.value.detail
        assert _tags(db, ref, mod) == [MI_NEEDED]


@patch(ATP_MOCK, load_name_to_atp_and_relationships_mock)
class TestRemoval:

    def test_transition_to_the_process_term_removes_the_tag(self, wb_ref):  # noqa
        db, mod, ref = wb_ref  # noqa
        _row(db, mod, MI_COMPLETE, MANUAL_INDEXING)
        _tag(db, ref, mod, MI_COMPLETE)
        _tag(db, ref, mod, FILE_NEEDED)  # other processes are untouched
        wtc.transition_to_workflow_status(db, REFERENCE_CURIE, "WB", MANUAL_INDEXING)
        assert _tags(db, ref, mod) == [FILE_NEEDED]

    def test_removal_needs_a_row(self, wb_ref):  # noqa
        db, mod, ref = wb_ref  # noqa
        _row(db, mod, MI_NEEDED, MANUAL_INDEXING)  # from another state only
        _tag(db, ref, mod, MI_COMPLETE)
        with pytest.raises(HTTPException) as exc:
            wtc.transition_to_workflow_status(db, REFERENCE_CURIE, "WB", MANUAL_INDEXING)
        assert exc.value.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert _tags(db, ref, mod) == [MI_COMPLETE]

    def test_removal_without_a_tag_is_refused(self, wb_ref):  # noqa
        db, mod, ref = wb_ref  # noqa
        _row(db, mod, MI_COMPLETE, MANUAL_INDEXING)
        with pytest.raises(HTTPException) as exc:
            wtc.transition_to_workflow_status(db, REFERENCE_CURIE, "WB", MANUAL_INDEXING)
        assert "no tag in it" in exc.value.detail

    def test_removal_over_the_api(self, wb_ref, auth_headers):  # noqa
        db, mod, ref = wb_ref  # noqa
        _row(db, mod, MI_COMPLETE, MANUAL_INDEXING, transition_type="manual_only")
        _tag(db, ref, mod, MI_COMPLETE)
        with TestClient(app) as client:
            response = client.post(url="/workflow_tag/transition_to_workflow_status", headers=auth_headers,
                                   json={"curie_or_reference_id": REFERENCE_CURIE, "mod_abbreviation": "WB",
                                         "new_workflow_tag_atp_id": MANUAL_INDEXING,
                                         "transition_type": "manual"})
            assert response.status_code == status.HTTP_200_OK
        assert _tags(db, ref, mod) == []


@patch(ATP_MOCK, load_name_to_atp_and_relationships_mock)
class TestActionRowsAreNotCallable:

    def test_transition_to_workflow_status_skips_action_rows(self, wb_ref):  # noqa
        db, mod, ref = wb_ref  # noqa
        _row(db, mod, MANUAL_INDEXING, MI_NEEDED, transition_type=wtc.NON_CALLABLE_TRANSITION_TYPE)
        with pytest.raises(HTTPException):
            wtc.transition_to_workflow_status(db, REFERENCE_CURIE, "WB", MI_NEEDED)
        assert _tags(db, ref, mod) == []

    def test_job_change_atp_code_skips_action_rows(self, wb_ref):  # noqa
        """Both rows match on_success; only the callable one may fire."""
        db, mod, ref = wb_ref  # noqa
        _row(db, mod, TEXT_CONVERSION_NEEDED, CONVERTED_TO_TEXT, condition="on_success")
        _row(db, mod, TEXT_CONVERSION_NEEDED, TEXT_CONVERSION_IN_PROGRESS,
             condition="text_convert_job,on_success", transition_type=wtc.NON_CALLABLE_TRANSITION_TYPE)
        tag = _tag(db, ref, mod, TEXT_CONVERSION_NEEDED)
        wtc.job_change_atp_code(db, tag.reference_workflow_tag_id, "on_success")
        assert _tags(db, ref, mod) == [CONVERTED_TO_TEXT]


@patch(ATP_MOCK, load_name_to_atp_and_relationships_mock)
class TestTransitionOptions:

    def test_curation_tag_and_note_are_set_and_kept(self, wb_ref, auth_headers):  # noqa
        db, mod, ref = wb_ref  # noqa
        _row(db, mod, MANUAL_INDEXING, MI_WONT)
        _row(db, mod, MI_WONT, MI_COMPLETE)
        with TestClient(app) as client:
            response = client.post(url="/workflow_tag/transition_to_workflow_status", headers=auth_headers,
                                   json={"curie_or_reference_id": REFERENCE_CURIE, "mod_abbreviation": "WB",
                                         "new_workflow_tag_atp_id": MI_WONT, "curation_tag": "ATP:0000226",
                                         "note": "molecular probe paper"})
            assert response.status_code == status.HTTP_200_OK
        wtc.transition_to_workflow_status(db, REFERENCE_CURIE, "WB", MI_COMPLETE)
        db.expire_all()
        tag = db.query(WorkflowTagModel).filter(WorkflowTagModel.reference_id == ref.reference_id).one()
        assert (tag.workflow_tag_id, tag.curation_tag, tag.note) == \
            (MI_COMPLETE, "ATP:0000226", "molecular probe paper")

    def test_run_actions_false_skips_the_actions(self, wb_ref):  # noqa
        db, mod, ref = wb_ref  # noqa
        _row(db, mod, MANUAL_INDEXING, MI_NEEDED, actions=["proceed_on_value::all::ATP:0000162"])
        _row(db, mod, MI_NEEDED, MI_COMPLETE, actions=["proceed_on_value::all::ATP:0000162"])
        with patch.object(wtc, "process_transition_actions") as actions:
            wtc.transition_to_workflow_status(db, REFERENCE_CURIE, "WB", MI_NEEDED, run_actions=False)
            actions.assert_not_called()
            wtc.transition_to_workflow_status(db, REFERENCE_CURIE, "WB", MI_COMPLETE)
            actions.assert_called_once()
        assert _tags(db, ref, mod) == [MI_COMPLETE]

    def test_commit_false_leaves_the_transaction_to_the_caller(self, wb_ref):  # noqa
        db, mod, ref = wb_ref  # noqa
        _row(db, mod, MANUAL_INDEXING, MI_NEEDED)
        wtc.transition_to_workflow_status(db, REFERENCE_CURIE, "WB", MI_NEEDED, commit=False)
        db.rollback()
        assert _tags(db, ref, mod) == []


@patch(ATP_MOCK, load_name_to_atp_and_relationships_mock)
class TestTopicEntityTagPaths:
    """The manual indexing status a curator sets with a TET (index_wft) or with "no data", and
    the file needed a TET's corpus entry adds, go through transitions."""

    def test_no_data_puts_the_paper_in_the_corpus_and_sets_complete(self, wb_ref):  # noqa
        db, mod, ref = wb_ref  # noqa
        _row(db, mod, MANUAL_INDEXING, MI_COMPLETE)
        tet_crud.set_indexing_status_for_no_tet_data(db, "WB", REFERENCE_CURIE, None)
        assert _tags(db, ref, mod) == [FILE_NEEDED, MI_COMPLETE]

    def test_index_status_without_a_row_is_refused(self, wb_ref):  # noqa
        db, mod, ref = wb_ref  # noqa
        _tag(db, ref, mod, MI_WONT)
        with pytest.raises(HTTPException):
            tet_crud.update_manual_indexing_workflow_tag(db, "WB", ref.reference_id, MI_NEEDED)
        assert _tags(db, ref, mod) == [MI_WONT]

    def test_index_status_moves_the_current_state(self, wb_ref):  # noqa
        db, mod, ref = wb_ref  # noqa
        _tag(db, ref, mod, MI_NEEDED)
        _row(db, mod, MI_NEEDED, MI_COMPLETE)
        tet_crud.update_manual_indexing_workflow_tag(db, "WB", ref.reference_id, MI_COMPLETE)
        assert _tags(db, ref, mod) == [MI_COMPLETE]

    def test_index_status_already_set_needs_no_row(self, wb_ref):  # noqa
        db, mod, ref = wb_ref  # noqa
        _tag(db, ref, mod, MI_COMPLETE)
        tet_crud.update_manual_indexing_workflow_tag(db, "WB", ref.reference_id, MI_COMPLETE)
        tet_crud.update_manual_indexing_workflow_tag(db, "WB", ref.reference_id, None)
        assert _tags(db, ref, mod) == [MI_COMPLETE]
