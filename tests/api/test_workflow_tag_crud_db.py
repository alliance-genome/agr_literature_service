"""
DB-backed unit tests for workflow_tag_crud functions that run SQL directly.

The A-Team ontology lookups (get_workflow_tags_from_process, the atpterm name
map, and _ensure_atp_loaded) are mocked so no network access is needed.
"""
import pytest
from unittest.mock import patch
from fastapi import HTTPException

from agr_literature_service.api.crud import workflow_tag_crud as wtc
from agr_literature_service.api.models import (ModModel, ReferenceModel,
                                               WorkflowTagModel)
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from ..fixtures import db # noqa

HELPERS = "agr_literature_service.api.crud.workflow_tag_crud"
FILE_NEEDED = "ATP:0000141"
FILES_UPLOADED = "ATP:0000134"


def _mod(db, abbreviation): # noqa
    return db.query(ModModel).filter(ModModel.abbreviation == abbreviation).first()


def _reference(db, curie): # noqa
    ref = ReferenceModel(curie=curie, category="research_article")
    db.add(ref)
    db.commit()
    db.refresh(ref)
    return ref


def _wft(db, reference, mod, workflow_tag_id): # noqa
    tag = WorkflowTagModel(reference_id=reference.reference_id, mod_id=mod.mod_id,
                           workflow_tag_id=workflow_tag_id)
    db.add(tag)
    db.commit()
    db.refresh(tag)
    return tag


@pytest.fixture
def wb_ref(db): # noqa
    populate_test_mods()
    mod = _mod(db, "WB")
    ref = _reference(db, "AGRKB:101000200001")
    return db, mod, ref


class TestDeleteWorkflowTags:

    def test_deletes_tags_for_reference_and_mod(self, wb_ref): # noqa
        db, mod, ref = wb_ref  # noqa
        _wft(db, ref, mod, FILE_NEEDED)
        _wft(db, ref, mod, "ATP:0000139")
        wtc.delete_workflow_tags(db, ref.curie, "WB")
        remaining = db.query(WorkflowTagModel).filter(
            WorkflowTagModel.reference_id == ref.reference_id).all()
        assert remaining == []

    def test_unknown_reference_raises(self, wb_ref): # noqa
        db, _, _ = wb_ref  # noqa
        with pytest.raises(HTTPException) as exc:
            wtc.delete_workflow_tags(db, "AGRKB:000000000000", "WB")
        assert exc.value.status_code in (404, 422)

    def test_unknown_mod_raises_422(self, wb_ref): # noqa
        db, _, ref = wb_ref  # noqa
        with pytest.raises(HTTPException) as exc:
            wtc.delete_workflow_tags(db, ref.curie, "NOPE")
        assert exc.value.status_code == 422


class TestGetRefIdsWithWorkflowStatus:

    def test_filters_by_tag_and_optional_mod(self, wb_ref): # noqa
        db, mod, ref = wb_ref  # noqa
        _wft(db, ref, mod, FILE_NEEDED)
        assert wtc.get_ref_ids_with_workflow_status(db, FILE_NEEDED) == [ref.reference_id]
        assert wtc.get_ref_ids_with_workflow_status(db, FILE_NEEDED, "WB") == [ref.reference_id]
        assert wtc.get_ref_ids_with_workflow_status(db, FILE_NEEDED, "SGD") == []


class TestGetCurrentWorkflowStatus:

    def test_returns_tag_id_for_mod(self, wb_ref): # noqa
        db, mod, ref = wb_ref  # noqa
        _wft(db, ref, mod, FILE_NEEDED)
        with patch(f"{HELPERS}.get_workflow_tags_from_process",
                   return_value=[FILE_NEEDED, "ATP:0000139"]):
            status = wtc.get_current_workflow_status(db, ref.curie, "ATP:0000140", "WB")
        assert status == FILE_NEEDED

    def test_returns_none_when_no_matching_tag(self, wb_ref): # noqa
        db, _, ref = wb_ref  # noqa
        with patch(f"{HELPERS}.get_workflow_tags_from_process",
                   return_value=[FILE_NEEDED]):
            status = wtc.get_current_workflow_status(db, ref.curie, "ATP:0000140", "WB")
        assert status is None

    def test_returns_none_when_process_has_no_tags(self, wb_ref): # noqa
        db, _, ref = wb_ref  # noqa
        with patch(f"{HELPERS}.get_workflow_tags_from_process", return_value=[]):
            status = wtc.get_current_workflow_status(db, ref.curie, "ATP:0000140", "WB")
        assert status is None

    def test_all_returns_list_of_tag_dicts(self, wb_ref): # noqa
        db, mod, ref = wb_ref  # noqa
        _wft(db, ref, mod, FILE_NEEDED)
        with patch(f"{HELPERS}.get_workflow_tags_from_process", return_value=[FILE_NEEDED]), \
                patch(f"{HELPERS}.get_map_ateam_curies_to_names",
                      return_value={FILE_NEEDED: "file needed"}):
            tags = wtc.get_current_workflow_status(db, ref.curie, "ATP:0000140", "ALL")
        assert isinstance(tags, list)
        assert len(tags) == 1
        assert tags[0]["workflow_tag_id"] == FILE_NEEDED
        assert tags[0]["workflow_tag_name"] == "file needed"


class TestResetWorkflowTagsAfterDeletingMainPdf:

    def test_change_file_status_true_deletes_and_returns_early(self, wb_ref, monkeypatch): # noqa
        db, mod, ref = wb_ref  # noqa
        monkeypatch.setattr(wtc, "_ensure_atp_loaded", lambda: None)
        _wft(db, ref, mod, "ATP:0000178")  # a ref-classification tag
        with patch(f"{HELPERS}.get_workflow_tags_from_process",
                   side_effect=lambda proc: ["ATP:0000178", "ATP:0000198", "ATP:0000166"]):
            result = wtc.reset_workflow_tags_after_deleting_main_pdf(
                db, ref.curie, "WB", change_file_status=True)
        assert result is None
        remaining = db.query(WorkflowTagModel).filter(
            WorkflowTagModel.workflow_tag_id == "ATP:0000178").all()
        assert remaining == []

    def test_change_file_status_false_runs_file_upload_reset(self, wb_ref, monkeypatch): # noqa
        db, mod, ref = wb_ref  # noqa
        monkeypatch.setattr(wtc, "_ensure_atp_loaded", lambda: None)
        # a "files uploaded" tag that should be flipped since there are no files
        _wft(db, ref, mod, FILES_UPLOADED)
        with patch(f"{HELPERS}.get_workflow_tags_from_process",
                   side_effect=lambda proc: ["ATP:0000178"]):
            wtc.reset_workflow_tags_after_deleting_main_pdf(
                db, ref.curie, "WB", change_file_status=False)
        db.commit()
        tag = db.query(WorkflowTagModel).filter(
            WorkflowTagModel.reference_id == ref.reference_id,
            WorkflowTagModel.mod_id == mod.mod_id).one()
        # no referencefiles -> flipped to "file needed"
        assert tag.workflow_tag_id == FILE_NEEDED

    def test_unknown_reference_raises(self, wb_ref): # noqa
        db, _, _ = wb_ref  # noqa
        with pytest.raises(HTTPException) as exc:
            wtc.reset_workflow_tags_after_deleting_main_pdf(db, "AGRKB:000000000000", "WB")
        assert exc.value.status_code in (404, 422)


class TestIsFileUploadBlocked:

    def test_returns_job_type_when_in_progress_tag_present(self, wb_ref): # noqa
        db, mod, ref = wb_ref  # noqa
        _wft(db, ref, mod, "ATP:0000198")  # text conversion in progress
        with patch(f"{HELPERS}.get_workflow_tags_from_process", return_value=[]):
            job = wtc.is_file_upload_blocked(db, ref.curie, "WB")
        assert job == "text conversion"

    def test_returns_none_when_no_job_tag(self, wb_ref): # noqa
        db, _, ref = wb_ref  # noqa
        with patch(f"{HELPERS}.get_workflow_tags_from_process", return_value=[]):
            assert wtc.is_file_upload_blocked(db, ref.curie, "WB") is None

    def test_unknown_mod_raises_422(self, wb_ref): # noqa
        db, _, ref = wb_ref  # noqa
        with pytest.raises(HTTPException) as exc:
            wtc.is_file_upload_blocked(db, ref.curie, "NOPE")
        assert exc.value.status_code == 422


class TestWorkflowSubsetList:

    def test_unknown_mod_raises_404(self, wb_ref): # noqa
        db, _, _ = wb_ref  # noqa
        with pytest.raises(HTTPException) as exc:
            wtc.workflow_subset_list("reference classification", "NOPE", db)
        assert exc.value.status_code == 404
