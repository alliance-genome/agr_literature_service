"""
DB-backed unit tests for workflow_tag_crud functions that run SQL directly.

The A-Team ontology lookups (get_workflow_tags_from_process, the atpterm name
map, and _ensure_atp_loaded) are mocked so no network access is needed.
"""
import pytest
from unittest.mock import patch
from fastapi import HTTPException

from agr_literature_service.api.crud import workflow_tag_crud as wtc
from agr_literature_service.api.models import (ModModel, ReferenceModel, ReferencefileModel,
                                               ReferencefileModAssociationModel, WorkflowTagModel,
                                               WorkflowTransitionModel)
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from ..fixtures import db # noqa
from ..fixtures import load_name_to_atp_and_relationships_mock

HELPERS = "agr_literature_service.api.crud.workflow_tag_crud"
ATP_MOCK = "agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships"
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


def _file_upload_reset_rows(db, mod): # noqa
    """The two rows out of "files uploaded" every MOD has, with their requirements."""
    db.add(WorkflowTransitionModel(mod_id=mod.mod_id, transition_from=FILES_UPLOADED, transition_to="ATP:0000139",
                                   requirements=["referencefiles_present"]))
    db.add(WorkflowTransitionModel(mod_id=mod.mod_id, transition_from=FILES_UPLOADED, transition_to=FILE_NEEDED,
                                   requirements=["not_referencefiles_present"]))
    db.commit()


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

    @patch(ATP_MOCK, load_name_to_atp_and_relationships_mock)
    def test_change_file_status_false_runs_file_upload_reset(self, wb_ref): # noqa
        """The reset is the files uploaded -> file needed transition, whose requirement agrees
        that the paper has no file left (SCRUM-6845)."""
        db, mod, ref = wb_ref  # noqa
        load_name_to_atp_and_relationships_mock()
        _file_upload_reset_rows(db, mod)
        _wft(db, ref, mod, FILES_UPLOADED)
        _wft(db, ref, mod, "ATP:0000162")  # text conversion needed: removed with the main PDF
        wtc.reset_workflow_tags_after_deleting_main_pdf(db, ref.curie, "WB", change_file_status=False)
        db.commit()
        db.expire_all()
        tags = db.query(WorkflowTagModel).filter(
            WorkflowTagModel.reference_id == ref.reference_id,
            WorkflowTagModel.mod_id == mod.mod_id).all()
        assert [t.workflow_tag_id for t in tags] == [FILE_NEEDED]

    @patch(ATP_MOCK, load_name_to_atp_and_relationships_mock)
    def test_reset_with_a_file_left_goes_to_in_progress(self, wb_ref): # noqa
        """A file without a MOD (PMC) still counts for the MOD, in the reset's SQL and in the
        referencefiles_present requirement alike."""
        db, mod, ref = wb_ref  # noqa
        load_name_to_atp_and_relationships_mock()
        _file_upload_reset_rows(db, mod)
        _wft(db, ref, mod, FILES_UPLOADED)
        referencefile = ReferencefileModel(reference_id=ref.reference_id, display_name="supp", file_class="supplement",
                                           file_publication_status="final", file_extension="pdf",
                                           md5sum="0123456789abcdef0123456789abcdef")
        db.add(referencefile)
        db.flush()
        db.add(ReferencefileModAssociationModel(referencefile_id=referencefile.referencefile_id, mod_id=None))
        db.commit()
        wtc.reset_workflow_tags_after_deleting_main_pdf(db, ref.curie, "WB", change_file_status=False)
        db.commit()
        db.expire_all()
        tag = db.query(WorkflowTagModel).filter(
            WorkflowTagModel.reference_id == ref.reference_id,
            WorkflowTagModel.mod_id == mod.mod_id).one()
        assert tag.workflow_tag_id == "ATP:0000139"

    @patch(ATP_MOCK, load_name_to_atp_and_relationships_mock)
    def test_reset_leaves_other_file_states_alone(self, wb_ref): # noqa
        db, mod, ref = wb_ref  # noqa
        load_name_to_atp_and_relationships_mock()
        _wft(db, ref, mod, FILE_NEEDED)
        wtc.reset_workflow_tags_after_deleting_main_pdf(db, ref.curie, "WB", change_file_status=False)
        db.expire_all()
        assert db.query(WorkflowTagModel).filter(
            WorkflowTagModel.reference_id == ref.reference_id).one().workflow_tag_id == FILE_NEEDED

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
