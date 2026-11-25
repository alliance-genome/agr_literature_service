"""
Tests for the on-demand conversion API (SCRUM-5795).

Covers:
- Unit tests for ConversionJobManager (no DB).
- Integration tests for GET /reference/referencefile/conversion_request/{curie_or_reference_id}
  and GET /reference/referencefile/conversion_status/{job_id}.

The new endpoint reports only conversion status — callers use the existing
/reference/referencefile/show_all/{curie_or_reference_id} endpoint to fetch
the resulting file listing.

Conversion primitives (process_nxml_to_markdown / process_pdf_for_reference) are
mocked — the real ones would call PDFX and S3.
"""
from datetime import timedelta
from unittest.mock import patch, MagicMock

import pytest

import agr_literature_service.api.crud.file_conversion_crud as file_conversion_crud
import agr_literature_service.api.crud.referencefile_crud as referencefile_crud

from agr_cognito_py import ModAccess
from fastapi import HTTPException, status
from starlette.testclient import TestClient

from agr_literature_service.api.main import app
from agr_literature_service.api.schemas import ReferencefileSchemaPost
from agr_literature_service.api.crud.referencefile_crud import create_metadata
from agr_literature_service.api.utils.conversion_job_manager import (
    WORKER_GONE_ERROR,
    ConversionJob,
    ConversionJobManager,
    conversion_manager,
)

from .test_reference import test_reference  # noqa: F401
from .test_mod import test_mod  # noqa: F401
from ..fixtures import db  # noqa: F401
from .fixtures import auth_headers  # noqa: F401


# ---------------------------------------------------------------------------
# Unit tests for ConversionJobManager (no DB, no HTTP)
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clear_global_manager(tmp_path, monkeypatch):
    """Give the module-level singleton a fresh, empty state directory per test
    (SCRUM-6855: job state lives in files shared by the API workers)."""
    monkeypatch.setattr(conversion_manager, "state_dir", str(tmp_path / "global_jobs"))
    monkeypatch.setattr(file_conversion_crud, "_last_worker_down_alert", None)
    yield


def _worker_running():
    """SCRUM-6998: PDF conversions are queued for the conversion worker, which a
    test does not run; this makes the API see a live worker, so no worker-down
    alert is written."""
    return patch.object(conversion_manager, "worker_alive", return_value=True)


@pytest.fixture
def manager(tmp_path):
    return ConversionJobManager(state_dir=str(tmp_path / "jobs"))


class TestSuppressUploadGuardrails:
    """SCRUM-6246: a one-off process (the figure-metadata backfill) flips a
    process-level toggle so file_upload() persists auxiliary files WITHOUT the
    interactive-upload guardrails -- it skips the is_file_upload_blocked
    "job in progress" check and the post-upload workflow transition / main-PDF
    cleanup. Default (live API / conversion path) is unchanged."""

    @staticmethod
    def _metadata(mod=None):
        # A figure-metadata sidecar: not main/pdf/final. mod-less by default so
        # the corpus / upload-blocked guards don't run; pass a mod to exercise
        # the is_file_upload_blocked guard.
        return {
            "reference_curie": "AGRKB:101000000000001",
            "mod_abbreviation": mod,
            "file_class": "converted_main_figure_metadata",
            "file_publication_status": "final",
            "file_extension": "json",
            "pdf_type": None,
        }

    def test_default_fires_transition_and_cleanup(self):
        mock_db = MagicMock()
        with patch.object(referencefile_crud, "normalize_reference_curie",
                          side_effect=lambda _db, curie: curie), \
                patch.object(referencefile_crud, "file_upload_single",
                             return_value=MagicMock()), \
                patch.object(referencefile_crud, "cleanup_old_pdf_file") as cleanup, \
                patch.object(referencefile_crud,
                             "transition_WFT_for_uploaded_file") as transition:
            referencefile_crud.file_upload(mock_db, self._metadata(), MagicMock())
            transition.assert_called_once()
            cleanup.assert_called_once()

    def test_suppress_skips_transition_and_cleanup_but_commits(self):
        mock_db = MagicMock()
        with patch.object(referencefile_crud, "normalize_reference_curie",
                          side_effect=lambda _db, curie: curie), \
                patch.object(referencefile_crud, "file_upload_single",
                             return_value=MagicMock()), \
                patch.object(referencefile_crud, "cleanup_old_pdf_file") as cleanup, \
                patch.object(referencefile_crud,
                             "transition_WFT_for_uploaded_file") as transition:
            referencefile_crud.set_suppress_upload_guardrails(True)
            try:
                referencefile_crud.file_upload(mock_db, self._metadata(), MagicMock())
            finally:
                referencefile_crud.set_suppress_upload_guardrails(False)
            transition.assert_not_called()
            cleanup.assert_not_called()
            # The created referencefile must still be persisted even though the
            # transition (which used to be the committing step) was skipped.
            mock_db.commit.assert_called_once()

    def test_default_blocks_upload_when_job_in_progress(self):
        mock_db = MagicMock()
        with patch.object(referencefile_crud, "normalize_reference_curie",
                          side_effect=lambda _db, curie: curie), \
                patch.object(referencefile_crud, "check_if_paper_in_corpus",
                             return_value=True), \
                patch.object(referencefile_crud, "is_file_upload_blocked",
                             return_value="entity extraction"), \
                patch.object(referencefile_crud, "file_upload_single") as single:
            with pytest.raises(HTTPException) as exc:
                referencefile_crud.file_upload(mock_db, self._metadata(mod="WB"), MagicMock())
        assert exc.value.status_code == 422
        assert "in progress" in exc.value.detail
        single.assert_not_called()  # blocked before the upload happens

    def test_suppress_bypasses_job_in_progress_block(self):
        mock_db = MagicMock()
        with patch.object(referencefile_crud, "normalize_reference_curie",
                          side_effect=lambda _db, curie: curie), \
                patch.object(referencefile_crud, "check_if_paper_in_corpus",
                             return_value=True), \
                patch.object(referencefile_crud, "is_file_upload_blocked",
                             return_value="entity extraction") as blocked, \
                patch.object(referencefile_crud, "file_upload_single",
                             return_value=MagicMock()) as single, \
                patch.object(referencefile_crud, "cleanup_old_pdf_file"), \
                patch.object(referencefile_crud, "transition_WFT_for_uploaded_file"):
            referencefile_crud.set_suppress_upload_guardrails(True)
            try:
                referencefile_crud.file_upload(mock_db, self._metadata(mod="WB"), MagicMock())
            finally:
                referencefile_crud.set_suppress_upload_guardrails(False)
            single.assert_called_once()   # upload proceeded despite the in-progress job
            blocked.assert_not_called()   # the guard was skipped entirely

    def test_setter_toggles_and_resets_module_state(self):
        assert referencefile_crud.is_upload_guardrails_suppressed() is False
        referencefile_crud.set_suppress_upload_guardrails(True)
        try:
            assert referencefile_crud.is_upload_guardrails_suppressed() is True
        finally:
            referencefile_crud.set_suppress_upload_guardrails(False)
        assert referencefile_crud.is_upload_guardrails_suppressed() is False


class TestConversionJob:
    def test_to_dict_serializes_timestamps(self):
        job = ConversionJob(
            job_id="j1",
            reference_id=42,
            reference_curie="AGRKB:1",
            user_id="u1",
            status="running",
        )
        d = job.to_dict()
        assert d["job_id"] == "j1"
        assert d["reference_id"] == 42
        assert d["status"] == "running"
        assert isinstance(d["started_at"], str)
        assert d["completed_at"] is None
        assert d["per_file_progress"] == []

    def test_duration_seconds_before_completion(self):
        job = ConversionJob(
            job_id="j2",
            reference_id=1,
            reference_curie="AGRKB:2",
            user_id="u",
            status="running",
        )
        assert job.duration_seconds >= 0.0


class TestConversionJobManager:
    def test_create_new_job(self, manager):
        job = manager.create_or_get_job(reference_id=10, reference_curie="AGRKB:X",
                                        user_id="u1")
        assert job.status == "queued"  # until the conversion worker claims it (SCRUM-6998)
        assert manager.get_job(job.job_id) == job
        assert manager.get_active_job_for_reference(10).job_id == job.job_id

    def test_create_or_get_returns_existing_running(self, manager):
        first = manager.create_or_get_job(reference_id=11, reference_curie="AGRKB:Y",
                                          user_id="u")
        second = manager.create_or_get_job(reference_id=11, reference_curie="AGRKB:Y",
                                           user_id="u")
        assert first.job_id == second.job_id

    def test_new_job_after_completion(self, manager):
        first = manager.create_or_get_job(reference_id=12, reference_curie="AGRKB:Z",
                                          user_id="u")
        manager.complete_job(first.job_id, success=True)
        assert manager.get_active_job_for_reference(12) is None
        second = manager.create_or_get_job(reference_id=12, reference_curie="AGRKB:Z",
                                           user_id="u")
        assert second.job_id != first.job_id
        assert second.status == "queued"

    def test_record_progress_and_complete(self, manager):
        job = manager.create_or_get_job(reference_id=13, reference_curie="AGRKB:Q",
                                        user_id="u")
        manager.record_file_progress(
            job.job_id,
            source_display_name="file.pdf",
            source_file_class="main",
            converted_display_name="file.pdf_merged",
            converted_file_class="converted_merged_main",
            success=True,
        )
        manager.complete_job(job.job_id, success=True)
        final = manager.get_job(job.job_id)
        assert final.status == "completed"
        assert final.completed_at is not None
        assert len(final.per_file_progress) == 1
        entry = final.per_file_progress[0]
        assert entry.source_display_name == "file.pdf"
        assert entry.source_file_class == "main"
        assert entry.converted_file_class == "converted_merged_main"
        assert entry.status == "success"

    def test_complete_job_with_failure(self, manager):
        job = manager.create_or_get_job(reference_id=14, reference_curie="AGRKB:R",
                                        user_id="u")
        manager.complete_job(job.job_id, success=False, error="boom")
        final = manager.get_job(job.job_id)
        assert final.status == "failed"
        assert final.error_message == "boom"


# ---------------------------------------------------------------------------
# Unit tests for the figures-in-response helpers (no DB, no HTTP)
# ---------------------------------------------------------------------------

class TestFiguresHelpers:
    """Tests for _figures_for_source and _attach_figures.

    These verify that extracted-figure rows in the DB are surfaced under the
    correct per-file-progress entry in the conversion_request response,
    grouped by their source PDF's display_name and file_class.
    """

    @staticmethod
    def _make_ref_file(file_class, display_name, referencefile_id,
                       file_extension="png", file_publication_status="final"):
        from unittest.mock import MagicMock
        rf = MagicMock()
        rf.file_class = file_class
        rf.display_name = display_name
        rf.referencefile_id = referencefile_id
        rf.file_extension = file_extension
        rf.file_publication_status = file_publication_status
        return rf

    def _make_reference(self, ref_files):
        from unittest.mock import MagicMock
        ref = MagicMock()
        ref.referencefiles = ref_files
        return ref

    def test_figures_for_source_returns_matching_rows_sorted(self):
        from agr_literature_service.api.crud.file_conversion_crud import (
            _figures_for_source,
        )
        # Two figures for "paper" (main), plus an unrelated supp figure and
        # a different paper's figure that must be excluded.
        rfs = [
            self._make_ref_file("converted_main_figure", "paper_image_002", 11),
            self._make_ref_file("converted_main_figure", "paper_image_001", 10),
            self._make_ref_file("converted_supplement_figure", "supp_image_001", 20),
            self._make_ref_file("converted_main_figure", "other_image_001", 30),
        ]
        result = _figures_for_source(self._make_reference(rfs), "paper", "main")
        # Sorted by display_name; only main-class figures whose name has the
        # 'paper_image_' prefix.
        assert [r["display_name"] for r in result] == ["paper_image_001", "paper_image_002"]
        assert [r["referencefile_id"] for r in result] == [10, 11]
        assert all(r["file_class"] == "converted_main_figure" for r in result)

    def test_figures_for_source_supplement_uses_supplement_class(self):
        from agr_literature_service.api.crud.file_conversion_crud import (
            _figures_for_source,
        )
        rfs = [
            self._make_ref_file("converted_supplement_figure", "supp1_image_001", 1),
            self._make_ref_file("converted_main_figure", "supp1_image_002", 2),
        ]
        result = _figures_for_source(self._make_reference(rfs), "supp1", "supplement")
        assert [r["referencefile_id"] for r in result] == [1]

    def test_figures_for_source_skips_non_final_rows(self):
        from agr_literature_service.api.crud.file_conversion_crud import (
            _figures_for_source,
        )
        rfs = [
            self._make_ref_file("converted_main_figure", "paper_image_001", 1,
                                file_publication_status="temp"),
            self._make_ref_file("converted_main_figure", "paper_image_002", 2),
        ]
        result = _figures_for_source(self._make_reference(rfs), "paper", "main")
        assert [r["referencefile_id"] for r in result] == [2]

    def test_figures_for_source_unknown_file_class_returns_empty(self):
        from agr_literature_service.api.crud.file_conversion_crud import (
            _figures_for_source,
        )
        rfs = [self._make_ref_file("converted_main_figure", "paper_image_001", 1)]
        # nXML / tei / unknown source classes have no figure mapping.
        assert _figures_for_source(self._make_reference(rfs), "paper", "nXML") == []
        assert _figures_for_source(self._make_reference(rfs), "paper", None) == []
        assert _figures_for_source(self._make_reference(rfs), None, "main") == []

    def test_figures_for_source_no_referencefiles_returns_empty(self):
        from agr_literature_service.api.crud.file_conversion_crud import (
            _figures_for_source,
        )
        ref = self._make_reference([])
        assert _figures_for_source(ref, "paper", "main") == []

    def test_figures_for_source_links_metadata_sidecar(self):
        """Each figure PNG is linked to its JSON metadata sidecar (same
        display_name, parallel metadata file_class) via metadata_referencefile_id."""
        from agr_literature_service.api.crud.file_conversion_crud import (
            _figures_for_source,
        )
        rfs = [
            self._make_ref_file("converted_main_figure", "paper_image_001", 10),
            self._make_ref_file("converted_main_figure", "paper_image_002", 11),
            self._make_ref_file("converted_main_figure_metadata", "paper_image_001", 110, file_extension="json"),
            self._make_ref_file("converted_main_figure_metadata", "paper_image_002", 111, file_extension="json"),
        ]
        result = _figures_for_source(self._make_reference(rfs), "paper", "main")
        assert [r["referencefile_id"] for r in result] == [10, 11]
        assert [r["metadata_referencefile_id"] for r in result] == [110, 111]

    def test_figures_for_source_metadata_id_none_when_no_sidecar(self):
        from agr_literature_service.api.crud.file_conversion_crud import (
            _figures_for_source,
        )
        rfs = [
            self._make_ref_file("converted_main_figure", "paper_image_001", 10),
        ]
        result = _figures_for_source(self._make_reference(rfs), "paper", "main")
        assert result[0]["metadata_referencefile_id"] is None

    def test_figures_for_source_metadata_requires_json_and_final(self):
        """A sidecar row that is not json, or not final, is not linked."""
        from agr_literature_service.api.crud.file_conversion_crud import (
            _figures_for_source,
        )
        rfs = [
            self._make_ref_file("converted_main_figure", "paper_image_001", 10),
            # wrong extension
            self._make_ref_file("converted_main_figure_metadata", "paper_image_001", 110, file_extension="txt"),
            self._make_ref_file("converted_main_figure", "paper_image_002", 11),
            # non-final
            self._make_ref_file("converted_main_figure_metadata", "paper_image_002", 111, file_extension="json", file_publication_status="temp"),
        ]
        result = _figures_for_source(self._make_reference(rfs), "paper", "main")
        assert [r["metadata_referencefile_id"] for r in result] == [None, None]

    def test_figures_for_source_supplement_links_supplement_metadata(self):
        from agr_literature_service.api.crud.file_conversion_crud import (
            _figures_for_source,
        )
        rfs = [
            self._make_ref_file("converted_supplement_figure", "supp1_image_001", 1),
            self._make_ref_file("converted_supplement_figure_metadata", "supp1_image_001", 101, file_extension="json"),
        ]
        result = _figures_for_source(self._make_reference(rfs), "supp1", "supplement")
        assert result[0]["metadata_referencefile_id"] == 101

    def test_attach_figures_mutates_each_progress_entry(self):
        from agr_literature_service.api.crud.file_conversion_crud import (
            _attach_figures,
        )
        rfs = [
            self._make_ref_file("converted_main_figure", "paper_image_001", 100),
            self._make_ref_file("converted_supplement_figure", "supp_image_001", 200),
            self._make_ref_file("converted_supplement_figure", "supp_image_002", 201),
        ]
        progress = [
            {
                "source": {"display_name": "paper", "file_class": "main",
                           "referencefile_id": 1},
                "converted": {"display_name": "paper_merged",
                              "file_class": "converted_merged_main",
                              "referencefile_id": 50},
                "figures": [],
                "status": "success",
                "error": None,
            },
            {
                "source": {"display_name": "supp", "file_class": "supplement",
                           "referencefile_id": 2},
                "converted": {"display_name": "supp_merged",
                              "file_class": "converted_merged_supplement",
                              "referencefile_id": 51},
                "figures": [],
                "status": "success",
                "error": None,
            },
        ]
        _attach_figures(self._make_reference(rfs), progress)
        assert [f["referencefile_id"] for f in progress[0]["figures"]] == [100]
        assert [f["referencefile_id"] for f in progress[1]["figures"]] == [200, 201]

    def test_attach_figures_handles_entries_with_null_source(self):
        from agr_literature_service.api.crud.file_conversion_crud import (
            _attach_figures,
        )
        rfs = [self._make_ref_file("converted_main_figure", "paper_image_001", 1)]
        progress = [
            {
                "source": None,
                "converted": {"display_name": "orphan_merged",
                              "file_class": "converted_merged_main",
                              "referencefile_id": 7},
                "figures": [],
                "status": "success",
                "error": None,
            },
        ]
        _attach_figures(self._make_reference(rfs), progress)
        assert progress[0]["figures"] == []


# ---------------------------------------------------------------------------
# Integration tests for the /converted and /conversion_status endpoints
# ---------------------------------------------------------------------------

def _make_referencefile(db, reference_curie, display_name, file_class,  # noqa: F811
                        file_extension, md5sum, pdf_type=None):
    payload = {
        "display_name": display_name,
        "reference_curie": reference_curie,
        "file_class": file_class,
        "file_publication_status": "final",
        "file_extension": file_extension,
        "pdf_type": pdf_type,
        "md5sum": md5sum,
    }
    return create_metadata(db, ReferencefileSchemaPost(**payload))


class TestConvertedEndpoint:

    def test_no_sources_returns_no_sources_status(self, db, test_reference, auth_headers):  # noqa: F811
        """A reference with no files and no sources reports no_sources."""
        with TestClient(app) as client:
            response = client.get(
                url=f"/reference/referencefile/conversion_request/{test_reference.new_ref_curie}",
                headers=auth_headers,
            )
            assert response.status_code == status.HTTP_200_OK
            body = response.json()
            assert body["reference_curie"] == test_reference.new_ref_curie
            assert body["status"] == "no_sources"
            assert body["job_id"] is None
            assert "files" not in body

    def test_cached_main_and_supplement(self, db, test_reference, auth_headers):  # noqa: F811
        """With converted_merged_main + converted_merged_supplement present → cached."""
        _make_referencefile(db, test_reference.new_ref_curie, "pdf_main",
                            "main", "pdf", "md5_main_pdf", pdf_type="pdf")
        _make_referencefile(db, test_reference.new_ref_curie, "pdf_main_merged",
                            "converted_merged_main", "md", "md5_main_md")
        _make_referencefile(db, test_reference.new_ref_curie, "pdf_supp",
                            "supplement", "pdf", "md5_supp_pdf")
        _make_referencefile(db, test_reference.new_ref_curie, "pdf_supp_merged",
                            "converted_merged_supplement", "md", "md5_supp_md")

        with TestClient(app) as client:
            response = client.get(
                url=f"/reference/referencefile/conversion_request/{test_reference.new_ref_curie}",
                headers=auth_headers,
            )
            assert response.status_code == status.HTTP_200_OK
            body = response.json()
            assert body["status"] == "converted"
            assert sorted(body["converted_classes"]) == [
                "converted_merged_main", "converted_merged_supplement"
            ]

    def test_nxml_sync_path_completes(self, db, test_reference, auth_headers):  # noqa: F811
        """nXML source but no converted row → sync conversion, 200 with status=completed."""
        _make_referencefile(db, test_reference.new_ref_curie, "nxml_source",
                            "nXML", "xml", "md5_nxml")

        def fake_process_nxml(*, db, nxml_ref_file, reference_curie, mod_abbreviation,
                              s3_client=None):
            create_metadata(
                db,
                ReferencefileSchemaPost(
                    reference_curie=reference_curie,
                    display_name=f"{nxml_ref_file.display_name}_nxml",
                    file_class="converted_merged_main",
                    file_publication_status="final",
                    file_extension="md",
                    md5sum="md5_generated_nxml",
                ),
            )
            return True, None

        with patch(
            "agr_literature_service.api.crud.file_conversion_crud.process_nxml_to_markdown",
            side_effect=fake_process_nxml,
        ):
            with TestClient(app) as client:
                response = client.get(
                    url=f"/reference/referencefile/conversion_request/{test_reference.new_ref_curie}",
                    headers=auth_headers,
                )
                assert response.status_code == status.HTTP_200_OK
                body = response.json()
                assert body["status"] == "converted"

    def test_pdf_async_default_returns_202(self, db, test_reference, auth_headers):  # noqa: F811
        """Main PDF with no nXML → 202 with job_id, background task scheduled, and
        a pending per_file_progress entry seeded for the main PDF."""
        _make_referencefile(db, test_reference.new_ref_curie, "paper",
                            "main", "pdf", "md5_pdf_main", pdf_type="pdf")

        with _worker_running():
            with TestClient(app) as client:
                response = client.get(
                    url=f"/reference/referencefile/conversion_request/{test_reference.new_ref_curie}",
                    headers=auth_headers,
                )
                assert response.status_code == status.HTTP_202_ACCEPTED
                body = response.json()
                assert body["status"] == "running"
                assert body["job_id"] is not None
                # The main PDF should appear as a pending entry.
                assert len(body["per_file_progress"]) == 1
                entry = body["per_file_progress"][0]
                assert entry["source"]["display_name"] == "paper"
                assert entry["source"]["file_class"] == "main"
                assert entry["source"]["referencefile_id"] is not None
                assert entry["status"] == "pending"
                assert entry["converted"]["display_name"] == "paper_merged"
                assert entry["converted"]["file_class"] == "converted_merged_main"
                # Converted row doesn't exist yet — id is None until processing finishes.
                assert entry["converted"]["referencefile_id"] is None

    def test_pdf_idempotency_same_job_id(self, db, test_reference, auth_headers):  # noqa: F811
        """Two consecutive calls while a job is running share the same job_id."""
        _make_referencefile(db, test_reference.new_ref_curie, "paper",
                            "main", "pdf", "md5_pdf_idem", pdf_type="pdf")

        with _worker_running():
            with TestClient(app) as client:
                r1 = client.get(
                    url=f"/reference/referencefile/conversion_request/{test_reference.new_ref_curie}",
                    headers=auth_headers,
                )
                r2 = client.get(
                    url=f"/reference/referencefile/conversion_request/{test_reference.new_ref_curie}",
                    headers=auth_headers,
                )
                assert r1.status_code == status.HTTP_202_ACCEPTED
                assert r2.status_code == status.HTTP_202_ACCEPTED
                assert r1.json()["job_id"] == r2.json()["job_id"]

    def test_converted_classes_reflects_partial_state(self, db, test_reference, auth_headers):  # noqa: F811
        """converted_classes lists already-cached rows even while a job is still running."""
        # Already-cached main from a prior run:
        _make_referencefile(db, test_reference.new_ref_curie, "paper_main_merged",
                            "converted_merged_main", "md", "md5_partial_main_md")
        # A supplement PDF that still needs converting:
        _make_referencefile(db, test_reference.new_ref_curie, "paper_supp",
                            "supplement", "pdf", "md5_partial_supp_pdf")

        with _worker_running():
            with TestClient(app) as client:
                response = client.get(
                    url=f"/reference/referencefile/conversion_request/{test_reference.new_ref_curie}",
                    headers=auth_headers,
                )
                assert response.status_code == status.HTTP_202_ACCEPTED
                body = response.json()
                assert body["status"] == "running"
                # Main was already cached, supp conversion in progress:
                assert body["converted_classes"] == ["converted_merged_main"]

    def test_pdf_wait_true_blocks(self, db, test_reference, auth_headers):  # noqa: F811
        """wait=true waits for the queued job and returns 200 once the (mocked)
        conversion worker has created the row and finished the job."""
        _make_referencefile(db, test_reference.new_ref_curie, "paper",
                            "main", "pdf", "md5_pdf_wait", pdf_type="pdf")
        ref_curie = test_reference.new_ref_curie

        def fake_worker(job_id, timeout):
            reference_curie = conversion_manager.get_job(job_id).reference_curie
            create_metadata(
                db,
                ReferencefileSchemaPost(
                    reference_curie=reference_curie,
                    display_name="paper_merged",
                    file_class="converted_merged_main",
                    file_publication_status="final",
                    file_extension="md",
                    md5sum="md5_wait_generated",
                ),
            )
            conversion_manager.complete_job(job_id, success=True)
            return conversion_manager.get_job(job_id)

        with patch.object(conversion_manager, "wait_for_job", side_effect=fake_worker), _worker_running():
            with TestClient(app) as client:
                response = client.get(
                    url=f"/reference/referencefile/conversion_request/{ref_curie}?wait=true",
                    headers=auth_headers,
                )
                assert response.status_code == status.HTTP_200_OK
                body = response.json()
                assert body["status"] == "converted"

    def test_pdf_wait_true_returns_running_when_the_worker_has_not_finished(
            self, db, test_reference, auth_headers):  # noqa: F811
        """SCRUM-6998: wait=true waits at most WAIT_LIMIT_SECONDS for the worker and
        then reports the job as running, like a request without wait."""
        _make_referencefile(db, test_reference.new_ref_curie, "paper",
                            "main", "pdf", "md5_pdf_wait_limit", pdf_type="pdf")
        url = f"/reference/referencefile/conversion_request/{test_reference.new_ref_curie}?wait=true"
        with patch.object(conversion_manager, "wait_for_job",
                          side_effect=lambda job_id, timeout: conversion_manager.get_job(job_id)) as wait, \
                _worker_running(), TestClient(app) as client:
            response = client.get(url=url, headers=auth_headers)
        assert response.status_code == status.HTTP_202_ACCEPTED
        assert response.json()["status"] == "running"
        assert wait.call_args.args[1] == file_conversion_crud.WAIT_LIMIT_SECONDS

    def test_a_pdf_conversion_is_queued_for_the_worker(self, db, test_reference, auth_headers):  # noqa: F811
        """SCRUM-6998: the API queues the job and reports it as running; it does not
        run the conversion itself."""
        from agr_literature_service.api.models import ReferenceModel
        _make_referencefile(db, test_reference.new_ref_curie, "paper",
                            "main", "pdf", "md5_pdf_queued", pdf_type="pdf")
        url = f"/reference/referencefile/conversion_request/{test_reference.new_ref_curie}"
        with patch("agr_literature_service.api.utils.conversion_processor.run_conversion_job") as run_job, \
                _worker_running(), TestClient(app) as client:
            first = client.get(url=f"{url}?overwrite_tei_md=true", headers=auth_headers)
            second = client.get(url=url, headers=auth_headers)
        run_job.assert_not_called()
        assert first.status_code == second.status_code == status.HTTP_202_ACCEPTED
        assert first.json()["status"] == second.json()["status"] == "running"
        assert first.json()["job_id"] == second.json()["job_id"]
        reference_id = db.query(ReferenceModel.reference_id).filter(
            ReferenceModel.curie == test_reference.new_ref_curie).scalar()
        job = conversion_manager.get_active_job_for_reference(reference_id)
        assert job.status == "queued"
        assert job.overwrite_tei_md is True

    def test_worker_down_is_alerted_on_stderr_once(self, db, test_reference, auth_headers, capsys):  # noqa: F811
        """SCRUM-6998: with no live conversion worker, the API writes one fixed line to
        stderr (an ERROR in the log store), at most every 10 minutes per process."""
        _make_referencefile(db, test_reference.new_ref_curie, "paper",
                            "main", "pdf", "md5_pdf_worker_down", pdf_type="pdf")
        url = f"/reference/referencefile/conversion_request/{test_reference.new_ref_curie}"
        capsys.readouterr()
        with patch.object(conversion_manager, "worker_alive", return_value=False), \
                TestClient(app) as client:
            client.get(url=url, headers=auth_headers)  # queues the job
            client.get(url=url, headers=auth_headers)  # polls it
        err = capsys.readouterr().err
        assert err.count(file_conversion_crud.WORKER_DOWN_ALERT) == 1

    @pytest.mark.parametrize("wait", [False, True])
    def test_request_that_loses_the_race_does_not_run_the_job_again(
            self, db, test_reference, auth_headers, wait):  # noqa: F811
        """SCRUM-6855: two requests can both miss the "job already running" check (it is
        not atomic with starting a job). The one that does not create the job must
        report it as running, not run the conversion a second time."""
        from agr_literature_service.api.models import ReferenceModel
        _make_referencefile(db, test_reference.new_ref_curie, "paper",
                            "main", "pdf", "md5_pdf_race", pdf_type="pdf")
        reference_id = db.query(ReferenceModel.reference_id).filter(
            ReferenceModel.curie == test_reference.new_ref_curie).scalar()
        first, created = conversion_manager.start_job(reference_id, test_reference.new_ref_curie, "first")
        assert created is True
        with patch.object(conversion_manager, "get_active_job_for_reference", return_value=None), \
                patch.object(conversion_manager, "wait_for_job") as run_job:
            url = (f"/reference/referencefile/conversion_request/{test_reference.new_ref_curie}"
                   f"?wait={str(wait).lower()}")
            with TestClient(app) as client:
                response = client.get(url=url, headers=auth_headers)
        assert response.status_code == status.HTTP_202_ACCEPTED
        assert response.json()["status"] == "running"
        assert response.json()["job_id"] == first.job_id
        run_job.assert_not_called()

    def test_a_recent_failure_is_reported_until_retry(self, db, test_reference, auth_headers):  # noqa: F811
        """SCRUM-6855 review: a poll without wait used to start a new job after every
        failure, so the poller only ever saw "running". A recent failure is now
        reported as failed, and retry=true starts again, naming the failed job."""
        from agr_literature_service.api.models import ReferenceModel
        _make_referencefile(db, test_reference.new_ref_curie, "paper",
                            "main", "pdf", "md5_pdf_failed_hold", pdf_type="pdf")
        reference_id = db.query(ReferenceModel.reference_id).filter(
            ReferenceModel.curie == test_reference.new_ref_curie).scalar()
        failed, _ = conversion_manager.start_job(reference_id, test_reference.new_ref_curie, "first")
        conversion_manager.complete_job(failed.job_id, success=False, error="pdfx 500")
        url = f"/reference/referencefile/conversion_request/{test_reference.new_ref_curie}"
        with _worker_running(), TestClient(app) as client:
            polled = client.get(url=url, headers=auth_headers)
            assert polled.status_code == status.HTTP_200_OK
            assert polled.json()["status"] == "failed"
            assert polled.json()["job_id"] == failed.job_id
            assert "pdfx 500" in polled.json()["error_message"]
            assert conversion_manager.get_active_job_for_reference(reference_id) is None

            retried = client.get(url=f"{url}?retry=true", headers=auth_headers)
        assert retried.status_code == status.HTTP_202_ACCEPTED
        assert retried.json()["status"] == "running"
        assert retried.json()["job_id"] != failed.job_id
        assert retried.json()["previous_job_id"] == failed.job_id
        queued = conversion_manager.get_active_job_for_reference(reference_id)
        assert queued.job_id == retried.json()["job_id"]
        assert queued.status == "queued"

    def test_reference_not_found(self, db, auth_headers):  # noqa: F811
        """Unknown curie → 404."""
        with TestClient(app) as client:
            response = client.get(
                url="/reference/referencefile/conversion_request/AGRKB:999999999999999",
                headers=auth_headers,
            )
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_per_file_progress_surfaces_in_response(self, db, test_reference, auth_headers):  # noqa: F811
        """After a job records per-file progress, subsequent polls of the same
        endpoint surface it in `per_file_progress`."""
        _make_referencefile(db, test_reference.new_ref_curie, "paper",
                            "main", "pdf", "md5_pdf_progress", pdf_type="pdf")

        with _worker_running():
            with TestClient(app) as client:
                r1 = client.get(
                    url=f"/reference/referencefile/conversion_request/{test_reference.new_ref_curie}",
                    headers=auth_headers,
                )
                assert r1.status_code == status.HTTP_202_ACCEPTED
                job_id = r1.json()["job_id"]

                conversion_manager.record_file_progress(
                    job_id,
                    source_display_name="paper",
                    source_file_class="main",
                    success=False,
                    error="pdfx 500",
                )

                r2 = client.get(
                    url=f"/reference/referencefile/conversion_request/{test_reference.new_ref_curie}",
                    headers=auth_headers,
                )
                body = r2.json()
                assert body["job_id"] == job_id
                assert len(body["per_file_progress"]) == 1
                entry = body["per_file_progress"][0]
                assert entry["source"]["display_name"] == "paper"
                assert entry["source"]["file_class"] == "main"
                assert entry["source"]["referencefile_id"] is not None
                assert entry["converted"] is None
                assert entry["status"] == "failed"
                assert entry["error"] == "pdfx 500"


class TestDryRun:
    """SCRUM-6980: ``dry_run=true`` reports the conversion status without ever
    starting, restarting or running a conversion."""

    @staticmethod
    def _url(curie):
        return f"/reference/referencefile/conversion_request/{curie}?dry_run=true"

    @staticmethod
    def _reference_id(db, curie):  # noqa: F811
        from agr_literature_service.api.models import ReferenceModel
        return db.query(ReferenceModel.reference_id).filter(
            ReferenceModel.curie == curie).scalar()

    def test_pending_pdf_is_not_converted_and_starts_nothing(
            self, db, test_reference, auth_headers):  # noqa: F811
        curie = test_reference.new_ref_curie
        _make_referencefile(db, curie, "paper", "main", "pdf", "md5_dry_pdf", pdf_type="pdf")
        with patch.object(conversion_manager, "start_job") as run_job, TestClient(app) as client:
            response = client.get(url=self._url(curie), headers=auth_headers)
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["status"] == "not_converted"
        assert body["would_start"] is True
        assert body["failure_reason"] is None
        assert body["job_id"] is None
        assert len(body["per_file_progress"]) == 1
        entry = body["per_file_progress"][0]
        assert entry["status"] == "pending"
        assert entry["source"]["display_name"] == "paper"
        assert entry["source"]["referencefile_id"] is not None
        assert entry["converted"]["display_name"] == "paper_merged"
        assert entry["converted"]["file_class"] == "converted_merged_main"
        assert entry["converted"]["referencefile_id"] is None
        run_job.assert_not_called()
        assert conversion_manager.get_last_job_for_reference(
            self._reference_id(db, curie)) is None

    def test_pending_nxml_is_not_converted_inline(self, db, test_reference, auth_headers):  # noqa: F811
        """The nXML path normally converts inline; a dry run must not."""
        curie = test_reference.new_ref_curie
        _make_referencefile(db, curie, "nxml_source", "nXML", "xml", "md5_dry_nxml")
        with patch(f"{CRUD_MOD}.process_nxml_to_markdown") as convert_nxml, \
                TestClient(app) as client:
            response = client.get(url=self._url(curie), headers=auth_headers)
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["status"] == "not_converted"
        assert body["per_file_progress"][0]["converted"]["display_name"] == "nxml_source_nxml"
        convert_nxml.assert_not_called()

    def test_running_job_is_reported(self, db, test_reference, auth_headers):  # noqa: F811
        curie = test_reference.new_ref_curie
        _make_referencefile(db, curie, "paper", "main", "pdf", "md5_dry_running", pdf_type="pdf")
        job, _ = conversion_manager.start_job(self._reference_id(db, curie), curie, "someone")
        with patch.object(conversion_manager, "start_job") as run_job, TestClient(app) as client:
            response = client.get(url=self._url(curie), headers=auth_headers)
        assert response.status_code == status.HTTP_202_ACCEPTED
        assert response.json()["status"] == "running"
        assert response.json()["would_start"] is False
        assert response.json()["job_id"] == job.job_id
        run_job.assert_not_called()

    def _failed_job(self, db, curie, md5):  # noqa: F811
        _make_referencefile(db, curie, "paper", "main", "pdf", md5, pdf_type="pdf")
        failed, _ = conversion_manager.start_job(self._reference_id(db, curie), curie, "someone")
        conversion_manager.record_file_progress(failed.job_id, source_display_name="paper",
                                                source_file_class="main", success=False,
                                                error="pdfx 500")
        conversion_manager.complete_job(failed.job_id, success=False)
        return failed

    def test_failure_is_reported_even_with_retry(self, db, test_reference, auth_headers):  # noqa: F811
        """A request would restart the job with retry=true; a dry run reports the
        failure and ignores retry. Inside the hold a request without retry would
        not start a job either."""
        curie = test_reference.new_ref_curie
        failed = self._failed_job(db, curie, "md5_dry_failed")
        with patch.object(conversion_manager, "start_job") as run_job, TestClient(app) as client:
            response = client.get(url=f"{self._url(curie)}&retry=true", headers=auth_headers)
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["status"] == "failed"
        assert body["failure_reason"] == "conversion_failed"
        assert body["would_start"] is False
        assert body["job_id"] == failed.job_id
        assert "paper: pdfx 500" in body["error_message"]
        assert body["per_file_progress"][0]["status"] == "failed"
        run_job.assert_not_called()

    def test_failure_past_the_hold_would_start_but_starts_nothing(
            self, db, test_reference, auth_headers):  # noqa: F811
        """After FAILED_JOB_HOLD a request would start again: the dry run says so
        (would_start) and still reports the failure without starting a job."""
        curie = test_reference.new_ref_curie
        failed = self._failed_job(db, curie, "md5_dry_failed_old")
        with patch("agr_literature_service.api.utils.conversion_job_manager.FAILED_JOB_HOLD",
                   timedelta(0)), \
                patch.object(conversion_manager, "start_job") as run_job, TestClient(app) as client:
            response = client.get(url=self._url(curie), headers=auth_headers)
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["status"] == "failed"
        assert body["failure_reason"] == "conversion_failed"
        assert body["would_start"] is True
        assert body["job_id"] == failed.job_id
        run_job.assert_not_called()
        reference_id = self._reference_id(db, curie)
        assert conversion_manager.get_active_job_for_reference(reference_id) is None
        assert conversion_manager.get_last_job_for_reference(reference_id).job_id == failed.job_id

    def test_failure_in_hold_would_start_when_only_inline_work_is_left(
            self, db, test_reference, auth_headers):  # noqa: F811
        """Inside the hold, a request whose remaining work is Office only converts
        it inline without checking the hold, so would_start is true."""
        curie = test_reference.new_ref_curie
        failed = self._failed_job(db, curie, "md5_dry_inline_pdf")
        _make_referencefile(db, curie, "paper_merged", "converted_merged_main", "md", "md5_dry_inline_md")
        _make_referencefile(db, curie, "table_s1", "supplement", "xlsx", "md5_dry_inline_xlsx")
        with patch(f"{CRUD_MOD}.process_office_to_markdown") as convert_office, \
                patch.object(conversion_manager, "start_job") as run_job, TestClient(app) as client:
            response = client.get(url=self._url(curie), headers=auth_headers)
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["status"] == "failed"
        assert body["failure_reason"] == "conversion_failed"
        assert body["would_start"] is True
        assert body["job_id"] == failed.job_id
        convert_office.assert_not_called()
        run_job.assert_not_called()

    def test_worker_gone_is_reported_as_failed_not_restarted(
            self, db, test_reference, auth_headers):  # noqa: F811
        curie = test_reference.new_ref_curie
        _make_referencefile(db, curie, "paper", "main", "pdf", "md5_dry_gone", pdf_type="pdf")
        job, _ = conversion_manager.start_job(self._reference_id(db, curie), curie, "someone")
        # the conversion worker stopped before finishing it MAX_RESUMES times
        conversion_manager.claim_next_job("worker")
        conversion_manager.complete_job(job.job_id, success=False, error=WORKER_GONE_ERROR)
        with patch.object(conversion_manager, "start_job") as run_job, TestClient(app) as client:
            response = client.get(url=self._url(curie), headers=auth_headers)
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["status"] == "failed"
        assert body["failure_reason"] == "worker_exited"
        assert body["would_start"] is True
        assert body["job_id"] == job.job_id
        run_job.assert_not_called()
        # no second job: the worker-gone job is still the reference's last one
        reference_id = self._reference_id(db, curie)
        assert conversion_manager.get_active_job_for_reference(reference_id) is None
        assert conversion_manager.get_last_job_for_reference(reference_id).job_id == job.job_id

    def test_converted_does_not_sync_mods_or_tags(self, db, test_reference, auth_headers):  # noqa: F811
        curie = test_reference.new_ref_curie
        _make_referencefile(db, curie, "paper", "main", "pdf", "md5_dry_conv_pdf", pdf_type="pdf")
        _make_referencefile(db, curie, "paper_merged", "converted_merged_main", "md", "md5_dry_conv_md")
        with patch(f"{CRUD_MOD}.sync_converted_file_mods_to_sources") as sync_mods, \
                patch(f"{CRUD_MOD}.transition_completed_text_convert_tags") as transition, \
                TestClient(app) as client:
            response = client.get(url=self._url(curie), headers=auth_headers)
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["status"] == "converted"
        assert response.json()["would_start"] is False
        assert response.json()["converted_classes"] == ["converted_merged_main"]
        sync_mods.assert_not_called()
        transition.assert_not_called()

    def test_no_sources(self, db, test_reference, auth_headers):  # noqa: F811
        with TestClient(app) as client:
            response = client.get(url=self._url(test_reference.new_ref_curie), headers=auth_headers)
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["status"] == "no_sources"
        assert response.json()["would_start"] is False

    def test_pending_office_supplement_is_not_converted_inline(
            self, db, test_reference, auth_headers):  # noqa: F811
        """Office supplements normally convert inline; a dry run must not."""
        curie = test_reference.new_ref_curie
        _make_referencefile(db, curie, "table_s1", "supplement", "xlsx", "md5_dry_office")
        with patch(f"{CRUD_MOD}.process_office_to_markdown") as convert_office, \
                patch.object(conversion_manager, "start_job") as run_job, TestClient(app) as client:
            response = client.get(url=self._url(curie), headers=auth_headers)
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["status"] == "not_converted"
        assert body["would_start"] is True
        assert body["per_file_progress"][0]["converted"]["display_name"] == "table_s1_xlsx"
        convert_office.assert_not_called()
        run_job.assert_not_called()
        assert conversion_manager.get_last_job_for_reference(
            self._reference_id(db, curie)) is None

    def test_caller_without_access_gets_403(self, db, test_reference, auth_headers):  # noqa: F811
        curie = test_reference.new_ref_curie
        _make_referencefile(db, curie, "paper", "main", "pdf", "md5_dry_403", pdf_type="pdf")
        with patch("agr_literature_service.api.routers.referencefile_router.get_mod_access",
                   return_value=ModAccess.NO_ACCESS), \
                patch.object(conversion_manager, "start_job") as run_job, TestClient(app) as client:
            response = client.get(url=self._url(curie), headers=auth_headers)
        assert response.status_code == status.HTTP_403_FORBIDDEN
        run_job.assert_not_called()
        assert conversion_manager.get_last_job_for_reference(
            self._reference_id(db, curie)) is None


class TestPageProvenanceLinks:
    """SCRUM-6977: PDFX's page map (JSON, same display_name as the merged
    Markdown) is linked from the conversion response and from show_all."""

    @staticmethod
    def _paper(db, curie):  # noqa: F811
        _make_referencefile(db, curie, "paper", "main", "pdf", "md5_pp_pdf", pdf_type="pdf")
        _make_referencefile(db, curie, "paper_merged", "converted_merged_main", "md", "md5_pp_md")
        _make_referencefile(db, curie, "supp", "supplement", "pdf", "md5_pp_supp")
        _make_referencefile(db, curie, "supp_merged", "converted_merged_supplement", "md", "md5_pp_supp_md")
        main_map = _make_referencefile(db, curie, "paper_merged", "converted_page_provenance_main",
                                       "json", "md5_pp_json")
        supp_map = _make_referencefile(db, curie, "supp_merged", "converted_page_provenance_supplement",
                                       "json", "md5_pp_supp_json")
        return main_map, supp_map

    def test_conversion_response_names_each_page_map(self, db, test_reference, auth_headers):  # noqa: F811
        main_map, supp_map = self._paper(db, test_reference.new_ref_curie)
        with TestClient(app) as client:
            response = client.get(
                url=f"/reference/referencefile/conversion_request/{test_reference.new_ref_curie}",
                headers=auth_headers,
            )
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["status"] == "converted"
        by_name = {e["converted"]["display_name"]: e["converted"] for e in body["per_file_progress"]}
        assert by_name["paper_merged"]["page_provenance_referencefile_id"] == main_map
        assert by_name["supp_merged"]["page_provenance_referencefile_id"] == supp_map
        # the page maps are not converted files of their own
        assert len(body["per_file_progress"]) == 2

    def test_markdown_without_page_map_has_none(self, db, test_reference, auth_headers):  # noqa: F811
        curie = test_reference.new_ref_curie
        _make_referencefile(db, curie, "nxml_source", "nXML", "xml", "md5_pp_nxml")
        _make_referencefile(db, curie, "nxml_source_nxml", "converted_merged_main", "md", "md5_pp_nxml_md")
        with TestClient(app) as client:
            response = client.get(url=f"/reference/referencefile/conversion_request/{curie}",
                                  headers=auth_headers)
        converted = response.json()["per_file_progress"][0]["converted"]
        assert converted["display_name"] == "nxml_source_nxml"
        assert converted["page_provenance_referencefile_id"] is None

    def test_show_all_gives_each_page_map_its_source_pdf(self, db, test_reference, auth_headers):  # noqa: F811
        main_map, supp_map = self._paper(db, test_reference.new_ref_curie)
        with TestClient(app) as client:
            response = client.get(
                url=f"/reference/referencefile/show_all/{test_reference.new_ref_curie}",
                headers=auth_headers,
            )
        assert response.status_code == status.HTTP_200_OK
        by_id = {f["referencefile_id"]: f for f in response.json()}
        assert by_id[main_map]["source"]["display_name"] == "paper"
        assert by_id[main_map]["source"]["file_class"] == "main"
        assert by_id[supp_map]["source"]["display_name"] == "supp"
        assert by_id[supp_map]["source"]["file_class"] == "supplement"


# ---------------------------------------------------------------------------
# Office supplements (SCRUM-6589): xlsx / docx convert in-process, the
# converted row is ``{display_name}_{extension}``.
# ---------------------------------------------------------------------------

CRUD_MOD = "agr_literature_service.api.crud.file_conversion_crud"


def _ref_file_stub(display_name, file_class, file_extension, rf_id):
    rf = MagicMock()
    rf.display_name = display_name
    rf.file_class = file_class
    rf.file_extension = file_extension
    rf.referencefile_id = rf_id
    return rf


class TestOfficeSourceInference:
    """No DB: the display_name suffix convention for Office-derived Markdown."""

    def test_office_suffix_resolves_to_the_row_with_matching_extension(self):
        from agr_literature_service.api.crud.file_conversion_crud import _infer_source_info

        reference = MagicMock()
        reference.referencefiles = [
            _ref_file_stub("table_s1", "supplement", "pdf", 1),
            _ref_file_stub("table_s1", "supplement", "xlsx", 2),
            _ref_file_stub("table_s1", "supplement", "docx", 3),
        ]
        xlsx = _infer_source_info(reference, "table_s1_xlsx", "converted_merged_supplement")
        docx = _infer_source_info(reference, "table_s1_docx", "converted_merged_supplement")
        assert xlsx == {"display_name": "table_s1", "file_class": "supplement", "referencefile_id": 2}
        assert docx == {"display_name": "table_s1", "file_class": "supplement", "referencefile_id": 3}

    def test_office_suffix_only_applies_to_supplement_markdown(self):
        from agr_literature_service.api.crud.file_conversion_crud import _infer_source_info

        reference = MagicMock()
        reference.referencefiles = [_ref_file_stub("paper", "supplement", "xlsx", 2)]
        assert _infer_source_info(reference, "paper_xlsx", "converted_merged_main") is None

    def test_expected_source_files_include_pending_office_supplements(self):
        from agr_literature_service.api.crud.file_conversion_crud import (
            _expected_source_files_from_assessment,
        )

        assessment = {
            "pending_main": [],
            "pending_supplements": [_ref_file_stub("supp", "supplement", "pdf", 5)],
            "pending_office_supplements": [_ref_file_stub("table_s1", "supplement", "xlsx", 6)],
        }
        expected = _expected_source_files_from_assessment(assessment)
        assert [e["expected_converted_display_name"] for e in expected] == [
            "supp_merged", "table_s1_xlsx",
        ]
        assert {e["expected_converted_file_class"] for e in expected} == {
            "converted_merged_supplement",
        }
        assert expected[1]["source_file_class"] == "supplement"
        assert expected[1]["source_referencefile_id"] == 6


class TestOfficeSupplementConversion:

    def test_assessment_detects_pending_office_supplement(self, db, test_reference, auth_headers):  # noqa: F811
        from agr_literature_service.api.crud.file_conversion_crud import _assess_reference
        from agr_literature_service.api.crud.reference_utils import get_reference

        _make_referencefile(db, test_reference.new_ref_curie, "table_s1",
                            "supplement", "xlsx", "md5_office_xlsx_assess")

        reference = get_reference(db=db, curie_or_reference_id=test_reference.new_ref_curie,
                                  load_referencefiles=True)
        assessment = _assess_reference(db, reference)

        assert assessment["supp_office_available"] is True
        assert assessment["supp_pdf_available"] is False
        assert [f.display_name for f in assessment["pending_office_supplements"]] == ["table_s1"]
        assert assessment["supp_missing"] is True
        # Office supplements convert in-process: no PDFX, no background job.
        assert assessment["needs_async"] is False

    def test_office_supplement_converts_synchronously(self, db, test_reference, auth_headers):  # noqa: F811
        """An xlsx supplement with no converted row → converted inline, 200
        with status=converted and the Office-derived row in per_file_progress."""
        _make_referencefile(db, test_reference.new_ref_curie, "table_s1",
                            "supplement", "xlsx", "md5_office_xlsx_sync")

        def fake_process_office(*, db, office_ref_file, reference_curie, mod_abbreviation,
                                s3_client=None):
            create_metadata(
                db,
                ReferencefileSchemaPost(
                    reference_curie=reference_curie,
                    display_name=f"{office_ref_file.display_name}_{office_ref_file.file_extension}",
                    file_class="converted_merged_supplement",
                    file_publication_status="final",
                    file_extension="md",
                    md5sum="md5_generated_office_md",
                ),
            )
            return True, None

        with patch(f"{CRUD_MOD}.process_office_to_markdown", side_effect=fake_process_office), \
                patch.object(conversion_manager, "start_job") as background_job:
            with TestClient(app) as client:
                response = client.get(
                    url=f"/reference/referencefile/conversion_request/{test_reference.new_ref_curie}",
                    headers=auth_headers,
                )
        background_job.assert_not_called()
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["status"] == "converted"
        assert body["converted_classes"] == ["converted_merged_supplement"]
        assert len(body["per_file_progress"]) == 1
        entry = body["per_file_progress"][0]
        assert entry["status"] == "success"
        assert entry["source"]["display_name"] == "table_s1"
        assert entry["source"]["file_class"] == "supplement"
        assert entry["converted"]["display_name"] == "table_s1_xlsx"
        assert entry["converted"]["file_class"] == "converted_merged_supplement"
        assert entry["converted"]["referencefile_id"] is not None

    def test_office_conversion_failure_reports_failed(self, db, test_reference, auth_headers):  # noqa: F811
        _make_referencefile(db, test_reference.new_ref_curie, "methods",
                            "supplement", "docx", "md5_office_docx_fail")

        with patch(f"{CRUD_MOD}.process_office_to_markdown", return_value=(False, "bad zip")):
            with TestClient(app) as client:
                response = client.get(
                    url=f"/reference/referencefile/conversion_request/{test_reference.new_ref_curie}",
                    headers=auth_headers,
                )
        assert response.status_code == status.HTTP_200_OK
        body = response.json()
        assert body["status"] == "failed"
        assert "Office supplement 'methods': bad zip" in body["error_message"]

    def test_office_with_pdf_supplement_goes_async_and_seeds_both(self, db, test_reference, auth_headers):  # noqa: F811
        """A PDF supplement still needs PDFX, so the job runs in the background;
        the Office supplement is seeded as pending alongside it."""
        _make_referencefile(db, test_reference.new_ref_curie, "supp",
                            "supplement", "pdf", "md5_office_mix_pdf")
        _make_referencefile(db, test_reference.new_ref_curie, "table_s1",
                            "supplement", "xlsx", "md5_office_mix_xlsx")

        with _worker_running():
            with TestClient(app) as client:
                response = client.get(
                    url=f"/reference/referencefile/conversion_request/{test_reference.new_ref_curie}",
                    headers=auth_headers,
                )
        assert response.status_code == status.HTTP_202_ACCEPTED
        body = response.json()
        assert body["status"] == "running"
        pending = {e["converted"]["display_name"]: e for e in body["per_file_progress"]}
        assert set(pending) == {"supp_merged", "table_s1_xlsx"}
        assert all(e["status"] == "pending" for e in pending.values())
        assert pending["table_s1_xlsx"]["source"]["display_name"] == "table_s1"
        assert pending["table_s1_xlsx"]["converted"]["file_class"] == "converted_merged_supplement"

    def test_supplements_pending_outside_wb_zfin_fb(self, db, test_reference, auth_headers):  # noqa: F811
        """SCRUM-6978: the on-demand request converts supplements for every
        group. A reference in no WB/ZFIN/FB corpus (the batch job would skip
        its supplements) still has its PDF and Office supplements pending."""
        from agr_literature_service.api.crud.file_conversion_crud import _assess_reference
        from agr_literature_service.api.crud.reference_utils import get_reference
        from agr_literature_service.lit_processing.pdf2md.pdf2md_utils import (
            is_eligible_for_supplement_conversion,
        )

        _make_referencefile(db, test_reference.new_ref_curie, "supp",
                            "supplement", "pdf", "md5_any_group_supp_pdf")
        _make_referencefile(db, test_reference.new_ref_curie, "table_s1",
                            "supplement", "xlsx", "md5_any_group_supp_xlsx")

        reference = get_reference(db=db, curie_or_reference_id=test_reference.new_ref_curie,
                                  load_referencefiles=True)
        assert is_eligible_for_supplement_conversion(db, reference.reference_id) is False
        assessment = _assess_reference(db, reference)

        assert [f.display_name for f in assessment["pending_supplements"]] == ["supp"]
        assert [f.display_name for f in assessment["pending_office_supplements"]] == ["table_s1"]
        assert assessment["supp_missing"] is True
        assert assessment["needs_async"] is True


# ---------------------------------------------------------------------------
# Unit test for _assess_reference (uses DB but not the HTTP layer)
# ---------------------------------------------------------------------------

class TestAssessReference:

    def test_detects_cached_and_missing(self, db, test_reference, auth_headers):  # noqa: F811
        from agr_literature_service.api.crud.file_conversion_crud import _assess_reference
        from agr_literature_service.api.crud.reference_utils import get_reference

        _make_referencefile(db, test_reference.new_ref_curie, "paper",
                            "main", "pdf", "md5_assess_main", pdf_type="pdf")
        _make_referencefile(db, test_reference.new_ref_curie, "paper_merged",
                            "converted_merged_main", "md", "md5_assess_md")

        reference = get_reference(db=db,
                                  curie_or_reference_id=test_reference.new_ref_curie,
                                  load_referencefiles=True)
        assessment = _assess_reference(db, reference)

        assert assessment["main_cached"] is True
        assert assessment["main_missing"] is False
        assert assessment["supp_cached"] is False
        assert assessment["supp_missing"] is False
        assert assessment["needs_async"] is False

    def test_detects_main_needs_pdf_conversion(self, db, test_reference, auth_headers):  # noqa: F811
        from agr_literature_service.api.crud.file_conversion_crud import _assess_reference
        from agr_literature_service.api.crud.reference_utils import get_reference

        _make_referencefile(db, test_reference.new_ref_curie, "paper",
                            "main", "pdf", "md5_assess_pdf", pdf_type="pdf")

        reference = get_reference(db=db,
                                  curie_or_reference_id=test_reference.new_ref_curie,
                                  load_referencefiles=True)
        assessment = _assess_reference(db, reference)

        assert assessment["main_cached"] is False
        assert assessment["main_pdf_available"] is True
        assert assessment["main_missing"] is True
        assert assessment["nxml_source"] is None
        assert assessment["needs_async"] is True

    def test_detects_main_needs_nxml_conversion(self, db, test_reference, auth_headers):  # noqa: F811
        from agr_literature_service.api.crud.file_conversion_crud import _assess_reference
        from agr_literature_service.api.crud.reference_utils import get_reference

        _make_referencefile(db, test_reference.new_ref_curie, "nxml_only",
                            "nXML", "xml", "md5_assess_nxml")

        reference = get_reference(db=db,
                                  curie_or_reference_id=test_reference.new_ref_curie,
                                  load_referencefiles=True)
        assessment = _assess_reference(db, reference)

        assert assessment["main_cached"] is False
        assert assessment["main_missing"] is True
        assert assessment["nxml_source"] is not None
        assert assessment["needs_async"] is False

    def test_overwrite_tei_md_ignores_tei_derived_rows(self, db, test_reference, auth_headers):  # noqa: F811
        """A _tei-suffixed converted_merged_main counts as cached by default,
        but with overwrite_tei_md=True it's ignored so the endpoint re-runs."""
        from agr_literature_service.api.crud.file_conversion_crud import _assess_reference
        from agr_literature_service.api.crud.reference_utils import get_reference

        _make_referencefile(db, test_reference.new_ref_curie, "paper",
                            "main", "pdf", "md5_tei_main_pdf", pdf_type="pdf")
        _make_referencefile(db, test_reference.new_ref_curie, "paper_tei",
                            "converted_merged_main", "md", "md5_tei_main_md")

        reference = get_reference(db=db,
                                  curie_or_reference_id=test_reference.new_ref_curie,
                                  load_referencefiles=True)

        default_assessment = _assess_reference(db, reference)
        assert default_assessment["main_cached"] is True
        assert default_assessment["main_missing"] is False

        overwrite_assessment = _assess_reference(db, reference, overwrite_tei_md=True)
        assert overwrite_assessment["main_cached"] is False
        assert overwrite_assessment["main_missing"] is True


# ---------------------------------------------------------------------------
# SCRUM-6092: supplement failures must not block text_convert_job tag
# transition. When a reference's main source is converted, the per-MOD tag
# should land on on_success regardless of supplement outcomes (oversize PDF,
# GROBID returning no extractable text, transient PDFX errors, etc.).
# ---------------------------------------------------------------------------

class TestSupplementFailureTagTransition:

    @staticmethod
    def _fake_job_for(reference, mod_id):
        """Shape returned by workflow_tag_crud.get_jobs for a text_convert_job entry."""
        return [{
            "job_name": "text_convert_job",
            "workflow_tag_id": "ATP:0000162",
            "reference_id": reference.reference_id,
            "reference_curie": reference.curie,
            "reference_workflow_tag_id": 999_001,
            "mod_id": mod_id,
            "topic_id": None,
        }]

    def test_per_mod_status_exposes_main_converted_independently_of_supplements(
        self, db, test_reference, test_mod, auth_headers,  # noqa: F811
    ):
        """SCRUM-6092: per_mod_pending_status must surface a main_converted
        flag (and main_converted alone must be True when the main has a
        converted_merged_main row, even if a supplement is still pending)."""
        from agr_literature_service.api.crud.file_conversion_crud import (
            per_mod_pending_status,
        )
        from agr_literature_service.api.crud.reference_utils import get_reference

        _make_referencefile(db, test_reference.new_ref_curie, "paper",
                            "main", "pdf", "md5_main_pdf_6092", pdf_type="pdf")
        _make_referencefile(db, test_reference.new_ref_curie, "paper_merged",
                            "converted_merged_main", "md", "md5_main_md_6092")
        _make_referencefile(db, test_reference.new_ref_curie, "paper_supp",
                            "supplement", "pdf", "md5_supp_pdf_6092")

        reference = get_reference(
            db=db,
            curie_or_reference_id=test_reference.new_ref_curie,
            load_referencefiles=True,
        )

        with patch(
            "agr_literature_service.api.crud.workflow_tag_crud.get_jobs",
            return_value=self._fake_job_for(reference, test_mod.new_mod_id),
        ):
            statuses = per_mod_pending_status(db, reference)

        assert len(statuses) == 1
        entry = statuses[0]
        assert entry["mod_abbreviation"] == test_mod.new_mod_abbreviation
        assert entry["pending_main_count"] == 0
        assert entry["pending_supplement_count"] == 1
        assert entry["main_converted"] is True
        assert entry["all_converted"] is False

    def test_transition_lands_tag_when_supplement_pending_after_main_converted(
        self, db, test_reference, test_mod, auth_headers,  # noqa: F811
    ):
        """SCRUM-6092: transition_completed_text_convert_tags must transition
        the MOD's text_convert_job tag to on_success once the main source
        is converted, even when a supplement remains pending (this is the
        path the on-demand endpoint hits after a supplement conversion
        fails for any reason — oversize, GROBID empty, PDFX error, etc.)."""
        from agr_literature_service.api.crud.file_conversion_crud import (
            transition_completed_text_convert_tags,
        )
        from agr_literature_service.api.crud.reference_utils import get_reference

        _make_referencefile(db, test_reference.new_ref_curie, "paper",
                            "main", "pdf", "md5_main_pdf_6092b", pdf_type="pdf")
        _make_referencefile(db, test_reference.new_ref_curie, "paper_merged",
                            "converted_merged_main", "md", "md5_main_md_6092b")
        _make_referencefile(db, test_reference.new_ref_curie, "paper_supp",
                            "supplement", "pdf", "md5_supp_pdf_6092b")

        reference = get_reference(
            db=db,
            curie_or_reference_id=test_reference.new_ref_curie,
            load_referencefiles=True,
        )

        with patch(
            "agr_literature_service.api.crud.workflow_tag_crud.get_jobs",
            return_value=self._fake_job_for(reference, test_mod.new_mod_id),
        ), patch(
            "agr_literature_service.api.crud.workflow_tag_crud.job_change_atp_code",
        ) as mock_transition:
            transitioned = transition_completed_text_convert_tags(db, reference)

        assert transitioned == 1
        mock_transition.assert_called_once_with(db, 999_001, "on_success")

    def test_transition_holds_when_main_still_pending(
        self, db, test_reference, test_mod, auth_headers,  # noqa: F811
    ):
        """A main source that has not been converted must keep the tag in
        place — supplement-failure tolerance is scoped to supplements only."""
        from agr_literature_service.api.crud.file_conversion_crud import (
            transition_completed_text_convert_tags,
        )
        from agr_literature_service.api.crud.reference_utils import get_reference

        _make_referencefile(db, test_reference.new_ref_curie, "paper",
                            "main", "pdf", "md5_main_pdf_6092c", pdf_type="pdf")
        # No converted_merged_main row → main is still pending.
        _make_referencefile(db, test_reference.new_ref_curie, "paper_supp",
                            "supplement", "pdf", "md5_supp_pdf_6092c")

        reference = get_reference(
            db=db,
            curie_or_reference_id=test_reference.new_ref_curie,
            load_referencefiles=True,
        )

        with patch(
            "agr_literature_service.api.crud.workflow_tag_crud.get_jobs",
            return_value=self._fake_job_for(reference, test_mod.new_mod_id),
        ), patch(
            "agr_literature_service.api.crud.workflow_tag_crud.job_change_atp_code",
        ) as mock_transition:
            transitioned = transition_completed_text_convert_tags(db, reference)

        assert transitioned == 0
        mock_transition.assert_not_called()
