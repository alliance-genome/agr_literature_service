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
import pytest
from unittest.mock import patch

from fastapi import status
from starlette.testclient import TestClient

from agr_literature_service.api.main import app
from agr_literature_service.api.schemas import ReferencefileSchemaPost
from agr_literature_service.api.crud.referencefile_crud import create_metadata
from agr_literature_service.api.utils.conversion_job_manager import (
    ConversionJob,
    ConversionJobManager,
    conversion_manager,
)

from .test_reference import test_reference  # noqa: F401
from ..fixtures import db  # noqa: F401
from .fixtures import auth_headers  # noqa: F401


# ---------------------------------------------------------------------------
# Unit tests for ConversionJobManager (no DB, no HTTP)
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clear_global_manager():
    """Reset the module-level singleton between tests."""
    conversion_manager._jobs.clear()
    conversion_manager._active_by_reference.clear()
    yield
    conversion_manager._jobs.clear()
    conversion_manager._active_by_reference.clear()


@pytest.fixture
def manager():
    return ConversionJobManager()


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
        assert job.status == "running"
        assert manager.get_job(job.job_id) is job
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
        assert second.status == "running"

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

        with patch(
            "agr_literature_service.api.crud.file_conversion_crud.run_conversion_job",
            return_value=None,
        ):
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

        with patch(
            "agr_literature_service.api.crud.file_conversion_crud.run_conversion_job",
            return_value=None,
        ):
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

        with patch(
            "agr_literature_service.api.crud.file_conversion_crud.run_conversion_job",
            return_value=None,
        ):
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
        """wait=true should block and return 200 when the mocked job creates the row."""
        _make_referencefile(db, test_reference.new_ref_curie, "paper",
                            "main", "pdf", "md5_pdf_wait", pdf_type="pdf")
        ref_curie = test_reference.new_ref_curie

        def fake_sync_job(job_id, reference_id, reference_curie, overwrite_tei_md=False):
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

        with patch(
            "agr_literature_service.api.crud.file_conversion_crud.run_conversion_job",
            side_effect=fake_sync_job,
        ):
            with TestClient(app) as client:
                response = client.get(
                    url=f"/reference/referencefile/conversion_request/{ref_curie}?wait=true",
                    headers=auth_headers,
                )
                assert response.status_code == status.HTTP_200_OK
                body = response.json()
                assert body["status"] == "converted"

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

        with patch(
            "agr_literature_service.api.crud.file_conversion_crud.run_conversion_job",
            return_value=None,
        ):
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
