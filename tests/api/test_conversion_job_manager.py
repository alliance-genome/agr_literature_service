"""Unit tests for the thread-safe in-memory conversion job manager
(``agr_literature_service.api.utils.conversion_job_manager``).

A fresh ConversionJobManager is instantiated per test so the module-level
singleton is never touched.
"""
from datetime import datetime, timedelta

import pytest

from agr_literature_service.api.utils.conversion_job_manager import (
    ConversionJob,
    ConversionJobManager,
    PerFileProgress,
)


@pytest.fixture
def mgr():
    return ConversionJobManager()


class TestDataclasses:
    def test_per_file_progress_defaults(self):
        p = PerFileProgress(source_display_name="a.pdf", source_file_class="main")
        assert p.status == "pending"
        assert p.error is None
        assert p.converted_referencefile_id is None

    def test_conversion_job_to_dict_and_duration(self):
        job = ConversionJob(
            job_id="j1", reference_id=3, reference_curie="AGRKB:1",
            user_id="u", status="running",
        )
        job.per_file_progress.append(
            PerFileProgress(source_display_name="a.pdf", source_file_class="main",
                            status="success")
        )
        d = job.to_dict()
        assert d["job_id"] == "j1"
        assert d["status"] == "running"
        assert d["completed_at"] is None
        assert d["per_file_progress"][0]["status"] == "success"
        assert isinstance(d["duration_seconds"], float)
        assert job.duration_seconds >= 0.0


class TestCreateOrGetJob:
    def test_creates_new_running_job(self, mgr):
        job = mgr.create_or_get_job(3, "AGRKB:1", "user")
        assert job.status == "running"
        assert mgr.get_job(job.job_id) is job

    def test_returns_existing_running_job(self, mgr):
        first = mgr.create_or_get_job(3, "AGRKB:1", "user")
        second = mgr.create_or_get_job(3, "AGRKB:1", "user")
        assert first is second

    def test_creates_new_job_after_previous_completed(self, mgr):
        first = mgr.create_or_get_job(3, "AGRKB:1", "user")
        mgr.complete_job(first.job_id, success=True)
        second = mgr.create_or_get_job(3, "AGRKB:1", "user")
        assert second.job_id != first.job_id
        assert second.status == "running"

    def test_stale_active_entry_falls_through_to_new_job(self, mgr):
        first = mgr.create_or_get_job(3, "AGRKB:1", "user")
        # Simulate a stale active pointer: job finished but was never popped
        # from _active_by_reference (e.g. an unexpected crash path).
        first.status = "completed"
        second = mgr.create_or_get_job(3, "AGRKB:1", "user")
        assert second.job_id != first.job_id
        assert second.status == "running"

    def test_seeds_expected_source_files(self, mgr):
        job = mgr.create_or_get_job(
            3, "AGRKB:1", "user",
            expected_source_files=[
                {"source_display_name": "a.pdf", "source_file_class": "main",
                 "source_referencefile_id": "12",
                 "expected_converted_display_name": "a.pdf_merged",
                 "expected_converted_file_class": "converted_merged_main"},
                {"source_display_name": "b.pdf", "source_file_class": "supplement",
                 "source_referencefile_id": None},
            ],
        )
        assert len(job.per_file_progress) == 2
        assert job.per_file_progress[0].source_referencefile_id == 12
        assert job.per_file_progress[0].converted_display_name == "a.pdf_merged"
        assert job.per_file_progress[1].source_referencefile_id is None


class TestLookups:
    def test_get_job_missing(self, mgr):
        assert mgr.get_job("nope") is None

    def test_active_job_lookup(self, mgr):
        job = mgr.create_or_get_job(3, "AGRKB:1", "user")
        assert mgr.get_active_job_for_reference(3) is job
        assert mgr.get_active_job_for_reference(999) is None

    def test_active_job_none_after_complete(self, mgr):
        job = mgr.create_or_get_job(3, "AGRKB:1", "user")
        mgr.complete_job(job.job_id, success=True)
        assert mgr.get_active_job_for_reference(3) is None

    def test_active_job_none_when_pointer_stale(self, mgr):
        job = mgr.create_or_get_job(3, "AGRKB:1", "user")
        # Active pointer still present but the job is no longer running.
        job.status = "completed"
        assert mgr.get_active_job_for_reference(3) is None

    def test_last_job_survives_completion(self, mgr):
        job = mgr.create_or_get_job(3, "AGRKB:1", "user")
        mgr.complete_job(job.job_id, success=False)
        assert mgr.get_last_job_for_reference(3) is job
        assert mgr.get_last_job_for_reference(999) is None


class TestRecordFileProgress:
    def test_missing_job_is_noop(self, mgr):
        mgr.record_file_progress("nope", source_display_name="a", source_file_class="main",
                                 success=True)  # should not raise

    def test_updates_seeded_pending_entry_on_success(self, mgr):
        job = mgr.create_or_get_job(
            3, "AGRKB:1", "user",
            expected_source_files=[{"source_display_name": "a.pdf", "source_file_class": "main"}],
        )
        mgr.record_file_progress(
            job.job_id, source_display_name="a.pdf", source_file_class="main",
            success=True, source_referencefile_id=17, converted_display_name="a.pdf_merged",
            converted_file_class="converted_merged_main", converted_referencefile_id=5,
        )
        p = job.per_file_progress[0]
        assert p.status == "success"
        assert p.source_referencefile_id == 17
        assert p.converted_referencefile_id == 5

    def test_failure_clears_converted_fields(self, mgr):
        job = mgr.create_or_get_job(
            3, "AGRKB:1", "user",
            expected_source_files=[{
                "source_display_name": "a.pdf", "source_file_class": "main",
                "expected_converted_display_name": "a.pdf_merged",
                "expected_converted_file_class": "converted_merged_main",
            }],
        )
        mgr.record_file_progress(
            job.job_id, source_display_name="a.pdf", source_file_class="main",
            success=False, error="pdfx failed",
        )
        p = job.per_file_progress[0]
        assert p.status == "failed"
        assert p.error == "pdfx failed"
        assert p.converted_display_name is None

    def test_appends_entry_when_not_seeded(self, mgr):
        job = mgr.create_or_get_job(3, "AGRKB:1", "user")
        mgr.record_file_progress(
            job.job_id, source_display_name="new.pdf", source_file_class="main",
            success=True, source_referencefile_id=8, converted_display_name="new.pdf_merged",
        )
        assert len(job.per_file_progress) == 1
        assert job.per_file_progress[0].source_display_name == "new.pdf"
        assert job.per_file_progress[0].converted_display_name == "new.pdf_merged"


class TestCompleteJob:
    def test_missing_job_is_noop(self, mgr):
        mgr.complete_job("nope", success=True)  # should not raise

    def test_success_sets_completed(self, mgr):
        job = mgr.create_or_get_job(3, "AGRKB:1", "user")
        mgr.complete_job(job.job_id, success=True)
        assert job.status == "completed"
        assert job.completed_at is not None

    def test_failure_records_error(self, mgr):
        job = mgr.create_or_get_job(3, "AGRKB:1", "user")
        mgr.complete_job(job.job_id, success=False, error="boom")
        assert job.status == "failed"
        assert job.error_message == "boom"


class TestCleanupOldJobs:
    def test_removes_old_completed_keeps_running(self, mgr):
        old = mgr.create_or_get_job(1, "AGRKB:1", "user")
        mgr.complete_job(old.job_id, success=True)
        old.started_at = datetime.utcnow() - timedelta(hours=48)

        running = mgr.create_or_get_job(2, "AGRKB:2", "user")
        running.started_at = datetime.utcnow() - timedelta(hours=48)  # old but still running

        removed = mgr.cleanup_old_jobs(max_age_hours=24)
        assert removed == 1
        assert mgr.get_job(old.job_id) is None
        assert mgr.get_job(running.job_id) is running
        assert mgr.get_last_job_for_reference(1) is None

    def test_nothing_to_remove(self, mgr):
        job = mgr.create_or_get_job(1, "AGRKB:1", "user")
        mgr.complete_job(job.job_id, success=True)
        assert mgr.cleanup_old_jobs(max_age_hours=24) == 0
