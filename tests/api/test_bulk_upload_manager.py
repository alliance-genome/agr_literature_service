"""
Simplified tests for BulkUploadManager and BulkUploadJob.
"""

import pytest
from datetime import timedelta

from agr_literature_service.api.utils.bulk_upload_manager import (
    BulkUploadJob,
    BulkUploadManager,
    upload_manager
)


@pytest.fixture(autouse=True)
def clear_global_manager():
    upload_manager._jobs.clear()
    yield
    upload_manager._jobs.clear()


@pytest.fixture
def manager():
    return BulkUploadManager()


class TestBulkUploadJob:
    def test_basic_fields_and_to_dict(self):
        job = BulkUploadJob(
            job_id="job1",
            user_id="user1",
            mod_abbreviation="WB",
            filename="file.tar.gz",
            status="running"
        )
        d = job.to_dict()
        assert d["job_id"] == "job1"
        assert d["status"] == "running"
        assert isinstance(d["start_time"], str)
        assert d["end_time"] is None

    @pytest.mark.parametrize("success,fail_count", [(True, 0), (False, 1)])
    def test_update_progress_counts(self, success, fail_count):
        job = BulkUploadJob(
            job_id="job2",
            user_id="user2",
            mod_abbreviation="FB",
            filename="f.zip",
            status="running"
        )
        job.update_progress(
            processed=1,
            current_file="f1.pdf",
            success=success,
            error="err" if not success else ""
        )
        assert job.processed_files == 1
        assert job.successful_files == (1 if success else 0)
        assert job.failed_files == fail_count
        assert len(job.progress_log) == 1

    def test_progress_percentage_and_duration(self):
        job = BulkUploadJob(
            job_id="job3",
            user_id="user3",
            mod_abbreviation="WB",
            filename="x.tar.gz",
            status="running",
            total_files=4
        )
        job.processed_files = 2
        assert job.progress_percentage == 50.0
        job.end_time = job.start_time + timedelta(seconds=5)
        assert job.duration_seconds == pytest.approx(5.0)


class TestBulkUploadManager:
    def test_create_and_get_job(self, manager):
        jid = manager.create_job("u1", "WB", "f.pdf")
        job = manager.get_job(jid)
        assert job and job.job_id == jid

    def test_update_and_complete(self, manager):
        jid = manager.create_job("u2", "FB", "g.pdf")
        assert manager.update_job(jid, total_files=3)
        job = manager.get_job(jid)
        assert job.total_files == 3
        manager.complete_job(jid, success=False, error="fail")
        job = manager.get_job(jid)
        assert job.status == "failed"
        assert job.error_message == "fail"
        assert job.end_time is not None

    def test_active_and_recent_jobs(self, manager):
        ids = [manager.create_job(f"u{i}", "WB", f"f{i}.pdf") for i in range(5)]
        manager.complete_job(ids[-1], success=True)
        manager.complete_job(ids[-2], success=False)
        active = manager.get_active_jobs()
        assert len(active) == 3
        recent = manager.get_recent_jobs(limit=2)
        assert len(recent) == 2

    def test_cleanup_old_jobs(self, manager):
        j1 = manager.create_job("uA", "WB", "a.pdf")
        manager.complete_job(j1)
        job = manager.get_job(j1)
        job.start_time = job.start_time - timedelta(hours=25)
        cleaned = manager.cleanup_old_jobs()
        assert cleaned == 1
        assert manager.get_job(j1) is None

    def test_get_stats(self, manager):
        manager.create_job("uA", "WB", "a.pdf")
        j2 = manager.create_job("uB", "WB", "b.pdf")
        manager.complete_job(j2, success=True)
        stats = manager.get_stats()
        assert stats["total_jobs"] == 2
        assert stats["running_jobs"] == 1
        assert stats["completed_jobs"] == 1
        assert stats["failed_jobs"] == 0


class TestBulkUploadJobEdgeCases:
    def _job(self):
        return BulkUploadJob(job_id="j", user_id="u", mod_abbreviation="WB",
                             filename="f.pdf", status="running")

    def test_progress_log_capped_at_100(self):
        job = self._job()
        for i in range(150):
            job.update_progress(processed=i, current_file=f"f{i}.pdf", success=True)
        assert len(job.progress_log) == 100

    def test_progress_percentage_zero_when_no_files(self):
        job = self._job()  # total_files defaults to 0
        assert job.progress_percentage == 0.0


class TestBulkUploadManagerEdgeCases:
    def test_update_job_missing_returns_false(self, manager):
        assert manager.update_job("nope", total_files=3) is False

    def test_update_job_ignores_unknown_attr(self, manager):
        jid = manager.create_job("u", "WB", "f.pdf")
        assert manager.update_job(jid, not_a_field="x", total_files=2) is True
        job = manager.get_job(jid)
        assert job.total_files == 2
        assert not hasattr(job, "not_a_field")

    def test_update_progress_missing_returns_false(self, manager):
        assert manager.update_progress("nope", processed=1) is False

    def test_complete_missing_job_is_noop(self, manager):
        manager.complete_job("nope", success=True)  # should not raise

    def test_update_progress_via_manager(self, manager):
        jid = manager.create_job("u", "WB", "f.pdf")
        assert manager.update_progress(jid, processed=1, current_file="f1.pdf") is True
        assert manager.get_job(jid).processed_files == 1

    def test_get_active_jobs_filtered(self, manager):
        manager.create_job("alice", "WB", "a.pdf")
        manager.create_job("bob", "FB", "b.pdf")
        assert len(manager.get_active_jobs(user_id="alice")) == 1
        assert len(manager.get_active_jobs(mod_abbreviation="FB")) == 1

    def test_get_recent_jobs_filtered_by_user(self, manager):
        manager.create_job("alice", "WB", "a.pdf")
        manager.create_job("bob", "FB", "b.pdf")
        recent = manager.get_recent_jobs(user_id="alice")
        assert len(recent) == 1 and recent[0].user_id == "alice"


class TestGlobalManager:
    def test_global_instance(self):
        jid = upload_manager.create_job("uX", "FB", "z.pdf")
        assert upload_manager.get_job(jid) is not None

# end of tests
