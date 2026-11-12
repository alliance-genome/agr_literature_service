"""Unit tests for the file-backed conversion job manager
(``agr_literature_service.api.utils.conversion_job_manager``).

Each test gets a ConversionJobManager on its own temporary directory, so the
module-level singleton is never touched. Jobs are read back from files, so a
lookup returns an equal job, not the same object.
"""
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta

import pytest

from agr_literature_service.api.utils.conversion_job_manager import (
    CLEANUP_INTERVAL,
    WORKER_GONE_ERROR,
    ConversionJob,
    ConversionJobManager,
    PerFileProgress,
    _process_start_time,
)


@pytest.fixture
def mgr(tmp_path):
    return ConversionJobManager(state_dir=str(tmp_path))


def _set_job_field(mgr, job_id, **fields):
    """Rewrite fields of a stored job, as another worker (or time) would."""
    path = os.path.join(mgr.state_dir, f"job_{job_id}.json")
    with open(path) as handle:
        data = json.load(handle)
    data.update(fields)
    with open(path, "w") as handle:
        json.dump(data, handle)


def _age(path, delta):
    """Set a file's modification time ``delta`` in the past."""
    when = (datetime.now() - delta).timestamp()
    os.utime(path, (when, when))


def _dead_pid():
    """The pid of a process that has exited."""
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    return proc.pid


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
        assert job.owner_pid == os.getpid()
        assert mgr.get_job(job.job_id) == job

    def test_returns_existing_running_job(self, mgr):
        first = mgr.create_or_get_job(3, "AGRKB:1", "user")
        second = mgr.create_or_get_job(3, "AGRKB:1", "user")
        assert second.job_id == first.job_id

    def test_start_job_says_which_caller_created_the_job(self, mgr):
        first, first_created = mgr.start_job(3, "AGRKB:1", "user")
        second, second_created = mgr.start_job(3, "AGRKB:1", "user")
        assert (first_created, second_created) == (True, False)
        assert second.job_id == first.job_id

    def test_creates_new_job_after_previous_completed(self, mgr):
        first = mgr.create_or_get_job(3, "AGRKB:1", "user")
        mgr.complete_job(first.job_id, success=True)
        second = mgr.create_or_get_job(3, "AGRKB:1", "user")
        assert second.job_id != first.job_id
        assert second.status == "running"

    def test_stale_active_entry_falls_through_to_new_job(self, mgr):
        first = mgr.create_or_get_job(3, "AGRKB:1", "user")
        # Simulate a stale active pointer: the job finished but the index still
        # names it as active.
        _set_job_field(mgr, first.job_id, status="completed")
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
        stored = mgr.get_job(job.job_id)
        assert len(stored.per_file_progress) == 2
        assert stored.per_file_progress[0].source_referencefile_id == 12
        assert stored.per_file_progress[0].converted_display_name == "a.pdf_merged"
        assert stored.per_file_progress[1].source_referencefile_id is None


class TestSharedBetweenWorkers:
    """SCRUM-6855: every worker reads the same state, so a poll gets the same
    answer whichever worker it reaches. Two managers on one directory stand in for
    two gunicorn workers."""

    def test_second_worker_sees_and_joins_the_running_job(self, tmp_path):
        worker_a = ConversionJobManager(state_dir=str(tmp_path))
        worker_b = ConversionJobManager(state_dir=str(tmp_path))
        job = worker_a.create_or_get_job(3, "AGRKB:1", "user")
        assert worker_b.get_active_job_for_reference(3).job_id == job.job_id
        assert worker_b.create_or_get_job(3, "AGRKB:1", "user").job_id == job.job_id

    def test_failure_recorded_by_one_worker_is_seen_by_another(self, tmp_path):
        worker_a = ConversionJobManager(state_dir=str(tmp_path))
        worker_b = ConversionJobManager(state_dir=str(tmp_path))
        job = worker_a.create_or_get_job(3, "AGRKB:1", "user")
        worker_a.record_file_progress(job.job_id, source_display_name="a.pdf",
                                      source_file_class="main", success=False, error="pdfx 500")
        worker_a.complete_job(job.job_id, success=False, error="pdfx 500")
        last = worker_b.get_last_job_for_reference(3)
        assert last.status == "failed"
        assert last.error_message == "pdfx 500"
        assert last.per_file_progress[0].error == "pdfx 500"
        assert worker_b.get_active_job_for_reference(3) is None


class TestWorkerGone:
    """A running job whose worker process is gone is reported failed, so a poll
    does not wait on it forever and a new request starts a fresh job."""

    def test_process_start_time_of_self_and_of_a_gone_process(self):
        assert _process_start_time(os.getpid()) is not None
        assert _process_start_time(_dead_pid()) is None

    def test_job_of_a_gone_worker_is_reported_failed(self, mgr):
        job = mgr.create_or_get_job(3, "AGRKB:1", "user")
        _set_job_field(mgr, job.job_id, owner_pid=_dead_pid())
        stored = mgr.get_job(job.job_id)
        assert stored.status == "failed"
        assert stored.error_message == WORKER_GONE_ERROR
        assert stored.completed_at is not None
        assert mgr.get_active_job_for_reference(3) is None
        assert mgr.get_last_job_for_reference(3).status == "failed"

    def test_reused_pid_is_not_mistaken_for_the_owner(self, mgr):
        # Same pid, different start time: a new process that got the old pid.
        job = mgr.create_or_get_job(3, "AGRKB:1", "user")
        _set_job_field(mgr, job.job_id, owner_start_time=job.owner_start_time - 1)
        assert mgr.get_job(job.job_id).status == "failed"

    def test_without_a_recorded_start_time_the_pid_decides(self, mgr):
        job = mgr.create_or_get_job(3, "AGRKB:1", "user")
        _set_job_field(mgr, job.job_id, owner_start_time=None)
        assert mgr.get_job(job.job_id).status == "running"  # our own pid exists
        _set_job_field(mgr, job.job_id, owner_pid=_dead_pid())
        assert mgr.get_job(job.job_id).status == "failed"

    def test_new_request_replaces_a_gone_workers_job(self, mgr):
        first = mgr.create_or_get_job(3, "AGRKB:1", "user")
        _set_job_field(mgr, first.job_id, owner_pid=_dead_pid())
        second = mgr.create_or_get_job(3, "AGRKB:1", "user")
        assert second.job_id != first.job_id
        assert second.status == "running"
        assert mgr.get_job(first.job_id).status == "failed"


class TestLookups:
    def test_get_job_missing(self, mgr):
        assert mgr.get_job("nope") is None

    def test_active_job_lookup(self, mgr):
        job = mgr.create_or_get_job(3, "AGRKB:1", "user")
        assert mgr.get_active_job_for_reference(3).job_id == job.job_id
        assert mgr.get_active_job_for_reference(999) is None

    def test_active_job_none_after_complete(self, mgr):
        job = mgr.create_or_get_job(3, "AGRKB:1", "user")
        mgr.complete_job(job.job_id, success=True)
        assert mgr.get_active_job_for_reference(3) is None

    def test_active_job_none_when_pointer_stale(self, mgr):
        job = mgr.create_or_get_job(3, "AGRKB:1", "user")
        # Active pointer still present but the job is no longer running.
        _set_job_field(mgr, job.job_id, status="completed")
        assert mgr.get_active_job_for_reference(3) is None

    def test_last_job_survives_completion(self, mgr):
        job = mgr.create_or_get_job(3, "AGRKB:1", "user")
        mgr.complete_job(job.job_id, success=False)
        assert mgr.get_last_job_for_reference(3).job_id == job.job_id
        assert mgr.get_last_job_for_reference(999) is None

    def test_unreadable_state_file_is_ignored(self, mgr):
        job = mgr.create_or_get_job(3, "AGRKB:1", "user")
        with open(os.path.join(mgr.state_dir, f"job_{job.job_id}.json"), "w") as handle:
            handle.write("{not json")
        assert mgr.get_job(job.job_id) is None


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
        p = mgr.get_job(job.job_id).per_file_progress[0]
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
        p = mgr.get_job(job.job_id).per_file_progress[0]
        assert p.status == "failed"
        assert p.error == "pdfx failed"
        assert p.converted_display_name is None

    def test_appends_entry_when_not_seeded(self, mgr):
        job = mgr.create_or_get_job(3, "AGRKB:1", "user")
        mgr.record_file_progress(
            job.job_id, source_display_name="new.pdf", source_file_class="main",
            success=True, source_referencefile_id=8, converted_display_name="new.pdf_merged",
        )
        progress = mgr.get_job(job.job_id).per_file_progress
        assert len(progress) == 1
        assert progress[0].source_display_name == "new.pdf"
        assert progress[0].converted_display_name == "new.pdf_merged"


class TestCompleteJob:
    def test_missing_job_is_noop(self, mgr):
        mgr.complete_job("nope", success=True)  # should not raise

    def test_success_sets_completed(self, mgr):
        job = mgr.create_or_get_job(3, "AGRKB:1", "user")
        mgr.complete_job(job.job_id, success=True)
        stored = mgr.get_job(job.job_id)
        assert stored.status == "completed"
        assert stored.completed_at is not None

    def test_failure_records_error(self, mgr):
        job = mgr.create_or_get_job(3, "AGRKB:1", "user")
        mgr.complete_job(job.job_id, success=False, error="boom")
        stored = mgr.get_job(job.job_id)
        assert stored.status == "failed"
        assert stored.error_message == "boom"


class TestCleanupOldJobs:
    def test_removes_old_completed_keeps_running(self, mgr):
        # Both jobs exist before either is aged: create_or_get_job runs the cleanup
        # itself, so creating the second one would already remove an aged first one.
        old = mgr.create_or_get_job(1, "AGRKB:1", "user")
        mgr.complete_job(old.job_id, success=True)
        running = mgr.create_or_get_job(2, "AGRKB:2", "user")
        two_days_ago = (datetime.utcnow() - timedelta(hours=48)).isoformat()
        _set_job_field(mgr, old.job_id, started_at=two_days_ago)
        _set_job_field(mgr, running.job_id, started_at=two_days_ago)  # old but still running

        removed = mgr.cleanup_old_jobs(max_age_hours=24)
        assert removed == 1
        assert mgr.get_job(old.job_id) is None
        assert mgr.get_job(running.job_id).status == "running"
        assert mgr.get_last_job_for_reference(1) is None

    def test_old_job_of_a_gone_worker_is_removed(self, mgr):
        job = mgr.create_or_get_job(1, "AGRKB:1", "user")
        _set_job_field(mgr, job.job_id, owner_pid=_dead_pid(),
                       started_at=(datetime.utcnow() - timedelta(hours=48)).isoformat())
        assert mgr.cleanup_old_jobs(max_age_hours=24) == 1
        assert mgr.get_job(job.job_id) is None

    def test_nothing_to_remove(self, mgr):
        job = mgr.create_or_get_job(1, "AGRKB:1", "user")
        mgr.complete_job(job.job_id, success=True)
        assert mgr.cleanup_old_jobs(max_age_hours=24) == 0

    def test_missing_state_dir(self, tmp_path):
        assert ConversionJobManager(state_dir=str(tmp_path / "absent")).cleanup_old_jobs() == 0

    def test_creating_a_job_cleans_up_old_ones_now_and_then(self, mgr):
        old = mgr.create_or_get_job(1, "AGRKB:1", "user")
        mgr.complete_job(old.job_id, success=True)
        _set_job_field(mgr, old.job_id, started_at=(datetime.utcnow() - timedelta(hours=48)).isoformat())
        # the first creation just ran the cleanup, so the next one does not
        mgr.create_or_get_job(2, "AGRKB:2", "user")
        assert mgr.get_job(old.job_id) is not None
        # once the interval has passed, it does
        _age(os.path.join(mgr.state_dir, ".last_cleanup"), CLEANUP_INTERVAL + timedelta(minutes=1))
        mgr.create_or_get_job(3, "AGRKB:3", "user")
        assert mgr.get_job(old.job_id) is None

    def test_unusable_job_file_is_ignored_then_removed(self, mgr):
        path = os.path.join(mgr.state_dir, "job_bad.json")
        os.makedirs(mgr.state_dir, exist_ok=True)
        with open(path, "w") as handle:
            json.dump({"job_id": "bad", "no_such_field": 1}, handle)  # valid JSON, wrong shape
        assert mgr.get_job("bad") is None
        job = mgr.create_or_get_job(1, "AGRKB:1", "user")  # not broken by it
        assert job.status == "running"
        mgr.cleanup_old_jobs()
        assert os.path.exists(path)  # recent: kept for now
        _age(path, timedelta(hours=25))
        mgr.cleanup_old_jobs()
        assert not os.path.exists(path)

    def test_stale_temp_file_is_removed(self, mgr):
        os.makedirs(mgr.state_dir, exist_ok=True)
        stale = os.path.join(mgr.state_dir, ".job_x.json.abc123")
        fresh = os.path.join(mgr.state_dir, ".job_y.json.def456")
        for path in (stale, fresh):
            open(path, "w").close()
        _age(stale, timedelta(hours=2))
        mgr.cleanup_old_jobs()
        assert not os.path.exists(stale)
        assert os.path.exists(fresh)
