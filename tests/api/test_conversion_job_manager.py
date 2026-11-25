"""Unit tests for the file-backed conversion job manager
(``agr_literature_service.api.utils.conversion_job_manager``).

Each test gets a ConversionJobManager on its own temporary directory, so the
module-level singleton is never touched. Jobs are read back from files, so a
lookup returns an equal job, not the same object.
"""
import json
import multiprocessing
import os
from datetime import datetime, timedelta

import pytest

from agr_literature_service.api.utils.conversion_job_manager import (
    CLEANUP_INTERVAL,
    FAILED_JOB_HOLD,
    HEARTBEAT_STALE,
    JOB_DEADLINE,
    JOB_DEADLINE_ERROR,
    MAX_RESUMES,
    WORKER_GONE_ERROR,
    WORKER_HEARTBEAT_FILE,
    ConversionJob,
    ConversionJobManager,
    PerFileProgress,
    running_as,
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


def _ago(delta):
    """An ISO timestamp ``delta`` in the past, as stored in a job file."""
    return (datetime.utcnow() - delta).isoformat()


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
    def test_creates_a_queued_job_with_no_owner(self, mgr):
        job, created = mgr.start_job(3, "AGRKB:1", "user", overwrite_tei_md=True)
        assert created is True
        assert job.status == "queued"
        assert job.owner is None
        assert job.overwrite_tei_md is True
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
        assert second.status == "queued"

    def test_stale_active_entry_falls_through_to_new_job(self, mgr):
        first = mgr.create_or_get_job(3, "AGRKB:1", "user")
        # Simulate a stale active pointer: the job finished but the index still
        # names it as active.
        _set_job_field(mgr, first.job_id, status="completed")
        second = mgr.create_or_get_job(3, "AGRKB:1", "user")
        assert second.job_id != first.job_id
        assert second.status == "queued"

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


def _start_job_in_a_process(state_dir, ready, results):
    """One API worker: start a job for reference 9 together with the others."""
    manager = ConversionJobManager(state_dir=state_dir)
    ready.wait()
    job, created = manager.start_job(9, "AGRKB:9", f"pid-{os.getpid()}")
    results.put((job.job_id, created))


def _claim_in_a_process(state_dir, ready, results):
    """One conversion worker: claim the next job together with the others."""
    manager = ConversionJobManager(state_dir=state_dir)
    ready.wait()
    job = manager.claim_next_job(f"worker-{os.getpid()}")
    results.put(job.job_id if job is not None else None)


class TestTwoProcesses:
    """Two real processes on one directory, as two API workers or two conversion
    workers are: the flock decides, which two managers in one process cannot show."""

    def _run(self, target, state_dir):
        context = multiprocessing.get_context("fork")
        ready, results = context.Barrier(2), context.Queue()
        processes = [context.Process(target=target, args=(state_dir, ready, results)) for _ in range(2)]
        for process in processes:
            process.start()
        try:
            return [results.get(timeout=30) for _ in processes]
        finally:
            for process in processes:
                process.join(timeout=30)

    def test_two_processes_starting_one_reference_share_one_job(self, tmp_path):
        got = self._run(_start_job_in_a_process, str(tmp_path))
        assert len({job_id for job_id, _ in got}) == 1
        assert sorted(created for _, created in got) == [False, True]

    def test_two_workers_never_claim_the_same_job(self, tmp_path):
        job = ConversionJobManager(state_dir=str(tmp_path)).create_or_get_job(9, "AGRKB:9", "user")
        got = self._run(_claim_in_a_process, str(tmp_path))
        assert sorted(got, key=str) == sorted([job.job_id, None], key=str)


class TestFailedJobHold:
    """SCRUM-6855 review: a recent failure is reported rather than started again,
    unless the caller retries; a job whose worker exited is restarted at once."""

    def _failed(self, mgr, error="pdfx 500"):
        job, _ = mgr.start_job(3, "AGRKB:1", "user")
        mgr.complete_job(job.job_id, success=False, error=error)
        return job

    def test_a_recent_failure_is_returned_without_starting_a_job(self, mgr):
        failed = self._failed(mgr)
        job, created = mgr.start_job(3, "AGRKB:1", "user", retry=False)
        assert created is False
        assert job.job_id == failed.job_id
        assert job.status == "failed"
        assert job.error_message == "pdfx 500"

    def test_retry_starts_a_job_that_names_the_failed_one(self, mgr):
        failed = self._failed(mgr)
        job, created = mgr.start_job(3, "AGRKB:1", "user", retry=True)
        assert created is True
        assert job.previous_job_id == failed.job_id
        assert job.to_dict()["previous_job_id"] == failed.job_id

    def test_an_old_failure_is_started_again(self, mgr):
        failed = self._failed(mgr)
        _set_job_field(mgr, failed.job_id,
                       completed_at=(datetime.utcnow() - FAILED_JOB_HOLD - timedelta(minutes=1)).isoformat())
        job, created = mgr.start_job(3, "AGRKB:1", "user", retry=False)
        assert created is True
        assert job.previous_job_id == failed.job_id

    def test_a_job_whose_worker_kept_disappearing_is_restarted_without_retry(self, mgr):
        first, _ = mgr.start_job(3, "AGRKB:1", "user")
        mgr.claim_next_job("w1")
        mgr.complete_job(first.job_id, success=False, error=WORKER_GONE_ERROR)
        job, created = mgr.start_job(3, "AGRKB:1", "user", retry=False)
        assert created is True
        assert job.previous_job_id == first.job_id

    def test_a_job_after_a_success_names_no_previous_job(self, mgr):
        first, _ = mgr.start_job(3, "AGRKB:1", "user")
        mgr.complete_job(first.job_id, success=True)
        job, created = mgr.start_job(3, "AGRKB:1", "user", retry=False)
        assert created is True
        assert job.previous_job_id is None


class TestClaims:
    """SCRUM-6998: the conversion worker claims queued jobs, and the jobs of a worker
    that stopped heartbeating."""

    def test_claim_takes_the_oldest_queued_job_and_owns_it(self, mgr):
        first = mgr.create_or_get_job(1, "AGRKB:1", "user")
        second = mgr.create_or_get_job(2, "AGRKB:2", "user")
        _set_job_field(mgr, second.job_id, started_at=_ago(timedelta(minutes=5)))
        claimed = mgr.claim_next_job("w1")
        assert claimed.job_id == second.job_id
        assert claimed.status == "running"
        assert claimed.owner == "w1"
        assert claimed.heartbeat_at is not None and claimed.claimed_at is not None
        assert mgr.claim_next_job("w1").job_id == first.job_id
        assert mgr.claim_next_job("w1") is None

    def test_a_claimed_job_is_still_the_references_active_job(self, mgr):
        job = mgr.create_or_get_job(1, "AGRKB:1", "user")
        mgr.claim_next_job("w1")
        assert mgr.get_active_job_for_reference(1).job_id == job.job_id
        assert mgr.start_job(1, "AGRKB:1", "user")[1] is False

    def test_a_job_with_a_fresh_heartbeat_is_not_claimed(self, mgr):
        mgr.create_or_get_job(1, "AGRKB:1", "user")
        mgr.claim_next_job("w1")
        assert mgr.claim_next_job("w2") is None

    def test_a_job_whose_worker_stopped_heartbeating_is_resumed(self, mgr):
        job = mgr.create_or_get_job(1, "AGRKB:1", "user")
        mgr.claim_next_job("w1")
        _set_job_field(mgr, job.job_id, heartbeat_at=_ago(HEARTBEAT_STALE + timedelta(seconds=30)))
        # a status read leaves it running: only a worker takes it again
        assert mgr.get_active_job_for_reference(1).status == "running"
        resumed = mgr.claim_next_job("w2")
        assert resumed.job_id == job.job_id
        assert resumed.owner == "w2"
        assert resumed.resume_count == 1

    def test_a_job_resumed_max_times_is_failed_as_worker_gone(self, mgr):
        job = mgr.create_or_get_job(1, "AGRKB:1", "user")
        mgr.claim_next_job("w1")
        _set_job_field(mgr, job.job_id, resume_count=MAX_RESUMES,
                       heartbeat_at=_ago(HEARTBEAT_STALE + timedelta(seconds=30)))
        assert mgr.claim_next_job("w2") is None
        stored = mgr.get_job(job.job_id)
        assert stored.status == "failed"
        assert stored.error_message == WORKER_GONE_ERROR
        assert mgr.get_active_job_for_reference(1) is None

    def test_heartbeat_refreshes_only_the_workers_own_running_jobs(self, mgr):
        mine = mgr.create_or_get_job(1, "AGRKB:1", "user")
        mgr.claim_next_job("w1")
        theirs = mgr.create_or_get_job(2, "AGRKB:2", "user")
        mgr.claim_next_job("w2")
        old = _ago(timedelta(seconds=60))
        _set_job_field(mgr, mine.job_id, heartbeat_at=old)
        _set_job_field(mgr, theirs.job_id, heartbeat_at=old)
        assert mgr.heartbeat("w1", [mine.job_id, theirs.job_id]) == []
        assert mgr.get_job(mine.job_id).heartbeat_at.isoformat() > old
        assert mgr.get_job(theirs.job_id).heartbeat_at.isoformat() == old

    def test_heartbeat_fails_a_run_past_the_deadline(self, mgr):
        job = mgr.create_or_get_job(1, "AGRKB:1", "user")
        mgr.claim_next_job("w1")
        _set_job_field(mgr, job.job_id, claimed_at=_ago(JOB_DEADLINE + timedelta(minutes=1)))
        assert mgr.heartbeat("w1", [job.job_id]) == [job.job_id]
        stored = mgr.get_job(job.job_id)
        assert stored.status == "failed"
        assert stored.error_message == JOB_DEADLINE_ERROR

    def test_requeue_jobs_puts_a_stopping_workers_jobs_back(self, mgr):
        job = mgr.create_or_get_job(1, "AGRKB:1", "user")
        mgr.claim_next_job("w1")
        mgr.set_pdfx_process_id(job.job_id, "a.pdf", "main", "p1")
        assert mgr.requeue_jobs("w1") == 1
        stored = mgr.get_job(job.job_id)
        assert stored.status == "queued"
        assert stored.owner is None
        assert stored.resume_count == 1
        assert mgr.get_pdfx_process_id(job.job_id, "a.pdf", "main") == "p1"
        assert mgr.claim_next_job("w2").job_id == job.job_id

    def test_a_run_that_ends_after_the_deadline_failed_its_job_changes_nothing(self, mgr):
        job = mgr.create_or_get_job(1, "AGRKB:1", "user")
        mgr.claim_next_job("w1")
        _set_job_field(mgr, job.job_id, claimed_at=_ago(JOB_DEADLINE + timedelta(minutes=1)))
        mgr.heartbeat("w1", [job.job_id])
        mgr.complete_job(job.job_id, success=True)
        assert mgr.get_job(job.job_id).status == "failed"
        assert mgr.get_job(job.job_id).error_message == JOB_DEADLINE_ERROR

    def test_a_run_whose_job_was_requeued_cannot_write_to_it(self, mgr):
        """Review (Chris Tabone): between a shutdown's re-queue and the process exit,
        a finishing run must not complete or update the re-queued job."""
        job = mgr.create_or_get_job(
            1, "AGRKB:1", "user",
            expected_source_files=[{"source_display_name": "a.pdf", "source_file_class": "main"}])
        mgr.claim_next_job("w1")
        mgr.requeue_jobs("w1")
        with running_as("w1"):
            mgr.record_file_progress(job.job_id, source_display_name="a.pdf", source_file_class="main",
                                     success=True)
            mgr.set_pdfx_process_id(job.job_id, "a.pdf", "main", "p-old")
            mgr.complete_job(job.job_id, success=True)
        stored = mgr.get_job(job.job_id)
        assert stored.status == "queued"
        assert stored.per_file_progress[0].status == "pending"
        assert stored.per_file_progress[0].pdfx_process_id is None

    def test_only_the_current_owner_of_a_taken_over_job_can_complete_it(self, mgr):
        job = mgr.create_or_get_job(1, "AGRKB:1", "user")
        mgr.claim_next_job("w1")
        _set_job_field(mgr, job.job_id, heartbeat_at=_ago(HEARTBEAT_STALE + timedelta(seconds=30)))
        mgr.claim_next_job("w2")
        with running_as("w1"):  # the old run, still alive
            mgr.complete_job(job.job_id, success=False, error="stale run")
        assert mgr.get_job(job.job_id).status == "running"
        assert mgr.get_job(job.job_id).owner == "w2"
        with running_as("w2"):
            mgr.complete_job(job.job_id, success=True)
        assert mgr.get_job(job.job_id).status == "completed"

    def test_run_still_owns_ends_with_the_deadline(self, mgr):
        job = mgr.create_or_get_job(1, "AGRKB:1", "user")
        mgr.claim_next_job("w1")
        with running_as("w1"):
            assert mgr.run_still_owns(job.job_id) is True
        _set_job_field(mgr, job.job_id, claimed_at=_ago(JOB_DEADLINE + timedelta(minutes=1)))
        mgr.heartbeat("w1", [job.job_id])
        with running_as("w1"):
            assert mgr.run_still_owns(job.job_id) is False
        assert mgr.run_still_owns(job.job_id) is True  # outside a run nothing is restricted

    def test_worker_alive_follows_its_heartbeat_file(self, mgr):
        assert mgr.worker_alive() is False
        mgr.heartbeat("w1", [])
        assert mgr.worker_alive() is True
        with open(os.path.join(mgr.state_dir, WORKER_HEARTBEAT_FILE), "w") as handle:
            json.dump({"worker_id": "w1", "at": _ago(HEARTBEAT_STALE + timedelta(seconds=30))}, handle)
        assert mgr.worker_alive() is False

    def test_pdfx_process_id_round_trip(self, mgr):
        job = mgr.create_or_get_job(
            1, "AGRKB:1", "user",
            expected_source_files=[{"source_display_name": "a.pdf", "source_file_class": "main"}])
        assert mgr.get_pdfx_process_id(job.job_id, "a.pdf", "main") is None
        mgr.set_pdfx_process_id(job.job_id, "a.pdf", "main", "p1")
        mgr.set_pdfx_process_id(job.job_id, "s.pdf", "supplement", "p2")  # not seeded
        assert mgr.get_pdfx_process_id(job.job_id, "a.pdf", "main") == "p1"
        assert mgr.get_pdfx_process_id(job.job_id, "s.pdf", "supplement") == "p2"
        assert len(mgr.get_job(job.job_id).per_file_progress) == 2

    def test_a_job_file_from_the_old_code_still_loads(self, mgr):
        job = mgr.create_or_get_job(1, "AGRKB:1", "user")
        _set_job_field(mgr, job.job_id, owner_pid=123, owner_start_time=456)
        assert mgr.get_job(job.job_id).job_id == job.job_id

    def test_wait_for_job_returns_when_finished_or_on_timeout(self, mgr):
        job = mgr.create_or_get_job(1, "AGRKB:1", "user")
        assert mgr.wait_for_job(job.job_id, timeout=0.2, poll_interval=0.05).status == "queued"
        mgr.claim_next_job("w1")
        mgr.complete_job(job.job_id, success=True)
        assert mgr.wait_for_job(job.job_id, timeout=5).status == "completed"
        assert mgr.wait_for_job("nope", timeout=0.1) is None


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
        assert mgr.get_job(running.job_id).status == "queued"
        assert mgr.get_last_job_for_reference(1) is None

    def test_old_job_failed_as_worker_gone_is_removed(self, mgr):
        job = mgr.create_or_get_job(1, "AGRKB:1", "user")
        mgr.claim_next_job("w1")
        mgr.complete_job(job.job_id, success=False, error=WORKER_GONE_ERROR)
        _set_job_field(mgr, job.job_id, started_at=(datetime.utcnow() - timedelta(hours=48)).isoformat())
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
        assert job.status == "queued"
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
