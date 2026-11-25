"""The conversion worker claims queued jobs and runs them outside the API (SCRUM-6998).

Each test gets a ConversionJobManager on its own temporary directory and a fake
``run_job``, so no database, S3 or PDFX is touched.
"""
import json
import os
import threading
from datetime import datetime, timedelta

import pytest

from agr_literature_service.api.utils.conversion_job_manager import (
    HEARTBEAT_STALE,
    ConversionJobManager,
)
from agr_literature_service.api.utils.conversion_worker import ConversionWorker


@pytest.fixture
def mgr(tmp_path):
    return ConversionJobManager(state_dir=str(tmp_path))


def _completing_run(manager, ran):
    def run(job_id, reference_id, reference_curie, overwrite_tei_md):
        ran.append((job_id, reference_id, reference_curie, overwrite_tei_md))
        manager.complete_job(job_id, success=True)
    return run


def _wait_done(worker):
    with worker._running_lock:
        futures = list(worker._running.values())
    for future in futures:
        future.result(timeout=10)


class TestConversionWorker:
    def test_step_runs_a_queued_job(self, mgr):
        ran = []
        job, _ = mgr.start_job(3, "AGRKB:3", "user", overwrite_tei_md=True)
        worker = ConversionWorker(mgr, run_job=_completing_run(mgr, ran))
        assert worker.step() == job.job_id
        _wait_done(worker)
        assert ran == [(job.job_id, 3, "AGRKB:3", True)]
        assert mgr.get_job(job.job_id).status == "completed"
        assert worker.step() is None

    def test_step_respects_concurrency(self, mgr):
        release = threading.Event()

        def blocked_run(job_id, *_):
            release.wait(10)
            mgr.complete_job(job_id, success=True)

        first, _ = mgr.start_job(1, "AGRKB:1", "user")
        second, _ = mgr.start_job(2, "AGRKB:2", "user")
        worker = ConversionWorker(mgr, run_job=blocked_run, concurrency=1)
        assert worker.step() == first.job_id
        assert worker.step() is None
        assert mgr.get_job(second.job_id).status == "queued"
        release.set()
        _wait_done(worker)
        assert worker.step() == second.job_id
        _wait_done(worker)

    def test_a_run_that_raises_fails_its_job(self, mgr):
        def broken_run(*_):
            raise RuntimeError("boom")

        job, _ = mgr.start_job(1, "AGRKB:1", "user")
        worker = ConversionWorker(mgr, run_job=broken_run)
        worker.step()
        _wait_done(worker)
        assert mgr.get_job(job.job_id).status == "failed"
        assert mgr.get_job(job.job_id).error_message == "boom"

    def test_beat_keeps_its_jobs_alive_and_tells_the_api_it_runs(self, mgr):
        release = threading.Event()
        job, _ = mgr.start_job(1, "AGRKB:1", "user")
        worker = ConversionWorker(mgr, run_job=lambda *_: release.wait(10))
        worker.step()
        assert mgr.worker_alive() is False
        worker.beat()
        assert mgr.worker_alive() is True
        assert mgr.get_job(job.job_id).owner == worker.worker_id
        release.set()
        _wait_done(worker)

    def test_shutdown_requeues_its_running_jobs(self, mgr):
        release = threading.Event()
        job, _ = mgr.start_job(1, "AGRKB:1", "user")
        worker = ConversionWorker(mgr, run_job=lambda *_: release.wait(10))
        worker.step()
        assert worker.shutdown() == 1
        stored = mgr.get_job(job.job_id)
        assert stored.status == "queued"
        assert stored.resume_count == 1
        release.set()
        _wait_done(worker)

    def test_a_run_that_finishes_after_shutdown_leaves_its_job_queued(self, mgr):
        release = threading.Event()

        def run_then_complete(job_id, *_):
            release.wait(10)
            mgr.complete_job(job_id, success=True)

        job, _ = mgr.start_job(1, "AGRKB:1", "user")
        worker = ConversionWorker(mgr, run_job=run_then_complete)
        worker.step()
        worker.shutdown()
        release.set()
        _wait_done(worker)
        assert mgr.get_job(job.job_id).status == "queued"

    def test_a_job_left_by_a_dead_worker_is_resumed(self, mgr):
        ran = []
        job, _ = mgr.start_job(1, "AGRKB:1", "user")
        mgr.claim_next_job("dead-worker")
        path = os.path.join(mgr.state_dir, f"job_{job.job_id}.json")
        with open(path) as handle:
            data = json.load(handle)
        data["heartbeat_at"] = (datetime.utcnow() - HEARTBEAT_STALE - timedelta(seconds=30)).isoformat()
        with open(path, "w") as handle:
            json.dump(data, handle)
        worker = ConversionWorker(mgr, run_job=_completing_run(mgr, ran))
        assert worker.step() == job.job_id
        _wait_done(worker)
        stored = mgr.get_job(job.job_id)
        assert stored.status == "completed"
        assert stored.resume_count == 1

    def test_each_worker_process_gets_its_own_id(self, mgr):
        assert ConversionWorker(mgr).worker_id != ConversionWorker(mgr).worker_id
