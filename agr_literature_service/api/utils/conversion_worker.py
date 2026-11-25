"""
The conversion worker: runs on-demand file conversions outside the API (SCRUM-6998).

The API queues a job in the shared job-state directory (see
``conversion_job_manager``); this process, in its own container
(``conversion_worker`` in docker-compose), claims queued jobs and runs each one
with ``run_conversion_job`` in a thread pool. Gunicorn recycling the API's
workers, and redeploying the API, therefore never end a conversion.

While a job runs, a heartbeat thread refreshes its heartbeat (and the worker's
own ``worker.json``, which the API reads to tell whether a worker is running)
every HEARTBEAT_INTERVAL. If this process dies, its jobs' heartbeats go stale
and the next worker claims them again; a resumed PDF re-attaches to the PDFX job
it was submitted as. On SIGTERM (every deployment restarts the worker) it
re-queues its running jobs at once and exits.

Run with ``python -m agr_literature_service.api.utils.conversion_worker``.
"""
import logging
import logging.config
import os
import signal
import threading
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Callable, Dict, Optional

from agr_literature_service.api.utils.conversion_job_manager import (
    HEARTBEAT_INTERVAL,
    ConversionJobManager,
    conversion_manager,
)

logger = logging.getLogger(__name__)

DEFAULT_CONCURRENCY = 4
DEFAULT_POLL_INTERVAL = 2.0

RunJob = Callable[[str, int, str, bool], None]


def _default_run_job(job_id: str, reference_id: int, reference_curie: str,
                     overwrite_tei_md: bool) -> None:
    # imported here: it pulls in the database and the pdf2md code, which the
    # manager-only tests of this module do not need
    from agr_literature_service.api.utils.conversion_processor import run_conversion_job
    run_conversion_job(job_id, reference_id, reference_curie, overwrite_tei_md)


class ConversionWorker:
    """Claims queued conversion jobs and runs up to ``concurrency`` at once."""

    def __init__(self, manager: ConversionJobManager, run_job: Optional[RunJob] = None,
                 concurrency: int = DEFAULT_CONCURRENCY,
                 poll_interval: float = DEFAULT_POLL_INTERVAL) -> None:
        self.manager = manager
        self.run_job: RunJob = run_job or _default_run_job
        self.concurrency = max(1, concurrency)
        self.poll_interval = poll_interval
        # unique per process, so a restarted worker never takes a dead run's jobs for its own
        self.worker_id = f"{os.uname().nodename}-{uuid.uuid4().hex[:12]}"
        self.stop_event = threading.Event()
        self._executor = ThreadPoolExecutor(max_workers=self.concurrency,
                                            thread_name_prefix="conversion")
        self._running: Dict[str, Future] = {}
        self._running_lock = threading.Lock()

    def _unfinished_job_ids(self):
        with self._running_lock:
            for job_id in [job_id for job_id, future in self._running.items() if future.done()]:
                del self._running[job_id]
            return list(self._running)

    def step(self) -> Optional[str]:
        """Claim one job and start it, when below ``concurrency``; return its id, or
        None when the pool is full or nothing is waiting."""
        if len(self._unfinished_job_ids()) >= self.concurrency:
            return None
        job = self.manager.claim_next_job(self.worker_id)
        if job is None:
            return None
        future = self._executor.submit(self._run, job.job_id, job.reference_id,
                                       job.reference_curie, job.overwrite_tei_md)
        with self._running_lock:
            self._running[job.job_id] = future
        return job.job_id

    def _run(self, job_id: str, reference_id: int, reference_curie: str,
             overwrite_tei_md: bool) -> None:
        logger.info(f"Running conversion job {job_id} for {reference_curie}")
        try:
            self.run_job(job_id, reference_id, reference_curie, overwrite_tei_md)
        except Exception as exc:  # run_conversion_job never raises; a replacement might
            logger.exception(f"Conversion job {job_id} raised")
            self.manager.complete_job(job_id, success=False, error=str(exc))

    def beat(self) -> None:
        """Refresh the heartbeat of this worker and of the jobs it is running."""
        for job_id in self.manager.heartbeat(self.worker_id, self._unfinished_job_ids()):
            logger.error(f"Conversion job {job_id} passed its deadline and was failed")

    def shutdown(self) -> int:
        """Re-queue the jobs this worker is running, so the next worker takes them
        at once. Returns how many."""
        requeued = self.manager.requeue_jobs(self.worker_id)
        logger.info(f"Worker {self.worker_id} stopping: {requeued} running conversion jobs re-queued")
        return requeued

    def _heartbeat_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.beat()
            except Exception:
                logger.exception("Conversion worker heartbeat failed")
            self.stop_event.wait(HEARTBEAT_INTERVAL.total_seconds())

    def run_forever(self) -> None:
        """Claim and run jobs until SIGTERM/SIGINT, then re-queue the running ones and
        exit the process at once: the pool's threads are not daemon threads, and a
        conversion left running would keep writing to a job that is queued again."""
        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, lambda *_: self.stop_event.set())
        logger.info(f"Conversion worker {self.worker_id} started (concurrency {self.concurrency}, "
                    f"state dir {self.manager.state_dir})")
        self.beat()
        threading.Thread(target=self._heartbeat_loop, name="heartbeat", daemon=True).start()
        while not self.stop_event.is_set():
            try:
                if self.step() is not None:
                    continue  # look for another job straight away
            except Exception:
                logger.exception("Conversion worker failed to claim a job")
            self.stop_event.wait(self.poll_interval)
        self.shutdown()
        logging.shutdown()
        os._exit(0)


def _configure_logging() -> None:
    config_path = os.path.join(os.getcwd(), "logging.conf")
    if os.path.exists(config_path):
        logging.config.fileConfig(config_path, disable_existing_loggers=False)
    else:
        logging.basicConfig(level=logging.INFO,
                            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


def main() -> None:  # pragma: no cover - the container entry point
    _configure_logging()
    try:
        concurrency = int(os.environ.get("CONVERSION_WORKER_CONCURRENCY", DEFAULT_CONCURRENCY))
    except ValueError:
        concurrency = DEFAULT_CONCURRENCY
    ConversionWorker(conversion_manager, concurrency=concurrency).run_forever()


if __name__ == "__main__":  # pragma: no cover
    main()
