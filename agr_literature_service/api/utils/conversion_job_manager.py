"""
Manager for on-demand file-conversion jobs, shared by the API and the conversion worker.

Keyed by reference_id for idempotency: two concurrent requests to convert the
same reference share a single job rather than launching duplicate PDFX runs.

SCRUM-6855: the state lives in small JSON files in one directory, so every
process that reads it gets the same answer:

- ``job_<job_id>.json``: the job itself (status, per-file progress, owner).
- ``ref_<reference_id>.json``: the reference's ``active`` and ``last`` job ids.
- ``worker.json``: the conversion worker's last heartbeat.
- ``jobs.lock``: an flock serialising every write, so two processes cannot both
  create (or claim) a job for the same reference. Writes are a few small files,
  so one lock for all references costs nothing and leaves no per-reference lock
  files behind.

Files are written to a temporary name and renamed into place, so a reader never
sees a half-written file.

SCRUM-6998: the API only queues a job (status ``queued``); the conversion worker
(``conversion_worker.py``, its own container) claims it, runs it and marks it
``completed`` or ``failed``. Request traffic (gunicorn recycles its workers after
max_requests) and API releases therefore never end a conversion. The directory is
a Docker volume mounted into both containers (``CONVERSION_JOB_STATE_DIR``), so it
survives a redeploy.

A running job carries its worker's id and a heartbeat the worker refreshes every
HEARTBEAT_INTERVAL. A running job whose heartbeat is older than HEARTBEAT_STALE
belongs to a worker that is gone: the next claim takes it again (``resume_count``
+ 1), and the new run re-attaches to the PDFX job each of its PDFs was submitted
as (``pdfx_process_id``) instead of submitting it again. A worker that is stopped
cleanly re-queues its jobs at once. After MAX_RESUMES the job is failed with
WORKER_GONE_ERROR, and the next request starts a new one. A job running longer
than JOB_DEADLINE is failed by its worker's heartbeat.

A job that failed is reported as failed to the next request, for
FAILED_JOB_HOLD, instead of being started again: a paper that always fails would
otherwise be converted again on every poll, and the poller would only ever see
"running". The caller passes ``retry`` to start again anyway. A job that failed
only because its worker kept disappearing is restarted at once; the new job
names it in ``previous_job_id``, as does any job started after a failure.

Every process using the directory must be on one host: the flock does not work
across hosts. If the API or the worker is ever spread over several hosts, this
needs a store they all reach.
"""
import fcntl
import json
import logging
import os
import tempfile
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

logger = logging.getLogger(__name__)

DEFAULT_STATE_DIR = os.path.join(tempfile.gettempdir(), "abc_conversion_jobs")
WORKER_GONE_ERROR = "The conversion worker running this conversion stopped before it finished."
# job creation runs the cleanup at most this often
CLEANUP_INTERVAL = timedelta(minutes=10)
# a temporary file this old is a write whose process was killed before the rename
STALE_TEMP_FILE_AGE = timedelta(hours=1)
# a failed job is reported, not restarted, for this long unless the caller retries
FAILED_JOB_HOLD = timedelta(hours=1)
# the worker refreshes the heartbeat of its running jobs this often
HEARTBEAT_INTERVAL = timedelta(seconds=15)
# a running job (or the worker) whose heartbeat is older than this is gone
HEARTBEAT_STALE = timedelta(minutes=2)
# a job whose worker disappeared this many times is failed instead of resumed
MAX_RESUMES = 3
# a run of a job longer than this is failed by its worker
JOB_DEADLINE = timedelta(hours=2)
JOB_DEADLINE_ERROR = (f"The conversion did not finish within "
                      f"{int(JOB_DEADLINE.total_seconds() // 3600)} hours.")

ACTIVE_STATUSES = ("queued", "running")
WORKER_HEARTBEAT_FILE = "worker.json"


@dataclass
class PerFileProgress:
    source_display_name: str
    source_file_class: str
    # Per-file status: "pending" (not yet attempted), "success", or "failed".
    status: str = "pending"
    error: Optional[str] = None
    source_referencefile_id: Optional[int] = None
    converted_display_name: Optional[str] = None
    converted_file_class: Optional[str] = None
    converted_referencefile_id: Optional[int] = None
    # the PDFX job this source was submitted as, so a resumed run re-attaches to it
    pdfx_process_id: Optional[str] = None


@dataclass
class ConversionJob:
    job_id: str
    reference_id: int
    reference_curie: str
    user_id: str
    status: str  # 'queued', 'running', 'completed', 'failed'
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    last_update: datetime = field(default_factory=datetime.utcnow)
    error_message: str = ""
    per_file_progress: List[PerFileProgress] = field(default_factory=list)
    # the worker running the job, when it claimed it, and its last heartbeat
    owner: Optional[str] = None
    claimed_at: Optional[datetime] = None
    heartbeat_at: Optional[datetime] = None
    # how many times the job was taken again after its worker disappeared
    resume_count: int = 0
    overwrite_tei_md: bool = False
    # the failed job this one was started after, if any
    previous_job_id: Optional[str] = None

    @property
    def is_active(self) -> bool:
        return self.status in ACTIVE_STATUSES

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "reference_id": self.reference_id,
            "reference_curie": self.reference_curie,
            "status": self.status,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
            "error_message": self.error_message,
            "previous_job_id": self.previous_job_id,
            "resume_count": self.resume_count,
            "per_file_progress": [
                {
                    "source_display_name": p.source_display_name,
                    "source_file_class": p.source_file_class,
                    "source_referencefile_id": p.source_referencefile_id,
                    "converted_display_name": p.converted_display_name,
                    "converted_file_class": p.converted_file_class,
                    "converted_referencefile_id": p.converted_referencefile_id,
                    "status": p.status,
                    "error": p.error,
                }
                for p in self.per_file_progress
            ],
        }

    @property
    def duration_seconds(self) -> float:
        end = self.completed_at or datetime.utcnow()
        return (end - self.started_at).total_seconds()


def failure_is_held(job: ConversionJob, now: Optional[datetime] = None) -> bool:
    """Whether a request without ``retry`` reports this job's failure instead of
    starting another job: it failed, for any reason but its worker disappearing,
    less than FAILED_JOB_HOLD ago. Reads nothing and writes nothing; start_job and
    the conversion endpoint's dry run both decide by it (SCRUM-6980)."""
    if job.status != "failed" or job.error_message == WORKER_GONE_ERROR:
        return False
    now = now or datetime.utcnow()
    return now - (job.completed_at or job.last_update) < FAILED_JOB_HOLD


_DATETIME_FIELDS = ("started_at", "completed_at", "last_update", "claimed_at", "heartbeat_at")
_JOB_FIELDS = {f.name for f in fields(ConversionJob)}
_PROGRESS_FIELDS = {f.name for f in fields(PerFileProgress)}


def _job_to_json(job: ConversionJob) -> Dict[str, Any]:
    data = asdict(job)
    for key in _DATETIME_FIELDS:
        data[key] = data[key].isoformat() if data[key] else None
    return data


def _job_from_json(data: Dict[str, Any]) -> ConversionJob:
    # keys another code version wrote (e.g. owner_pid) are dropped, so its files still load
    data = {key: value for key, value in data.items() if key in _JOB_FIELDS}
    for key in _DATETIME_FIELDS:
        data[key] = datetime.fromisoformat(data[key]) if data.get(key) else None
    if data.get("started_at") is None:
        data.pop("started_at", None)
    if data.get("last_update") is None:
        data.pop("last_update", None)
    data["per_file_progress"] = [
        PerFileProgress(**{key: value for key, value in p.items() if key in _PROGRESS_FIELDS})
        for p in data.get("per_file_progress", [])
    ]
    return ConversionJob(**data)


def _is_stale(moment: Optional[datetime], now: datetime) -> bool:
    return moment is None or now - moment > HEARTBEAT_STALE


class ConversionJobManager:
    """File-backed manager for file-conversion jobs, shared by the API and the worker."""

    def __init__(self, state_dir: Optional[str] = None) -> None:
        self.state_dir = state_dir or os.environ.get("CONVERSION_JOB_STATE_DIR") or DEFAULT_STATE_DIR

    # -- storage -----------------------------------------------------------

    def _path(self, name: str) -> str:
        return os.path.join(self.state_dir, name)

    def _read(self, name: str) -> Optional[Dict[str, Any]]:
        try:
            with open(self._path(name)) as handle:
                return json.load(handle)
        except FileNotFoundError:
            return None
        except (OSError, ValueError):
            logger.warning(f"Unreadable conversion job state file {name}; ignoring it", exc_info=True)
            return None

    def _write(self, name: str, data: Dict[str, Any]) -> None:
        os.makedirs(self.state_dir, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=self.state_dir, prefix=f".{name}.")
        try:
            with os.fdopen(fd, "w") as handle:
                json.dump(data, handle)
            os.replace(tmp_path, self._path(name))
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def _remove(self, name: str) -> None:
        try:
            os.unlink(self._path(name))
        except FileNotFoundError:
            pass

    @contextmanager
    def _lock(self) -> Iterator[None]:
        os.makedirs(self.state_dir, exist_ok=True)
        with open(self._path("jobs.lock"), "a") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file, fcntl.LOCK_UN)

    def _load_job(self, job_id: str) -> Optional[ConversionJob]:
        data = self._read(f"job_{job_id}.json")
        if data is None:
            return None
        try:
            return _job_from_json(data)
        except (TypeError, ValueError, KeyError, AttributeError):
            # valid JSON in the wrong shape: treat it as no job rather than failing
            # every request (cleanup removes it once it is old)
            logger.warning(f"Unusable conversion job state file job_{job_id}.json; ignoring it",
                           exc_info=True)
            return None

    def _save_job(self, job: ConversionJob) -> None:
        self._write(f"job_{job.job_id}.json", _job_to_json(job))

    def _reference_index(self, reference_id: int) -> Dict[str, Optional[str]]:
        return self._read(f"ref_{reference_id}.json") or {"active": None, "last": None}

    def _all_jobs(self) -> List[ConversionJob]:
        try:
            names = os.listdir(self.state_dir)
        except FileNotFoundError:
            return []
        jobs = []
        for name in names:
            if name.startswith("job_") and name.endswith(".json"):
                job = self._load_job(name[len("job_"):-len(".json")])
                if job is not None:
                    jobs.append(job)
        return jobs

    def _finish(self, job: ConversionJob, status: str, error: str = "") -> None:
        """Mark a job completed or failed and clear the reference's active entry.
        Call with the lock held."""
        job.status = status
        job.completed_at = job.last_update = datetime.utcnow()
        if error:
            job.error_message = error
        self._save_job(job)
        index = self._reference_index(job.reference_id)
        if index.get("active") == job.job_id:
            index["active"] = None
            self._write(f"ref_{job.reference_id}.json", index)

    def _requeue(self, job: ConversionJob, reason: str) -> bool:
        """Put a job whose worker is gone (or stopping) back in the queue, or fail it
        after MAX_RESUMES. Returns whether it was re-queued. Call with the lock held."""
        if job.resume_count >= MAX_RESUMES:
            logger.warning(f"Conversion job {job.job_id} (reference_id={job.reference_id}): {reason}, "
                           f"and it was already resumed {job.resume_count} times; failing it")
            self._finish(job, "failed", WORKER_GONE_ERROR)
            return False
        logger.info(f"Conversion job {job.job_id} (reference_id={job.reference_id}): {reason}; "
                    f"re-queued (resume {job.resume_count + 1} of {MAX_RESUMES})")
        job.status = "queued"
        job.owner = None
        job.claimed_at = None
        job.heartbeat_at = None
        job.resume_count += 1
        job.last_update = datetime.utcnow()
        self._save_job(job)
        return True

    # -- API side ----------------------------------------------------------

    def create_or_get_job(self, reference_id: int, reference_curie: str, user_id: str,
                          expected_source_files: Optional[List[Dict[str, Optional[str]]]] = None
                          ) -> ConversionJob:
        """The active job for the reference, created if there is none. See start_job,
        which also says whether the job was created by this call."""
        return self.start_job(reference_id, reference_curie, user_id, expected_source_files)[0]

    def start_job(self, reference_id: int, reference_curie: str, user_id: str,
                  expected_source_files: Optional[List[Dict[str, Optional[str]]]] = None,
                  retry: bool = True, overwrite_tei_md: bool = False) -> Tuple[ConversionJob, bool]:
        """
        Atomically return the reference's active (queued or running) job, or queue a
        new one for the conversion worker, as ``(job, created)``.

        This is the idempotency primitive: two concurrent callers asking for the
        same reference, in any process, both receive the same job, and exactly one
        of them gets ``created=True``.

        ``expected_source_files`` — when creating a new job, seeds the job's
        ``per_file_progress`` with a ``pending`` entry for each eligible source.
        Each dict may include ``source_display_name``, ``source_file_class``,
        ``expected_converted_display_name``, ``expected_converted_file_class``.
        Seeding is skipped when an existing job is returned.

        ``retry=False``: when the reference's last job failed less than
        FAILED_JOB_HOLD ago, for any reason but its worker disappearing, return
        that failed job with ``created=False`` instead of queueing another; the
        caller reports the failure. A job started after a failure names it in
        ``previous_job_id``.

        ``overwrite_tei_md`` is stored on the job for the worker that runs it.
        """
        self._cleanup_now_and_then()
        with self._lock():
            index = self._reference_index(reference_id)
            active_id = index.get("active")
            if active_id:
                existing = self._load_job(active_id)
                if existing is not None and existing.is_active:
                    return existing, False

            last_id = index.get("last")
            previous = self._load_job(last_id) if last_id else None
            failed_before = previous is not None and previous.status == "failed"
            if previous is not None and not retry and failure_is_held(previous):
                return previous, False

            job = ConversionJob(
                job_id=str(uuid.uuid4()),
                reference_id=reference_id,
                reference_curie=reference_curie,
                user_id=user_id,
                status="queued",
                overwrite_tei_md=overwrite_tei_md,
                previous_job_id=previous.job_id if failed_before and previous is not None else None,
            )
            for ef in expected_source_files or []:
                source_id = ef.get("source_referencefile_id")
                job.per_file_progress.append(
                    PerFileProgress(
                        source_display_name=str(ef["source_display_name"]),
                        source_file_class=str(ef["source_file_class"]),
                        source_referencefile_id=int(source_id) if source_id is not None else None,
                        converted_display_name=ef.get("expected_converted_display_name"),
                        converted_file_class=ef.get("expected_converted_file_class"),
                        status="pending",
                    )
                )
            self._save_job(job)
            self._write(f"ref_{reference_id}.json", {"active": job.job_id, "last": job.job_id})
        logger.info(
            f"Queued conversion job {job.job_id} for reference_id={reference_id} "
            f"(curie={reference_curie}, user={user_id})"
        )
        return job, True

    def get_job(self, job_id: str) -> Optional[ConversionJob]:
        return self._load_job(job_id)

    def get_active_job_for_reference(self, reference_id: int) -> Optional[ConversionJob]:
        job_id = self._reference_index(reference_id).get("active")
        if not job_id:
            return None
        job = self.get_job(job_id)
        return job if job is not None and job.is_active else None

    def get_last_job_for_reference(self, reference_id: int) -> Optional[ConversionJob]:
        """Return the most recent job for this reference, regardless of status."""
        job_id = self._reference_index(reference_id).get("last")
        return self.get_job(job_id) if job_id else None

    def wait_for_job(self, job_id: str, timeout: float,
                     poll_interval: float = 1.0) -> Optional[ConversionJob]:
        """Read the job until it is no longer active or ``timeout`` seconds have
        passed; return it as last read (None if it does not exist)."""
        deadline = time.monotonic() + timeout
        while True:
            job = self.get_job(job_id)
            if job is None or not job.is_active or time.monotonic() >= deadline:
                return job
            time.sleep(min(poll_interval, max(deadline - time.monotonic(), 0)))

    def worker_alive(self) -> bool:
        """Whether the conversion worker wrote its heartbeat less than HEARTBEAT_STALE ago."""
        data = self._read(WORKER_HEARTBEAT_FILE)
        try:
            beat = datetime.fromisoformat(data["at"]) if data else None
        except (KeyError, TypeError, ValueError):
            beat = None
        return not _is_stale(beat, datetime.utcnow())

    # -- worker side -------------------------------------------------------

    def claim_next_job(self, worker_id: str) -> Optional[ConversionJob]:
        """Take the oldest queued job, or the oldest running job whose worker is gone
        (stale heartbeat), for ``worker_id``: it becomes ``running``, owned by that
        worker, with a fresh heartbeat. A gone worker's job counts a resume, and is
        failed instead once it has been resumed MAX_RESUMES times. None when there is
        nothing to run."""
        with self._lock():
            now = datetime.utcnow()
            candidates = []
            for job in self._all_jobs():
                if job.status == "running" and _is_stale(job.heartbeat_at, now):
                    if not self._requeue(job, f"its worker {job.owner} stopped heartbeating"):
                        continue
                if job.status == "queued":
                    candidates.append(job)
            if not candidates:
                return None
            job = min(candidates, key=lambda j: (j.started_at, j.job_id))
            job.status = "running"
            job.owner = worker_id
            job.claimed_at = job.heartbeat_at = job.last_update = now
            self._save_job(job)
        logger.info(f"Worker {worker_id} claimed conversion job {job.job_id} "
                    f"(reference_id={job.reference_id}, resume_count={job.resume_count})")
        return job

    def heartbeat(self, worker_id: str, job_ids: Iterable[str]) -> List[str]:
        """Refresh the worker's heartbeat file and the heartbeat of each of ``job_ids``
        it still owns and runs. A job whose run has lasted longer than JOB_DEADLINE is
        failed instead; returns the ids failed that way."""
        failed: List[str] = []
        with self._lock():
            now = datetime.utcnow()
            self._write(WORKER_HEARTBEAT_FILE, {"worker_id": worker_id, "at": now.isoformat()})
            for job_id in job_ids:
                job = self._load_job(job_id)
                if job is None or job.status != "running" or job.owner != worker_id:
                    continue
                if job.claimed_at is not None and now - job.claimed_at > JOB_DEADLINE:
                    logger.error(f"Conversion job {job_id} (reference_id={job.reference_id}) has run "
                                 f"longer than {JOB_DEADLINE}; failing it")
                    self._finish(job, "failed", JOB_DEADLINE_ERROR)
                    failed.append(job_id)
                    continue
                job.heartbeat_at = now
                self._save_job(job)
        return failed

    def requeue_jobs(self, worker_id: str) -> int:
        """Put every job ``worker_id`` is running back in the queue (a worker that is
        stopping), so the next worker takes them at once. Returns how many."""
        requeued = 0
        with self._lock():
            for job in self._all_jobs():
                if job.status == "running" and job.owner == worker_id:
                    if self._requeue(job, f"its worker {worker_id} is stopping"):
                        requeued += 1
        return requeued

    def _progress_entry(self, job: ConversionJob, source_display_name: str,
                        source_file_class: str) -> Optional[PerFileProgress]:
        for progress in job.per_file_progress:
            if (progress.source_display_name == source_display_name
                    and progress.source_file_class == source_file_class):
                return progress
        return None

    def set_pdfx_process_id(self, job_id: str, source_display_name: str, source_file_class: str,
                            process_id: str) -> None:
        """Record the PDFX job a source was submitted as (adding its entry if it was
        not seeded)."""
        with self._lock():
            job = self._load_job(job_id)
            if job is None:
                return
            entry = self._progress_entry(job, source_display_name, source_file_class)
            if entry is None:
                entry = PerFileProgress(source_display_name=source_display_name,
                                        source_file_class=source_file_class)
                job.per_file_progress.append(entry)
            entry.pdfx_process_id = process_id
            job.last_update = datetime.utcnow()
            self._save_job(job)

    def get_pdfx_process_id(self, job_id: str, source_display_name: str,
                            source_file_class: str) -> Optional[str]:
        job = self._load_job(job_id)
        entry = self._progress_entry(job, source_display_name, source_file_class) if job else None
        return entry.pdfx_process_id if entry is not None else None

    def record_file_progress(self, job_id: str, *,
                             source_display_name: str,
                             source_file_class: str,
                             success: bool,
                             error: Optional[str] = None,
                             source_referencefile_id: Optional[int] = None,
                             converted_display_name: Optional[str] = None,
                             converted_file_class: Optional[str] = None,
                             converted_referencefile_id: Optional[int] = None) -> None:
        """Update the pending entry for this source (matched by
        ``source_display_name`` + ``source_file_class``). If no pending entry
        was seeded, a new entry is appended (defensive)."""
        new_status = "success" if success else "failed"
        with self._lock():
            job = self._load_job(job_id)
            if job is None:
                return
            existing = self._progress_entry(job, source_display_name, source_file_class)
            if existing is not None:
                existing.status = new_status
                existing.error = error
                if source_referencefile_id is not None:
                    existing.source_referencefile_id = source_referencefile_id
                if success:
                    if converted_display_name is not None:
                        existing.converted_display_name = converted_display_name
                    if converted_file_class is not None:
                        existing.converted_file_class = converted_file_class
                    if converted_referencefile_id is not None:
                        existing.converted_referencefile_id = converted_referencefile_id
                else:
                    existing.converted_display_name = None
                    existing.converted_file_class = None
                    existing.converted_referencefile_id = None
            else:
                job.per_file_progress.append(
                    PerFileProgress(
                        source_display_name=source_display_name,
                        source_file_class=source_file_class,
                        source_referencefile_id=source_referencefile_id,
                        converted_display_name=converted_display_name if success else None,
                        converted_file_class=converted_file_class if success else None,
                        converted_referencefile_id=converted_referencefile_id if success else None,
                        status=new_status,
                        error=error,
                    )
                )
            job.last_update = datetime.utcnow()
            self._save_job(job)

    def complete_job(self, job_id: str, success: bool, error: str = "") -> None:
        """Mark a job completed or failed. A job that is already finished (failed by
        the deadline while its run was still going) is left as it is."""
        with self._lock():
            job = self._load_job(job_id)
            if job is None:
                return
            if not job.is_active:
                logger.warning(f"Conversion job {job_id} finished a run but is already {job.status}; "
                               f"leaving it {job.status}")
                return
            self._finish(job, "completed" if success else "failed", error)
        logger.info(f"Conversion job {job_id} {job.status} (reference_id={job.reference_id})")

    # -- cleanup -----------------------------------------------------------

    def _cleanup_now_and_then(self) -> None:
        """Run cleanup_old_jobs from job creation, at most every CLEANUP_INTERVAL,
        so a request does not pay for a full pass over the directory each time."""
        marker = self._path(".last_cleanup")
        try:
            if datetime.now().timestamp() - os.path.getmtime(marker) < CLEANUP_INTERVAL.total_seconds():
                return
        except OSError:
            pass  # no marker yet
        self.cleanup_old_jobs()

    def cleanup_old_jobs(self, max_age_hours: int = 24) -> int:
        """Delete finished jobs started more than ``max_age_hours`` ago, and the
        reference index entries that point at them; a queued or running job is never
        deleted. Unusable job files and temporary files a killed writer left behind
        are removed by age. Returns the number of jobs removed."""
        cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)
        try:
            names = os.listdir(self.state_dir)
        except FileNotFoundError:
            return 0
        os.makedirs(self.state_dir, exist_ok=True)
        with open(self._path(".last_cleanup"), "w"):
            pass  # touch: see _cleanup_now_and_then
        now = datetime.now().timestamp()
        removed = 0
        for name in names:
            if name.startswith(".job_") or name.startswith(".ref_") or name.startswith(".worker"):
                # a write that never reached its rename (the writer was killed)
                if self._older_than(name, now, STALE_TEMP_FILE_AGE.total_seconds()):
                    self._remove(name)
                continue
            if not (name.startswith("job_") and name.endswith(".json")):
                continue
            with self._lock():
                job = self._load_job(name[len("job_"):-len(".json")])
                if job is None:
                    if self._older_than(name, now, max_age_hours * 3600):
                        self._remove(name)  # unusable, see _load_job
                    continue
                if job.started_at >= cutoff or job.is_active:
                    continue
                self._remove(name)
                index = self._reference_index(job.reference_id)
                if index.get("last") == job.job_id:
                    self._remove(f"ref_{job.reference_id}.json")
            removed += 1
        if removed:
            logger.info(f"Cleaned up {removed} old conversion jobs")
        return removed

    def _older_than(self, name: str, now: float, seconds: float) -> bool:
        try:
            return now - os.path.getmtime(self._path(name)) > seconds
        except OSError:
            return False


conversion_manager = ConversionJobManager()
