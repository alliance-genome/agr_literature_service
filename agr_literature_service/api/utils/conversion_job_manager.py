"""
Manager for on-demand file-conversion jobs, shared by every API worker.

Keyed by reference_id for idempotency: two concurrent requests to convert the
same reference share a single job rather than launching duplicate PDFX runs.

SCRUM-6855: the state used to live in each worker process's memory. The API runs
several gunicorn workers, and a status poll that reached a different worker saw
no running job, so it could start a duplicate, report another job_id, or lose a
failure. It now lives in small JSON files in one directory that every worker of
the API container shares:

- ``job_<job_id>.json``: the job itself (status, per-file progress, owner).
- ``ref_<reference_id>.json``: the reference's ``active`` and ``last`` job ids.
- ``jobs.lock``: an flock serialising every write, so two workers cannot both
  create a job for the same reference. Writes are a few small files, so one lock
  for all references costs nothing and leaves no per-reference lock files behind.

Files are written to a temporary name and renamed into place, so a reader never
sees a half-written file.

A job runs in the worker that created it (a BackgroundTask, or inline with
``wait``). The job records that process's pid and start time; a "running" job
whose process is gone (gunicorn recycles workers after max_requests, a restart
kills them all) is reported failed instead of running forever. The start time
guards against pid reuse, which is likely in a container whose pids restart low.

The directory must be shared by every worker that serves the endpoint and must
not be shared across hosts: by default it is under /tmp inside the API container.
If the API is ever spread over several containers or hosts, this needs a store
they all reach, such as a database table.
"""
import fcntl
import json
import logging
import os
import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Iterator, List, Optional, Tuple

logger = logging.getLogger(__name__)

DEFAULT_STATE_DIR = os.path.join(tempfile.gettempdir(), "abc_conversion_jobs")
WORKER_GONE_ERROR = "The API worker running this conversion exited before it finished."
# job creation runs the cleanup at most this often
CLEANUP_INTERVAL = timedelta(minutes=10)
# a temporary file this old is a write whose process was killed before the rename
STALE_TEMP_FILE_AGE = timedelta(hours=1)


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


@dataclass
class ConversionJob:
    job_id: str
    reference_id: int
    reference_curie: str
    user_id: str
    status: str  # 'running', 'completed', 'failed'
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    last_update: datetime = field(default_factory=datetime.utcnow)
    error_message: str = ""
    per_file_progress: List[PerFileProgress] = field(default_factory=list)
    # The process running the job: its pid and its start time from /proc, which
    # together identify it even if the pid is later reused.
    owner_pid: Optional[int] = None
    owner_start_time: Optional[int] = None

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


def _process_start_time(pid: int) -> Optional[int]:
    """Start time of a live process, in clock ticks since boot (field 22 of
    /proc/<pid>/stat), or None when there is no such process."""
    try:
        with open(f"/proc/{pid}/stat") as stat_file:
            stat = stat_file.read()
    except OSError:
        return None
    # The command name (field 2) is in parentheses and may contain spaces, so
    # split after its closing parenthesis; starttime is then the 20th field.
    try:
        return int(stat[stat.rindex(")") + 2:].split()[19])
    except (ValueError, IndexError):
        return None


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # it exists, owned by another user
    return True


def _owner_alive(job: ConversionJob) -> bool:
    if job.owner_pid is None:
        return True  # nothing recorded to check against
    if job.owner_start_time is None:
        # The start time could not be read when the job was created (no /proc):
        # fall back to whether the pid exists, without the guard against reuse.
        return _pid_exists(job.owner_pid)
    return _process_start_time(job.owner_pid) == job.owner_start_time


def _job_to_json(job: ConversionJob) -> Dict[str, Any]:
    data = asdict(job)
    for key in ("started_at", "completed_at", "last_update"):
        data[key] = data[key].isoformat() if data[key] else None
    return data


def _job_from_json(data: Dict[str, Any]) -> ConversionJob:
    for key in ("started_at", "completed_at", "last_update"):
        data[key] = datetime.fromisoformat(data[key]) if data.get(key) else None
    data["per_file_progress"] = [PerFileProgress(**p) for p in data.get("per_file_progress", [])]
    return ConversionJob(**data)


class ConversionJobManager:
    """File-backed manager for file-conversion jobs, shared by every API worker."""

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
            # valid JSON in the wrong shape, e.g. left by another code version:
            # treat it as no job rather than failing every request (cleanup
            # removes it once it is old)
            logger.warning(f"Unusable conversion job state file job_{job_id}.json; ignoring it",
                           exc_info=True)
            return None

    def _save_job(self, job: ConversionJob) -> None:
        self._write(f"job_{job.job_id}.json", _job_to_json(job))

    def _reference_index(self, reference_id: int) -> Dict[str, Optional[str]]:
        return self._read(f"ref_{reference_id}.json") or {"active": None, "last": None}

    def _fail_if_owner_gone(self, job: ConversionJob) -> ConversionJob:
        """Mark a "running" job failed when the process running it is gone.
        Call with the lock held."""
        if job.status == "running" and not _owner_alive(job):
            logger.warning(f"Conversion job {job.job_id} (reference_id={job.reference_id}): "
                           f"worker pid {job.owner_pid} is gone; marking the job failed")
            job.status = "failed"
            job.completed_at = job.last_update = datetime.utcnow()
            job.error_message = WORKER_GONE_ERROR
            self._save_job(job)
            index = self._reference_index(job.reference_id)
            if index.get("active") == job.job_id:
                index["active"] = None
                self._write(f"ref_{job.reference_id}.json", index)
        return job

    # -- public API --------------------------------------------------------

    def create_or_get_job(self, reference_id: int, reference_curie: str, user_id: str,
                          expected_source_files: Optional[List[Dict[str, Optional[str]]]] = None) -> ConversionJob:
        """The running job for the reference, created if there is none. See start_job,
        which also says whether the job was created by this call."""
        return self.start_job(reference_id, reference_curie, user_id, expected_source_files)[0]

    def start_job(self, reference_id: int, reference_curie: str, user_id: str,
                  expected_source_files: Optional[List[Dict[str, Optional[str]]]] = None
                  ) -> Tuple[ConversionJob, bool]:
        """
        Atomically return the existing running job for the reference or create a new
        one, as ``(job, created)``.

        This is the idempotency primitive: two concurrent callers asking for the
        same reference, in any worker, both receive the same job, and exactly one of
        them gets ``created=True``. Only that caller may run the job: the others
        must report it as running, or the conversion would run twice.

        ``expected_source_files`` — when creating a new job, seeds the job's
        ``per_file_progress`` with a ``pending`` entry for each eligible source.
        Each dict may include ``source_display_name``, ``source_file_class``,
        ``expected_converted_display_name``, ``expected_converted_file_class``.
        Seeding is skipped when an existing running job is returned.

        The job is owned by the calling process, which is the one that runs it.
        """
        self._cleanup_now_and_then()
        with self._lock():
            index = self._reference_index(reference_id)
            active_id = index.get("active")
            if active_id:
                existing = self._load_job(active_id)
                if existing is not None:
                    existing = self._fail_if_owner_gone(existing)
                    if existing.status == "running":
                        return existing, False

            pid = os.getpid()
            job = ConversionJob(
                job_id=str(uuid.uuid4()),
                reference_id=reference_id,
                reference_curie=reference_curie,
                user_id=user_id,
                status="running",
                owner_pid=pid,
                owner_start_time=_process_start_time(pid),
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
            f"Created conversion job {job.job_id} for reference_id={reference_id} "
            f"(curie={reference_curie}, user={user_id}, pid={job.owner_pid})"
        )
        return job, True

    def get_job(self, job_id: str) -> Optional[ConversionJob]:
        job = self._load_job(job_id)
        if job is None or job.status != "running" or _owner_alive(job):
            return job
        with self._lock():
            job = self._load_job(job_id)
            return self._fail_if_owner_gone(job) if job is not None else None

    def get_active_job_for_reference(self, reference_id: int) -> Optional[ConversionJob]:
        job_id = self._reference_index(reference_id).get("active")
        if not job_id:
            return None
        job = self.get_job(job_id)
        return job if job is not None and job.status == "running" else None

    def get_last_job_for_reference(self, reference_id: int) -> Optional[ConversionJob]:
        """Return the most recent job for this reference, regardless of status."""
        job_id = self._reference_index(reference_id).get("last")
        return self.get_job(job_id) if job_id else None

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
            existing: Optional[PerFileProgress] = None
            for p in job.per_file_progress:
                if (p.source_display_name == source_display_name
                        and p.source_file_class == source_file_class):
                    existing = p
                    break
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
        with self._lock():
            job = self._load_job(job_id)
            if job is None:
                return
            job.status = "completed" if success else "failed"
            job.completed_at = datetime.utcnow()
            job.last_update = job.completed_at
            if error:
                job.error_message = error
            self._save_job(job)
            index = self._reference_index(job.reference_id)
            if index.get("active") == job_id:
                index["active"] = None
                self._write(f"ref_{job.reference_id}.json", index)
        logger.info(f"Conversion job {job_id} {job.status} (reference_id={job.reference_id})")

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
        reference index entries that point at them; a running job is never deleted.
        Unusable job files and temporary files a killed writer left behind are
        removed by age. Returns the number of jobs removed."""
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
            if name.startswith(".job_") or name.startswith(".ref_"):
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
                if job.started_at >= cutoff:
                    continue
                if self._fail_if_owner_gone(job).status == "running":
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
