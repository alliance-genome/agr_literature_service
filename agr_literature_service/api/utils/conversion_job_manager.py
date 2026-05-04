"""
Thread-safe in-memory manager for on-demand file-conversion jobs.

Mirrors BulkUploadManager but is keyed by reference_id for idempotency:
two concurrent requests to convert the same reference share a single job
rather than launching duplicate PDFX runs.

Job state is per-process and lost on restart (same tradeoff as
BulkUploadManager). Under multi-worker uvicorn the worst case is one
duplicate job per worker; acceptable for the current use case.
"""
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from threading import Lock
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


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


class ConversionJobManager:
    """Thread-safe in-memory manager for file-conversion jobs."""

    def __init__(self) -> None:
        self._jobs: Dict[str, ConversionJob] = {}
        self._active_by_reference: Dict[int, str] = {}
        self._last_by_reference: Dict[int, str] = {}
        self._lock = Lock()

    def create_or_get_job(self, reference_id: int, reference_curie: str, user_id: str,
                          expected_source_files: Optional[List[Dict[str, Optional[str]]]] = None) -> ConversionJob:
        """
        Atomically return the existing running job for the reference or create a new one.

        This is the idempotency primitive: two concurrent callers asking for the
        same reference both receive the same job.

        ``expected_source_files`` — when creating a new job, seeds the job's
        ``per_file_progress`` with a ``pending`` entry for each eligible source.
        Each dict may include ``source_display_name``, ``source_file_class``,
        ``expected_converted_display_name``, ``expected_converted_file_class``.
        Seeding is skipped when an existing running job is returned.
        """
        with self._lock:
            existing_job_id = self._active_by_reference.get(reference_id)
            if existing_job_id:
                existing = self._jobs.get(existing_job_id)
                if existing and existing.status == "running":
                    return existing
                # Stale entry (job was completed/failed but not cleaned up) — fall through.
                self._active_by_reference.pop(reference_id, None)

            job_id = str(uuid.uuid4())
            job = ConversionJob(
                job_id=job_id,
                reference_id=reference_id,
                reference_curie=reference_curie,
                user_id=user_id,
                status="running",
            )
            if expected_source_files:
                for ef in expected_source_files:
                    source_id = ef.get("source_referencefile_id")
                    source_id_int = int(source_id) if source_id is not None else None
                    job.per_file_progress.append(
                        PerFileProgress(
                            source_display_name=str(ef["source_display_name"]),
                            source_file_class=str(ef["source_file_class"]),
                            source_referencefile_id=source_id_int,
                            converted_display_name=ef.get("expected_converted_display_name"),
                            converted_file_class=ef.get("expected_converted_file_class"),
                            status="pending",
                        )
                    )
            self._jobs[job_id] = job
            self._active_by_reference[reference_id] = job_id
            self._last_by_reference[reference_id] = job_id
        logger.info(
            f"Created conversion job {job_id} for reference_id={reference_id} "
            f"(curie={reference_curie}, user={user_id})"
        )
        return job

    def get_job(self, job_id: str) -> Optional[ConversionJob]:
        with self._lock:
            return self._jobs.get(job_id)

    def get_active_job_for_reference(self, reference_id: int) -> Optional[ConversionJob]:
        with self._lock:
            job_id = self._active_by_reference.get(reference_id)
            if not job_id:
                return None
            job = self._jobs.get(job_id)
            if job and job.status == "running":
                return job
            return None

    def get_last_job_for_reference(self, reference_id: int) -> Optional[ConversionJob]:
        """Return the most recent job for this reference, regardless of status."""
        with self._lock:
            job_id = self._last_by_reference.get(reference_id)
            if not job_id:
                return None
            return self._jobs.get(job_id)

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
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
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

    def complete_job(self, job_id: str, success: bool, error: str = "") -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            job.status = "completed" if success else "failed"
            job.completed_at = datetime.utcnow()
            job.last_update = job.completed_at
            if error:
                job.error_message = error
            self._active_by_reference.pop(job.reference_id, None)
        logger.info(f"Conversion job {job_id} {job.status} (reference_id={job.reference_id})")

    def cleanup_old_jobs(self, max_age_hours: int = 24) -> int:
        cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)
        removed = 0
        with self._lock:
            to_remove = [
                jid for jid, job in self._jobs.items()
                if job.status != "running" and job.started_at < cutoff
            ]
            for jid in to_remove:
                job = self._jobs.pop(jid)
                if self._last_by_reference.get(job.reference_id) == jid:
                    self._last_by_reference.pop(job.reference_id, None)
                removed += 1
        if removed:
            logger.info(f"Cleaned up {removed} old conversion jobs")
        return removed


conversion_manager = ConversionJobManager()
