import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict, field
from threading import Lock
import logging

logger = logging.getLogger(__name__)


@dataclass
class BulkUploadJob:
    """Data class representing a bulk upload job."""
    job_id: str
    user_id: str
    mod_abbreviation: str
    filename: str
    status: str  # 'running', 'completed', 'failed'
    total_files: int = 0
    processed_files: int = 0
    successful_files: int = 0
    failed_files: int = 0
    start_time: datetime = field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None
    last_update: datetime = field(default_factory=datetime.utcnow)
    current_file: str = ""
    error_message: str = ""
    progress_log: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        data = asdict(self)
        # Convert datetime fields to ISO strings
        for t in ('start_time', 'end_time', 'last_update'):
            if data[t]:
                data[t] = data[t].isoformat()
        # Add computed fields
        data['progress_percentage'] = self.progress_percentage
        data['duration_seconds'] = self.duration_seconds
        return data

    def update_progress(self,
                        processed: Optional[int] = None,
                        current_file: str = "",
                        success: bool = True,
                        error: str = "") -> None:
        """Record progress update for a job."""
        if processed is not None:
            self.processed_files = processed
            if success:
                self.successful_files += 1
            else:
                self.failed_files += 1
        self.current_file = current_file
        if error:
            self.error_message = error
        self.last_update = datetime.utcnow()
        # Keep a log of last 100 updates
        entry = {
            'timestamp': self.last_update.isoformat(),
            'file': current_file,
            'success': success,
            'error': error or None
        }
        self.progress_log.append(entry)
        if len(self.progress_log) > 100:
            self.progress_log = self.progress_log[-100:]

    @property
    def progress_percentage(self) -> float:
        """Percentage of files processed."""
        if self.total_files <= 0:
            return 0.0
        return (self.processed_files / self.total_files) * 100.0

    @property
    def duration_seconds(self) -> float:
        """Total run duration in seconds."""
        end = self.end_time or datetime.utcnow()
        return (end - self.start_time).total_seconds()


class BulkUploadManager:
    """Thread-safe in-memory manager for bulk upload jobs."""

    def __init__(self) -> None:
        self._jobs: Dict[str, BulkUploadJob] = {}
        self._lock = Lock()

    def create_job(self,
                   user_id: str,
                   mod_abbreviation: str,
                   filename: str,
                   total_files: int = 0) -> str:
        """Create a new bulk upload job and return its ID."""
        job_id = str(uuid.uuid4())
        with self._lock:
            job = BulkUploadJob(
                job_id=job_id,
                user_id=user_id,
                mod_abbreviation=mod_abbreviation,
                filename=filename,
                status='running',
                total_files=total_files
            )
            self._jobs[job_id] = job
        logger.info(f"Created bulk upload job {job_id} for user {user_id}, MOD {mod_abbreviation}")
        return job_id

    def get_job(self, job_id: str) -> Optional[BulkUploadJob]:
        """Retrieve a job by its ID."""
        with self._lock:
            return self._jobs.get(job_id)

    def update_job(self, job_id: str, **kwargs: Any) -> bool:
        """Update metadata fields for an existing job."""
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return False
            for key, val in kwargs.items():
                if hasattr(job, key):
                    setattr(job, key, val)
            job.last_update = datetime.utcnow()
            return True

    def update_progress(self,
                        job_id: str,
                        processed: Optional[int] = None,
                        current_file: str = "",
                        success: bool = True,
                        error: str = "") -> bool:
        """Thread-safe progress update wrapper."""
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return False
            job.update_progress(processed, current_file, success, error)
            return True

    def complete_job(self, job_id: str, success: bool = True, error: str = "") -> None:
        """Mark a job as completed or failed."""
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            job.status = 'completed' if success else 'failed'
            job.end_time = datetime.utcnow()
            job.last_update = job.end_time
            if error:
                job.error_message = error
        logger.info(
            f"Completed bulk upload job {job_id}: "
            f"{job.successful_files}/{job.total_files} successful, "
            f"{job.failed_files} failed"
        )

    def get_active_jobs(self,
                        user_id: Optional[str] = None,
                        mod_abbreviation: Optional[str] = None) -> List[BulkUploadJob]:
        """List running jobs, optionally filtered by user or MOD."""
        with self._lock:
            jobs = [j for j in self._jobs.values() if j.status == 'running']
        if user_id:
            jobs = [j for j in jobs if j.user_id == user_id]
        if mod_abbreviation:
            jobs = [j for j in jobs if j.mod_abbreviation == mod_abbreviation]
        return sorted(jobs, key=lambda j: j.start_time, reverse=True)

    def get_recent_jobs(self, user_id: Optional[str] = None, limit: int = 10) -> List[BulkUploadJob]:
        """Return the most recent jobs for a user."""
        with self._lock:
            jobs = list(self._jobs.values())
        if user_id:
            jobs = [j for j in jobs if j.user_id == user_id]
        jobs.sort(key=lambda j: j.start_time, reverse=True)
        return jobs[:limit]

    def cleanup_old_jobs(self, max_age_hours: int = 24) -> int:
        """Remove non-running jobs older than specified age."""
        cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)
        removed = 0
        with self._lock:
            to_remove = [jid for jid, job in self._jobs.items()
                         if job.status != 'running' and job.start_time < cutoff]
            for jid in to_remove:
                del self._jobs[jid]
                removed += 1
        if removed:
            logger.info(f"Cleaned up {removed} old bulk upload jobs")
        return removed

    def get_stats(self) -> Dict[str, Any]:
        """Get summary stats for all jobs."""
        with self._lock:
            jobs = list(self._jobs.values())
        total = len(jobs)
        running = sum(j.status == 'running' for j in jobs)
        completed = sum(j.status == 'completed' for j in jobs)
        failed = sum(j.status == 'failed' for j in jobs)
        return {
            'total_jobs': total,
            'running_jobs': running,
            'completed_jobs': completed,
            'failed_jobs': failed,
        }


# Singleton instance
upload_manager = BulkUploadManager()
