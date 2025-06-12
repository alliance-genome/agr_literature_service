"""
In-memory bulk upload job manager for tracking upload progress without database overhead.
"""

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
    progress_log: List[Dict] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        data = asdict(self)
        # Convert datetime objects to ISO strings
        for field_name in ['start_time', 'end_time', 'last_update']:
            if data[field_name]:
                data[field_name] = data[field_name].isoformat()
        return data

    def update_progress(self, processed: int = None, current_file: str = "",
                        success: bool = True, error: str = ""):
        """Update job progress."""
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

        # Add to progress log (keep last 100 entries)
        log_entry = {
            "timestamp": self.last_update.isoformat(),
            "file": current_file,
            "success": success,
            "error": error if error else None
        }
        self.progress_log.append(log_entry)
        if len(self.progress_log) > 100:
            self.progress_log = self.progress_log[-100:]

    @property
    def progress_percentage(self) -> float:
        """Calculate progress percentage."""
        if self.total_files == 0:
            return 0.0
        return (self.processed_files / self.total_files) * 100.0

    @property
    def duration_seconds(self) -> float:
        """Calculate job duration in seconds."""
        end_time = self.end_time or datetime.utcnow()
        return (end_time - self.start_time).total_seconds()


class BulkUploadManager:
    """Thread-safe in-memory manager for bulk upload jobs."""

    def __init__(self):
        self._jobs: Dict[str, BulkUploadJob] = {}
        self._lock = Lock()

    def create_job(self, user_id: str, mod_abbreviation: str, filename: str) -> str:
        """Create new bulk upload job and return job ID."""
        job_id = str(uuid.uuid4())

        with self._lock:
            job = BulkUploadJob(
                job_id=job_id,
                user_id=user_id,
                mod_abbreviation=mod_abbreviation,
                filename=filename,
                status='running'
            )
            self._jobs[job_id] = job

        logger.info(f"Created bulk upload job {job_id} for user {user_id}, MOD {mod_abbreviation}")
        return job_id

    def get_job(self, job_id: str) -> Optional[BulkUploadJob]:
        """Get job by ID."""
        with self._lock:
            return self._jobs.get(job_id)

    def update_job(self, job_id: str, **kwargs) -> bool:
        """Update job with new data."""
        with self._lock:
            if job_id in self._jobs:
                job = self._jobs[job_id]
                for key, value in kwargs.items():
                    if hasattr(job, key):
                        setattr(job, key, value)
                job.last_update = datetime.utcnow()
                return True
            return False

    def complete_job(self, job_id: str, success: bool = True, error: str = ""):
        """Mark job as completed."""
        with self._lock:
            if job_id in self._jobs:
                job = self._jobs[job_id]
                job.status = 'completed' if success else 'failed'
                job.end_time = datetime.utcnow()
                if error:
                    job.error_message = error

                logger.info(f"Completed bulk upload job {job_id}: "
                            f"{job.successful_files}/{job.total_files} successful, "
                            f"{job.failed_files} failed")

    def get_active_jobs(self, user_id: str = None, mod_abbreviation: str = None) -> List[BulkUploadJob]:
        """Get list of active (running) jobs, optionally filtered."""
        with self._lock:
            jobs = []
            for job in self._jobs.values():
                if job.status == 'running':
                    if user_id and job.user_id != user_id:
                        continue
                    if mod_abbreviation and job.mod_abbreviation != mod_abbreviation:
                        continue
                    jobs.append(job)
            return sorted(jobs, key=lambda x: x.start_time, reverse=True)

    def get_recent_jobs(self, user_id: str = None, limit: int = 10) -> List[BulkUploadJob]:
        """Get recent jobs (completed and running)."""
        with self._lock:
            jobs = []
            for job in self._jobs.values():
                if user_id and job.user_id != user_id:
                    continue
                jobs.append(job)

            # Sort by start time, most recent first
            jobs.sort(key=lambda x: x.start_time, reverse=True)
            return jobs[:limit]

    def cleanup_old_jobs(self, max_age_hours: int = 24) -> int:
        """Remove jobs older than specified hours."""
        cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)

        with self._lock:
            old_job_ids = []
            for job_id, job in self._jobs.items():
                if job.status != 'running' and job.start_time < cutoff:
                    old_job_ids.append(job_id)

            for job_id in old_job_ids:
                del self._jobs[job_id]

            if old_job_ids:
                logger.info(f"Cleaned up {len(old_job_ids)} old bulk upload jobs")

            return len(old_job_ids)

    def get_stats(self) -> Dict[str, Any]:
        """Get overall statistics."""
        with self._lock:
            total_jobs = len(self._jobs)
            running_jobs = sum(1 for job in self._jobs.values() if job.status == 'running')
            completed_jobs = sum(1 for job in self._jobs.values() if job.status == 'completed')
            failed_jobs = sum(1 for job in self._jobs.values() if job.status == 'failed')

            return {
                "total_jobs": total_jobs,
                "running_jobs": running_jobs,
                "completed_jobs": completed_jobs,
                "failed_jobs": failed_jobs
            }


# Global manager instance
upload_manager = BulkUploadManager()
