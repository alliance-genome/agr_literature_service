"""
Tests for bulk upload manager functionality.
"""

import threading
import time
from datetime import datetime, timedelta

from agr_literature_service.api.utils.bulk_upload_manager import (
    BulkUploadJob,
    BulkUploadManager,
    upload_manager
)


class TestBulkUploadJob:
    """Test BulkUploadJob data class."""

    def test_job_creation(self):
        """Test basic job creation."""
        job = BulkUploadJob(
            job_id="test-job-1",
            user_id="user123",
            mod_abbreviation="WB",
            filename="test_archive.tar.gz",
            status="running"
        )

        assert job.job_id == "test-job-1"
        assert job.user_id == "user123"
        assert job.mod_abbreviation == "WB"
        assert job.filename == "test_archive.tar.gz"
        assert job.status == "running"
        assert job.total_files == 0
        assert job.processed_files == 0
        assert job.successful_files == 0
        assert job.failed_files == 0
        assert job.current_file == ""
        assert job.error_message == ""
        assert isinstance(job.start_time, datetime)
        assert isinstance(job.last_update, datetime)
        assert job.end_time is None
        assert job.progress_log == []

    def test_to_dict_conversion(self):
        """Test conversion to dictionary for JSON serialization."""
        job = BulkUploadJob(
            job_id="test-job-1",
            user_id="user123",
            mod_abbreviation="WB",
            filename="test.tar.gz",
            status="running"
        )

        result = job.to_dict()

        assert isinstance(result, dict)
        assert result["job_id"] == "test-job-1"
        assert result["user_id"] == "user123"
        assert result["mod_abbreviation"] == "WB"
        assert result["status"] == "running"
        assert isinstance(result["start_time"], str)  # ISO format
        assert isinstance(result["last_update"], str)  # ISO format
        assert result["end_time"] is None

    def test_update_progress_success(self):
        """Test progress update with successful file."""
        job = BulkUploadJob(
            job_id="test-job-1",
            user_id="user123",
            mod_abbreviation="WB",
            filename="test.tar.gz",
            status="running"
        )

        job.update_progress(
            processed=1,
            current_file="12345_Doe2023.pdf",
            success=True
        )

        assert job.processed_files == 1
        assert job.successful_files == 1
        assert job.failed_files == 0
        assert job.current_file == "12345_Doe2023.pdf"
        assert job.error_message == ""
        assert len(job.progress_log) == 1

        log_entry = job.progress_log[0]
        assert log_entry["file"] == "12345_Doe2023.pdf"
        assert log_entry["success"] is True
        assert log_entry["error"] is None

    def test_update_progress_failure(self):
        """Test progress update with failed file."""
        job = BulkUploadJob(
            job_id="test-job-1",
            user_id="user123",
            mod_abbreviation="WB",
            filename="test.tar.gz",
            status="running"
        )

        job.update_progress(
            processed=1,
            current_file="invalid_file.pdf",
            success=False,
            error="File not found"
        )

        assert job.processed_files == 1
        assert job.successful_files == 0
        assert job.failed_files == 1
        assert job.current_file == "invalid_file.pdf"
        assert job.error_message == "File not found"
        assert len(job.progress_log) == 1

        log_entry = job.progress_log[0]
        assert log_entry["file"] == "invalid_file.pdf"
        assert log_entry["success"] is False
        assert log_entry["error"] == "File not found"

    def test_progress_log_limit(self):
        """Test progress log is limited to 100 entries."""
        job = BulkUploadJob(
            job_id="test-job-1",
            user_id="user123",
            mod_abbreviation="WB",
            filename="test.tar.gz",
            status="running"
        )

        # Add 150 progress entries
        for i in range(150):
            job.update_progress(
                processed=i + 1,
                current_file=f"file_{i}.pdf",
                success=True
            )

        # Should only keep last 100 entries
        assert len(job.progress_log) == 100
        assert job.progress_log[0]["file"] == "file_50.pdf"  # First kept entry
        assert job.progress_log[-1]["file"] == "file_149.pdf"  # Last entry

    def test_progress_percentage(self):
        """Test progress percentage calculation."""
        job = BulkUploadJob(
            job_id="test-job-1",
            user_id="user123",
            mod_abbreviation="WB",
            filename="test.tar.gz",
            status="running",
            total_files=10
        )

        # No files processed yet
        assert job.progress_percentage == 0.0

        # 5 files processed
        job.processed_files = 5
        assert job.progress_percentage == 50.0

        # All files processed
        job.processed_files = 10
        assert job.progress_percentage == 100.0

        # Handle zero total files
        job.total_files = 0
        assert job.progress_percentage == 0.0

    def test_duration_seconds(self):
        """Test duration calculation."""
        job = BulkUploadJob(
            job_id="test-job-1",
            user_id="user123",
            mod_abbreviation="WB",
            filename="test.tar.gz",
            status="running"
        )

        # Job still running (no end_time)
        time.sleep(0.1)  # Small delay
        duration = job.duration_seconds
        assert duration > 0
        assert duration < 1  # Should be very small

        # Job completed
        job.end_time = job.start_time + timedelta(seconds=5)
        assert job.duration_seconds == 5.0


class TestBulkUploadManager:
    """Test BulkUploadManager class."""

    def setup_method(self):
        """Set up fresh manager for each test."""
        self.manager = BulkUploadManager()

    def test_create_job(self):
        """Test job creation."""
        job_id = self.manager.create_job(
            user_id="user123",
            mod_abbreviation="WB",
            filename="test.tar.gz"
        )

        assert isinstance(job_id, str)
        assert len(job_id) > 0

        # Verify job was stored
        job = self.manager.get_job(job_id)
        assert job is not None
        assert job.job_id == job_id
        assert job.user_id == "user123"
        assert job.mod_abbreviation == "WB"
        assert job.filename == "test.tar.gz"
        assert job.status == "running"

    def test_get_nonexistent_job(self):
        """Test getting a job that doesn't exist."""
        job = self.manager.get_job("nonexistent-job-id")
        assert job is None

    def test_update_job(self):
        """Test job updates."""
        job_id = self.manager.create_job(
            user_id="user123",
            mod_abbreviation="WB",
            filename="test.tar.gz"
        )

        # Update job
        success = self.manager.update_job(
            job_id,
            total_files=10,
            processed_files=5
        )

        assert success is True

        job = self.manager.get_job(job_id)
        assert job.total_files == 10
        assert job.processed_files == 5

    def test_update_nonexistent_job(self):
        """Test updating a job that doesn't exist."""
        success = self.manager.update_job("nonexistent-job-id", total_files=10)
        assert success is False

    def test_complete_job_success(self):
        """Test completing a job successfully."""
        job_id = self.manager.create_job(
            user_id="user123",
            mod_abbreviation="WB",
            filename="test.tar.gz"
        )

        # Set some progress
        self.manager.update_job(job_id, total_files=5, successful_files=4, failed_files=1)

        self.manager.complete_job(job_id, success=True)

        job = self.manager.get_job(job_id)
        assert job.status == "completed"
        assert job.end_time is not None

    def test_complete_job_failure(self):
        """Test completing a job with failure."""
        job_id = self.manager.create_job(
            user_id="user123",
            mod_abbreviation="WB",
            filename="test.tar.gz"
        )

        self.manager.complete_job(job_id, success=False, error="Processing failed")

        job = self.manager.get_job(job_id)
        assert job.status == "failed"
        assert job.error_message == "Processing failed"
        assert job.end_time is not None

    def test_get_active_jobs(self):
        """Test getting active jobs."""
        # Create multiple jobs
        self.manager.create_job("user1", "WB", "file1.tar.gz")
        self.manager.create_job("user1", "FB", "file2.tar.gz")
        self.manager.create_job("user2", "WB", "file3.tar.gz")
        job4_id = self.manager.create_job("user1", "WB", "file4.tar.gz")

        # Complete one job
        self.manager.complete_job(job4_id, success=True)

        # Get all active jobs
        active_jobs = self.manager.get_active_jobs()
        assert len(active_jobs) == 3  # job4 is completed

        # Get active jobs for user1
        user1_jobs = self.manager.get_active_jobs(user_id="user1")
        assert len(user1_jobs) == 2  # job1 and job2

        # Get active WB jobs
        wb_jobs = self.manager.get_active_jobs(mod_abbreviation="WB")
        assert len(wb_jobs) == 2  # job1 and job3

        # Get active WB jobs for user1
        user1_wb_jobs = self.manager.get_active_jobs(user_id="user1", mod_abbreviation="WB")
        assert len(user1_wb_jobs) == 1  # job1 only

    def test_get_recent_jobs(self):
        """Test getting recent jobs."""
        # Create multiple jobs
        job_ids = []
        for i in range(15):
            job_id = self.manager.create_job(f"user{i % 3}", "WB", f"file{i}.tar.gz")
            job_ids.append(job_id)
            if i % 3 == 0:  # Complete some jobs
                self.manager.complete_job(job_id, success=True)

        # Get recent jobs (default limit 10)
        recent_jobs = self.manager.get_recent_jobs()
        assert len(recent_jobs) == 10

        # Get recent jobs with custom limit
        recent_jobs_5 = self.manager.get_recent_jobs(limit=5)
        assert len(recent_jobs_5) == 5

        # Get recent jobs for specific user
        user0_jobs = self.manager.get_recent_jobs(user_id="user0")
        assert len(user0_jobs) == 5  # user0, user3, user6, user9, user12

        # Jobs should be sorted by start time (most recent first)
        for i in range(len(recent_jobs) - 1):
            assert recent_jobs[i].start_time >= recent_jobs[i + 1].start_time

    def test_cleanup_old_jobs(self):
        """Test cleanup of old jobs."""
        # Create jobs
        job1_id = self.manager.create_job("user1", "WB", "file1.tar.gz")
        job2_id = self.manager.create_job("user2", "FB", "file2.tar.gz")
        job3_id = self.manager.create_job("user3", "WB", "file3.tar.gz")

        # Complete jobs and set old timestamps
        self.manager.complete_job(job1_id, success=True)
        self.manager.complete_job(job2_id, success=False)
        # job3 is still running

        # Manually set old timestamps
        old_time = datetime.utcnow() - timedelta(hours=25)
        job1 = self.manager.get_job(job1_id)
        job2 = self.manager.get_job(job2_id)
        job1.start_time = old_time
        job2.start_time = old_time

        # Cleanup old jobs (default 24 hours)
        cleaned_count = self.manager.cleanup_old_jobs()

        assert cleaned_count == 2
        assert self.manager.get_job(job1_id) is None  # Cleaned up
        assert self.manager.get_job(job2_id) is None  # Cleaned up
        assert self.manager.get_job(job3_id) is not None  # Still running, not cleaned

    def test_get_stats(self):
        """Test getting manager statistics."""
        # Create jobs in different states
        self.manager.create_job("user1", "WB", "file1.tar.gz")
        job2_id = self.manager.create_job("user2", "FB", "file2.tar.gz")
        job3_id = self.manager.create_job("user3", "WB", "file3.tar.gz")
        self.manager.create_job("user4", "FB", "file4.tar.gz")

        # Complete jobs with different outcomes
        self.manager.complete_job(job2_id, success=True)
        self.manager.complete_job(job3_id, success=False)
        # job1 and job4 still running

        stats = self.manager.get_stats()

        assert stats["total_jobs"] == 4
        assert stats["running_jobs"] == 2
        assert stats["completed_jobs"] == 1
        assert stats["failed_jobs"] == 1

    def test_thread_safety(self):
        """Test thread safety of manager operations."""
        job_ids = []
        errors = []

        def create_jobs():
            try:
                for i in range(10):
                    job_id = self.manager.create_job(f"user{i}", "WB", f"file{i}.tar.gz")
                    job_ids.append(job_id)
                    time.sleep(0.001)  # Small delay to interleave operations
            except Exception as e:
                errors.append(e)

        def update_jobs():
            try:
                time.sleep(0.005)  # Let some jobs be created first
                for job_id in job_ids[:5]:  # Update first 5 jobs
                    self.manager.update_job(job_id, total_files=100)
                    time.sleep(0.001)
            except Exception as e:
                errors.append(e)

        # Run operations in parallel
        threads = [
            threading.Thread(target=create_jobs),
            threading.Thread(target=create_jobs),
            threading.Thread(target=update_jobs)
        ]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        # Check no errors occurred
        assert len(errors) == 0

        # Check all jobs were created
        assert len(job_ids) == 20

        # Check jobs can be retrieved
        stats = self.manager.get_stats()
        assert stats["total_jobs"] == 20


class TestGlobalUploadManager:
    """Test the global upload_manager instance."""

    def test_global_manager_instance(self):
        """Test that global upload_manager is available and functional."""
        # The global instance should be importable and usable
        job_id = upload_manager.create_job(
            user_id="test_user",
            mod_abbreviation="WB",
            filename="test_global.tar.gz"
        )

        assert isinstance(job_id, str)

        job = upload_manager.get_job(job_id)
        assert job is not None
        assert job.user_id == "test_user"

        # Clean up
        upload_manager.complete_job(job_id, success=True)


class TestManagerErrorHandling:
    """Test error handling in bulk upload manager."""

    def setup_method(self):
        """Set up fresh manager for each test."""
        self.manager = BulkUploadManager()

    def test_update_job_invalid_attributes(self):
        """Test updating job with invalid attributes."""
        job_id = self.manager.create_job("user1", "WB", "test.tar.gz")

        # Try to update non-existent attribute
        success = self.manager.update_job(job_id, invalid_attribute="value")

        # Should still return True, but attribute won't be set
        assert success is True

        job = self.manager.get_job(job_id)
        assert not hasattr(job, "invalid_attribute")

    def test_complete_nonexistent_job(self):
        """Test completing a job that doesn't exist."""
        # Should not raise an error
        self.manager.complete_job("nonexistent-job-id", success=True)

        # Verify no job was created
        job = self.manager.get_job("nonexistent-job-id")
        assert job is None

    def test_manager_state_consistency(self):
        """Test that manager state remains consistent under various operations."""
        # Create several jobs
        job_ids = []
        for i in range(5):
            job_id = self.manager.create_job(f"user{i}", "WB", f"file{i}.tar.gz")
            job_ids.append(job_id)

        # Perform various operations
        self.manager.update_job(job_ids[0], total_files=10)
        self.manager.complete_job(job_ids[1], success=True)
        self.manager.complete_job(job_ids[2], success=False, error="Test error")

        # Verify state consistency
        stats = self.manager.get_stats()
        assert stats["total_jobs"] == 5
        assert stats["running_jobs"] == 3  # jobs 0, 3, and 4
        assert stats["completed_jobs"] == 1  # job 1
        assert stats["failed_jobs"] == 1   # job 2

        # Verify individual job states
        assert self.manager.get_job(job_ids[0]).status == "running"
        assert self.manager.get_job(job_ids[1]).status == "completed"
        assert self.manager.get_job(job_ids[2]).status == "failed"
        assert self.manager.get_job(job_ids[3]).status == "running"
        assert self.manager.get_job(job_ids[4]).status == "running"
