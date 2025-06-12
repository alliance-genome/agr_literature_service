"""
Comprehensive tests for bulk upload functionality including endpoints, manager, patterns and utils.
"""

import io
import json
import os
import tarfile
import tempfile
import threading
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from fastapi import UploadFile
from fastapi.testclient import TestClient

from agr_literature_service.api.main import app
from agr_literature_service.api.utils.bulk_upload_manager import (
    BulkUploadJob,
    BulkUploadManager,
    upload_manager
)
from agr_literature_service.api.utils.bulk_upload_utils import (
    classify_and_parse_file,
    extract_and_classify_files,
    parse_filename_by_mod,
    parse_supplement_file,
    process_single_file,
    validate_archive_structure
)


# ========================================
# FIXTURES
# ========================================

@pytest.fixture
def client():
    """Create test client."""
    return TestClient(app)


@pytest.fixture
def mock_okta_user():
    """Create mock Okta user."""
    user = Mock()
    user.cid = "test_user_123"
    user.groups = ["WBCurator"]
    return user


@pytest.fixture
def wb_test_archive():
    """Create test WB archive."""
    archive_buffer = io.BytesIO()
    
    with tarfile.open(fileobj=archive_buffer, mode="w:gz") as tar:
        # Main files
        main1 = tarfile.TarInfo("12345_Doe2023.pdf")
        main1.size = 100
        tar.addfile(main1, io.BytesIO(b"WB main file 1 content" + b"x" * 77))
        
        main2 = tarfile.TarInfo("67890_Smith2022_temp.pdf")
        main2.size = 100
        tar.addfile(main2, io.BytesIO(b"WB main file 2 content" + b"y" * 77))
        
        # Supplement files
        supp1 = tarfile.TarInfo("12345/figure_1.png")
        supp1.size = 50
        tar.addfile(supp1, io.BytesIO(b"WB supplement 1 content" + b"z" * 26))
        
        supp2 = tarfile.TarInfo("67890/data.xlsx")
        supp2.size = 50
        tar.addfile(supp2, io.BytesIO(b"WB supplement 2 content" + b"w" * 26))
    
    archive_buffer.seek(0)
    return archive_buffer


@pytest.fixture
def fb_test_archive():
    """Create test FB archive."""
    archive_buffer = io.BytesIO()
    
    with zipfile.ZipFile(archive_buffer, 'w') as zip_file:
        # Main files
        zip_file.writestr("12345678_Brown2023.pdf", b"FB main file 1 content" + b"a" * 77)
        zip_file.writestr("87654321_Wilson2022_html.html", b"FB main file 2 content" + b"b" * 76)
        
        # Supplement files
        zip_file.writestr("12345678/protocol.txt", b"FB supplement 1 content" + b"c" * 25)
        zip_file.writestr("87654321/figure.png", b"FB supplement 2 content" + b"d" * 26)
    
    archive_buffer.seek(0)
    return archive_buffer


@pytest.fixture
def empty_archive():
    """Create empty archive."""
    archive_buffer = io.BytesIO()
    
    with tarfile.open(fileobj=archive_buffer, mode="w:gz") as tar:
        pass  # Empty archive
    
    archive_buffer.seek(0)
    return archive_buffer


@pytest.fixture
def invalid_archive():
    """Create invalid archive."""
    return io.BytesIO(b"This is not a valid archive format")


# ========================================
# ENDPOINT TESTS
# ========================================

class TestBulkUploadValidateEndpoint:
    """Test /bulk_upload_validate/ endpoint."""

    @patch('agr_literature_service.api.routers.referencefile_router.set_global_user_from_okta')
    def test_validate_valid_wb_archive(self, mock_set_user, client, mock_okta_user, wb_test_archive):
        """Test validation of valid WB archive."""
        with patch('agr_literature_service.api.routers.authentication.auth.get_user', return_value=mock_okta_user):
            response = client.post(
                "/reference/referencefile/bulk_upload_validate/",
                files={"archive": ("test_wb.tar.gz", wb_test_archive, "application/gzip")}
            )
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["valid"] is True
        assert data["total_files"] == 4
        assert data["main_files"] == 2
        assert data["supplement_files"] == 2
        assert len(data["main_file_list"]) == 2
        assert len(data["supplement_file_list"]) == 2

    @patch('agr_literature_service.api.routers.referencefile_router.set_global_user_from_okta')
    def test_validate_valid_fb_archive(self, mock_set_user, client, mock_okta_user, fb_test_archive):
        """Test validation of valid FB archive."""
        with patch('agr_literature_service.api.routers.authentication.auth.get_user', return_value=mock_okta_user):
            response = client.post(
                "/reference/referencefile/bulk_upload_validate/",
                files={"archive": ("test_fb.zip", fb_test_archive, "application/zip")}
            )
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["valid"] is True
        assert data["total_files"] == 4
        assert data["main_files"] == 2
        assert data["supplement_files"] == 2

    @patch('agr_literature_service.api.routers.referencefile_router.set_global_user_from_okta')
    def test_validate_empty_archive(self, mock_set_user, client, mock_okta_user, empty_archive):
        """Test validation of empty archive."""
        with patch('agr_literature_service.api.routers.authentication.auth.get_user', return_value=mock_okta_user):
            response = client.post(
                "/reference/referencefile/bulk_upload_validate/",
                files={"archive": ("empty.tar.gz", empty_archive, "application/gzip")}
            )
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["valid"] is True
        assert data["total_files"] == 0
        assert data["main_files"] == 0
        assert data["supplement_files"] == 0

    @patch('agr_literature_service.api.routers.referencefile_router.set_global_user_from_okta')
    def test_validate_invalid_archive(self, mock_set_user, client, mock_okta_user, invalid_archive):
        """Test validation of invalid archive."""
        with patch('agr_literature_service.api.routers.authentication.auth.get_user', return_value=mock_okta_user):
            response = client.post(
                "/reference/referencefile/bulk_upload_validate/",
                files={"archive": ("invalid.txt", invalid_archive, "text/plain")}
            )
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["valid"] is False
        assert "error" in data
        assert data["total_files"] == 0


class TestBulkUploadArchiveEndpoint:
    """Test /bulk_upload_archive/ endpoint."""

    @patch('agr_literature_service.api.routers.referencefile_router.set_global_user_from_okta')
    @patch('agr_literature_service.api.routers.referencefile_router.asyncio.create_task')
    def test_start_wb_upload(self, mock_create_task, mock_set_user, client, mock_okta_user, wb_test_archive):
        """Test starting WB bulk upload."""
        with patch('agr_literature_service.api.routers.authentication.auth.get_user', return_value=mock_okta_user):
            response = client.post(
                "/reference/referencefile/bulk_upload_archive/",
                params={"mod_abbreviation": "WB"},
                files={"archive": ("test_wb.tar.gz", wb_test_archive, "application/gzip")}
            )
        
        assert response.status_code == 202
        data = response.json()
        
        assert "job_id" in data
        assert data["status"] == "started"
        assert data["message"] == "Bulk upload job started for WB"
        assert data["total_files"] == 4
        assert data["main_files"] == 2
        assert data["supplement_files"] == 2
        assert "status_url" in data
        
        # Verify background task was created
        mock_create_task.assert_called_once()
        
        # Verify job was created in manager
        job = upload_manager.get_job(data["job_id"])
        assert job is not None
        assert job.user_id == "test_user_123"
        assert job.mod_abbreviation == "WB"
        assert job.status == "running"

    @patch('agr_literature_service.api.routers.referencefile_router.set_global_user_from_okta')
    @patch('agr_literature_service.api.routers.referencefile_router.asyncio.create_task')
    def test_start_fb_upload(self, mock_create_task, mock_set_user, client, mock_okta_user, fb_test_archive):
        """Test starting FB bulk upload."""
        with patch('agr_literature_service.api.routers.authentication.auth.get_user', return_value=mock_okta_user):
            response = client.post(
                "/reference/referencefile/bulk_upload_archive/",
                params={"mod_abbreviation": "FB"},
                files={"archive": ("test_fb.zip", fb_test_archive, "application/zip")}
            )
        
        assert response.status_code == 202
        data = response.json()
        
        assert data["message"] == "Bulk upload job started for FB"
        
        # Verify job was created with correct MOD
        job = upload_manager.get_job(data["job_id"])
        assert job.mod_abbreviation == "FB"

    @patch('agr_literature_service.api.routers.referencefile_router.set_global_user_from_okta')
    def test_upload_empty_archive(self, mock_set_user, client, mock_okta_user, empty_archive):
        """Test uploading empty archive returns error."""
        with patch('agr_literature_service.api.routers.authentication.auth.get_user', return_value=mock_okta_user):
            response = client.post(
                "/reference/referencefile/bulk_upload_archive/",
                params={"mod_abbreviation": "WB"},
                files={"archive": ("empty.tar.gz", empty_archive, "application/gzip")}
            )
        
        assert response.status_code == 422
        data = response.json()
        assert "Archive contains no files" in data["detail"]

    @patch('agr_literature_service.api.routers.referencefile_router.set_global_user_from_okta')
    def test_upload_invalid_archive(self, mock_set_user, client, mock_okta_user, invalid_archive):
        """Test uploading invalid archive returns error."""
        with patch('agr_literature_service.api.routers.authentication.auth.get_user', return_value=mock_okta_user):
            response = client.post(
                "/reference/referencefile/bulk_upload_archive/",
                params={"mod_abbreviation": "WB"},
                files={"archive": ("invalid.txt", invalid_archive, "text/plain")}
            )
        
        assert response.status_code == 422
        data = response.json()
        assert "Invalid archive format" in data["detail"]


class TestBulkUploadStatusEndpoint:
    """Test /bulk_upload_status/{job_id} endpoint."""

    @patch('agr_literature_service.api.routers.referencefile_router.set_global_user_from_okta')
    def test_get_job_status(self, mock_set_user, client, mock_okta_user):
        """Test getting job status."""
        # Create a job manually
        job_id = upload_manager.create_job(
            user_id="test_user_123",
            mod_abbreviation="WB",
            filename="test.tar.gz"
        )
        
        # Update job with some progress
        upload_manager.update_job(job_id, total_files=10, processed_files=5, successful_files=4, failed_files=1)
        
        with patch('agr_literature_service.api.routers.authentication.auth.get_user', return_value=mock_okta_user):
            response = client.get(f"/reference/referencefile/bulk_upload_status/{job_id}")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["job_id"] == job_id
        assert data["user_id"] == "test_user_123"
        assert data["mod_abbreviation"] == "WB"
        assert data["status"] == "running"
        assert data["total_files"] == 10
        assert data["processed_files"] == 5
        assert data["successful_files"] == 4
        assert data["failed_files"] == 1

    @patch('agr_literature_service.api.routers.referencefile_router.set_global_user_from_okta')
    def test_get_nonexistent_job_status(self, mock_set_user, client, mock_okta_user):
        """Test getting status of non-existent job."""
        with patch('agr_literature_service.api.routers.authentication.auth.get_user', return_value=mock_okta_user):
            response = client.get("/reference/referencefile/bulk_upload_status/nonexistent-job-id")
        
        assert response.status_code == 404
        data = response.json()
        assert data["detail"] == "Job not found"

    @patch('agr_literature_service.api.routers.referencefile_router.set_global_user_from_okta')
    def test_get_other_user_job_status(self, mock_set_user, client, mock_okta_user):
        """Test getting status of another user's job."""
        # Create job for different user
        job_id = upload_manager.create_job(
            user_id="other_user_456",
            mod_abbreviation="WB",
            filename="test.tar.gz"
        )
        
        with patch('agr_literature_service.api.routers.authentication.auth.get_user', return_value=mock_okta_user):
            response = client.get(f"/reference/referencefile/bulk_upload_status/{job_id}")
        
        assert response.status_code == 403
        data = response.json()
        assert data["detail"] == "Access denied"


class TestBulkUploadActiveEndpoint:
    """Test /bulk_upload_active/ endpoint."""

    @patch('agr_literature_service.api.routers.referencefile_router.set_global_user_from_okta')
    def test_get_active_jobs(self, mock_set_user, client, mock_okta_user):
        """Test getting active jobs."""
        # Create multiple jobs for current user
        job1_id = upload_manager.create_job("test_user_123", "WB", "file1.tar.gz")
        job2_id = upload_manager.create_job("test_user_123", "FB", "file2.zip")
        job3_id = upload_manager.create_job("other_user_456", "WB", "file3.tar.gz")
        job4_id = upload_manager.create_job("test_user_123", "WB", "file4.tar.gz")
        
        # Complete one job
        upload_manager.complete_job(job4_id, success=True)
        
        with patch('agr_literature_service.api.routers.authentication.auth.get_user', return_value=mock_okta_user):
            response = client.get("/reference/referencefile/bulk_upload_active/")
        
        assert response.status_code == 200
        data = response.json()
        
        # Should only get current user's active jobs
        assert len(data) == 2
        job_ids = [job["job_id"] for job in data]
        assert job1_id in job_ids
        assert job2_id in job_ids
        assert job3_id not in job_ids  # Different user
        assert job4_id not in job_ids  # Completed

    @patch('agr_literature_service.api.routers.referencefile_router.set_global_user_from_okta')
    def test_get_active_jobs_filtered_by_mod(self, mock_set_user, client, mock_okta_user):
        """Test getting active jobs filtered by MOD."""
        # Create jobs with different MODs
        wb_job_id = upload_manager.create_job("test_user_123", "WB", "wb_file.tar.gz")
        fb_job_id = upload_manager.create_job("test_user_123", "FB", "fb_file.zip")
        sgd_job_id = upload_manager.create_job("test_user_123", "SGD", "sgd_file.tar.gz")
        
        with patch('agr_literature_service.api.routers.authentication.auth.get_user', return_value=mock_okta_user):
            response = client.get("/reference/referencefile/bulk_upload_active/?mod_abbreviation=WB")
        
        assert response.status_code == 200
        data = response.json()
        
        # Should only get WB jobs
        assert len(data) == 1
        assert data[0]["job_id"] == wb_job_id
        assert data[0]["mod_abbreviation"] == "WB"

    @patch('agr_literature_service.api.routers.referencefile_router.set_global_user_from_okta')
    def test_get_active_jobs_no_jobs(self, mock_set_user, client, mock_okta_user):
        """Test getting active jobs when none exist."""
        with patch('agr_literature_service.api.routers.authentication.auth.get_user', return_value=mock_okta_user):
            response = client.get("/reference/referencefile/bulk_upload_active/")
        
        assert response.status_code == 200
        data = response.json()
        assert data == []


class TestBulkUploadHistoryEndpoint:
    """Test /bulk_upload_history/ endpoint."""

    @patch('agr_literature_service.api.routers.referencefile_router.set_global_user_from_okta')
    def test_get_job_history(self, mock_set_user, client, mock_okta_user):
        """Test getting job history."""
        # Create multiple jobs for current user
        job_ids = []
        for i in range(15):
            job_id = upload_manager.create_job("test_user_123", "WB", f"file{i}.tar.gz")
            job_ids.append(job_id)
            if i % 3 == 0:  # Complete some jobs
                upload_manager.complete_job(job_id, success=True)
        
        # Create jobs for other user (should not appear)
        upload_manager.create_job("other_user_456", "WB", "other_file.tar.gz")
        
        with patch('agr_literature_service.api.routers.authentication.auth.get_user', return_value=mock_okta_user):
            response = client.get("/reference/referencefile/bulk_upload_history/")
        
        assert response.status_code == 200
        data = response.json()
        
        # Should get default limit of 10 jobs for current user
        assert len(data) == 10
        
        # All jobs should belong to current user
        for job in data:
            assert job["user_id"] == "test_user_123"

    @patch('agr_literature_service.api.routers.referencefile_router.set_global_user_from_okta')
    def test_get_job_history_custom_limit(self, mock_set_user, client, mock_okta_user):
        """Test getting job history with custom limit."""
        # Create multiple jobs
        for i in range(8):
            upload_manager.create_job("test_user_123", "WB", f"file{i}.tar.gz")
        
        with patch('agr_literature_service.api.routers.authentication.auth.get_user', return_value=mock_okta_user):
            response = client.get("/reference/referencefile/bulk_upload_history/?limit=5")
        
        assert response.status_code == 200
        data = response.json()
        
        # Should get custom limit of 5 jobs
        assert len(data) == 5

    @patch('agr_literature_service.api.routers.referencefile_router.set_global_user_from_okta')
    def test_get_job_history_no_jobs(self, mock_set_user, client, mock_okta_user):
        """Test getting job history when no jobs exist."""
        with patch('agr_literature_service.api.routers.authentication.auth.get_user', return_value=mock_okta_user):
            response = client.get("/reference/referencefile/bulk_upload_history/")
        
        assert response.status_code == 200
        data = response.json()
        assert data == []


class TestEndpointErrorHandling:
    """Test error handling across all bulk upload endpoints."""

    def test_missing_authentication(self, client, wb_test_archive):
        """Test endpoints without authentication."""
        # Test bulk upload without auth
        response = client.post(
            "/reference/referencefile/bulk_upload_archive/",
            params={"mod_abbreviation": "WB"},
            files={"archive": ("test.tar.gz", wb_test_archive, "application/gzip")}
        )
        assert response.status_code == 403  # Should require authentication
        
        # Test status without auth
        response = client.get("/reference/referencefile/bulk_upload_status/test-job-id")
        assert response.status_code == 403
        
        # Test active jobs without auth
        response = client.get("/reference/referencefile/bulk_upload_active/")
        assert response.status_code == 403
        
        # Test history without auth
        response = client.get("/reference/referencefile/bulk_upload_history/")
        assert response.status_code == 403
        
        # Test validate without auth
        response = client.post(
            "/reference/referencefile/bulk_upload_validate/",
            files={"archive": ("test.tar.gz", wb_test_archive, "application/gzip")}
        )
        assert response.status_code == 403

    @patch('agr_literature_service.api.routers.referencefile_router.set_global_user_from_okta')
    def test_missing_parameters(self, mock_set_user, client, mock_okta_user, wb_test_archive):
        """Test endpoints with missing required parameters."""
        with patch('agr_literature_service.api.routers.authentication.auth.get_user', return_value=mock_okta_user):
            # Test bulk upload without mod_abbreviation
            response = client.post(
                "/reference/referencefile/bulk_upload_archive/",
                files={"archive": ("test.tar.gz", wb_test_archive, "application/gzip")}
            )
            assert response.status_code == 422  # Missing required parameter
            
            # Test bulk upload without archive file
            response = client.post(
                "/reference/referencefile/bulk_upload_archive/",
                params={"mod_abbreviation": "WB"}
            )
            assert response.status_code == 422  # Missing required file


class TestBulkUploadIntegration:
    """Integration tests for complete bulk upload workflow."""

    @patch('agr_literature_service.api.routers.referencefile_router.set_global_user_from_okta')
    @patch('agr_literature_service.api.routers.referencefile_router.asyncio.create_task')
    def test_complete_wb_workflow(self, mock_create_task, mock_set_user, client, mock_okta_user, wb_test_archive):
        """Test complete WB bulk upload workflow."""
        with patch('agr_literature_service.api.routers.authentication.auth.get_user', return_value=mock_okta_user):
            # 1. Validate archive
            response = client.post(
                "/reference/referencefile/bulk_upload_validate/",
                files={"archive": ("test_wb.tar.gz", wb_test_archive, "application/gzip")}
            )
            assert response.status_code == 200
            validation = response.json()
            assert validation["valid"] is True
            
            # 2. Start upload
            wb_test_archive.seek(0)  # Reset file pointer
            response = client.post(
                "/reference/referencefile/bulk_upload_archive/",
                params={"mod_abbreviation": "WB"},
                files={"archive": ("test_wb.tar.gz", wb_test_archive, "application/gzip")}
            )
            assert response.status_code == 202
            upload_result = response.json()
            job_id = upload_result["job_id"]
            
            # 3. Check job status
            response = client.get(f"/reference/referencefile/bulk_upload_status/{job_id}")
            assert response.status_code == 200
            status = response.json()
            assert status["job_id"] == job_id
            assert status["status"] == "running"
            
            # 4. Check active jobs
            response = client.get("/reference/referencefile/bulk_upload_active/")
            assert response.status_code == 200
            active_jobs = response.json()
            assert len(active_jobs) >= 1
            assert any(job["job_id"] == job_id for job in active_jobs)
            
            # 5. Check history
            response = client.get("/reference/referencefile/bulk_upload_history/")
            assert response.status_code == 200
            history = response.json()
            assert len(history) >= 1
            assert any(job["job_id"] == job_id for job in history)

    def teardown_method(self):
        """Clean up after each test."""
        # Clear all jobs from upload_manager
        upload_manager._jobs.clear()


# ========================================
# MANAGER TESTS
# ========================================

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
        job1_id = self.manager.create_job("user1", "WB", "file1.tar.gz")
        job2_id = self.manager.create_job("user1", "FB", "file2.tar.gz")
        job3_id = self.manager.create_job("user2", "WB", "file3.tar.gz")
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
        job1_id = self.manager.create_job("user1", "WB", "file1.tar.gz")
        job2_id = self.manager.create_job("user2", "FB", "file2.tar.gz")
        job3_id = self.manager.create_job("user3", "WB", "file3.tar.gz")
        job4_id = self.manager.create_job("user4", "FB", "file4.tar.gz")
        
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


# ========================================
# PATTERN TESTS
# ========================================

class TestWBPatterns:
    """Test WormBase (WB) specific filename patterns."""

    def test_wb_basic_patterns(self):
        """Test basic WB filename patterns."""
        test_cases = [
            # Standard pattern: {wbpaper_id}_{author_year}.{ext}
            ("12345_Doe2023.pdf", "WB:WBPaper12345", "Doe2023", "final", None),
            ("678_Smith2022.html", "WB:WBPaper678", "Smith2022", "final", None),
            ("999999_Johnson2021.txt", "WB:WBPaper999999", "Johnson2021", "final", None),
            
            # Numbers only: {wbpaper_id}.{ext}
            ("12345.pdf", "WB:WBPaper12345", "", "final", None),
            ("1.pdf", "WB:WBPaper1", "", "final", None),
            ("999999.html", "WB:WBPaper999999", "", "final", None),
        ]
        
        for filename, expected_curie, expected_author, expected_status, expected_pdf_type in test_cases:
            result = parse_filename_by_mod(filename, "WB")
            
            assert result["reference_curie"] == expected_curie
            assert result["author_and_year"] == expected_author
            assert result["file_publication_status"] == expected_status
            assert result["pdf_type"] == expected_pdf_type
            assert result["mod_abbreviation"] == "WB"

    def test_wb_with_temp_status(self):
        """Test WB filenames with temp status."""
        test_cases = [
            ("12345_Doe2023_temp.pdf", "temp"),
            ("678_Smith2022_temp.html", "temp"),
            ("999_Jones2021_TEMP.txt", "temp"),  # Case insensitive
        ]
        
        for filename, expected_status in test_cases:
            result = parse_filename_by_mod(filename, "WB")
            assert result["file_publication_status"] == expected_status
            assert result["pdf_type"] is None

    def test_wb_with_pdf_types(self):
        """Test WB filenames with PDF type options."""
        test_cases = [
            ("12345_Doe2023_aut.pdf", "aut"),
            ("678_Smith2022_ocr.pdf", "ocr"), 
            ("999_Jones2021_html.html", "html"),
            ("111_Wilson2020_htm.htm", "html"),  # htm converts to html
            ("222_Brown2019_lib.pdf", "lib"),
            ("333_Taylor2018_tif.tif", "tif"),
            ("444_Davis2017_AUT.pdf", "aut"),  # Case insensitive
            ("555_Miller2016_OCR.pdf", "ocr"),  # Case insensitive
        ]
        
        for filename, expected_pdf_type in test_cases:
            result = parse_filename_by_mod(filename, "WB")
            assert result["pdf_type"] == expected_pdf_type
            assert result["file_publication_status"] == "final"

    def test_wb_real_world_examples(self):
        """Test real-world WB filename examples."""
        test_cases = [
            # Real WB paper patterns
            ("00001234_Brenner1974.pdf", "WB:WBPaper00001234", "Brenner1974"),
            ("00005678_Fire1998_temp.pdf", "WB:WBPaper00005678", "Fire1998"),
            ("00009999_Mello2006_ocr.pdf", "WB:WBPaper00009999", "Mello2006"),
            ("00001111_Horvitz2002.html", "WB:WBPaper00001111", "Horvitz2002"),
            
            # Edge cases
            ("1_A2023.pdf", "WB:WBPaper1", "A2023"),
            ("123456789_VeryLongAuthorName2023.pdf", "WB:WBPaper123456789", "VeryLongAuthorName2023"),
            ("42_AuthorWith123Numbers2023.pdf", "WB:WBPaper42", "AuthorWith123Numbers2023"),
        ]
        
        for filename, expected_curie, expected_author in test_cases:
            result = parse_filename_by_mod(filename, "WB")
            assert result["reference_curie"] == expected_curie
            assert result["author_and_year"] == expected_author

    def test_wb_file_extensions(self):
        """Test WB files with various extensions."""
        extensions = ["pdf", "html", "htm", "txt", "doc", "docx", "xml", "tei", "json"]
        
        for ext in extensions:
            filename = f"12345_Author2023.{ext}"
            result = parse_filename_by_mod(filename, "WB")
            
            assert result["reference_curie"] == "WB:WBPaper12345"
            assert result["file_extension"] == ext
            assert result["display_name"] == f"12345_Author2023"

    def test_wb_complex_author_patterns(self):
        """Test WB files with complex author/year patterns."""
        test_cases = [
            # The regex pattern ^([0-9]+)[_]([^_]+)[_]?(.*)?\..*$ captures until the first underscore
            # So "Smith_Jones2021" would be captured as "Smith" (author) and "Jones2021" (options)
            ("12345_SmithAndJones2023.pdf", "SmithAndJones2023"),  # No underscore in author
            ("678_Smith-Jones2022.pdf", "Smith-Jones2022"),        # Dash is allowed
            ("999_Smith_Jones2021.pdf", "Smith"),                  # First underscore splits it
            
            # Year variations
            ("12345_Author23.pdf", "Author23"),
            ("678_Author2023a.pdf", "Author2023a"),
            ("999_Author2023b.pdf", "Author2023b"),
            
            # Special characters in author names (no underscores)
            ("12345_O'Brien2023.pdf", "O'Brien2023"),
            ("678_van-der-Berg2022.pdf", "van-der-Berg2022"),  # Using dash instead of underscore
            ("999_Al-Smith2021.pdf", "Al-Smith2021"),
        ]
        
        for filename, expected_author in test_cases:
            result = parse_filename_by_mod(filename, "WB")
            assert result["author_and_year"] == expected_author


class TestFBPatterns:
    """Test FlyBase (FB) specific filename patterns."""

    def test_fb_basic_patterns(self):
        """Test basic FB filename patterns with PMID."""
        test_cases = [
            # Standard pattern: {pmid}_{author_year}.{ext}
            ("12345678_Doe2023.pdf", "PMID:12345678", "Doe2023", "final", None),
            ("87654321_Smith2022.html", "PMID:87654321", "Smith2022", "final", None),
            ("11111111_Johnson2021.txt", "PMID:11111111", "Johnson2021", "final", None),
            
            # Numbers only: {pmid}.{ext}
            ("12345678.pdf", "PMID:12345678", "", "final", None),
            ("87654321.html", "PMID:87654321", "", "final", None),
            ("11111111.txt", "PMID:11111111", "", "final", None),
        ]
        
        for filename, expected_curie, expected_author, expected_status, expected_pdf_type in test_cases:
            result = parse_filename_by_mod(filename, "FB")
            
            assert result["reference_curie"] == expected_curie
            assert result["author_and_year"] == expected_author
            assert result["file_publication_status"] == expected_status
            assert result["pdf_type"] == expected_pdf_type
            assert result["mod_abbreviation"] == "FB"

    def test_fb_pmid_variations(self):
        """Test FB with various PMID lengths and formats."""
        test_cases = [
            # Short PMIDs (older papers)
            ("123_Author2023.pdf", "PMID:123"),
            ("1234_Author2022.pdf", "PMID:1234"),
            ("12345_Author2021.pdf", "PMID:12345"),
            
            # Standard PMIDs
            ("1234567_Author2020.pdf", "PMID:1234567"),
            ("12345678_Author2019.pdf", "PMID:12345678"),
            
            # Longer PMIDs (newer papers)
            ("123456789_Author2018.pdf", "PMID:123456789"),
            ("1234567890_Author2017.pdf", "PMID:1234567890"),
        ]
        
        for filename, expected_curie in test_cases:
            result = parse_filename_by_mod(filename, "FB")
            assert result["reference_curie"] == expected_curie

    def test_fb_with_temp_status(self):
        """Test FB filenames with temp status."""
        test_cases = [
            ("12345678_Doe2023_temp.pdf", "temp"),
            ("87654321_Smith2022_temp.html", "temp"),
            ("11111111_Jones2021_TEMP.txt", "temp"),  # Case insensitive
        ]
        
        for filename, expected_status in test_cases:
            result = parse_filename_by_mod(filename, "FB")
            assert result["file_publication_status"] == expected_status
            assert result["pdf_type"] is None

    def test_fb_with_pdf_types(self):
        """Test FB filenames with PDF type options."""
        test_cases = [
            ("12345678_Doe2023_aut.pdf", "aut"),
            ("87654321_Smith2022_ocr.pdf", "ocr"),
            ("11111111_Jones2021_html.html", "html"),
            ("22222222_Wilson2020_htm.htm", "html"),  # htm converts to html
            ("33333333_Brown2019_lib.pdf", "lib"),
            ("44444444_Taylor2018_tif.tif", "tif"),
        ]
        
        for filename, expected_pdf_type in test_cases:
            result = parse_filename_by_mod(filename, "FB")
            assert result["pdf_type"] == expected_pdf_type
            assert result["file_publication_status"] == "final"

    def test_fb_real_world_examples(self):
        """Test real-world FB filename examples."""
        test_cases = [
            # Real PubMed ID patterns
            ("12345678_Lewis1978.pdf", "PMID:12345678", "Lewis1978"),
            ("87654321_Nusslein-Volhard1980_temp.pdf", "PMID:87654321", "Nusslein-Volhard1980"),
            ("11111111_Wieschaus1984_ocr.pdf", "PMID:11111111", "Wieschaus1984"),
            ("22222222_Brand1993.html", "PMID:22222222", "Brand1993"),
            
            # Edge cases
            ("1_A2023.pdf", "PMID:1", "A2023"),
            ("999999999_VeryLongAuthorName2023.pdf", "PMID:999999999", "VeryLongAuthorName2023"),
            ("12345_AuthorWith123Numbers2023.pdf", "PMID:12345", "AuthorWith123Numbers2023"),
        ]
        
        for filename, expected_curie, expected_author in test_cases:
            result = parse_filename_by_mod(filename, "FB")
            assert result["reference_curie"] == expected_curie
            assert result["author_and_year"] == expected_author

    def test_fb_file_extensions(self):
        """Test FB files with various extensions."""
        extensions = ["pdf", "html", "htm", "txt", "doc", "docx", "xml", "tei", "json"]
        
        for ext in extensions:
            filename = f"12345678_Author2023.{ext}"
            result = parse_filename_by_mod(filename, "FB")
            
            assert result["reference_curie"] == "PMID:12345678"
            assert result["file_extension"] == ext
            assert result["display_name"] == f"12345678_Author2023"

    def test_fb_html_files_special_handling(self):
        """Test FB HTML files with specific handling."""
        test_cases = [
            ("12345678_Author2023_html.html", "html", "html"),
            ("87654321_Author2022_htm.htm", "html", "htm"),  # htm -> html conversion
            ("11111111_Author2021.html", None, "html"),      # No pdf_type for regular HTML
        ]
        
        for filename, expected_pdf_type, expected_extension in test_cases:
            result = parse_filename_by_mod(filename, "FB")
            assert result["pdf_type"] == expected_pdf_type
            assert result["file_extension"] == expected_extension


class TestOtherMODPatterns:
    """Test other MOD patterns (SGD, MGI, RGD, ZFIN)."""

    def test_sgd_patterns(self):
        """Test SGD filename patterns."""
        test_cases = [
            ("12345_Author2023.pdf", "SGD", "AGRKB:12345"),  # Short ID
            ("123456789012345_Author2023.pdf", "SGD", "AGRKB:123456789012345"),  # 15-digit AGRKB
        ]
        
        for filename, mod, expected_curie in test_cases:
            result = parse_filename_by_mod(filename, mod)
            assert result["reference_curie"] == expected_curie
            assert result["mod_abbreviation"] == mod

    def test_mgi_patterns(self):
        """Test MGI filename patterns."""
        test_cases = [
            ("12345_Author2023.pdf", "MGI", "AGRKB:12345"),
            ("123456789012345_Author2023.pdf", "MGI", "AGRKB:123456789012345"),
        ]
        
        for filename, mod, expected_curie in test_cases:
            result = parse_filename_by_mod(filename, mod)
            assert result["reference_curie"] == expected_curie

    def test_agrkb_15_digit_detection(self):
        """Test 15-digit AGRKB ID detection across MODs."""
        mods = ["SGD", "MGI", "RGD", "ZFIN", "XB"]
        
        for mod in mods:
            # 15-digit ID should become AGRKB
            result = parse_filename_by_mod("123456789012345_Author2023.pdf", mod)
            assert result["reference_curie"] == "AGRKB:123456789012345"
            
            # Shorter ID should use fallback
            result = parse_filename_by_mod("12345_Author2023.pdf", mod)
            assert result["reference_curie"] == "AGRKB:12345"


class TestEdgeCasesAndErrorConditions:
    """Test edge cases and error conditions."""

    def test_invalid_filename_patterns(self):
        """Test filenames that should raise ValueError."""
        invalid_patterns = [
            "invalid_filename.pdf",           # No numbers at start
            "text_only_filename.pdf",         # No numbers at all
            "123-456_Author2023.pdf",         # Dash instead of underscore
            "123 456_Author2023.pdf",         # Space instead of underscore
            "_Author2023.pdf",                # Starts with underscore
            "Author2023.pdf",                 # No ID at all
            "123_",                           # Incomplete pattern
            "",                               # Empty filename
            ".pdf",                           # Only extension
        ]
        
        for invalid_pattern in invalid_patterns:
            with pytest.raises(ValueError, match="does not match expected patterns"):
                parse_filename_by_mod(invalid_pattern, "WB")

    def test_unusual_but_valid_patterns(self):
        """Test unusual but valid filename patterns."""
        test_cases = [
            # Very short IDs
            ("1_A.pdf", "WB", "WB:WBPaper1", "A"),
            ("12_AB.pdf", "FB", "PMID:12", "AB"),
            
            # Very long author/year (no underscores in author section)
            ("123_VeryVeryVeryLongAuthorNameWithLotsOfCharacters2023.pdf", "WB", 
             "WB:WBPaper123", "VeryVeryVeryLongAuthorNameWithLotsOfCharacters2023"),
            
            # Numbers in author names (regex captures only up to first underscore after author)
            ("123_Author123_2023.pdf", "WB", "WB:WBPaper123", "Author123"),
            
            # Special characters in filenames (that are still valid)
            ("123_Author-Smith2023.pdf", "WB", "WB:WBPaper123", "Author-Smith2023"),
            ("123_Author.Smith2023.pdf", "WB", "WB:WBPaper123", "Author.Smith2023"),
        ]
        
        for filename, mod, expected_curie, expected_author in test_cases:
            result = parse_filename_by_mod(filename, mod)
            assert result["reference_curie"] == expected_curie
            assert result["author_and_year"] == expected_author

    def test_case_insensitive_options(self):
        """Test that PDF type and temp options are case insensitive."""
        test_cases = [
            # Different cases for temp
            ("123_Author2023_temp.pdf", "temp"),
            ("123_Author2023_TEMP.pdf", "temp"),
            ("123_Author2023_Temp.pdf", "temp"),
            ("123_Author2023_TeMp.pdf", "temp"),
            
            # Different cases for PDF types
            ("123_Author2023_aut.pdf", "aut"),
            ("123_Author2023_AUT.pdf", "aut"),
            ("123_Author2023_Aut.pdf", "aut"),
            ("123_Author2023_ocr.pdf", "ocr"),
            ("123_Author2023_OCR.pdf", "ocr"),
            ("123_Author2023_html.pdf", "html"),
            ("123_Author2023_HTML.pdf", "html"),
            ("123_Author2023_Html.pdf", "html"),
        ]
        
        for filename, expected_value in test_cases:
            result = parse_filename_by_mod(filename, "WB")
            if expected_value == "temp":
                assert result["file_publication_status"] == expected_value
            else:
                assert result["pdf_type"] == expected_value

    def test_filename_without_extension(self):
        """Test filenames without extensions."""
        # These should still work as the regex focuses on the base name
        with pytest.raises(ValueError):
            # This will fail because the pattern expects an extension
            parse_filename_by_mod("123_Author2023", "WB")

    def test_multiple_underscores_in_author(self):
        """Test filenames with multiple underscores in author section."""
        result = parse_filename_by_mod("123_Author_Name_2023.pdf", "WB")
        assert result["reference_curie"] == "WB:WBPaper123"
        # Regex captures only up to first underscore: Author (not Author_Name_2023)
        assert result["author_and_year"] == "Author"

    def test_empty_additional_options(self):
        """Test filenames with empty additional options."""
        # Pattern: 123_Author2023_.pdf (trailing underscore)
        result = parse_filename_by_mod("123_Author2023_.pdf", "WB")
        assert result["reference_curie"] == "WB:WBPaper123"
        assert result["author_and_year"] == "Author2023"
        assert result["file_publication_status"] == "final"  # Empty option -> final
        assert result["pdf_type"] is None


class TestComprehensiveMODComparison:
    """Test comprehensive comparison across different MODs."""

    def test_same_filename_different_mods(self):
        """Test how the same filename is parsed for different MODs."""
        filename = "12345_Author2023.pdf"
        
        # WB should create WBPaper reference
        wb_result = parse_filename_by_mod(filename, "WB")
        assert wb_result["reference_curie"] == "WB:WBPaper12345"
        assert wb_result["mod_abbreviation"] == "WB"
        
        # FB should create PMID reference
        fb_result = parse_filename_by_mod(filename, "FB")
        assert fb_result["reference_curie"] == "PMID:12345"
        assert fb_result["mod_abbreviation"] == "FB"
        
        # Other MODs should create AGRKB reference
        for mod in ["SGD", "MGI", "RGD", "ZFIN", "XB"]:
            result = parse_filename_by_mod(filename, mod)
            assert result["reference_curie"] == "AGRKB:12345"
            assert result["mod_abbreviation"] == mod

    def test_15_digit_ids_across_mods(self):
        """Test 15-digit IDs are consistently handled across MODs."""
        filename = "123456789012345_Author2023.pdf"
        
        # All MODs should recognize 15-digit as AGRKB
        for mod in ["WB", "FB", "SGD", "MGI", "RGD", "ZFIN", "XB"]:
            result = parse_filename_by_mod(filename, mod)
            assert result["reference_curie"] == "AGRKB:123456789012345"
            assert result["mod_abbreviation"] == mod

    def test_consistent_metadata_across_mods(self):
        """Test that metadata fields are consistent across MODs."""
        filename = "12345_Author2023_temp.pdf"
        
        for mod in ["WB", "FB", "SGD", "MGI", "RGD", "ZFIN", "XB"]:
            result = parse_filename_by_mod(filename, mod)
            
            # These fields should be the same regardless of MOD
            assert result["display_name"] == "12345_Author2023_temp"
            assert result["file_extension"] == "pdf"
            assert result["file_publication_status"] == "temp"
            assert result["pdf_type"] is None
            assert result["author_and_year"] == "Author2023"
            assert result["mod_abbreviation"] == mod
            
            # Only reference_curie should differ by MOD
            if mod == "WB":
                assert result["reference_curie"] == "WB:WBPaper12345"
            elif mod == "FB":
                assert result["reference_curie"] == "PMID:12345"
            else:
                assert result["reference_curie"] == "AGRKB:12345"


# ========================================
# UTILS TESTS
# ========================================

class TestParseFilenameByMod:
    """Test MOD-specific filename parsing."""

    def test_wb_wbpaper_id_parsing(self):
        """Test WB filename parsing with WBPaper IDs."""
        # WB pattern: {wbpaper_id}_{author_year}[_{options}].{ext}
        result = parse_filename_by_mod("12345_Doe2023.pdf", "WB")
        
        assert result["reference_curie"] == "WB:WBPaper12345"
        assert result["display_name"] == "12345_Doe2023"
        assert result["file_extension"] == "pdf"
        assert result["file_publication_status"] == "final"
        assert result["pdf_type"] is None
        assert result["author_and_year"] == "Doe2023"
        assert result["mod_abbreviation"] == "WB"

    def test_wb_with_options(self):
        """Test WB filename with additional options."""
        result = parse_filename_by_mod("12345_Smith2022_temp.pdf", "WB")
        
        assert result["reference_curie"] == "WB:WBPaper12345"
        assert result["file_publication_status"] == "temp"
        assert result["pdf_type"] is None

    def test_wb_with_pdf_type(self):
        """Test WB filename with PDF type options."""
        result = parse_filename_by_mod("12345_Jones2021_ocr.pdf", "WB")
        
        assert result["reference_curie"] == "WB:WBPaper12345"
        assert result["file_publication_status"] == "final"
        assert result["pdf_type"] == "ocr"

    def test_fb_pmid_parsing(self):
        """Test FB filename parsing with PMID patterns."""
        # FB pattern: {pmid}_{author_year}[_{options}].{ext}
        result = parse_filename_by_mod("12345678_Brown2023.pdf", "FB")
        
        assert result["reference_curie"] == "PMID:12345678"
        assert result["display_name"] == "12345678_Brown2023"
        assert result["file_extension"] == "pdf"
        assert result["file_publication_status"] == "final"
        assert result["pdf_type"] is None
        assert result["author_and_year"] == "Brown2023"
        assert result["mod_abbreviation"] == "FB"

    def test_fb_with_html_type(self):
        """Test FB filename with HTML type."""
        result = parse_filename_by_mod("87654321_Wilson2022_html.html", "FB")
        
        assert result["reference_curie"] == "PMID:87654321"
        assert result["pdf_type"] == "html"
        assert result["file_extension"] == "html"

    def test_fb_with_htm_type(self):
        """Test FB filename with HTM type (should convert to html)."""
        result = parse_filename_by_mod("87654321_Wilson2022_htm.htm", "FB")
        
        assert result["reference_curie"] == "PMID:87654321"
        assert result["pdf_type"] == "html"  # Should convert htm to html
        assert result["file_extension"] == "htm"

    def test_agrkb_15_digit_id(self):
        """Test 15-digit AGRKB ID parsing."""
        result = parse_filename_by_mod("123456789012345_Author2023.pdf", "SGD")
        
        assert result["reference_curie"] == "AGRKB:123456789012345"
        assert result["display_name"] == "123456789012345_Author2023"
        assert result["author_and_year"] == "Author2023"

    def test_numbers_only_filename(self):
        """Test filename with numbers only (no author/year)."""
        result = parse_filename_by_mod("12345.pdf", "WB")
        
        assert result["reference_curie"] == "WB:WBPaper12345"
        assert result["display_name"] == "12345"
        assert result["author_and_year"] == ""

    def test_invalid_filename_pattern(self):
        """Test invalid filename pattern raises ValueError."""
        with pytest.raises(ValueError, match="does not match expected patterns"):
            parse_filename_by_mod("invalid_filename_pattern.pdf", "WB")

    def test_all_pdf_types(self):
        """Test all supported PDF type options."""
        pdf_types = ["aut", "ocr", "html", "htm", "lib", "tif"]
        
        for pdf_type in pdf_types:
            result = parse_filename_by_mod(f"12345_Author2023_{pdf_type}.pdf", "WB")
            expected_type = "html" if pdf_type == "htm" else pdf_type
            assert result["pdf_type"] == expected_type


class TestParseSupplementFile:
    """Test supplement file parsing."""

    def test_wb_supplement_parsing(self):
        """Test WB supplement file parsing."""
        result = parse_supplement_file("supplementary_data.xlsx", "12345", "WB")
        
        assert result["reference_curie"] == "WB:WBPaper12345"
        assert result["display_name"] == "supplementary_data"
        assert result["file_extension"] == "xlsx"
        assert result["file_publication_status"] == "final"
        assert result["pdf_type"] is None
        assert result["mod_abbreviation"] == "WB"

    def test_fb_supplement_parsing(self):
        """Test FB supplement file parsing."""
        result = parse_supplement_file("figure_1.png", "87654321", "FB")
        
        assert result["reference_curie"] == "PMID:87654321"
        assert result["display_name"] == "figure_1"
        assert result["file_extension"] == "png"

    def test_agrkb_supplement_parsing(self):
        """Test AGRKB 15-digit supplement file parsing."""
        result = parse_supplement_file("data.csv", "123456789012345", "SGD")
        
        assert result["reference_curie"] == "AGRKB:123456789012345"


class TestClassifyAndParseFile:
    """Test file classification and parsing."""

    def test_main_file_classification(self):
        """Test main file classification (root directory)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a main file
            main_file = os.path.join(temp_dir, "12345_Doe2023.pdf")
            
            result = classify_and_parse_file(main_file, temp_dir, "WB")
            
            assert result["file_class"] == "main"
            assert result["reference_curie"] == "WB:WBPaper12345"
            assert result["is_annotation"] is False

    def test_supplement_file_classification(self):
        """Test supplement file classification (subdirectory)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create subdirectory and supplement file
            subdir = os.path.join(temp_dir, "12345")
            os.makedirs(subdir)
            supplement_file = os.path.join(subdir, "figure_1.png")
            
            result = classify_and_parse_file(supplement_file, temp_dir, "WB")
            
            assert result["file_class"] == "supplement"
            assert result["reference_curie"] == "WB:WBPaper12345"
            assert result["display_name"] == "figure_1"


class TestExtractAndClassifyFiles:
    """Test archive extraction and file classification."""

    def create_test_tar_archive(self) -> io.BytesIO:
        """Create a test tar.gz archive with WB and FB files."""
        archive_buffer = io.BytesIO()
        
        with tarfile.open(fileobj=archive_buffer, mode="w:gz") as tar:
            # Add main files
            wb_content = b"WB main file content" + b"x" * 79
            wb_main = tarfile.TarInfo("12345_Doe2023.pdf")
            wb_main.size = len(wb_content)
            tar.addfile(wb_main, io.BytesIO(wb_content))
            
            fb_content = b"FB main file content" + b"y" * 79
            fb_main = tarfile.TarInfo("87654321_Smith2022.pdf")
            fb_main.size = len(fb_content)
            tar.addfile(fb_main, io.BytesIO(fb_content))
            
            # Add supplement files in subdirectories
            wb_supp_content = b"WB supplement content" + b"z" * 27
            wb_supp = tarfile.TarInfo("12345/figure_1.png")
            wb_supp.size = len(wb_supp_content)
            tar.addfile(wb_supp, io.BytesIO(wb_supp_content))
            
            fb_supp_content = b"FB supplement content" + b"w" * 27
            fb_supp = tarfile.TarInfo("87654321/data.csv")
            fb_supp.size = len(fb_supp_content)
            tar.addfile(fb_supp, io.BytesIO(fb_supp_content))
        
        archive_buffer.seek(0)
        return archive_buffer

    def create_test_zip_archive(self) -> io.BytesIO:
        """Create a test zip archive with WB and FB files."""
        archive_buffer = io.BytesIO()
        
        with zipfile.ZipFile(archive_buffer, 'w') as zip_file:
            # Add main files
            zip_file.writestr("12345_Doe2023.pdf", b"WB main file content" + b"x" * 79)
            zip_file.writestr("87654321_Smith2022.pdf", b"FB main file content" + b"y" * 79)
            
            # Add supplement files
            zip_file.writestr("12345/figure_1.png", b"WB supplement content" + b"z" * 27)
            zip_file.writestr("87654321/data.csv", b"FB supplement content" + b"w" * 27)
        
        archive_buffer.seek(0)
        return archive_buffer

    def test_tar_extraction(self):
        """Test tar.gz archive extraction and classification."""
        archive = self.create_test_tar_archive()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            files = extract_and_classify_files(archive, temp_dir)
            
            assert len(files) == 4
            
            # Check that we have both main and supplement files
            main_files = [f for f in files if f[1]]  # is_main = True
            supplement_files = [f for f in files if not f[1]]  # is_main = False
            
            assert len(main_files) == 2
            assert len(supplement_files) == 2
            
            # Verify files exist
            for file_path, _ in files:
                assert os.path.exists(file_path)

    def test_zip_extraction(self):
        """Test zip archive extraction and classification."""
        archive = self.create_test_zip_archive()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            files = extract_and_classify_files(archive, temp_dir)
            
            assert len(files) == 4
            
            main_files = [f for f in files if f[1]]
            supplement_files = [f for f in files if not f[1]]
            
            assert len(main_files) == 2
            assert len(supplement_files) == 2

    def test_invalid_archive_format(self):
        """Test invalid archive format raises ValueError."""
        invalid_archive = io.BytesIO(b"not an archive")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            with pytest.raises(ValueError, match="Archive format not supported"):
                extract_and_classify_files(invalid_archive, temp_dir)


class TestValidateArchiveStructure:
    """Test archive structure validation."""

    def test_valid_tar_archive_validation(self):
        """Test validation of valid tar.gz archive."""
        archive_buffer = io.BytesIO()
        
        with tarfile.open(fileobj=archive_buffer, mode="w:gz") as tar:
            # Add files
            main_file = tarfile.TarInfo("12345_Doe2023.pdf")
            main_file.size = 100
            tar.addfile(main_file, io.BytesIO(b"x" * 100))
            
            supp_file = tarfile.TarInfo("12345/figure_1.png")
            supp_file.size = 50
            tar.addfile(supp_file, io.BytesIO(b"y" * 50))
        
        archive_buffer.seek(0)
        result = validate_archive_structure(archive_buffer)
        
        assert result["valid"] is True
        assert result["total_files"] == 2
        assert result["main_files"] == 1
        assert result["supplement_files"] == 1
        assert len(result["main_file_list"]) == 1
        assert len(result["supplement_file_list"]) == 1

    def test_valid_zip_archive_validation(self):
        """Test validation of valid zip archive."""
        archive_buffer = io.BytesIO()
        
        with zipfile.ZipFile(archive_buffer, 'w') as zip_file:
            zip_file.writestr("87654321_Smith2022.pdf", b"x" * 100)
            zip_file.writestr("87654321/data.csv", b"y" * 50)
        
        archive_buffer.seek(0)
        result = validate_archive_structure(archive_buffer)
        
        assert result["valid"] is True
        assert result["total_files"] == 2
        assert result["main_files"] == 1
        assert result["supplement_files"] == 1

    def test_empty_archive_validation(self):
        """Test validation of empty archive."""
        archive_buffer = io.BytesIO()
        
        with tarfile.open(fileobj=archive_buffer, mode="w:gz") as tar:
            pass  # Empty archive
        
        archive_buffer.seek(0)
        result = validate_archive_structure(archive_buffer)
        
        assert result["valid"] is True
        assert result["total_files"] == 0
        assert result["main_files"] == 0
        assert result["supplement_files"] == 0

    def test_invalid_archive_validation(self):
        """Test validation of invalid archive."""
        invalid_archive = io.BytesIO(b"not an archive")
        
        result = validate_archive_structure(invalid_archive)
        
        assert result["valid"] is False
        assert "error" in result
        assert result["total_files"] == 0


class TestProcessSingleFile:
    """Test single file processing with mocked dependencies."""

    @patch('agr_literature_service.api.crud.referencefile_crud.file_upload')
    def test_successful_file_processing(self, mock_file_upload):
        """Test successful file processing."""
        mock_file_upload.return_value = None  # Successful upload
        
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(b"test file content")
            temp_file_path = temp_file.name
        
        try:
            metadata = {
                "reference_curie": "WB:WBPaper12345",
                "display_name": "12345_Doe2023",
                "file_class": "main",
                "file_extension": "pdf",
                "file_publication_status": "final",
                "pdf_type": None,
                "mod_abbreviation": "WB"
            }
            
            mock_db = Mock()
            result = process_single_file(temp_file_path, metadata, mock_db)
            
            assert result["status"] == "success"
            assert result["reference_curie"] == "WB:WBPaper12345"
            assert result["file_class"] == "main"
            
            # Verify file_upload was called
            mock_file_upload.assert_called_once()
            call_args = mock_file_upload.call_args
            assert call_args[0][0] == mock_db  # db session
            assert call_args[0][1] == metadata  # metadata
            assert call_args[1]["upload_if_already_converted"] is True
            
        finally:
            os.unlink(temp_file_path)

    @patch('agr_literature_service.api.crud.referencefile_crud.file_upload')
    def test_failed_file_processing(self, mock_file_upload):
        """Test failed file processing."""
        mock_file_upload.side_effect = Exception("Upload failed")
        
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(b"test file content")
            temp_file_path = temp_file.name
        
        try:
            metadata = {
                "reference_curie": "FB:87654321",
                "display_name": "87654321_Smith2022",
                "file_class": "main"
            }
            
            mock_db = Mock()
            result = process_single_file(temp_file_path, metadata, mock_db)
            
            assert result["status"] == "error"
            assert "Upload failed" in result["error"]
            assert result["reference_curie"] == "FB:87654321"
            
        finally:
            os.unlink(temp_file_path)

    def test_file_not_found(self):
        """Test processing non-existent file."""
        metadata = {"reference_curie": "WB:WBPaper12345"}
        mock_db = Mock()
        
        result = process_single_file("/non/existent/file.pdf", metadata, mock_db)
        
        assert result["status"] == "error"
        assert "No such file" in result["error"]


class TestIntegrationScenarios:
    """Integration tests with complete WB and FB scenarios."""

    def create_realistic_wb_archive(self) -> io.BytesIO:
        """Create a realistic WB archive with multiple files."""
        archive_buffer = io.BytesIO()
        
        with tarfile.open(fileobj=archive_buffer, mode="w:gz") as tar:
            # Main papers
            papers = [
                ("12345_Doe2023.pdf", b"WB paper 12345 content"),
                ("67890_Smith2022_temp.pdf", b"WB paper 67890 temp content"),
                ("11111_Jones2021_ocr.pdf", b"WB paper 11111 OCR content"),
                ("22222.pdf", b"WB paper 22222 no author content"),
            ]
            
            for filename, content in papers:
                info = tarfile.TarInfo(filename)
                info.size = len(content)
                tar.addfile(info, io.BytesIO(content))
            
            # Supplement files
            supplements = [
                ("12345/figure_1.png", b"Figure 1 for paper 12345"),
                ("12345/data.xlsx", b"Data file for paper 12345"),
                ("67890/supplementary.pdf", b"Supplementary for paper 67890"),
            ]
            
            for filename, content in supplements:
                info = tarfile.TarInfo(filename)
                info.size = len(content)
                tar.addfile(info, io.BytesIO(content))
        
        archive_buffer.seek(0)
        return archive_buffer

    def create_realistic_fb_archive(self) -> io.BytesIO:
        """Create a realistic FB archive with multiple files."""
        archive_buffer = io.BytesIO()
        
        with zipfile.ZipFile(archive_buffer, 'w') as zip_file:
            # Main papers
            papers = [
                ("12345678_Brown2023.pdf", b"FB paper 12345678 content"),
                ("87654321_Wilson2022_html.html", b"FB paper 87654321 HTML content"),
                ("11223344_Taylor2021.pdf", b"FB paper 11223344 content"),
            ]
            
            for filename, content in papers:
                zip_file.writestr(filename, content)
            
            # Supplement files
            supplements = [
                ("12345678/figure_1.png", b"Figure 1 for paper 12345678"),
                ("12345678/data.csv", b"Data for paper 12345678"),
                ("87654321/protocol.txt", b"Protocol for paper 87654321"),
            ]
            
            for filename, content in supplements:
                zip_file.writestr(filename, content)
        
        archive_buffer.seek(0)
        return archive_buffer

    def test_wb_archive_processing(self):
        """Test complete WB archive processing workflow."""
        archive = self.create_realistic_wb_archive()
        
        # Test validation
        validation = validate_archive_structure(archive)
        assert validation["valid"] is True
        assert validation["total_files"] == 7  # 4 main + 3 supplements
        assert validation["main_files"] == 4
        assert validation["supplement_files"] == 3
        
        # Test extraction
        archive.seek(0)  # Reset for extraction
        with tempfile.TemporaryDirectory() as temp_dir:
            files = extract_and_classify_files(archive, temp_dir)
            
            assert len(files) == 7
            
            # Test file classification and parsing
            for file_path, is_main in files:
                result = classify_and_parse_file(file_path, temp_dir, "WB")
                
                assert result["mod_abbreviation"] == "WB"
                assert result["reference_curie"].startswith("WB:WBPaper")
                assert result["file_class"] in ["main", "supplement"]
                assert result["is_annotation"] is False
                
                if is_main:
                    assert result["file_class"] == "main"
                else:
                    assert result["file_class"] == "supplement"

    def test_fb_archive_processing(self):
        """Test complete FB archive processing workflow."""
        archive = self.create_realistic_fb_archive()
        
        # Test validation
        validation = validate_archive_structure(archive)
        assert validation["valid"] is True
        assert validation["total_files"] == 6  # 3 main + 3 supplements
        assert validation["main_files"] == 3
        assert validation["supplement_files"] == 3
        
        # Test extraction
        archive.seek(0)
        with tempfile.TemporaryDirectory() as temp_dir:
            files = extract_and_classify_files(archive, temp_dir)
            
            assert len(files) == 6
            
            # Test file classification and parsing
            for file_path, is_main in files:
                result = classify_and_parse_file(file_path, temp_dir, "FB")
                
                assert result["mod_abbreviation"] == "FB"
                assert result["reference_curie"].startswith("PMID:")
                assert result["file_class"] in ["main", "supplement"]
                
                if is_main:
                    assert result["file_class"] == "main"
                else:
                    assert result["file_class"] == "supplement"

    def test_mixed_file_types_validation(self):
        """Test archive with various file types and extensions."""
        archive_buffer = io.BytesIO()
        
        with tarfile.open(fileobj=archive_buffer, mode="w:gz") as tar:
            # Various file types
            files = [
                ("12345_Paper2023.pdf", b"PDF content"),
                ("67890_Paper2022.html", b"HTML content"),
                ("11111_Paper2021.txt", b"Text content"),
                ("12345/figure.png", b"PNG image"),
                ("12345/data.xlsx", b"Excel data"),
                ("67890/protocol.docx", b"Word document"),
            ]
            
            for filename, content in files:
                info = tarfile.TarInfo(filename)
                info.size = len(content)
                tar.addfile(info, io.BytesIO(content))
        
        archive_buffer.seek(0)
        validation = validate_archive_structure(archive_buffer)
        
        assert validation["valid"] is True
        assert validation["total_files"] == 6
        assert validation["main_files"] == 3
        assert validation["supplement_files"] == 3