"""
Tests for bulk upload API endpoints.
"""

import io
import tarfile
import zipfile
from unittest.mock import Mock, patch

import pytest
from fastapi.testclient import TestClient

from agr_literature_service.api.main import app
from agr_literature_service.api.utils.bulk_upload_manager import upload_manager


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

    with tarfile.open(fileobj=archive_buffer, mode="w:gz"):
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
        upload_manager.create_job("test_user_123", "FB", "fb_file.zip")
        upload_manager.create_job("test_user_123", "SGD", "sgd_file.tar.gz")

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
