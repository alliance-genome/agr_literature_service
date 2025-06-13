"""
Simplified tests for bulk upload API endpoints.
"""
import io
import tarfile
import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient

from agr_literature_service.api.main import app
from agr_literature_service.api.utils.bulk_upload_manager import upload_manager

# Initialize TestClient
d = TestClient(app)


@pytest.fixture(autouse=True)
def clear_jobs():
    """Clear job manager state before each test."""
    upload_manager._jobs.clear()
    yield
    upload_manager._jobs.clear()


@patch('agr_literature_service.api.routers.authentication.auth.get_user')
def test_validate_and_start_and_status(mock_user):
    # Mock authenticated user
    mock_user.return_value = type('U', (), {'cid': 'user1', 'groups': ['WBCurator']})()

    # Create a small archive
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode='w:gz') as tar:
        info = tarfile.TarInfo('123_test.txt')
        info.size = 4
        tar.addfile(info, io.BytesIO(b'data'))
    buf.seek(0)

    # 1. Validate
    resp = d.post(
        '/reference/referencefile/bulk_upload_validate/',
        files={'archive': ('a.tar.gz', buf, 'application/gzip')}
    )
    assert resp.status_code == 200
    assert resp.json()['valid']

    # 2. Start upload
    buf.seek(0)
    resp = d.post(
        '/reference/referencefile/bulk_upload_archive/?mod_abbreviation=WB',
        files={'archive': ('a.tar.gz', buf, 'application/gzip')}
    )
    assert resp.status_code == 202
    data = resp.json()
    job_id = data['job_id']
    assert data['status'] == 'started'

    # 3. Status
    resp = d.get(f'/reference/referencefile/bulk_upload_status/{job_id}')
    assert resp.status_code == 200
    status = resp.json()
    assert status['job_id'] == job_id
    assert status['status'] in ('running', 'completed')


@patch('agr_literature_service.api.routers.authentication.auth.get_user')
def test_status_not_found(mock_user):
    mock_user.return_value = type('U', (), {'cid': 'user1', 'groups': ['WBCurator']})()
    resp = d.get('/reference/referencefile/bulk_upload_status/does_not_exist')
    assert resp.status_code == 404
    assert resp.json()['detail'] == 'Job not found'


@patch('agr_literature_service.api.routers.authentication.auth.get_user')
def test_active_and_history_empty(mock_user):
    mock_user.return_value = type('U', (), {'cid': 'user1', 'groups': ['WBCurator']})()
    # Active should be empty
    resp = d.get('/reference/referencefile/bulk_upload_active/')
    assert resp.status_code == 200
    assert resp.json() == []

    # History should be empty
    resp = d.get('/reference/referencefile/bulk_upload_history/')
    assert resp.status_code == 200
    assert resp.json() == []


@patch('agr_literature_service.api.routers.authentication.auth.get_user')
def test_missing_authentication(mock_user):
    # Simulate auth failure
    mock_user.side_effect = Exception()
    assert d.post('/reference/referencefile/bulk_upload_archive/?mod_abbreviation=WB', files={}).status_code == 403
    assert d.get('/reference/referencefile/bulk_upload_status/foo').status_code == 403
    assert d.get('/reference/referencefile/bulk_upload_active/').status_code == 403
    assert d.get('/reference/referencefile/bulk_upload_history/').status_code == 403
    assert d.post('/reference/referencefile/bulk_upload_validate/', files={}).status_code == 403
