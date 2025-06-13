"""""
Simplified tests for bulk upload API endpoints.
"""
import io
import tarfile
from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient

from agr_literature_service.api.main import app
from agr_literature_service.api.utils.bulk_upload_manager import upload_manager

client = TestClient(app)


@pytest.fixture(autouse=True)
def clear_jobs():
    # Clear manager state before each test
    upload_manager._jobs.clear()
    yield
    upload_manager._jobs.clear()


def make_test_archive() -> io.BytesIO:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode='w:gz') as tar:
        info = tarfile.TarInfo('123_test.txt')
        info.size = len(b'content')
        tar.addfile(info, io.BytesIO(b'content'))
    buf.seek(0)
    return buf


@patch('agr_literature_service.api.routers.authentication.auth.get_user')
def test_validate_and_start_and_status(mock_user):
    # Mock authenticated user
    mock_user.return_value = type('U', (), {'cid': 'user1', 'groups': ['WBCurator']})()

    archive = make_test_archive()
    # 1. Validate
    resp = client.post('/reference/referencefile/bulk_upload_validate/', files={'archive': ('a.tar.gz', archive, 'application/gzip')})
    assert resp.status_code == 200
    assert resp.json()['valid']

    # 2. Start upload
    archive.seek(0)
    resp = client.post('/reference/referencefile/bulk_upload_archive/?mod_abbreviation=WB', files={'archive': ('a.tar.gz', archive, 'application/gzip')})
    assert resp.status_code == 202
    data = resp.json()
    job_id = data['job_id']
    assert data['status'] == 'started'

    # 3. Status
    resp = client.get(f'/reference/referencefile/bulk_upload_status/{job_id}')
    assert resp.status_code == 200
    status = resp.json()
    assert status['job_id'] == job_id
    assert status['status'] in ('running', 'completed')


def test_status_not_found(mock_user):
    mock_user.return_value = type('U', (), {'cid': 'user1', 'groups': ['WBCurator']})()
    resp = client.get('/reference/referencefile/bulk_upload_status/does_not_exist')
    assert resp.status_code == 404
    assert resp.json()['detail'] == 'Job not found'


@patch('agr_literature_service.api.routers.authentication.auth.get_user')
def test_active_and_history_empty(mock_user):
    mock_user.return_value = type('U', (), {'cid': 'user1', 'groups': ['WBCurator']})()
    # Active should be empty
    resp = client.get('/reference/referencefile/bulk_upload_active/')
    assert resp.status_code == 200
    assert resp.json() == []

    # History should be empty
    resp = client.get('/reference/referencefile/bulk_upload_history/')
    assert resp.status_code == 200
    assert resp.json() == []


@patch('agr_literature_service.api.routers.authentication.auth.get_user')
def test_missing_authentication(mock_user):
    # No auth -> 403
    mock_user.side_effect = Exception()
    resp = client.post('/reference/referencefile/bulk_upload_archive/?mod_abbreviation=WB')
    assert resp.status_code == 403
    resp = client.get('/reference/referencefile/bulk_upload_status/foo')
    assert resp.status_code == 403
    resp = client.get('/reference/referencefile/bulk_upload_active/')
    assert resp.status_code == 403
    resp = client.get('/reference/referencefile/bulk_upload_history/')
    assert resp.status_code == 403
    resp = client.post('/reference/referencefile/bulk_upload_validate/')
    assert resp.status_code == 403
