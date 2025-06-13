"""
Simplified tests for bulk upload API endpoints with dependency overrides.
"""
import io
import tarfile
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from agr_literature_service.api.main import app
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.utils.bulk_upload_manager import upload_manager

# Initialize TestClient
client = TestClient(app)


@pytest.fixture(autouse=True)
def clear_jobs_and_auth_override():
    """Clear jobs and override auth for tests."""
    upload_manager._jobs.clear()
    # Mock user with required attributes
    user = type('U', (), {
        'cid': 'test_user',
        'uid': 'test_user',
        'email': 'test@example.com',
        'groups': ['WBCurator']
    })()
    app.dependency_overrides[auth.get_user] = lambda: user
    yield
    upload_manager._jobs.clear()
    app.dependency_overrides.clear()


def make_archive():
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode='w:gz') as tar:
        info = tarfile.TarInfo('file.txt')
        info.size = len(b'x')
        tar.addfile(info, io.BytesIO(b'x'))
    buf.seek(0)
    return buf


def test_validate_and_start_and_status():
    buf = make_archive()
    # Validate archive
    resp = client.post(
        '/reference/referencefile/bulk_upload_validate/',
        files={'archive': ('a.tar.gz', buf, 'application/gzip')}
    )
    assert resp.status_code == 200
    assert resp.json()['valid'] is True

    # Start upload
    buf = make_archive()
    resp = client.post(
        '/reference/referencefile/bulk_upload_archive/?mod_abbreviation=WB',
        files={'archive': ('a.tar.gz', buf, 'application/gzip')}
    )
    assert resp.status_code == 202
    data = resp.json()
    jid = data['job_id']
    assert data['status'] == 'started'

    # Check status
    resp = client.get(f'/reference/referencefile/bulk_upload_status/{jid}')
    assert resp.status_code == 200
    result = resp.json()
    assert result['job_id'] == jid
    assert result['status'] in ('running', 'completed')


@pytest.mark.parametrize('jid,code,detail', [
    ('nonexistent', 404, 'Job not found'),
])
def test_status_not_found(jid, code, detail):
    resp = client.get(f'/reference/referencefile/bulk_upload_status/{jid}')
    assert resp.status_code == code
    assert resp.json()['detail'] == detail


def test_active_and_history_empty():
    # No jobs yet
    resp = client.get('/reference/referencefile/bulk_upload_active/')
    assert resp.status_code == 200
    assert resp.json() == []

    resp = client.get('/reference/referencefile/bulk_upload_history/')
    assert resp.status_code == 200
    assert resp.json() == []


def test_missing_authentication():
    # Override auth to fail
    app.dependency_overrides[auth.get_user] = lambda: (_ for _ in ()).throw(HTTPException(status_code=403))
    buf = make_archive()
    # Validate
    assert client.post(
        '/reference/referencefile/bulk_upload_validate/',
        files={'archive': ('a.tar.gz', buf, 'application/gzip')}
    ).status_code == 403
    # Upload
    buf = make_archive()
    assert client.post(
        '/reference/referencefile/bulk_upload_archive/?mod_abbreviation=WB',
        files={'archive': ('a.tar.gz', buf, 'application/gzip')}
    ).status_code == 403
    # Status
    assert client.get('/reference/referencefile/bulk_upload_status/foo').status_code == 403
    # Active
    assert client.get('/reference/referencefile/bulk_upload_active/').status_code == 403
    # History
    assert client.get('/reference/referencefile/bulk_upload_history/').status_code == 403
