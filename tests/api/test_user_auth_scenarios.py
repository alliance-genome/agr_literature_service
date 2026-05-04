"""
Tests for user authentication and authorization scenarios.

Verifies that:
1. Authenticated users can read regardless of DB user record
2. Write endpoints require a DB user record (403 without one)
3. Unauthenticated requests get 401 on everything
"""

from unittest.mock import patch
from starlette.testclient import TestClient
from fastapi import HTTPException, status

from agr_literature_service.api.main import app
from agr_literature_service.api.auth import get_authenticated_user
from agr_literature_service.api import auth as auth_module
from ..fixtures import db  # noqa


# Mock user dicts that mimic what get_authenticated_user returns
# ID token user whose email has NO matching users row in the DB
VALID_USER_NO_DB_RECORD = {
    "sub": "orphan-user-id",
    "email": "orphan_no_db_user@example.com",
    "name": "Orphan User",
    "cognito:groups": [],
    "token_type": "id",
}

# Access token user (service account) - always works via default_user
ACCESS_TOKEN_USER = {
    "sub": "service-account",
    "token_type": "access",
    "cognito:groups": [],
}


def _no_auth():
    """Mock that simulates no credentials provided."""
    raise HTTPException(status_code=401, detail="Missing authentication token")


def _auth_no_db_user():
    """Mock that returns a valid ID token user with no DB record."""
    return VALID_USER_NO_DB_RECORD


def _auth_access_token():
    """Mock that returns an access token user (service account)."""
    return ACCESS_TOKEN_USER


class TestReadEndpointsNoUserLookup:
    """Read endpoints should work for any authenticated user,
    even without a DB user record."""

    def test_read_with_valid_auth_no_db_user(self, db):  # noqa
        """Authenticated user without DB user row can still read."""
        app.dependency_overrides[get_authenticated_user] = _auth_no_db_user
        try:
            with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False), \
                 patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
                with TestClient(app) as client:
                    # GET /mod/{abbreviation} is a read endpoint
                    response = client.get(url="/mod/WB")
                    # Should succeed or 404 — NOT 403 or 500
                    assert response.status_code in [
                        status.HTTP_200_OK, status.HTTP_404_NOT_FOUND
                    ]
        finally:
            app.dependency_overrides.pop(get_authenticated_user, None)

    def test_read_with_access_token_works(self, db):  # noqa
        """Access token user (service account) can read."""
        app.dependency_overrides[get_authenticated_user] = _auth_access_token
        try:
            with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False), \
                 patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
                with TestClient(app) as client:
                    response = client.get(url="/mod/WB")
                    assert response.status_code in [
                        status.HTTP_200_OK, status.HTTP_404_NOT_FOUND
                    ]
        finally:
            app.dependency_overrides.pop(get_authenticated_user, None)

    def test_read_returns_401_without_auth(self, db):  # noqa
        """No auth should return 401."""
        app.dependency_overrides[get_authenticated_user] = _no_auth
        try:
            with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False), \
                 patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
                with TestClient(app) as client:
                    response = client.get(url="/mod/WB")
                    assert response.status_code == status.HTTP_401_UNAUTHORIZED
        finally:
            app.dependency_overrides.pop(get_authenticated_user, None)


class TestWriteEndpointsRequireDbUser:
    """Write endpoints call set_global_user_from_cognito and need a DB user."""

    def test_write_with_access_token_works(self, db):  # noqa
        """Access token user (service account) can write via default_user."""
        app.dependency_overrides[get_authenticated_user] = _auth_access_token
        try:
            with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False), \
                 patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
                with TestClient(app) as client:
                    new_mod = {
                        "abbreviation": "TESTAUTH",
                        "short_name": "TestAuth",
                        "full_name": "Test Auth MOD"
                    }
                    response = client.post(url="/mod/", json=new_mod)
                    # Should succeed — access token uses default_user
                    assert response.status_code == status.HTTP_201_CREATED
        finally:
            app.dependency_overrides.pop(get_authenticated_user, None)

    def test_write_with_no_db_user_returns_403(self, db):  # noqa
        """ID token user with no DB user row should get 403 on write."""
        app.dependency_overrides[get_authenticated_user] = _auth_no_db_user
        try:
            with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False), \
                 patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
                with TestClient(app) as client:
                    new_mod = {
                        "abbreviation": "TESTFAIL",
                        "short_name": "TestFail",
                        "full_name": "Test Fail MOD"
                    }
                    response = client.post(url="/mod/", json=new_mod)
                    assert response.status_code == status.HTTP_403_FORBIDDEN

        finally:
            app.dependency_overrides.pop(get_authenticated_user, None)

    def test_write_returns_401_without_auth(self, db):  # noqa
        """No auth should return 401 on write."""
        app.dependency_overrides[get_authenticated_user] = _no_auth
        try:
            with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False), \
                 patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
                with TestClient(app) as client:
                    new_mod = {
                        "abbreviation": "TESTNOAUTH",
                        "short_name": "TestNoAuth",
                        "full_name": "Test NoAuth MOD"
                    }
                    response = client.post(url="/mod/", json=new_mod)
                    assert response.status_code == status.HTTP_401_UNAUTHORIZED
        finally:
            app.dependency_overrides.pop(get_authenticated_user, None)
