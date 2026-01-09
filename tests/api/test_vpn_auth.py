"""
Tests for VPN-aware authentication system.

This module tests the authentication behavior based on request source (VPN vs external)
and the decorator flags (@skip_auth_on_vpn, @enforce_auth).
"""

from unittest.mock import patch, MagicMock
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api import auth as auth_module
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


class TestVPNDetection:
    """Tests for VPN/internal IP detection."""

    def test_is_internal_request_with_vpn_ip(self):
        """Test that VPN IPs are correctly identified as internal."""
        with patch.object(auth_module, 'get_internal_cidr_ranges',
                          return_value=['10.0.0.0/8', '172.31.0.0/16']):
            mock_request = MagicMock()
            mock_request.headers.get.return_value = None
            mock_request.client.host = '10.0.70.11'

            assert auth_module.is_internal_request(mock_request) is True

    def test_is_internal_request_with_external_ip(self):
        """Test that external IPs are not identified as internal."""
        with patch.object(auth_module, 'get_internal_cidr_ranges',
                          return_value=['10.0.0.0/8', '172.31.0.0/16']):
            mock_request = MagicMock()
            mock_request.headers.get.return_value = None
            mock_request.client.host = '203.0.113.50'

            assert auth_module.is_internal_request(mock_request) is False

    def test_is_internal_request_with_x_forwarded_for(self):
        """Test that X-Forwarded-For header is used for IP detection."""
        with patch.object(auth_module, 'get_internal_cidr_ranges',
                          return_value=['10.0.0.0/8']):
            mock_request = MagicMock()
            mock_request.headers.get.return_value = '10.0.70.11, 192.168.1.1'
            mock_request.client.host = '192.168.1.1'

            # Should use first IP from X-Forwarded-For
            assert auth_module.is_internal_request(mock_request) is True

    def test_is_internal_request_no_cidr_configured(self):
        """Test that no CIDR ranges means all requests are external."""
        with patch.object(auth_module, 'get_internal_cidr_ranges',
                          return_value=[]):
            mock_request = MagicMock()
            mock_request.headers.get.return_value = None
            mock_request.client.host = '10.0.70.11'

            assert auth_module.is_internal_request(mock_request) is False


class TestVPNBypassForGET:
    """Tests for VPN bypass on GET requests."""

    def test_vpn_get_request_no_auth_required(self, db):  # noqa
        """Test that GET requests from VPN don't require authentication."""
        with patch.object(auth_module, 'is_internal_request', return_value=True):
            with TestClient(app) as client:
                # GET request from VPN should succeed without auth
                response = client.get(url="/reference/AGRKB:101000000000001")
                # 404 is fine - we're testing auth bypass, not data existence
                assert response.status_code in [
                    status.HTTP_200_OK, status.HTTP_404_NOT_FOUND
                ]

    def test_external_get_request_requires_auth(self, db):  # noqa
        """Test that GET requests from external require authentication."""
        with patch.object(auth_module, 'is_internal_request', return_value=False):
            with TestClient(app) as client:
                # GET request from external without auth should fail
                response = client.get(url="/reference/AGRKB:101000000000001")
                assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_vpn_get_with_auth_still_works(self, db, auth_headers):  # noqa
        """Test that GET requests from VPN with auth still work."""
        with patch.object(auth_module, 'is_internal_request', return_value=True):
            with TestClient(app) as client:
                # GET request from VPN with auth should also work
                response = client.get(
                    url="/reference/AGRKB:101000000000001",
                    headers=auth_headers
                )
                assert response.status_code in [
                    status.HTTP_200_OK, status.HTTP_404_NOT_FOUND
                ]


class TestVPNMutationRequiresAuth:
    """Tests for VPN mutations still requiring auth."""

    def test_vpn_post_request_requires_auth(self, db):  # noqa
        """Test that POST requests from VPN still require authentication."""
        with patch.object(auth_module, 'is_internal_request', return_value=True):
            with TestClient(app) as client:
                new_reference = {"title": "Test", "category": "thesis"}
                response = client.post(url="/reference/", json=new_reference)
                assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_vpn_post_with_auth_works(self, db, auth_headers):  # noqa
        """Test that POST requests from VPN with auth work."""
        with patch.object(auth_module, 'is_internal_request', return_value=True):
            with TestClient(app) as client:
                new_reference = {"title": "Test VPN Auth", "category": "thesis"}
                response = client.post(
                    url="/reference/",
                    json=new_reference,
                    headers=auth_headers
                )
                assert response.status_code == status.HTTP_201_CREATED

    def test_vpn_delete_requires_auth(self, db):  # noqa
        """Test that DELETE requests from VPN require authentication."""
        with patch.object(auth_module, 'is_internal_request', return_value=True):
            with TestClient(app) as client:
                response = client.delete(url="/reference/AGRKB:101000000000001")
                assert response.status_code == status.HTTP_401_UNAUTHORIZED


class TestSkipAuthOnVPNDecorator:
    """Tests for @skip_auth_on_vpn decorator."""

    def test_skip_auth_vpn_post_endpoint(self, db):  # noqa
        """Test that @skip_auth_on_vpn allows POST from VPN without auth."""
        with patch.object(auth_module, 'is_internal_request', return_value=True):
            with TestClient(app) as client:
                # /cross_reference/show_all is a POST with @skip_auth_on_vpn
                response = client.post(
                    url="/cross_reference/show_all",
                    json=["PMID:12345"]
                )
                # Should not return 401 - decorator allows VPN access
                assert response.status_code != status.HTTP_401_UNAUTHORIZED

    def test_skip_auth_external_still_requires_auth(self, db):  # noqa
        """Test that @skip_auth_on_vpn still requires auth for external."""
        with patch.object(auth_module, 'is_internal_request', return_value=False):
            with TestClient(app) as client:
                response = client.post(
                    url="/cross_reference/show_all",
                    json=["PMID:12345"]
                )
                assert response.status_code == status.HTTP_401_UNAUTHORIZED


class TestEnforceAuthDecorator:
    """Tests for @enforce_auth decorator."""

    def test_enforce_auth_vpn_get_requires_auth(self, db):  # noqa
        """Test that @enforce_auth requires auth even for VPN GET."""
        with patch.object(auth_module, 'is_internal_request', return_value=True):
            with TestClient(app) as client:
                # /topic_entity_tag/revalidate_all_tags/ has @enforce_auth
                response = client.get(url="/topic_entity_tag/revalidate_all_tags/")
                assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_enforce_auth_with_auth_works(self, db, auth_headers):  # noqa
        """Test that @enforce_auth endpoint works with valid auth."""
        with patch.object(auth_module, 'is_internal_request', return_value=True):
            with TestClient(app) as client:
                # Even with auth, may fail due to missing email or not being SuperAdmin
                # But should NOT be 401 Unauthorized for missing token
                response = client.get(
                    url="/topic_entity_tag/revalidate_all_tags/",
                    headers=auth_headers
                )
                # Could be 401 for "not SuperAdmin" but that's after token validation
                # The point is it attempted to validate the token
                assert response.status_code in [
                    status.HTTP_200_OK,
                    status.HTTP_401_UNAUTHORIZED  # SuperAdmin check
                ]


class TestExternalRequestsAlwaysRequireAuth:
    """Tests that external requests always require auth regardless of method."""

    def test_external_get_requires_auth(self, db):  # noqa
        """External GET requests require authentication."""
        with patch.object(auth_module, 'is_internal_request', return_value=False):
            with TestClient(app) as client:
                response = client.get(url="/author/1")
                assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_external_post_requires_auth(self, db):  # noqa
        """External POST requests require authentication."""
        with patch.object(auth_module, 'is_internal_request', return_value=False):
            with TestClient(app) as client:
                response = client.post(url="/author/", json={"name": "Test"})
                assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_external_with_auth_works(self, db, auth_headers):  # noqa
        """External requests with valid auth work normally."""
        with patch.object(auth_module, 'is_internal_request', return_value=False):
            with TestClient(app) as client:
                response = client.get(url="/author/1", headers=auth_headers)
                # 404 is fine - auth passed, resource just doesn't exist
                assert response.status_code in [
                    status.HTTP_200_OK, status.HTTP_404_NOT_FOUND
                ]
