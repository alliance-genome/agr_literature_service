"""
Tests for IP-aware authentication system.

This module tests the authentication behavior based on IP address
and the decorator flags (@skip_auth_on_vpn, @enforce_auth).
"""

from unittest.mock import patch, MagicMock
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api import auth as auth_module
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


class TestIPDetection:
    """Tests for IP-based auth bypass detection."""

    def test_ip_in_ranges_with_matching_ip(self):
        """Test that matching IPs are correctly identified."""
        assert auth_module._ip_in_ranges(['10.0.70.11'], ['10.0.0.0/8', '172.31.0.0/16']) is True

    def test_ip_in_ranges_with_non_matching_ip(self):
        """Test that non-matching IPs are not identified."""
        assert auth_module._ip_in_ranges(['203.0.113.50'], ['10.0.0.0/8', '172.31.0.0/16']) is False

    def test_ip_in_ranges_single_ip_without_cidr(self):
        """Test that single IPs without CIDR notation work."""
        assert auth_module._ip_in_ranges(['10.0.70.11'], ['10.0.70.11']) is True
        assert auth_module._ip_in_ranges(['10.0.70.12'], ['10.0.70.11']) is False

    def test_ip_in_ranges_with_strict_false(self):
        """Test that strict=False allows host addresses in CIDR."""
        # 10.0.0.5/24 would fail with strict=True because host bits are set
        assert auth_module._ip_in_ranges(['10.0.0.100'], ['10.0.0.5/24']) is True

    def test_ip_in_ranges_empty_ranges(self):
        """Test that empty ranges means no match."""
        assert auth_module._ip_in_ranges(['10.0.70.11'], []) is False

    def test_ip_in_ranges_invalid_cidr_logs_warning(self):
        """Test that invalid CIDR format is handled gracefully."""
        # Invalid CIDR should be skipped, but valid ones should still work
        assert auth_module._ip_in_ranges(['10.0.70.11'], ['invalid', '10.0.0.0/8']) is True
        assert auth_module._ip_in_ranges(['10.0.70.11'], ['invalid', '192.168.0.0/16']) is False

    def test_ip_in_ranges_multiple_client_ips(self):
        """Test that any matching IP from multiple client IPs triggers a match."""
        # First IP doesn't match, but second does
        assert auth_module._ip_in_ranges(['203.0.113.50', '10.0.70.11'], ['10.0.0.0/8']) is True
        # Neither IP matches
        assert auth_module._ip_in_ranges(['203.0.113.50', '8.8.8.8'], ['10.0.0.0/8']) is False

    def test_ip_in_ranges_empty_client_ips(self):
        """Test that empty client IPs list means no match."""
        assert auth_module._ip_in_ranges([], ['10.0.0.0/8']) is False

    def test_is_skip_read_auth_ip(self):
        """Test is_skip_read_auth_ip function."""
        with patch.object(auth_module, 'get_read_skip_ip_ranges',
                          return_value=['10.0.0.0/8', '172.31.0.0/16']):
            mock_request = MagicMock()
            mock_request.headers.get.return_value = None
            mock_request.client.host = '10.0.70.11'

            assert auth_module.is_skip_read_auth_ip(mock_request) is True

    def test_is_skip_read_auth_ip_with_external_ip(self):
        """Test that external IPs are not identified for read skip."""
        with patch.object(auth_module, 'get_read_skip_ip_ranges',
                          return_value=['10.0.0.0/8', '172.31.0.0/16']):
            mock_request = MagicMock()
            mock_request.headers.get.return_value = None
            mock_request.client.host = '203.0.113.50'

            assert auth_module.is_skip_read_auth_ip(mock_request) is False

    def test_is_skip_all_auth_ip(self):
        """Test is_skip_all_auth_ip function."""
        with patch.object(auth_module, 'get_all_skip_ip_ranges',
                          return_value=['192.168.1.0/24']):
            mock_request = MagicMock()
            mock_request.headers.get.return_value = None
            mock_request.client.host = '192.168.1.100'

            assert auth_module.is_skip_all_auth_ip(mock_request) is True

    def test_is_skip_all_auth_ip_with_external_ip(self):
        """Test that external IPs are not identified for all skip."""
        with patch.object(auth_module, 'get_all_skip_ip_ranges',
                          return_value=['192.168.1.0/24']):
            mock_request = MagicMock()
            mock_request.headers.get.return_value = None
            mock_request.client.host = '203.0.113.50'

            assert auth_module.is_skip_all_auth_ip(mock_request) is False

    def test_x_forwarded_for_used_for_ip_detection(self):
        """Test that X-Forwarded-For header is used for IP detection."""
        with patch.object(auth_module, 'get_read_skip_ip_ranges',
                          return_value=['10.0.0.0/8']):
            mock_request = MagicMock()
            mock_request.headers.get.return_value = '10.0.70.11, 192.168.1.1'
            mock_request.client.host = '192.168.1.1'

            # Should use first IP from X-Forwarded-For
            assert auth_module.is_skip_read_auth_ip(mock_request) is True

    def test_no_cidr_configured_means_no_bypass(self):
        """Test that no CIDR ranges means all requests require auth."""
        with patch.object(auth_module, 'get_read_skip_ip_ranges',
                          return_value=[]):
            mock_request = MagicMock()
            mock_request.headers.get.return_value = None
            mock_request.client.host = '10.0.70.11'

            assert auth_module.is_skip_read_auth_ip(mock_request) is False


class TestReadEndpointBypass:
    """Tests for SKIP_AUTH_ON_READ_ENDPOINTS_FOR_IP on GET requests."""

    def test_trusted_ip_get_request_no_auth_required(self, db):  # noqa
        """Test that GET requests from trusted IPs don't require authentication."""
        with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=True), \
             patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
            with TestClient(app) as client:
                # GET request from trusted IP should succeed without auth
                response = client.get(url="/reference/AGRKB:101000000000001")
                # 404 is fine - we're testing auth bypass, not data existence
                assert response.status_code in [
                    status.HTTP_200_OK, status.HTTP_404_NOT_FOUND
                ]

    def test_external_get_request_requires_auth(self, db):  # noqa
        """Test that GET requests from external require authentication."""
        with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False), \
             patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
            with TestClient(app) as client:
                # GET request from external without auth should fail
                response = client.get(url="/reference/AGRKB:101000000000001")
                assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_trusted_ip_get_with_auth_still_works(self, db, auth_headers):  # noqa
        """Test that GET requests from trusted IP with auth still work."""
        with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=True), \
             patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
            with TestClient(app) as client:
                # GET request from trusted IP with auth should also work
                response = client.get(
                    url="/reference/AGRKB:101000000000001",
                    headers=auth_headers
                )
                assert response.status_code in [
                    status.HTTP_200_OK, status.HTTP_404_NOT_FOUND
                ]


class TestReadBypassMutationsRequireAuth:
    """Tests for SKIP_AUTH_ON_READ_ENDPOINTS_FOR_IP still requiring auth for mutations."""

    def test_trusted_ip_post_request_requires_auth(self, db):  # noqa
        """Test that POST requests from trusted IP (read-only bypass) still require auth."""
        with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=True), \
             patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
            with TestClient(app) as client:
                new_reference = {"title": "Test", "category": "thesis"}
                response = client.post(url="/reference/", json=new_reference)
                assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_trusted_ip_post_with_auth_works(self, db, auth_headers):  # noqa
        """Test that POST requests from trusted IP with auth work."""
        with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=True), \
             patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
            with TestClient(app) as client:
                new_reference = {"title": "Test VPN Auth", "category": "thesis"}
                response = client.post(
                    url="/reference/",
                    json=new_reference,
                    headers=auth_headers
                )
                assert response.status_code == status.HTTP_201_CREATED

    def test_trusted_ip_delete_requires_auth(self, db):  # noqa
        """Test that DELETE requests from trusted IP (read-only bypass) require auth."""
        with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=True), \
             patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
            with TestClient(app) as client:
                response = client.delete(url="/reference/AGRKB:101000000000001")
                assert response.status_code == status.HTTP_401_UNAUTHORIZED


class TestAllEndpointsSkipAuth:
    """Tests for SKIP_AUTH_ON_ALL_ENDPOINTS_FOR_IP bypassing all auth."""

    def test_skip_all_get_no_auth_required(self, db):  # noqa
        """Test that GET requests from skip-all IPs don't require auth."""
        with patch.object(auth_module, 'is_skip_all_auth_ip', return_value=True), \
             patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False):
            with TestClient(app) as client:
                response = client.get(url="/reference/AGRKB:101000000000001")
                assert response.status_code in [
                    status.HTTP_200_OK, status.HTTP_404_NOT_FOUND
                ]

    def test_skip_all_post_no_auth_required(self, db):  # noqa
        """Test that POST requests from skip-all IPs don't require auth."""
        with patch.object(auth_module, 'is_skip_all_auth_ip', return_value=True), \
             patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False):
            with TestClient(app) as client:
                new_reference = {"title": "Test Skip All Auth", "category": "thesis"}
                response = client.post(url="/reference/", json=new_reference)
                # Should not be 401 - auth is completely skipped
                assert response.status_code in [
                    status.HTTP_201_CREATED, status.HTTP_422_UNPROCESSABLE_ENTITY
                ]

    def test_skip_all_delete_no_auth_required(self, db):  # noqa
        """Test that DELETE requests from skip-all IPs don't require auth."""
        with patch.object(auth_module, 'is_skip_all_auth_ip', return_value=True), \
             patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False):
            with TestClient(app) as client:
                response = client.delete(url="/reference/AGRKB:101000000000001")
                # Should not be 401 - auth is completely skipped
                assert response.status_code in [
                    status.HTTP_204_NO_CONTENT, status.HTTP_404_NOT_FOUND
                ]

    def test_skip_all_overrides_read_only(self, db):  # noqa
        """Test that skip-all takes precedence over read-only for mutations."""
        with patch.object(auth_module, 'is_skip_all_auth_ip', return_value=True), \
             patch.object(auth_module, 'is_skip_read_auth_ip', return_value=True):
            with TestClient(app) as client:
                new_reference = {"title": "Test Override", "category": "thesis"}
                response = client.post(url="/reference/", json=new_reference)
                # Should not be 401 - skip-all takes precedence
                assert response.status_code != status.HTTP_401_UNAUTHORIZED


class TestReadAuthBypassDecorator:
    """Tests for @read_auth_bypass decorator."""

    def test_read_auth_bypass_post_endpoint(self, db):  # noqa
        """Test that @read_auth_bypass allows POST from read-level IP without auth."""
        with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=True), \
             patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
            with TestClient(app) as client:
                # /cross_reference/show_all is a POST with @read_auth_bypass
                response = client.post(
                    url="/cross_reference/show_all",
                    json=["PMID:12345"]
                )
                # Should not return 401 - decorator allows access
                assert response.status_code != status.HTTP_401_UNAUTHORIZED

    def test_read_auth_bypass_external_still_requires_auth(self, db):  # noqa
        """Test that @read_auth_bypass still requires auth for external."""
        with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False), \
             patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
            with TestClient(app) as client:
                response = client.post(
                    url="/cross_reference/show_all",
                    json=["PMID:12345"]
                )
                assert response.status_code == status.HTTP_401_UNAUTHORIZED


class TestNoReadAuthBypassDecorator:
    """Tests for @no_read_auth_bypass decorator."""

    def test_no_read_bypass_blocks_read_ips(self, db):  # noqa
        """Test that @no_read_auth_bypass blocks read-level IPs."""
        with patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False), \
             patch.object(auth_module, 'is_skip_read_auth_ip', return_value=True):
            with TestClient(app) as client:
                # /topic_entity_tag/revalidate_all_tags/ has @no_read_auth_bypass
                response = client.get(url="/topic_entity_tag/revalidate_all_tags/")
                assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_no_read_bypass_allows_full_bypass_ips(self, db):  # noqa
        """Test that @no_read_auth_bypass allows full-bypass IPs with default user."""
        with patch.object(auth_module, 'is_skip_all_auth_ip', return_value=True), \
             patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False):
            with TestClient(app) as client:
                # /topic_entity_tag/revalidate_all_tags/ has @no_read_auth_bypass
                # Full-bypass IPs get DEFAULT_BYPASS_USER (not None)
                # The endpoint requires SuperAdmin, so we get 401 for that reason
                # but NOT for "Missing authentication token"
                response = client.get(url="/topic_entity_tag/revalidate_all_tags/")
                # Should not be 401 for missing token - user is set to default
                if response.status_code == status.HTTP_401_UNAUTHORIZED:
                    assert "Missing authentication" not in response.text

    def test_no_read_bypass_full_bypass_gets_default_user(self, db):  # noqa
        """Test that full-bypass IPs receive the DEFAULT_BYPASS_USER."""
        with patch.object(auth_module, 'is_skip_all_auth_ip', return_value=True), \
             patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False):
            with TestClient(app) as client:
                # /person/whoami returns user info
                response = client.get(url="/person/whoami")
                assert response.status_code == status.HTTP_200_OK
                data = response.json()
                assert data["user_id"] == "default_user"
                # default_user has no person_id, so email and name are null
                assert data["email"] is None
                assert data["name"] is None

    def test_no_read_bypass_with_auth_works(self, db, auth_headers):  # noqa
        """Test that @no_read_auth_bypass endpoint works with valid auth."""
        with patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False), \
             patch.object(auth_module, 'is_skip_read_auth_ip', return_value=True):
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
        with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False), \
             patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
            with TestClient(app) as client:
                response = client.get(url="/author/1")
                assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_external_post_requires_auth(self, db):  # noqa
        """External POST requests require authentication."""
        with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False), \
             patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
            with TestClient(app) as client:
                response = client.post(url="/author/", json={"name": "Test"})
                assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_external_with_auth_works(self, db, auth_headers):  # noqa
        """External requests with valid auth work normally."""
        with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False), \
             patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
            with TestClient(app) as client:
                response = client.get(url="/author/1", headers=auth_headers)
                # 404 is fine - auth passed, resource just doesn't exist
                assert response.status_code in [
                    status.HTTP_200_OK, status.HTTP_404_NOT_FOUND
                ]


class TestPreviouslyUnprotectedEndpointsRequireAuth:
    """Tests that previously unprotected endpoints now require authentication.

    All these endpoints were open prior to SCRUM-5767 and must now return
    401 when accessed without credentials from a non-bypass IP.
    """

    def _assert_401(self, client, method, url, json_body=None):
        """Helper: call an endpoint and assert 401 Unauthorized."""
        if method == "get":
            resp = client.get(url=url)
        else:
            resp = client.post(url=url, json=json_body or {})
        assert resp.status_code == status.HTTP_401_UNAUTHORIZED, (
            f"{method.upper()} {url} returned {resp.status_code}, expected 401"
        )

    def test_check_endpoints_require_auth(self, db):  # noqa
        """All /check/* endpoints require auth."""
        with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False), \
             patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
            with TestClient(app) as client:
                for path in [
                    "/check/ateamapi",
                    "/check/database",
                    "/check/check_obsolete_entities",
                    "/check/check_redacted_references_with_tags",
                    "/check/check_obsolete_pmids",
                    "/check/check_duplicate_orcids",
                    "/check/environments",
                    "/check/debezium_status",
                ]:
                    self._assert_401(client, "get", path)

    def test_ontology_endpoints_require_auth(self, db):  # noqa
        """All /ontology/* endpoints require auth."""
        with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False), \
             patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
            with TestClient(app) as client:
                for path in [
                    "/ontology/entity_validation/NCBITaxon:6239/gene/WBGene00000001",
                    "/ontology/map_curie_to_name/gene/WB:WBGene00000001",
                    "/ontology/search_topic/ATP:0000001",
                    "/ontology/search_descendants/ATP:0000001",
                    "/ontology/search_species/NCBITaxon:6239",
                ]:
                    self._assert_401(client, "get", path)

    def test_search_endpoint_requires_auth(self, db):  # noqa
        """POST /search/references/ requires auth."""
        with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False), \
             patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
            with TestClient(app) as client:
                self._assert_401(
                    client, "post", "/search/references/",
                    json_body={"query": "test"},
                )

    def test_sort_endpoints_require_auth(self, db):  # noqa
        """All /sort/* endpoints require auth."""
        with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False), \
             patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
            with TestClient(app) as client:
                for path in [
                    "/sort/need_review?mod_abbreviation=WB",
                    "/sort/need_prioritization?mod_abbreviation=WB",
                    "/sort/prepublication_pipeline?mod_abbreviation=WB",
                    "/sort/recently_sorted?mod_abbreviation=WB",
                ]:
                    self._assert_401(client, "get", path)

    def test_database_endpoints_require_auth(self, db):  # noqa
        """All /database/* endpoints require auth."""
        with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False), \
             patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
            with TestClient(app) as client:
                for path in [
                    "/database/schema/download",
                    "/database/configuration",
                ]:
                    self._assert_401(client, "get", path)

    def test_workflow_tag_job_endpoints_require_auth(self, db):  # noqa
        """POST /workflow_tag/job/* endpoints require auth."""
        with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False), \
             patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
            with TestClient(app) as client:
                for path in [
                    "/workflow_tag/job/failed/999",
                    "/workflow_tag/job/retry/999",
                    "/workflow_tag/job/success/999",
                    "/workflow_tag/job/started/999",
                ]:
                    self._assert_401(client, "post", path)

    def test_workflow_tag_set_priority_requires_auth(self, db):  # noqa
        """POST /workflow_tag/set_priority requires auth."""
        with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=False), \
             patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
            with TestClient(app) as client:
                self._assert_401(
                    client, "post",
                    "/workflow_tag/set_priority/AGRKB:101000000000001/WB/high",
                )


class TestSearchReadAuthBypass:
    """Tests that the search POST allows read-level IP bypass
    while workflow_tag job POSTs do NOT.
    """

    def test_search_post_allows_read_ip_bypass(self, db):  # noqa
        """POST /search/references/ has @read_auth_bypass, so read IPs can access it."""
        with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=True), \
             patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
            with TestClient(app) as client:
                response = client.post(
                    url="/search/references/",
                    json={"query": "test"},
                )
                # Should NOT be 401 — read IP bypass allows access
                assert response.status_code != status.HTTP_401_UNAUTHORIZED

    def test_workflow_job_post_denies_read_ip_bypass(self, db):  # noqa
        """POST /workflow_tag/job/* does NOT have @read_auth_bypass,
        so read IPs should still get 401."""
        with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=True), \
             patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
            with TestClient(app) as client:
                for path in [
                    "/workflow_tag/job/failed/999",
                    "/workflow_tag/job/retry/999",
                    "/workflow_tag/job/success/999",
                    "/workflow_tag/job/started/999",
                ]:
                    resp = client.post(url=path)
                    assert resp.status_code == status.HTTP_401_UNAUTHORIZED, (
                        f"POST {path} returned {resp.status_code}, expected 401"
                    )

    def test_workflow_set_priority_denies_read_ip_bypass(self, db):  # noqa
        """POST /workflow_tag/set_priority does NOT have @read_auth_bypass."""
        with patch.object(auth_module, 'is_skip_read_auth_ip', return_value=True), \
             patch.object(auth_module, 'is_skip_all_auth_ip', return_value=False):
            with TestClient(app) as client:
                resp = client.post(
                    url="/workflow_tag/set_priority/AGRKB:101000000000001/WB/high",
                )
                assert resp.status_code == status.HTTP_401_UNAUTHORIZED
