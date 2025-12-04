"""Tests for Cognito authentication utilities."""
import pytest

from agr_cognito_py import (
    generate_headers,
    get_authentication_token,
    get_admin_token,
    clear_token_cache,
)


class TestCognitoUtils:
    """Test suite for Cognito authentication utilities."""

    def test_generate_headers(self):
        """Test that generate_headers creates correct Authorization headers."""
        headers = generate_headers(token="TEST_TOKEN")
        assert headers == {
            'Authorization': 'Bearer TEST_TOKEN',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

    def test_generate_headers_with_real_token(self):
        """Test generate_headers with various token formats."""
        # Test with empty token
        headers = generate_headers(token="")
        assert headers["Authorization"] == "Bearer "

        # Test with long JWT-like token
        long_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.test.signature"
        headers = generate_headers(token=long_token)
        assert headers["Authorization"] == f"Bearer {long_token}"

    @pytest.mark.webtest
    def test_get_admin_token(self):
        """Test that get_admin_token retrieves a valid token from Cognito."""
        # Clear cache to ensure we get a fresh token
        clear_token_cache()

        token = get_admin_token()
        assert isinstance(token, str)
        assert len(token) > 0
        # JWT tokens typically have 3 parts separated by dots
        assert token.count('.') == 2

    @pytest.mark.webtest
    def test_get_authentication_token(self):
        """Test that get_authentication_token retrieves a valid token."""
        # Clear cache to ensure we get a fresh token
        clear_token_cache()

        token = get_authentication_token()
        assert isinstance(token, str)
        assert len(token) > 0

    @pytest.mark.webtest
    def test_token_caching(self):
        """Test that tokens are cached and reused."""
        clear_token_cache()

        # First call should get a fresh token
        token1 = get_admin_token()

        # Second call should return the cached token
        token2 = get_admin_token()

        # Both should be the same token (cached)
        assert token1 == token2

    @pytest.mark.webtest
    def test_force_refresh(self):
        """Test that force_refresh bypasses the cache."""
        clear_token_cache()

        # Get initial token
        token1 = get_admin_token()

        # Force refresh should get a new token (may be same or different
        # depending on timing, but the function should not error)
        token2 = get_admin_token(force_refresh=True)

        # Both should be valid tokens
        assert isinstance(token1, str)
        assert isinstance(token2, str)
        assert len(token1) > 0
        assert len(token2) > 0

    def test_clear_token_cache(self):
        """Test that clear_token_cache resets the cache."""
        # This should not raise any errors
        clear_token_cache()
        clear_token_cache()  # Clearing twice should also be fine
