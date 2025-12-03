import pytest

from agr_cognito_auth import get_authentication_token, generate_headers


@pytest.fixture(scope="session")
def auth_headers():
    """Session-scoped fixture for Cognito admin token headers."""
    print("***** Generating Cognito admin token *****")
    yield generate_headers(get_authentication_token())
