import pytest

from agr_literature_service.lit_processing.utils.okta_utils import get_authentication_token, generate_headers


@pytest.fixture(scope="session")
def auth_headers():
    print("***** Generating Okta token *****")
    yield generate_headers(get_authentication_token())
