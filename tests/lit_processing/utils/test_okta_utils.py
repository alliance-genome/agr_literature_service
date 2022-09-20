import os.path
from os import environ

import pytest

from agr_literature_service.lit_processing.utils.okta_utils import generate_headers, update_okta_token, \
    get_authentication_token
from ...fixtures import cleanup_tmp_files_when_done # noqa


class TestOktaUtils:
    def test_generate_headers(self):
        headers = generate_headers(token="TEST_TOKEN")
        assert headers == {
            'Authorization': 'Bearer TEST_TOKEN',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

    @pytest.mark.webtest
    def test_update_okta_token(self, cleanup_tmp_files_when_done):
        token = update_okta_token()
        assert type(token) == str
        assert len(token) > 0

    @pytest.mark.webtest
    def test_get_authentication_token(self):
        token = get_authentication_token()
        assert type(token) == str
        assert len(token) > 0

        base_path = environ.get("XML_PATH", "")
        token_file = os.path.join(base_path, "okta_token")
        assert os.path.exists(token_file)
        creation_time = os.path.getatime(token_file)
        # this call does not create a new file but uses the cached one
        get_authentication_token()
        assert os.path.getatime(token_file) == creation_time

