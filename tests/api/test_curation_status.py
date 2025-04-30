from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import CurationStatusModel
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from .test_mod import test_mod # noqa
from .test_reference import test_reference # noqa

TestCurationStatusData = namedtuple('TestCurationStatusData', ['response', 'new_curation_status_id', 'new_reference_id'])


@pytest.fixture
def test_curation_status(db, auth_headers, test_reference, test_mod): # noqa
    print("***** Adding a test curation_status *****")
    with TestClient(app) as client:
        new_curation_status = {
            "mod_abbreviation": test_mod.new_mod_abbreviation,
            "reference_curie": test_reference.new_ref_curie
        }
        response = client.post(url="/curation_status/", json=new_curation_status, headers=auth_headers)
        yield TestCurationStatusData(response, response.json()['new_curation_status_id'], test_reference.new_ref_curie)

class TestCurationStatus:

    def test_create(self, test_curation_status, auth_headers): # noqa
        with TestClient(app):
            assert test_curation_status.response.status_code == status.HTTP_201_CREATED
