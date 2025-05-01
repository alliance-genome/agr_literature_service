from collections import namedtuple
import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
# from agr_literature_service.api.models import CurationStatusModel
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from .test_mod import test_mod # noqa
from .test_reference import test_reference # noqa

TestCurationStatusData = namedtuple('TestCurationStatusData', ['response', 'new_curation_status_id', 'new_reference_id', 'new_mod_id'])


@pytest.fixture
def test_curation_status(db, auth_headers, test_reference, test_mod): # noqa
    print("***** Adding a test curation_status *****")
    with TestClient(app) as client:
        new_curation_status = {
            "mod_abbreviation": test_mod.new_mod_abbreviation,
            "reference_curie": test_reference.new_ref_curie,
            "topic": "ATP:curation_test"
        }
        response = client.post(url="/curation_status/", json=new_curation_status, headers=auth_headers)
        yield TestCurationStatusData(response, response.json(), test_reference.new_ref_curie, test_mod.new_mod_abbreviation)


class TestCurationStatus:

    def test_create(self, test_curation_status, auth_headers): # noqa
        with TestClient(app):
            assert test_curation_status.response.status_code == status.HTTP_201_CREATED

    def test_list(self, test_curation_status, auth_headers): # noqa
        with TestClient(app) as client:
            url = f"/curation_status/{test_curation_status.new_reference_id}/{test_curation_status.new_mod_id}"
            print(url)
            response = client.get(url=url, headers=auth_headers)
            print(response)
            assert response.status_code == status.HTTP_200_OK
            res = response.json()
            assert res['data'][0]["topic"] == "ATP:curation_test"
            assert res['data'][0]['curation_status_id'] == test_curation_status.new_curation_status_id

    def test_show(self, test_curation_status, auth_headers): # noqa
        with TestClient(app) as client:
            url = f"/curation_status/{test_curation_status.new_curation_status_id}"
            print(url)
            response = client.get(url=url, headers=auth_headers)
            print(response)
            assert response.status_code == status.HTTP_200_OK
            res = response.json()
            assert res["topic"] == "ATP:curation_test"

    def test_patch(self, test_curation_status, auth_headers): # noqa
        with TestClient(app) as client:
            patch_data = {
                "controlled_note": "ATP:cont_note",
                "note": "some notes",
                "curation_status": "ATP:curation_started"
            }
            url = f"/curation_status/{test_curation_status.new_curation_status_id}"
            print(url)
            response = client.patch(url=url, headers=auth_headers, json=patch_data)
            assert response.status_code == status.HTTP_202_ACCEPTED
            response = client.get(f"/curation_status/{test_curation_status.new_curation_status_id}")
            assert response.status_code == status.HTTP_200_OK
            resp_data = response.json()
            for key, value in patch_data.items():
                assert resp_data[key] == value
