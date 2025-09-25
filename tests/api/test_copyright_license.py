from collections import namedtuple
import pytest
from starlette.testclient import TestClient
from fastapi import status
# from agr_literature_service.api.crud.copyright_license_crud import show_all
from agr_literature_service.api.main import app
# from agr_literature_service.api.models import CopyrightLicenseModel
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa

LicenseTestData = namedtuple('LicenseTestData', ['response', 'new_copyright_license_id', 'new_license_name'])


@pytest.fixture
def test_copyright_license(db, auth_headers): # noqa
    print("***** Adding a test copyright_license *****")
    with TestClient(app) as client:
        new_license = {
            "name": "test license name",
            "url": "test url",
            "description": "test description",
            "open_access": True
        }
        response = client.post(url="/copyright_license/", json=new_license, headers=auth_headers)
        yield LicenseTestData(response, response.json(), new_license["name"])


class TestCopyrightLicense:

    def test_show_all(self, test_copyright_license): # noqa
        with TestClient(app) as client:
            response = client.get(url="/copyright_license/all")
            assert response.status_code == status.HTTP_200_OK
            assert response.json()[0]["name"] == "test license name"
            assert response.json()[0]["open_access"] is True
