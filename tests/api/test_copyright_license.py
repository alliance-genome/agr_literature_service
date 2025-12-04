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

    def test_create_copyright_license(self, db, auth_headers): # noqa
        """Test creating a new copyright license via POST endpoint."""
        with TestClient(app) as client:
            new_license = {
                "name": "CC BY 4.0",
                "url": "https://creativecommons.org/licenses/by/4.0/",
                "description": "Creative Commons Attribution 4.0 International",
                "open_access": True
            }
            response = client.post(url="/copyright_license/", json=new_license, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            assert isinstance(response.json(), int)
            assert response.json() > 0

    def test_create_copyright_license_closed_access(self, db, auth_headers): # noqa
        """Test creating a closed access copyright license."""
        with TestClient(app) as client:
            new_license = {
                "name": "Proprietary License",
                "url": "https://example.com/proprietary",
                "description": "Restricted access license",
                "open_access": False
            }
            response = client.post(url="/copyright_license/", json=new_license, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            assert isinstance(response.json(), int)

    def test_show_all(self, test_copyright_license): # noqa
        with TestClient(app) as client:
            response = client.get(url="/copyright_license/all")
            assert response.status_code == status.HTTP_200_OK
            assert response.json()[0]["name"] == "test license name"
            assert response.json()[0]["open_access"] is True
