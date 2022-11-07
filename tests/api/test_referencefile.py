from collections import namedtuple

import pytest
from starlette.testclient import TestClient

from agr_literature_service.api.main import app
from fastapi import status

from agr_literature_service.api.models import ReferencefileModel
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from .test_reference import test_reference # noqa
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa


TestReferencefileData = namedtuple('TestReferencefileData', ['response', 'new_referencefile_id'])


@pytest.fixture
def test_referencefile(db, auth_headers, test_reference): # noqa
    print("***** Adding a test referencefile *****")
    with TestClient(app) as client:
        new_referencefile = {
            "display_name": "Bob",
            "reference_curie": test_reference.new_ref_curie,
            "file_class": "main",
            "file_publication_status": "final",
            "file_extension": "pdf",
            "pdf_type": "pdf",
            "md5sum": "1234567890"
        }
        response = client.post(url="/reference/referencefile/", json=new_referencefile, headers=auth_headers)
        yield TestReferencefileData(response, response.json())


class TestReferencefile():

    def test_create_referencefile(self, test_referencefile): # noqa
        assert test_referencefile.response.status_code == status.HTTP_201_CREATED

    def test_show_referencefile(self, test_referencefile):
        with TestClient(app) as client:
            response = client.get(url=f"/reference/referencefile/{test_referencefile.new_referencefile_id}")
            assert response.status_code == status.HTTP_200_OK
            response2 = client.get(url="/reference/referencefile/1234567890")
            assert response2.status_code == status.HTTP_200_OK

    def test_patch_referencefile(self, db, test_referencefile, auth_headers): # noqa
        patch_referencefile = {
            "display_name": "Bob2"
        }
        with TestClient(app) as client:
            response = client.patch(url=f"/reference/referencefile/{test_referencefile.new_referencefile_id}",
                                    json=patch_referencefile, headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED
            response = client.get(url=f"/reference/referencefile/{test_referencefile.new_referencefile_id}")
            assert response.json()["display_name"] == patch_referencefile["display_name"]
            ref_file_obj = db.query(ReferencefileModel).filter(
                ReferencefileModel.referencefile_id == test_referencefile.new_referencefile_id).one_or_none()
            assert ref_file_obj.display_name == patch_referencefile["display_name"]

    def test_destroy_referencefile(self, test_referencefile, auth_headers): # noqa
        with TestClient(app) as client:
            response = client.delete(url=f"/reference/referencefile/{test_referencefile.new_referencefile_id}",
                                     headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT
            response = client.get(url=f"/reference/referencefile/{test_referencefile.new_referencefile_id}")
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_show_reference_referencefiles(self, db, test_referencefile): # noqa
        with TestClient(app) as client:
            response_file = client.get(url=f"/reference/referencefile/{test_referencefile.new_referencefile_id}")
            response_ref = client.get(url=f"/reference/{response_file.json()['reference_curie']}")
            assert "referencefiles" in response_ref.json()
            assert response_ref.json()["referencefiles"][0]["display_name"] == "Bob"

    def test_delete_reference_cascade(self, test_referencefile, auth_headers): # noqa
        with TestClient(app) as client:
            response_file = client.get(url=f"/reference/referencefile/{test_referencefile.new_referencefile_id}")
            client.delete(url=f"/reference/{response_file.json()['reference_curie']}", headers=auth_headers)
            response_file = client.get(url=f"/reference/referencefile/{test_referencefile.new_referencefile_id}")
            assert response_file.status_code == status.HTTP_404_NOT_FOUND

    def test_create_referencefile_with_mod(self, test_reference, auth_headers): # noqa
        populate_test_mods()
        new_referencefile = {
            "display_name": "Bob",
            "reference_curie": test_reference.new_ref_curie,
            "file_class": "main",
            "file_publication_status": "final",
            "file_extension": "pdf",
            "pdf_type": "pdf",
            "md5sum": "1234567890",
            "mod_abbreviation": "WB"
        }
        with TestClient(app) as client:
            response = client.post(url="/reference/referencefile/", json=new_referencefile, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            response_file = client.get(url=f"/reference/referencefile/{response.json()}")
            assert response_file.status_code == status.HTTP_200_OK
            assert response_file.json()["referencefile_mods"][0]["mod_abbreviation"] == "WB"

    def test_create_referencefile_pmc(self, test_reference, auth_headers):  # noqa
        populate_test_mods()
        new_referencefile = {
            "display_name": "Bob",
            "reference_curie": test_reference.new_ref_curie,
            "file_class": "main",
            "file_publication_status": "final",
            "file_extension": "pdf",
            "pdf_type": "pdf",
            "md5sum": "1234567890",
            "mod_abbreviation": None
        }
        with TestClient(app) as client:
            response = client.post(url="/reference/referencefile/", json=new_referencefile, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            response_file = client.get(url=f"/reference/referencefile/{response.json()}")
            assert response_file.status_code == status.HTTP_200_OK
            assert response_file.json()["referencefile_mods"][0]["mod_abbreviation"] is None

    def test_create_referencefile_annotation(self, test_reference, auth_headers):  # noqa
        populate_test_mods()
        new_referencefile = {
            "display_name": "Bob",
            "reference_curie": test_reference.new_ref_curie,
            "file_class": "main",
            "file_publication_status": "final",
            "file_extension": "pdf",
            "pdf_type": "pdf",
            "md5sum": "1234567890",
            "mod_abbreviation": "WB",
            "is_annotation": True
        }
        with TestClient(app) as client:
            response = client.post(url="/reference/referencefile/", json=new_referencefile, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            response_file = client.get(url=f"/reference/referencefile/{response.json()}")
            assert response_file.status_code == status.HTTP_200_OK
            assert response_file.json()["is_annotation"] is True

    def test_create_referencefile_pdf_type(self, test_reference, auth_headers):  # noqa
        populate_test_mods()
        new_referencefile = {
            "display_name": "Bob",
            "reference_curie": test_reference.new_ref_curie,
            "file_class": "main",
            "file_publication_status": "final",
            "file_extension": "doc",
            "pdf_type": None,
            "md5sum": "1234567890",
            "mod_abbreviation": "WB"
        }
        with TestClient(app) as client:
            response = client.post(url="/reference/referencefile/", json=new_referencefile, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            response_file = client.get(url=f"/reference/referencefile/{response.json()}")
            assert response_file.status_code == status.HTTP_200_OK
            assert response_file.json()["pdf_type"] is None
