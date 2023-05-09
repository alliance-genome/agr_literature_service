import pytest
from starlette.testclient import TestClient

from agr_literature_service.api.main import app
from fastapi import status

from agr_literature_service.api.models import ReferencefileModel
from agr_literature_service.api.schemas.referencefile_schemas import ReferencefileSchemaPost
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from .test_reference import test_reference # noqa
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from agr_literature_service.api.crud.referencefile_crud import create_metadata


@pytest.fixture
def test_referencefile(db, auth_headers, test_reference): # noqa
    print("***** Adding a test referencefile *****")
    new_referencefile = {
        "display_name": "Bob",
        "reference_curie": test_reference.new_ref_curie,
        "file_class": "main",
        "file_publication_status": "final",
        "file_extension": "pdf",
        "pdf_type": "pdf",
        "md5sum": "1234567890"
    }
    yield create_metadata(db, ReferencefileSchemaPost(**new_referencefile))


class TestReferencefile:

    def test_show_referencefile(self, test_referencefile):
        with TestClient(app) as client:
            response = client.get(url=f"/reference/referencefile/{test_referencefile}")
            assert response.status_code == status.HTTP_200_OK

    def test_patch_referencefile(self, db, test_referencefile, auth_headers): # noqa
        patch_referencefile = {
            "display_name": "Bob2"
        }
        with TestClient(app) as client:
            response = client.patch(url=f"/reference/referencefile/{test_referencefile}",
                                    json=patch_referencefile, headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED
            response = client.get(url=f"/reference/referencefile/{test_referencefile}")
            assert response.json()["display_name"] == patch_referencefile["display_name"]
            ref_file_obj = db.query(ReferencefileModel).filter(
                ReferencefileModel.referencefile_id == test_referencefile).one_or_none()
            assert ref_file_obj.display_name == patch_referencefile["display_name"]


    def test_show_all(self, db, test_referencefile): # noqa
        with TestClient(app) as client:
            response_file = client.get(url=f"/reference/referencefile/{test_referencefile}")
            response = client.get(url=f"/reference/referencefile/show_all/"
                                      f"{response_file.json()['reference_curie']}")
            assert len(response.json()) > 0
            assert response.json()[0]["display_name"] == "Bob"

    def test_delete_reference_cascade(self, test_referencefile, auth_headers): # noqa
        with TestClient(app) as client:
            response_file = client.get(url=f"/reference/referencefile/{test_referencefile}")
            client.delete(url=f"/reference/{response_file.json()['reference_curie']}", headers=auth_headers)
            response_file = client.get(url=f"/reference/referencefile/{test_referencefile}")
            assert response_file.status_code == status.HTTP_404_NOT_FOUND

    def test_create_referencefile_with_mod(self, db, test_reference, auth_headers): # noqa
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
        new_referencefile_id = create_metadata(db, ReferencefileSchemaPost(**new_referencefile))
        with TestClient(app) as client:
            response_file = client.get(url=f"/reference/referencefile/{new_referencefile_id}")
            assert response_file.status_code == status.HTTP_200_OK
            assert response_file.json()["referencefile_mods"][0]["mod_abbreviation"] == "WB"

    def test_create_referencefile_pmc(self, db, test_reference, auth_headers):  # noqa
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
        new_referencefile_id = create_metadata(db, ReferencefileSchemaPost(**new_referencefile))
        with TestClient(app) as client:
            response_file = client.get(url=f"/reference/referencefile/{new_referencefile_id}")
            assert response_file.status_code == status.HTTP_200_OK
            assert response_file.json()["referencefile_mods"][0]["mod_abbreviation"] is None

    def test_create_referencefile_annotation(self, db, test_reference, auth_headers):  # noqa
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
        new_referencefile_id = create_metadata(db, ReferencefileSchemaPost(**new_referencefile))
        with TestClient(app) as client:
            response_file = client.get(url=f"/reference/referencefile/{new_referencefile_id}")
            assert response_file.status_code == status.HTTP_200_OK
            assert response_file.json()["is_annotation"] is True

    def test_create_referencefile_pdf_type(self, db, test_reference, auth_headers):  # noqa
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
        new_referencefile_id = create_metadata(db, ReferencefileSchemaPost(**new_referencefile))
        with TestClient(app) as client:
            response_file = client.get(url=f"/reference/referencefile/{new_referencefile_id}")
            assert response_file.status_code == status.HTTP_200_OK
            assert response_file.json()["pdf_type"] is None
            response = client.get(url=f"/reference/referencefile/show_all/{test_reference.new_ref_curie}")
            assert response.status_code == status.HTTP_200_OK
            assert response.json()[0]["pdf_type"] is None
