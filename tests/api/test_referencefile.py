import pytest
from starlette.testclient import TestClient

from agr_literature_service.api.main import app
from fastapi import status

from agr_literature_service.api.models import ReferencefileModel
from agr_literature_service.api.schemas.referencefile_schemas import ReferencefileSchemaPost
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from .test_reference import test_reference # noqa
from .test_reference import test_reference as test_reference2 # noqa
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
    try:
        new_referencefile_id = create_metadata(db, ReferencefileSchemaPost(**new_referencefile))
        referencefile = db.query(ReferencefileModel).filter_by(referencefile_id=new_referencefile_id).one()
        yield referencefile
    finally:
        db.rollback()


class TestReferencefile:

    def test_show_referencefile(self, test_referencefile, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/reference/referencefile/{test_referencefile.referencefile_id}",
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK

    def test_patch_referencefile(self, db, test_referencefile, auth_headers): # noqa
        patch_referencefile = {
            "display_name": "Bob2"
        }
        with TestClient(app) as client:
            response = client.patch(url=f"/reference/referencefile/{test_referencefile.referencefile_id}",
                                    json=patch_referencefile, headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            response = client.get(url=f"/reference/referencefile/{test_referencefile.referencefile_id}",
                                  headers=auth_headers)
            assert response.json()["display_name"] == patch_referencefile["display_name"]
            ref_file_obj = db.query(ReferencefileModel).filter(
                ReferencefileModel.referencefile_id == test_referencefile.referencefile_id).one_or_none()
            db.refresh(ref_file_obj)
            assert ref_file_obj.display_name == patch_referencefile["display_name"]

    def test_patch_referencefile_same_name(self, db, test_referencefile, auth_headers, test_reference2):  # noqa
        referencefile_ref2 = {
            "display_name": "Bob",
            "reference_curie": test_reference2.new_ref_curie,
            "file_class": "main",
            "file_publication_status": "final",
            "file_extension": "pdf",
            "pdf_type": "pdf",
            "md5sum": "1234567891"
        }
        create_metadata(db, ReferencefileSchemaPost(**referencefile_ref2))
        patch_referencefile_ref1 = {
            "reference_curie": test_reference2.new_ref_curie
        }
        with TestClient(app) as client:
            response = client.patch(url=f"/reference/referencefile/{test_referencefile.referencefile_id}",
                                    json=patch_referencefile_ref1, headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            response = client.get(url=f"/reference/referencefile/{test_referencefile.referencefile_id}",
                                  headers=auth_headers)
            assert response.json()["display_name"] == "Bob_1"
            assert response.json()["reference_curie"] == test_reference2.new_ref_curie

            ref_file_obj: ReferencefileModel = db.query(ReferencefileModel).filter(
                ReferencefileModel.referencefile_id == test_referencefile.referencefile_id).one_or_none()
            db.refresh(ref_file_obj)
            assert ref_file_obj.display_name == "Bob_1"
            assert ref_file_obj.reference.curie == test_reference2.new_ref_curie

    def test_show_all(self, db, test_referencefile, auth_headers):  # noqa
        with TestClient(app) as client:
            response_file = client.get(url=f"/reference/referencefile/{test_referencefile.referencefile_id}",
                                       headers=auth_headers)
            response = client.get(url=f"/reference/referencefile/show_all/"
                                      f"{response_file.json()['reference_curie']}",
                                  headers=auth_headers)
            assert len(response.json()) > 0
            assert response.json()[0]["display_name"] == "Bob"

    def test_delete_reference_cascade(self, test_referencefile, auth_headers):  # noqa
        with TestClient(app) as client:
            response_file = client.get(url=f"/reference/referencefile/{test_referencefile.referencefile_id}",
                                       headers=auth_headers)
            client.delete(url=f"/reference/{response_file.json()['reference_curie']}", headers=auth_headers)
            response_file = client.get(url=f"/reference/referencefile/{test_referencefile.referencefile_id}",
                                       headers=auth_headers)
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
            response_file = client.get(url=f"/reference/referencefile/{new_referencefile_id}",
                                       headers=auth_headers)
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
            response_file = client.get(url=f"/reference/referencefile/{new_referencefile_id}",
                                       headers=auth_headers)
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
            response_file = client.get(url=f"/reference/referencefile/{new_referencefile_id}",
                                       headers=auth_headers)
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
            response_file = client.get(url=f"/reference/referencefile/{new_referencefile_id}",
                                       headers=auth_headers)
            assert response_file.status_code == status.HTTP_200_OK
            assert response_file.json()["pdf_type"] is None
            response = client.get(url=f"/reference/referencefile/show_all/{test_reference.new_ref_curie}",
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert response.json()[0]["pdf_type"] is None

    def test_merge(self, db, test_referencefile, test_reference, test_reference2, auth_headers):  # noqa                                     
        populate_test_mods()
        referencefile_ref2 = {
            "display_name": "Bob",
            "reference_curie": test_reference2.new_ref_curie,
            "file_class": "main",
            "file_publication_status": "final",
            "file_extension": "pdf",
            "pdf_type": "pdf",
            "md5sum": "1234567890",
            "mod_abbreviation": "WB"
        }
        test_referencefile2 = create_metadata(db, ReferencefileSchemaPost(**referencefile_ref2))

        with TestClient(app) as client:
            # Use referencefile_id when referring to test_referencefile and test_referencefile2
            for mod_abbreviation in ["WB", "ZFIN"]:
                new_referencefile_mod = {
                    "referencefile_id": test_referencefile.referencefile_id,  # Access referencefile_id
                    "mod_abbreviation": mod_abbreviation
                }
                client.post(url="/reference/referencefile_mod/", json=new_referencefile_mod, headers=auth_headers)

        for mod_abbreviation in ["FB"]:
            new_referencefile_mod = {
                "referencefile_id": test_referencefile2,  # Use test_referencefile2's referencefile_id
                "mod_abbreviation": mod_abbreviation
            }
            client.post(url="/reference/referencefile_mod/", json=new_referencefile_mod, headers=auth_headers)

            # Pass referencefile_id in the URL instead of the whole object
            request = client.post(url=f"/reference/referencefile/merge/{test_reference.new_ref_curie}/"
                                  f"{test_referencefile.referencefile_id}/{test_referencefile2}", headers=auth_headers)
            assert request.status_code == status.HTTP_201_CREATED

            # Get the updated referencefile and validate the response
            request = client.get(url=f"/reference/referencefile/{test_referencefile2}", headers=auth_headers)
            assert request.json()["display_name"] == "Bob"
            assert request.json()["reference_curie"] == test_reference.new_ref_curie
            assert len(request.json()["referencefile_mods"]) == 4

            mods_to_check = {None, "WB", "FB", "ZFIN"}
            for mod in request.json()["referencefile_mods"]:
                assert mod["mod_abbreviation"] in mods_to_check
                mods_to_check.remove(mod["mod_abbreviation"])
            assert len(mods_to_check) == 0

    def test_show_by_md5_no_match(self, db, test_referencefile, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get(url="/reference/referencefile/by_md5/nonexistentmd5",
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert response.json() == []

    def test_show_by_md5_single_match(self, db, test_referencefile, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get(
                url=f"/reference/referencefile/by_md5/{test_referencefile.md5sum}",
                headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            matches = response.json()
            assert len(matches) == 1
            match = matches[0]
            assert match["referencefile_id"] == test_referencefile.referencefile_id
            assert match["md5sum"] == test_referencefile.md5sum
            assert match["display_name"] == "Bob"
            assert match["file_class"] == "main"
            assert match["file_extension"] == "pdf"
            assert match["pdf_type"] == "pdf"
            assert match["reference_curie"] == test_referencefile.reference.curie
            assert match["reference_id"] == test_referencefile.reference_id
            # No copyright license attached → open_access defaults to False
            assert match["open_access"] is False
            assert match["copyright_license_name"] is None
            # No converted Markdown rows exist for this PDF
            assert match["converted_referencefiles"] == []

    def test_show_by_md5_multi_reference_match(self, db, test_referencefile, test_reference2, auth_headers):  # noqa
        populate_test_mods()
        shared_md5 = test_referencefile.md5sum
        second_file = {
            "display_name": "Bob",
            "reference_curie": test_reference2.new_ref_curie,
            "file_class": "main",
            "file_publication_status": "final",
            "file_extension": "pdf",
            "pdf_type": "pdf",
            "md5sum": shared_md5,
            "mod_abbreviation": "WB"
        }
        second_file_id = create_metadata(db, ReferencefileSchemaPost(**second_file))
        with TestClient(app) as client:
            response = client.get(
                url=f"/reference/referencefile/by_md5/{shared_md5}",
                headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            matches = response.json()
            assert len(matches) == 2
            ids = {m["referencefile_id"] for m in matches}
            assert ids == {test_referencefile.referencefile_id, second_file_id}
            curies = {m["reference_curie"] for m in matches}
            assert curies == {test_referencefile.reference.curie,
                              test_reference2.new_ref_curie}
            mods_for_second = next(
                m["referencefile_mods"] for m in matches
                if m["referencefile_id"] == second_file_id
            )
            assert any(rm["mod_abbreviation"] == "WB" for rm in mods_for_second)

    def test_show_by_md5_includes_converted_markdown(self, db, test_referencefile, auth_headers):  # noqa
        converted = {
            "display_name": f"{test_referencefile.display_name}_merged",
            "reference_curie": test_referencefile.reference.curie,
            "file_class": "converted_merged_main",
            "file_publication_status": "final",
            "file_extension": "md",
            "pdf_type": None,
            "md5sum": "9999999999"
        }
        converted_id = create_metadata(db, ReferencefileSchemaPost(**converted))
        unrelated = {
            "display_name": "Other_merged",
            "reference_curie": test_referencefile.reference.curie,
            "file_class": "converted_merged_main",
            "file_publication_status": "final",
            "file_extension": "md",
            "pdf_type": None,
            "md5sum": "8888888888"
        }
        create_metadata(db, ReferencefileSchemaPost(**unrelated))
        with TestClient(app) as client:
            response = client.get(
                url=f"/reference/referencefile/by_md5/{test_referencefile.md5sum}",
                headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            matches = response.json()
            assert len(matches) == 1
            derived = matches[0]["converted_referencefiles"]
            assert len(derived) == 1
            assert derived[0]["referencefile_id"] == converted_id
            assert derived[0]["file_class"] == "converted_merged_main"
            assert derived[0]["display_name"].startswith(test_referencefile.display_name)
