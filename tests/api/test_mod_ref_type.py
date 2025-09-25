from collections import namedtuple

import pytest
from sqlalchemy import text
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.crud.mod_reference_type_crud import insert_mod_reference_type_into_db
from agr_literature_service.api.main import app
from agr_literature_service.api.models import ReferenceModReferencetypeAssociationModel, ReferenceModel, \
    ModReferencetypeAssociationModel, ModModel
from ..fixtures import db, populate_test_mod_reference_types # noqa
from .fixtures import auth_headers # noqa
from .test_reference import test_reference # noqa

test_reference2 = test_reference

ModRefTypeTestData = namedtuple('ModRefTypeTestData', ['response', 'new_mod_ref_type_id', 'related_ref_curie'])


@pytest.fixture
def test_mod_ref_type(db, auth_headers, test_reference, populate_test_mod_reference_types): # noqa
    print("***** Adding a test mod reference type *****")
    with TestClient(app) as client:
        new_mod_ref_type = {
            "reference_curie": test_reference.new_ref_curie,
            "reference_type": "Journal",
            "mod_abbreviation": "ZFIN"
        }
        response = client.post(url="/reference/mod_reference_type/", json=new_mod_ref_type, headers=auth_headers)
        yield ModRefTypeTestData(response, response.json(), test_reference.new_ref_curie)


class TestModReferenceType:

    def test_get_bad_mrt(self):
        with TestClient(app) as client:
            response = client.get(url="/reference/mod_reference_type/-1")
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_create_mrt(self, db, test_mod_ref_type): # noqa
        assert test_mod_ref_type.response.status_code == status.HTTP_201_CREATED
        # check db for mrt
        rmrt = db.query(ReferenceModReferencetypeAssociationModel).filter(
            ReferenceModReferencetypeAssociationModel.reference_mod_referencetype_id == test_mod_ref_type
            .new_mod_ref_type_id).one()
        assert rmrt.mod_referencetype.referencetype.label == "Journal"
        ref_curie = db.query(ReferenceModel.curie).filter(
            ReferenceModel.reference_id == rmrt.reference_id).one_or_none()[0]
        assert ref_curie == test_mod_ref_type.related_ref_curie
        assert rmrt.mod_referencetype.mod.abbreviation == "ZFIN"

    def test_patch_mrt(self, db, test_mod_ref_type, test_reference2, auth_headers): # noqa
        with TestClient(app) as client:
            patch_data = {
                "reference_curie": test_reference2.new_ref_curie,
                "reference_type": "Review",
                "mod_abbreviation": "ZFIN"
            }
            response = client.patch(url=f"/reference/mod_reference_type/{test_mod_ref_type.new_mod_ref_type_id}",
                                    json=patch_data, headers=auth_headers)
            # check db for mrt
            assert response.status_code == status.HTTP_202_ACCEPTED
            response = client.get(url=f"/reference/mod_reference_type/{test_mod_ref_type.new_mod_ref_type_id}")
            mrt = response.json()
            assert mrt["reference_type"] == "Review"
            assert mrt["reference_curie"] == test_reference2.new_ref_curie
            assert mrt["mod_abbreviation"] == "ZFIN"

            from_id = client.get(url=f"/reference/{test_mod_ref_type.related_ref_curie}").json()["reference_id"]
            to_id = client.get(url=f"/reference/{test_reference2.new_ref_curie}").json()["reference_id"]

            response = client.get(url=f"/reference/mod_reference_type/{test_mod_ref_type.new_mod_ref_type_id}/versions")
            transactions = response.json()
            assert transactions[0]['changeset']['reference_id'][1] == from_id
            mod_referencetype_id_orig = db.execute(text("select mod_referencetype_id from mod_referencetype where mod_id = "
                                                        "(select mod_id from mod where abbreviation = 'ZFIN') and "
                                                        "referencetype_id = (select referencetype_id from referencetype "
                                                        "where label = 'Journal')")).first()[0]
            assert transactions[0]['changeset']['mod_referencetype_id'][1] == mod_referencetype_id_orig
            assert transactions[1]['changeset']['reference_id'][1] == to_id
            mod_referencetype_id_new = db.execute(text("select mod_referencetype_id from mod_referencetype where mod_id = "
                                                       "(select mod_id from mod where abbreviation = 'ZFIN') and "
                                                       "referencetype_id = (select referencetype_id from referencetype "
                                                       "where label = 'Review')")).first()[0]
            assert transactions[1]['changeset']['mod_referencetype_id'][1] == mod_referencetype_id_new

    # NOTE: BAD... recursion error. NEEDS fixing.
    def test_show_mrt(self, test_mod_ref_type): # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/reference/mod_reference_type/{test_mod_ref_type.new_mod_ref_type_id}")
            assert response.status_code == status.HTTP_200_OK

    def test_destroy_mrt(self, test_mod_ref_type, auth_headers): # noqa
        with TestClient(app) as client:
            response = client.delete(url=f"/reference/mod_reference_type/{test_mod_ref_type.new_mod_ref_type_id}",
                                     headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT
            # It should now give an error on lookup.
            response = client.get(url=f"/reference/mod_reference_type/{test_mod_ref_type.new_mod_ref_type_id}")
            assert response.status_code == status.HTTP_404_NOT_FOUND

            # Deleting it again should give an error as the lookup will fail.
            response = client.delete(url=f"/reference/mod_reference_type/{test_mod_ref_type.new_mod_ref_type_id}",
                                     headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_display_order(self, db, test_mod_ref_type, auth_headers): # noqa
        # mod_id = db.query(ModModel.mod_id).filter(ModModel.abbreviation == "ZFIN").one_or_none()
        mod_id = db.query(ModModel.mod_id).filter(ModModel.abbreviation == "ZFIN").scalar()

        mrts = db.query(ModReferencetypeAssociationModel).filter(
            ModReferencetypeAssociationModel.mod_id == mod_id).all()
        for idx, mrt in enumerate(mrts):
            assert mrt.display_order == (idx + 1) * 10

        reference_id = db.query(ReferenceModel.reference_id).filter(
            ReferenceModel.curie == test_mod_ref_type.related_ref_curie).scalar()

        allowed_pubmed_types = ("test1", "test2")

        insert_mod_reference_type_into_db(
            db, pubmed_types=allowed_pubmed_types, mod_abbreviation="SGD", referencetype_label="test1",
            reference_id=reference_id)

        new_ref_mod_reftype_id = insert_mod_reference_type_into_db(
            db, pubmed_types=allowed_pubmed_types, mod_abbreviation="SGD", referencetype_label="test2",
            reference_id=reference_id)

        new_ref_mod_reftype = db.query(ReferenceModReferencetypeAssociationModel).filter(
            ReferenceModReferencetypeAssociationModel.reference_mod_referencetype_id == new_ref_mod_reftype_id).one()
        assert new_ref_mod_reftype.mod_referencetype.display_order == 30
