from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import ModReferenceTypeModel
from .fixtures import auth_headers, db # noqa
from .test_reference import test_reference # noqa

test_reference2 = test_reference

TestModRefTypeData = namedtuple('TestModRefTypeData', ['response', 'new_mod_ref_type_id', 'related_ref_curie'])


@pytest.fixture
def test_mod_ref_type(db, auth_headers, test_reference): # noqa
    print("***** Adding a test mod reference type *****")
    with TestClient(app) as client:
        new_mod_ref_type = {
            "reference_curie": test_reference.new_ref_curie,
            "reference_type": "string1",
            "source": "string2"
        }
        response = client.post(url="/reference/mod_reference_type/", json=new_mod_ref_type, headers=auth_headers)
        yield TestModRefTypeData(response, response.json(), test_reference.new_ref_curie)


class TestModReferenceType:

    def test_get_bad_mrt(self):
        with TestClient(app) as client:
            response = client.get(url="/reference/mod_reference_type/-1")
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_create_mrt(self, db, test_mod_ref_type): # noqa
        assert test_mod_ref_type.response.status_code == status.HTTP_201_CREATED
        # check db for mrt
        mrt = db.query(ModReferenceTypeModel).filter(
            ModReferenceTypeModel.mod_reference_type_id == test_mod_ref_type.new_mod_ref_type_id).one()
        assert mrt.reference_type == "string1"
        assert mrt.reference.curie == test_mod_ref_type.related_ref_curie
        assert mrt.source == "string2"

    def test_patch_mrt(self, test_mod_ref_type, test_reference2, auth_headers): # noqa
        with TestClient(app) as client:
            patch_data = {
                "reference_curie": test_reference2.new_ref_curie,
                "reference_type": "string3",
                "source": "string4"
            }
            response = client.patch(url=f"/reference/mod_reference_type/{test_mod_ref_type.new_mod_ref_type_id}",
                                    json=patch_data, headers=auth_headers)
            # check db for mrt
            assert response.status_code == status.HTTP_202_ACCEPTED
            response = client.get(url=f"/reference/mod_reference_type/{test_mod_ref_type.new_mod_ref_type_id}")
            mrt = response.json()
            assert mrt["reference_type"] == "string3"
            assert mrt["reference_curie"] == test_reference2.new_ref_curie
            assert mrt["source"] == "string4"

            from_id = client.get(url=f"/reference/{test_mod_ref_type.related_ref_curie}").json()["reference_id"]
            to_id = client.get(url=f"/reference/{test_reference2.new_ref_curie}").json()["reference_id"]

            response = client.get(url=f"/reference/mod_reference_type/{test_mod_ref_type.new_mod_ref_type_id}/versions")
            transactions = response.json()
            assert transactions[0]['changeset']['reference_id'][1] == from_id
            assert transactions[0]['changeset']['reference_type'][1] == "string1"
            assert transactions[0]['changeset']['source'][1] == "string2"
            assert transactions[1]['changeset']['reference_id'][1] == to_id
            assert transactions[1]['changeset']['reference_type'][1] == "string3"
            assert transactions[1]['changeset']['source'][1] == "string4"

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
