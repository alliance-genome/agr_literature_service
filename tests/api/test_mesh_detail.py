from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import MeshDetailModel
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from .test_reference import test_reference # noqa

test_reference2 = test_reference

MeshTestData = namedtuple('TestMeshData', ['response', 'new_mesh_detail_id', 'related_ref_curie'])


@pytest.fixture
def test_mesh_detail(db, auth_headers, test_reference): # noqa
    print("***** Adding a test mesh detail *****")
    with TestClient(app) as client:
        new_mesh_detail = {"reference_curie": test_reference.new_ref_curie,
                           "heading_term": "Head1",
                           "qualifier_term": "Qual1"
                           }
        response = client.post(url="/reference/mesh_detail/", json=new_mesh_detail, headers=auth_headers)
        yield MeshTestData(response, response.json(), test_reference.new_ref_curie)


class TestMeshDetail:

    def test_get_bad_mesh_detail(self):
        with TestClient(app) as client:
            response = client.get(url="/reference/mesh_detail/-1")
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_create_mesh(self, test_mesh_detail): # noqa
        assert test_mesh_detail.response.status_code == status.HTTP_201_CREATED

    def test_show_mesh(self, db, test_mesh_detail): # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/reference/mesh_detail/{test_mesh_detail.new_mesh_detail_id}")
            res = response.json()
            assert res['heading_term'] == "Head1"
            assert res['qualifier_term'] == "Qual1"
            assert res['reference_curie'] == test_mesh_detail.related_ref_curie

            # and in the db
            mesh_detail_obj = db.query(MeshDetailModel).filter(MeshDetailModel.heading_term == "Head1").one()
            assert mesh_detail_obj.reference.curie == test_mesh_detail.related_ref_curie
            assert mesh_detail_obj.qualifier_term == "Qual1"

    def test_patch_mesh(self, db, test_mesh_detail, test_reference2, auth_headers): # noqa
        with TestClient(app) as client:
            patched = {"heading_term": "Head2",
                       "qualifier_term": "Qual2",
                       "reference_curie": test_reference2.new_ref_curie
                       }
            response = client.patch(url=f"/reference/mesh_detail/{test_mesh_detail.new_mesh_detail_id}",
                                    json=patched, headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED
            mesh_detail_obj = db.query(MeshDetailModel).filter(
                MeshDetailModel.mesh_detail_id == test_mesh_detail.new_mesh_detail_id).one()
            assert mesh_detail_obj.heading_term == "Head2"
            assert mesh_detail_obj.reference.curie == test_reference2.new_ref_curie
            assert mesh_detail_obj.qualifier_term == "Qual2"


    def test_destroy_mesh_detail(self, test_mesh_detail, auth_headers): # noqa
        with TestClient(app) as client:
            response = client.delete(url=f"/reference/mesh_detail/{test_mesh_detail.new_mesh_detail_id}",
                                     headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT
            # It should now give an error on lookup.
            response = client.get(url=f"/reference/mesh_detail/{test_mesh_detail.new_mesh_detail_id}")
            assert response.status_code == status.HTTP_404_NOT_FOUND

            # Deleting it again should give an error as the lookup will fail.
            response = client.delete(url=f"/reference/mesh_detail{test_mesh_detail.new_mesh_detail_id}",
                                     headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND
