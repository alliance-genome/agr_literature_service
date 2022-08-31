import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import MeshDetailModel
from .fixtures import auth_headers, db # noqa
from .test_reference import create_test_reference # noqa

create_test_reference2 = create_test_reference


@pytest.fixture
def create_test_mesh_detail(db, auth_headers, create_test_reference): # noqa
    print("***** Adding a test mesh detail *****")
    with TestClient(app) as client:
        new_mesh_detail = {"reference_curie": create_test_reference.json(),
                           "heading_term": "Head1",
                           "qualifier_term": "Qual1"
                           }
        response = client.post(url="/reference/mesh_detail/", json=new_mesh_detail, headers=auth_headers)
        yield response, create_test_reference.json()


class TestMeshDetail:

    def test_get_bad_mesh_detail(self):
        with TestClient(app) as client:
            response = client.get(url="/reference/mesh_detail/-1")
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_create_mesh(self, create_test_mesh_detail): # noqa
        assert create_test_mesh_detail[0].status_code == status.HTTP_201_CREATED

    def test_show_mesh(self, db, create_test_mesh_detail): # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/reference/mesh_detail/{create_test_mesh_detail[0].json()}")
            res = response.json()
            assert res['heading_term'] == "Head1"
            assert res['qualifier_term'] == "Qual1"
            assert res['reference_curie'] == create_test_mesh_detail[1]

            # and in the db
            mesh_detail_obj = db.query(MeshDetailModel).filter(MeshDetailModel.heading_term == "Head1").one()
            assert mesh_detail_obj.reference.curie == create_test_mesh_detail[1]
            assert mesh_detail_obj.qualifier_term == "Qual1"

    def test_patch_mesh(self, db, create_test_mesh_detail, create_test_reference2, auth_headers): # noqa
        with TestClient(app) as client:
            patched = {"heading_term": "Head2",
                       "qualifier_term": "Qual2",
                       "reference_curie": create_test_reference2.json()
                       }
            response = client.patch(url=f"/reference/mesh_detail/{create_test_mesh_detail[0].json()}", json=patched,
                                    headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED
            mesh_detail_obj = db.query(MeshDetailModel).filter(
                MeshDetailModel.mesh_detail_id == create_test_mesh_detail[0].json()).one()
            assert mesh_detail_obj.heading_term == "Head2"
            assert mesh_detail_obj.reference.curie == create_test_reference2.json()
            assert mesh_detail_obj.qualifier_term == "Qual2"

            response = client.get(url=f"/reference/mesh_detail/{create_test_mesh_detail[0].json()}/versions")

            # reference_curie : None -> 1 -> 3
            # reference_id_from      : None -> orig -> new
            from_id = client.get(url=f"/reference/{create_test_mesh_detail[1]}").json()["reference_id"]
            # reference_id_to        : None -> new -> orig
            to_id = client.get(url=f"/reference/{create_test_reference2.json()}").json()["reference_id"]
            # heading_term            : None -> Head1 -> Head2
            # qualifier_term          : None -> Qual1 -> Qual2
            for transaction in response.json():
                if not transaction['changeset']['reference_id'][0]:
                    assert transaction['changeset']['reference_id'][1] == from_id
                    assert transaction['changeset']['heading_term'][1] == "Head1"
                    assert transaction['changeset']['qualifier_term'][1] == "Qual1"
                else:
                    assert transaction['changeset']['reference_id'][1] == to_id
                    assert transaction['changeset']['heading_term'][1] == "Head2"
                    assert transaction['changeset']['qualifier_term'][1] == "Qual2"

    def test_destroy_mesh_detail(self, create_test_mesh_detail, auth_headers): # noqa
        with TestClient(app) as client:
            response = client.delete(url=f"/reference/mesh_detail/{create_test_mesh_detail[0].json()}",
                                     headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT
            # It should now give an error on lookup.
            response = client.get(url=f"/reference/mesh_detail/{create_test_mesh_detail[0].json()}")
            assert response.status_code == status.HTTP_404_NOT_FOUND

            # Deleting it again should give an error as the lookup will fail.
            response = client.delete(url=f"/reference/mesh_detail{create_test_mesh_detail[0].json()}",
                                     headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND
