from collections import namedtuple

import pytest
from starlette.testclient import TestClient

from agr_literature_service.api.main import app
from fastapi import status

from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from .test_referencefile import test_referencefile # noqa
from .test_reference import test_reference # noqa
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa


TestReferencefileModData = namedtuple('TestReferencefileModData', ['response', 'new_referencefile_mod_id'])


@pytest.fixture
def test_referencefile_mod(db, auth_headers, test_referencefile): # noqa
    print("***** Adding a test referencefile_mod *****")
    populate_test_mods()
    with TestClient(app) as client:
        new_referencefile_mod = {
            "referencefile_id": test_referencefile.referencefile_id,
            "mod_abbreviation": "WB"
        }
        response = client.post(url="/reference/referencefile_mod/", json=new_referencefile_mod, headers=auth_headers)
        yield TestReferencefileModData(response, response.json())


class TestReferencefileMod:

    def test_create_referencefile_mod(self, test_referencefile_mod): # noqa
        assert test_referencefile_mod.response.status_code == status.HTTP_201_CREATED

    def test_show_referencefile_mod(self, test_referencefile_mod):
        with TestClient(app) as client:
            response = client.get(url=f"/reference/referencefile_mod/{test_referencefile_mod.new_referencefile_mod_id}")
            assert response.status_code == status.HTTP_200_OK

    def test_patch_referencefile_mod(self, db, test_referencefile_mod, auth_headers): # noqa
        patch_referencefile_mod = {
            "mod_abbreviation": "FB"
        }
        with TestClient(app) as client:
            response = client.patch(url=f"/reference/referencefile_mod/"
                                        f"{test_referencefile_mod.new_referencefile_mod_id}",
                                    json=patch_referencefile_mod, headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED
            response = client.get(url=f"/reference/referencefile_mod/{test_referencefile_mod.new_referencefile_mod_id}")
            assert response.json()["mod_abbreviation"] == patch_referencefile_mod["mod_abbreviation"]

    def test_destroy_referencefile_mod(self, test_referencefile_mod, auth_headers): # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/reference/referencefile_mod/{test_referencefile_mod.new_referencefile_mod_id}")
            referencefile_id = response.json()['referencefile_id']
            response = client.delete(url=f"/reference/referencefile_mod/"
                                         f"{test_referencefile_mod.new_referencefile_mod_id}", headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT
            response = client.get(url=f"/reference/referencefile_mod/{test_referencefile_mod.new_referencefile_mod_id}")
            assert response.status_code == status.HTTP_404_NOT_FOUND
            response_file = client.get(url=f"/reference/referencefile/{referencefile_id}")
            for referencefile_mod in response_file.json()["referencefile_mods"]:
                referencefile_mod_id = referencefile_mod["referencefile_mod_id"]
                response = client.delete(url=f"/reference/referencefile_mod/"
                                             f"{referencefile_mod_id}", headers=auth_headers)
                assert response.status_code == status.HTTP_204_NO_CONTENT
            response_file = client.get(url=f"/reference/referencefile/{referencefile_id}")
            assert response_file.status_code == status.HTTP_404_NOT_FOUND

    def test_show_reference_referencefiles_referencefile_mods(self, db, test_referencefile_mod): # noqa
        with TestClient(app) as client:
            response_file_mod = client.get(url=f"/reference/referencefile_mod/"
                                           f"{test_referencefile_mod.new_referencefile_mod_id}")
            response_file = client.get(url=f"/reference/referencefile/{response_file_mod.json()['referencefile_id']}")
            response = client.get(url=f"/reference/referencefile/show_all/{response_file.json()['reference_curie']}")
            print(response)
            assert response.status_code == status.HTTP_200_OK
            ok = False
            for mod in response.json():

                if mod["referencefile_mods"][1]["mod_abbreviation"] == "WB":
                    ok = True
            assert ok

    def test_add_referencefile_to_mod(self, test_referencefile_mod, auth_headers): # noqa
        with TestClient(app) as client:
            response_file_mod = client.get(url=f"/reference/referencefile_mod/"
                                               f"{test_referencefile_mod.new_referencefile_mod_id}")
            new_referencefile_mod = {
                "referencefile_id": int(response_file_mod.json()['referencefile_id']),
                "mod_abbreviation": "FB"
            }
            response = client.post(url="/reference/referencefile_mod/", json=new_referencefile_mod,
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
