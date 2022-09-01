from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status, HTTPException

from agr_literature_service.api.crud.mod_crud import destroy as destroy_mod, show as show_mod
from agr_literature_service.api.main import app
from agr_literature_service.api.models import ModModel
from .fixtures import auth_headers, db # noqa

TestModData = namedtuple('TestMod', ['response', 'new_mod_abbreviation'])


@pytest.fixture
def create_test_mod(db, auth_headers): # noqa
    print("***** Adding a test mod *****")
    with TestClient(app) as client:
        new_mod = {
            "abbreviation": "0015_AtDB",
            "short_name": "AtDB",
            "full_name": "Test genome database"
        }
        response = client.post(url="/mod/", json=new_mod, headers=auth_headers)
        yield TestModData(response, new_mod["abbreviation"])


class TestMod:

    def test_get_bad_mod(self):
        with TestClient(app) as client:
            response = client.get(url="/mod/does_not_exist")
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_create_mod(self, db, create_test_mod):
        assert create_test_mod.response.status_code == status.HTTP_201_CREATED
        mod = db.query(ModModel).filter_by(abbreviation=create_test_mod[1]).one()
        assert mod.short_name == "AtDB"
        assert mod.full_name == "Test genome database"

    def test_patch_mod(self, create_test_mod, auth_headers):
        with TestClient(app) as client:
            patched_data = {"abbreviation": "0015_AtDB",
                            "short_name": "AtDB2",
                            "full_name": "Test genome database2"
                            }
            response = client.patch(url=f"/mod/{create_test_mod.response.json()}", json=patched_data, headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED
            res = client.get(url=f"/mod/{create_test_mod.new_mod_abbreviation}").json()
            assert res["full_name"] == "Test genome database2"
            transactions = client.get(url=f"/mod/{create_test_mod.response.json()}/versions").json()
            assert transactions[0]["changeset"]["full_name"][1] == "Test genome database"
            assert transactions[1]["changeset"]["full_name"][0] == "Test genome database"
            assert transactions[1]["changeset"]["full_name"][1] == "Test genome database2"

    def test_show_mod(self, create_test_mod):
        with TestClient(app) as client:
            response = client.get(url=f"/mod/{create_test_mod.new_mod_abbreviation}")
            assert response.status_code == status.HTTP_200_OK
            assert response.json()["full_name"] == "Test genome database"

    def test_destroy_mod(self, db, create_test_mod):
        mod = db.query(ModModel).filter_by(abbreviation="0015_AtDB").one()
        destroy_mod(db, mod.mod_id)

        # it should now give an error on lookup.
        with pytest.raises(HTTPException):
            show_mod(db, mod.abbreviation)

        # deleting it again should give an error as the lookup will fail.
        with pytest.raises(HTTPException):
            destroy_mod(db, mod.mod_id)
