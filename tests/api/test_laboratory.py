# flake8: noqa: F811
from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import LaboratoryModel
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


LaboratoryTestData = namedtuple(
    "LaboratoryTestData",
    ["response", "new_laboratory_id", "curie"],
)


@pytest.fixture
def test_laboratory(db, auth_headers):  # noqa
    with TestClient(app) as client:
        payload = {
            "name": "Test Lab",
            "strain_designation": "TS1",
            "institution": ["Caltech", "MIT"],
            "webpage": ["http://example.org"],
            "email": ["lab@example.org"],
        }
        response = client.post("/laboratory/", json=payload, headers=auth_headers)
        body = response.json() if response.status_code == status.HTTP_201_CREATED else {}
        yield LaboratoryTestData(
            response=response,
            new_laboratory_id=body.get("laboratory_id"),
            curie=body.get("curie"),
        )


class TestLaboratory:

    def test_get_bad_laboratory(self, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get("/laboratory/-1", headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_create_laboratory(self, db, test_laboratory):  # noqa
        assert test_laboratory.response.status_code == status.HTTP_201_CREATED
        lab = (
            db.query(LaboratoryModel)
            .filter(LaboratoryModel.laboratory_id == test_laboratory.new_laboratory_id)
            .one()
        )
        assert lab.name == "Test Lab"
        assert lab.institution == ["Caltech", "MIT"]
        assert lab.email == ["lab@example.org"]
        # defaults
        assert lab.status == "active"
        assert lab.lab_is_open is False
        assert lab.email_visibility == "not_shown"

    def test_curie_derived_from_id(self, test_laboratory):  # noqa
        expected = f"AGRKB:705{test_laboratory.new_laboratory_id:012d}"
        assert test_laboratory.curie == expected

    def test_lookup_by_curie(self, auth_headers, test_laboratory):  # noqa
        with TestClient(app) as client:
            res = client.get(f"/laboratory/{test_laboratory.curie}", headers=auth_headers)
            assert res.status_code == status.HTTP_200_OK
            assert res.json()["laboratory_id"] == test_laboratory.new_laboratory_id

    def test_at_least_one_field_required(self, auth_headers):  # noqa
        """A laboratory created with only defaulted fields is rejected."""
        with TestClient(app) as client:
            res = client.post("/laboratory/", json={"status": "active"}, headers=auth_headers)
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            res = client.post("/laboratory/", json={}, headers=auth_headers)
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_bad_status_rejected(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/laboratory/",
                json={"name": "X", "status": "nonsense"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_bad_email_visibility_rejected(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/laboratory/",
                json={"name": "X", "email_visibility": "everyone"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_show_laboratory(self, test_laboratory, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get(
                f"/laboratory/{test_laboratory.new_laboratory_id}",
                headers=auth_headers,
            )
            assert response.status_code == status.HTTP_200_OK
            body = response.json()
            assert body["laboratory_id"] == test_laboratory.new_laboratory_id
            assert body["webpage"] == ["http://example.org"]

    def test_patch_laboratory(self, auth_headers, test_laboratory):  # noqa
        with TestClient(app) as client:
            res = client.patch(
                f"/laboratory/{test_laboratory.new_laboratory_id}",
                json={"name": "Renamed Lab", "status": "closed", "lab_is_open": True},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            body = res.json()
            assert body["name"] == "Renamed Lab"
            assert body["status"] == "closed"
            assert body["lab_is_open"] is True

    def test_patch_bad_vocab_rejected(self, auth_headers, test_laboratory):  # noqa
        with TestClient(app) as client:
            res = client.patch(
                f"/laboratory/{test_laboratory.new_laboratory_id}",
                json={"status": "bogus"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_destroy_laboratory(self, test_laboratory, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.delete(
                f"/laboratory/{test_laboratory.new_laboratory_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_204_NO_CONTENT

            res = client.get(
                f"/laboratory/{test_laboratory.new_laboratory_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_create_with_cross_references(self, auth_headers):  # noqa
        with TestClient(app) as client:
            payload = {
                "name": "Lab with xrefs",
                "cross_references": [{"curie": "WB:WBlab0001"}],
            }
            res = client.post("/laboratory/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            xrefs = res.json()["cross_references"]
            assert len(xrefs) == 1
            assert xrefs[0]["curie"] == "WB:WBlab0001"
            assert xrefs[0]["curie_prefix"] == "WB"
