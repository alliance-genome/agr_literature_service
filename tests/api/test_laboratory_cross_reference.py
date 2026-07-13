# flake8: noqa: F811
from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import LaboratoryModel, LaboratoryCrossReferenceModel
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


XrefTestData = namedtuple(
    "XrefTestData",
    ["response", "new_id", "laboratory_id"],
)


@pytest.fixture
def seeded_laboratory(db):  # noqa
    lab = LaboratoryModel(curie="AGRKB:104test-xref", name="Xref Lab", status="active", lab_is_open=False)
    db.add(lab)
    db.commit()
    db.refresh(lab)
    return {"laboratory_id": lab.laboratory_id}


@pytest.fixture
def test_xref(db, auth_headers, seeded_laboratory):  # noqa
    with TestClient(app) as client:
        response = client.post(
            "/laboratory_cross_reference/",
            json={"laboratory_curie": str(seeded_laboratory["laboratory_id"]), "curie": "WB:WBlab9001"},
            headers=auth_headers,
        )
        body = response.json() if response.status_code == status.HTTP_201_CREATED else {}
        yield XrefTestData(
            response=response,
            new_id=body.get("laboratory_cross_reference_id"),
            laboratory_id=seeded_laboratory["laboratory_id"],
        )


class TestLaboratoryCrossReference:

    def test_create_xref(self, db, test_xref):  # noqa
        assert test_xref.response.status_code == status.HTTP_201_CREATED
        obj = (
            db.query(LaboratoryCrossReferenceModel)
            .filter(LaboratoryCrossReferenceModel.laboratory_cross_reference_id == test_xref.new_id)
            .one()
        )
        assert obj.curie == "WB:WBlab9001"
        assert obj.curie_prefix == "WB"

    def test_create_for_invalid_laboratory(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/laboratory_cross_reference/",
                json={"laboratory_curie": "9999999", "curie": "WB:WBlab1"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_duplicate_curie_rejected(self, auth_headers, test_xref):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/laboratory_cross_reference/",
                json={"laboratory_curie": str(test_xref.laboratory_id), "curie": "WB:WBlab9001"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_duplicate_prefix_rejected(self, auth_headers, test_xref):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/laboratory_cross_reference/",
                json={"laboratory_curie": str(test_xref.laboratory_id), "curie": "WB:WBlab9002"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_bad_curie_rejected(self, auth_headers, seeded_laboratory):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/laboratory_cross_reference/",
                json={"laboratory_curie": str(seeded_laboratory["laboratory_id"]), "curie": "no-colon"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_get_laboratory_by_cross_reference(self, auth_headers, test_xref):  # noqa
        with TestClient(app) as client:
            # by curie
            res = client.get(
                "/laboratory/by_laboratory_cross_reference/WB:WBlab9001",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            assert res.json()["laboratory_id"] == test_xref.laboratory_id
            # by cross-reference internal id
            res = client.get(
                f"/laboratory/by_laboratory_cross_reference/{test_xref.new_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            assert res.json()["laboratory_id"] == test_xref.laboratory_id

    def test_get_laboratory_by_unknown_cross_reference(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.get(
                "/laboratory/by_laboratory_cross_reference/WB:does-not-exist",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_list_for_laboratory(self, auth_headers, test_xref):  # noqa
        with TestClient(app) as client:
            res = client.get(
                f"/laboratory_cross_reference/laboratory/{test_xref.laboratory_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            rows = res.json()
            assert any(r["laboratory_cross_reference_id"] == test_xref.new_id for r in rows)

    def test_patch_xref(self, auth_headers, test_xref):  # noqa
        with TestClient(app) as client:
            res = client.patch(
                f"/laboratory_cross_reference/{test_xref.new_id}",
                json={"is_obsolete": True},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            assert res.json()["is_obsolete"] is True

    def test_destroy_xref(self, auth_headers, test_xref):  # noqa
        with TestClient(app) as client:
            res = client.delete(
                f"/laboratory_cross_reference/{test_xref.new_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_204_NO_CONTENT
            res = client.get(
                f"/laboratory_cross_reference/{test_xref.new_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_404_NOT_FOUND
