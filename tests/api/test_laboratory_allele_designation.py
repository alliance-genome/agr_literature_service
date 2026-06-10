# flake8: noqa: F811
from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import (
    LaboratoryModel,
    LaboratoryAlleleDesignationModel,
    ModModel,
)
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


AlleleTestData = namedtuple(
    "AlleleTestData",
    ["response", "new_id", "laboratory_id", "mod_abbreviation"],
)


@pytest.fixture
def seeded_lab_and_mod(db):  # noqa
    lab = LaboratoryModel(name="Allele Lab", status="active", lab_is_open=False)
    db.add(lab)
    mod = db.query(ModModel).filter(ModModel.abbreviation == "WB").one_or_none()
    if mod is None:
        mod = ModModel(abbreviation="WB", short_name="WB", full_name="WormBase")
        db.add(mod)
    db.commit()
    db.refresh(lab)
    return {"laboratory_id": lab.laboratory_id, "mod_abbreviation": "WB"}


@pytest.fixture
def test_allele(db, auth_headers, seeded_lab_and_mod):  # noqa
    with TestClient(app) as client:
        response = client.post(
            f"/laboratory_allele_designation/laboratory/{seeded_lab_and_mod['laboratory_id']}",
            json={"mod_abbreviation": "WB", "allele_designation": "e"},
            headers=auth_headers,
        )
        body = response.json() if response.status_code == status.HTTP_201_CREATED else {}
        yield AlleleTestData(
            response=response,
            new_id=body.get("laboratory_allele_designation_id"),
            laboratory_id=seeded_lab_and_mod["laboratory_id"],
            mod_abbreviation="WB",
        )


class TestLaboratoryAlleleDesignation:

    def test_create_allele(self, db, test_allele):  # noqa
        assert test_allele.response.status_code == status.HTTP_201_CREATED
        obj = (
            db.query(LaboratoryAlleleDesignationModel)
            .filter(
                LaboratoryAlleleDesignationModel.laboratory_allele_designation_id == test_allele.new_id
            )
            .one()
        )
        assert obj.allele_designation == "e"
        # mod_abbreviation resolved to a real mod_id
        assert obj.mod_id is not None
        assert test_allele.response.json()["mod_abbreviation"] == "WB"

    def test_unknown_mod_abbreviation(self, auth_headers, seeded_lab_and_mod):  # noqa
        with TestClient(app) as client:
            res = client.post(
                f"/laboratory_allele_designation/laboratory/{seeded_lab_and_mod['laboratory_id']}",
                json={"mod_abbreviation": "NOPE", "allele_designation": "x"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_duplicate_lab_mod_rejected(self, auth_headers, test_allele):  # noqa
        with TestClient(app) as client:
            res = client.post(
                f"/laboratory_allele_designation/laboratory/{test_allele.laboratory_id}",
                json={"mod_abbreviation": "WB", "allele_designation": "other"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_show_and_patch(self, auth_headers, test_allele):  # noqa
        with TestClient(app) as client:
            res = client.get(
                f"/laboratory_allele_designation/{test_allele.new_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK

            res = client.patch(
                f"/laboratory_allele_designation/{test_allele.new_id}",
                json={"allele_designation": "ce"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            assert res.json()["allele_designation"] == "ce"

    def test_destroy(self, auth_headers, test_allele):  # noqa
        with TestClient(app) as client:
            res = client.delete(
                f"/laboratory_allele_designation/{test_allele.new_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_204_NO_CONTENT
