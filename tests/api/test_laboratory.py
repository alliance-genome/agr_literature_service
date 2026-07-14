# flake8: noqa: F811
from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import (
    LaboratoryModel,
    LaboratoryCrossReferenceModel,
    LaboratoryAlleleDesignationModel,
    ModModel,
)
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

    def test_curie_from_mati(self, test_laboratory):  # noqa
        # Curie is allocated via the MATI path (like reference/resource/person). In
        # tests the local fallback is used, prefixing AGRKB:104 with a zero-padded
        # 12-digit counter.
        curie = test_laboratory.curie
        assert curie.startswith("AGRKB:104")
        suffix = curie[len("AGRKB:104"):]
        assert len(suffix) == 12 and suffix.isdigit()

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

    def test_name_or_strain_required(self, db, auth_headers):  # noqa
        """A laboratory with a substantive field but no name/strain_designation is
        rejected by the ck_laboratory_name_or_strain check constraint (surfaced as
        422 by the CRUD layer)."""
        with TestClient(app) as client:
            res = client.post(
                "/laboratory/",
                json={"institution": ["Caltech"]},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_patch_clearing_name_and_strain_rejected(self, auth_headers, test_laboratory):  # noqa
        """Clearing both name and strain_designation violates
        ck_laboratory_name_or_strain; surfaced as 422, not a 500."""
        with TestClient(app) as client:
            res = client.patch(
                f"/laboratory/{test_laboratory.new_laboratory_id}",
                json={"name": None, "strain_designation": None},
                headers=auth_headers,
            )
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

    def test_create_with_inline_allele_designations(self, db, auth_headers):  # noqa
        mod = db.query(ModModel).filter(ModModel.abbreviation == "WB").one_or_none()
        if mod is None:
            db.add(ModModel(abbreviation="WB", short_name="WB", full_name="WormBase"))
            db.commit()
        with TestClient(app) as client:
            payload = {
                "name": "Lab with alleles",
                "allele_designations": [{"mod_abbreviation": "WB", "allele_designation": "e"}],
            }
            res = client.post("/laboratory/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            alleles = res.json()["allele_designations"]
            assert len(alleles) == 1
            assert alleles[0]["allele_designation"] == "e"
            assert alleles[0]["mod_abbreviation"] == "WB"

    def test_inline_invalid_mod_abbreviation_is_atomic(self, db, auth_headers):  # noqa
        # An invalid mod_abbreviation fails the whole request: no laboratory,
        # cross-reference, or allele is created.
        with TestClient(app) as client:
            payload = {
                "name": "Atomic Fail Lab",
                "cross_references": [{"curie": "WB:WBlabATOMIC"}],
                "allele_designations": [{"mod_abbreviation": "NOPE", "allele_designation": "x"}],
            }
            res = client.post("/laboratory/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_404_NOT_FOUND

        assert db.query(LaboratoryModel).filter(LaboratoryModel.name == "Atomic Fail Lab").count() == 0
        assert (
            db.query(LaboratoryCrossReferenceModel)
            .filter(LaboratoryCrossReferenceModel.curie == "WB:WBlabATOMIC")
            .count()
        ) == 0
        assert (
            db.query(LaboratoryAlleleDesignationModel)
            .filter(LaboratoryAlleleDesignationModel.allele_designation == "x")
            .count()
        ) == 0

    def test_find_by_strain_designation_exact(self, auth_headers):  # noqa
        with TestClient(app) as client:
            client.post(
                "/laboratory/",
                json={"name": "Strain Exact Lab ZZQ1", "strain_designation": "QX"},
                headers=auth_headers,
            )
            res = client.get(
                "/laboratory/by_strain_designation", params={"query": "QX"}, headers=auth_headers
            )
            assert res.status_code == status.HTTP_200_OK
            rows = res.json()
            assert len(rows) == 1
            assert rows[0]["strain_designation"] == "QX"
            # case-insensitive
            res2 = client.get(
                "/laboratory/by_strain_designation", params={"query": "qx"}, headers=auth_headers
            )
            assert len(res2.json()) == 1

    def test_find_by_strain_shared_returns_list(self, auth_headers):  # noqa
        # A strain code shared by >1 lab returns all of them (a pick-list).
        with TestClient(app) as client:
            for nm in ("Shared Strain A ZZQ", "Shared Strain B ZZQ"):
                client.post(
                    "/laboratory/",
                    json={"name": nm, "strain_designation": "SHX"},
                    headers=auth_headers,
                )
            res = client.get(
                "/laboratory/by_strain_designation", params={"query": "SHX"}, headers=auth_headers
            )
            assert res.status_code == status.HTTP_200_OK
            assert len(res.json()) == 2

    def test_find_by_name_substring_single_and_multiple(self, auth_headers):  # noqa
        with TestClient(app) as client:
            client.post("/laboratory/", json={"name": "Unique Kappa Lab UNIQK"}, headers=auth_headers)
            res = client.get(
                "/laboratory/by_name", params={"query": "UNIQK"}, headers=auth_headers
            )
            assert len(res.json()) == 1

            client.post("/laboratory/", json={"name": "Multi MMTOKEN One"}, headers=auth_headers)
            client.post("/laboratory/", json={"name": "Multi MMTOKEN Two"}, headers=auth_headers)
            res2 = client.get(
                "/laboratory/by_name", params={"query": "MMTOKEN"}, headers=auth_headers
            )
            assert len(res2.json()) == 2

    def test_name_and_strain_endpoints_are_separate(self, auth_headers):  # noqa
        # One lab has the query as its strain code (name does NOT contain it);
        # another merely contains it in its name. The two endpoints keep the
        # lookups cleanly separated: by_strain_designation returns only the
        # exact-strain lab, by_name returns only the name-containing lab.
        with TestClient(app) as client:
            client.post(
                "/laboratory/",
                json={"name": "Has strain code only", "strain_designation": "SCQ"},
                headers=auth_headers,
            )
            client.post("/laboratory/", json={"name": "Name contains SCQ here"}, headers=auth_headers)

            strain_res = client.get(
                "/laboratory/by_strain_designation", params={"query": "SCQ"}, headers=auth_headers
            )
            strain_rows = strain_res.json()
            assert len(strain_rows) == 1
            assert strain_rows[0]["strain_designation"] == "SCQ"

            name_res = client.get(
                "/laboratory/by_name", params={"query": "SCQ"}, headers=auth_headers
            )
            name_rows = name_res.json()
            assert len(name_rows) == 1
            assert name_rows[0]["name"] == "Name contains SCQ here"

    def test_find_no_match_empty_list(self, auth_headers):  # noqa
        with TestClient(app) as client:
            for endpoint in ("/laboratory/by_name", "/laboratory/by_strain_designation"):
                res = client.get(
                    endpoint,
                    params={"query": "NOSUCHLABTOKENXYZ"},
                    headers=auth_headers,
                )
                assert res.status_code == status.HTTP_200_OK
                assert res.json() == []

    def test_find_results_include_joins(self, db, auth_headers):  # noqa
        mod = db.query(ModModel).filter(ModModel.abbreviation == "WB").one_or_none()
        if mod is None:
            db.add(ModModel(abbreviation="WB", short_name="WB", full_name="WormBase"))
            db.commit()
        with TestClient(app) as client:
            payload = {
                "name": "Joins Lab JNQ",
                "strain_designation": "JNQ",
                "cross_references": [{"curie": "WB:WBlabJNQ"}],
                "allele_designations": [{"mod_abbreviation": "WB", "allele_designation": "j"}],
            }
            client.post("/laboratory/", json=payload, headers=auth_headers)
            res = client.get(
                "/laboratory/by_strain_designation", params={"query": "JNQ"}, headers=auth_headers
            )
            rows = res.json()
            assert len(rows) == 1
            lab = rows[0]
            assert lab["cross_references"][0]["curie"] == "WB:WBlabJNQ"
            assert lab["allele_designations"][0]["allele_designation"] == "j"
            assert "lab_persons" in lab
