# flake8: noqa: F811
from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import (
    PersonModel,
    PersonCrossReferenceModel,
)
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


PersonXrefTestData = namedtuple(
    "PersonXrefTestData",
    [
        "response",
        "new_person_cross_reference_id",
        "person_id",
    ],
)


@pytest.fixture
def seeded_person(db):
    """Create a Person for cross-reference tests."""
    person = PersonModel(
        display_name="Xref Test Person",
        curie="AGRKB:test-xref-person",
    )
    db.add(person)
    db.commit()
    db.refresh(person)
    return {"person_id": person.person_id}


@pytest.fixture
def test_person_xref(db, auth_headers, seeded_person):  # noqa
    """Create a baseline person_cross_reference row to reuse across tests."""
    with TestClient(app) as client:
        payload = {"curie": "ORCID:0000-0001-2345-6789"}
        response = client.post(
            f"/person_cross_reference/person/{seeded_person['person_id']}",
            json=payload,
            headers=auth_headers,
        )
        body = response.json() if response.status_code == status.HTTP_201_CREATED else {}
        yield PersonXrefTestData(
            response=response,
            new_person_cross_reference_id=body.get("person_cross_reference_id"),
            person_id=seeded_person["person_id"],
        )


class TestPersonCrossReference:

    def test_get_bad_person_cross_reference(self, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get("/person_cross_reference/-1", headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_create_person_cross_reference(self, db, test_person_xref):  # noqa
        assert test_person_xref.response.status_code == status.HTTP_201_CREATED

        xref = (
            db.query(PersonCrossReferenceModel)
            .filter(
                PersonCrossReferenceModel.person_cross_reference_id
                == test_person_xref.new_person_cross_reference_id
            )
            .one()
        )
        assert xref.person_id == test_person_xref.person_id
        assert xref.curie == "ORCID:0000-0001-2345-6789"
        assert xref.curie_prefix == "ORCID"
        assert xref.is_obsolete is False

    def test_create_person_xref_invalid_person(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/person_cross_reference/person/9999999",
                json={"curie": "ORCID:0000-0000-0000-0001"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_create_with_orcid_curie(self, auth_headers, seeded_person):  # noqa
        with TestClient(app) as client:
            res = client.post(
                f"/person_cross_reference/person/{seeded_person['person_id']}",
                json={"curie": "ORCID:0000-0002-1234-5678"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_201_CREATED
            body = res.json()
            assert body["curie"] == "ORCID:0000-0002-1234-5678"
            assert body["curie_prefix"] == "ORCID"

    def test_create_with_wb_curie(self, auth_headers, seeded_person):  # noqa
        """WormBase person curie."""
        with TestClient(app) as client:
            res = client.post(
                f"/person_cross_reference/person/{seeded_person['person_id']}",
                json={"curie": "WB:WBPerson12345"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_201_CREATED
            body = res.json()
            assert body["curie"] == "WB:WBPerson12345"
            assert body["curie_prefix"] == "WB"

    def test_create_with_zfin_curie(self, auth_headers, seeded_person):  # noqa
        """ZFIN person curie."""
        with TestClient(app) as client:
            res = client.post(
                f"/person_cross_reference/person/{seeded_person['person_id']}",
                json={"curie": "ZFIN:ZDB-PERS-200101-1"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_201_CREATED
            assert res.json()["curie_prefix"] == "ZFIN"

    def test_create_with_xenbase_curie(self, auth_headers, seeded_person):  # noqa
        """XenBase person curie."""
        with TestClient(app) as client:
            res = client.post(
                f"/person_cross_reference/person/{seeded_person['person_id']}",
                json={"curie": "XenBase:XB-PERS-3617"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_201_CREATED
            assert res.json()["curie_prefix"] == "XenBase"

    def test_create_invalid_curie_no_colon(self, auth_headers, seeded_person):  # noqa
        with TestClient(app) as client:
            res = client.post(
                f"/person_cross_reference/person/{seeded_person['person_id']}",
                json={"curie": "invalidcurie"},
                headers=auth_headers,
            )
            # Pydantic field_validator on PersonCrossReferenceSchemaCreate rejects this
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_create_invalid_curie_two_colons(self, auth_headers, seeded_person):  # noqa
        with TestClient(app) as client:
            res = client.post(
                f"/person_cross_reference/person/{seeded_person['person_id']}",
                json={"curie": "ORCID:0000:0001"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_create_duplicate_curie_same_person(self, auth_headers, test_person_xref):  # noqa
        """Posting the same curie for the same person should be rejected."""
        with TestClient(app) as client:
            res = client.post(
                f"/person_cross_reference/person/{test_person_xref.person_id}",
                json={"curie": "ORCID:0000-0001-2345-6789"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_list_for_person(self, auth_headers, test_person_xref):  # noqa
        with TestClient(app) as client:
            res = client.get(
                f"/person_cross_reference/person/{test_person_xref.person_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            rows = res.json()
            assert isinstance(rows, list)
            assert any(
                r["person_cross_reference_id"] == test_person_xref.new_person_cross_reference_id
                for r in rows
            )

    def test_list_for_nonexistent_person(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.get("/person_cross_reference/person/9999999", headers=auth_headers)
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_show_person_cross_reference(self, auth_headers, test_person_xref):  # noqa
        with TestClient(app) as client:
            res = client.get(
                f"/person_cross_reference/{test_person_xref.new_person_cross_reference_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            body = res.json()
            assert body["person_cross_reference_id"] == test_person_xref.new_person_cross_reference_id
            assert body["curie"] == "ORCID:0000-0001-2345-6789"
            assert body["curie_prefix"] == "ORCID"

    def test_patch_curie(self, db, auth_headers, test_person_xref):  # noqa
        """PATCH curie updates curie and re-derives curie_prefix."""
        with TestClient(app) as client:
            res = client.patch(
                f"/person_cross_reference/{test_person_xref.new_person_cross_reference_id}",
                json={"curie": "WB:WBPerson99999"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_202_ACCEPTED

            obj = (
                db.query(PersonCrossReferenceModel)
                .filter(
                    PersonCrossReferenceModel.person_cross_reference_id
                    == test_person_xref.new_person_cross_reference_id
                )
                .one()
            )
            db.refresh(obj)
            assert obj.curie == "WB:WBPerson99999"
            assert obj.curie_prefix == "WB"

    def test_patch_is_obsolete(self, db, auth_headers, test_person_xref):  # noqa
        with TestClient(app) as client:
            res = client.patch(
                f"/person_cross_reference/{test_person_xref.new_person_cross_reference_id}",
                json={
                    "curie": "ORCID:0000-0001-2345-6789",
                    "is_obsolete": True,
                },
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_202_ACCEPTED

            obj = (
                db.query(PersonCrossReferenceModel)
                .filter(
                    PersonCrossReferenceModel.person_cross_reference_id
                    == test_person_xref.new_person_cross_reference_id
                )
                .one()
            )
            db.refresh(obj)
            assert obj.is_obsolete is True

    def test_destroy_person_cross_reference(self, auth_headers, test_person_xref):  # noqa
        with TestClient(app) as client:
            res = client.delete(
                f"/person_cross_reference/{test_person_xref.new_person_cross_reference_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_204_NO_CONTENT

            res = client.get(
                f"/person_cross_reference/{test_person_xref.new_person_cross_reference_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_destroy_nonexistent(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.delete("/person_cross_reference/-1", headers=auth_headers)
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_create_person_with_inline_cross_references(self, auth_headers):  # noqa
        """POST /person/ with inline cross_references array (mixed prefixes)."""
        with TestClient(app) as client:
            payload = {
                "display_name": "Inline Xref Test",
                "curie": "AGRKB:test-inline-xref",
                "cross_references": [
                    {"curie": "ORCID:0000-0003-9999-1111"},
                    {"curie": "WB:WBPerson55555"},
                    {"curie": "ZFIN:ZDB-PERS-200202-2"},
                ],
            }
            res = client.post("/person/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            person_id = res.json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            body = fetched.json()
            xrefs = body.get("cross_references", [])
            assert len(xrefs) == 3
            curies = {x["curie"] for x in xrefs}
            assert curies == {
                "ORCID:0000-0003-9999-1111",
                "WB:WBPerson55555",
                "ZFIN:ZDB-PERS-200202-2",
            }
            prefixes = {x["curie_prefix"] for x in xrefs}
            assert prefixes == {"ORCID", "WB", "ZFIN"}
