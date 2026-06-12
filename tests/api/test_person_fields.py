# flake8: noqa: F811
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.crud import person_crud
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


@pytest.fixture
def test_person_id(db, auth_headers):  # noqa
    """Create a minimal person and return person_id."""
    with TestClient(app) as client:
        payload = {"display_name": "Field Test Person"}
        response = client.post("/person/", json=payload, headers=auth_headers)
        assert response.status_code == status.HTTP_201_CREATED
        curie = response.json()['curie']
        fetched = client.get(f"/person/{curie}", headers=auth_headers)
        yield fetched.json()["person_id"]


class TestPersonFields:

    def test_create_person_with_all_new_fields(self, auth_headers):  # noqa
        with TestClient(app) as client:
            payload = {
                "display_name": "Full Fields Person",
                "webpage": ["https://example.com", "https://lab.example.com"],
                "active_status": "active",
                "city": "Davis",
                "state": "CA",
                "postal_code": "95616",
                "country": "USA",
                "street_address": "123 Main St",
            }
            res = client.post("/person/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            person_id = client.get(f"/person/{res.json()['curie']}", headers=auth_headers).json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            assert fetched.status_code == status.HTTP_200_OK
            body = fetched.json()
            assert body["webpage"] == ["https://example.com", "https://lab.example.com"]
            assert body["active_status"] == "active"
            assert body["city"] == "Davis"
            assert body["state"] == "CA"
            assert body["postal_code"] == "95616"
            assert body["country"] == "USA"
            assert body["street_address"] == "123 Main St"
            assert body["address_last_updated"] is not None

    def test_create_person_with_webpage_array(self, auth_headers):  # noqa
        with TestClient(app) as client:
            urls = ["https://one.com", "https://two.com", "https://three.com"]
            payload = {
                "display_name": "Webpage Person",
                "webpage": urls,
            }
            res = client.post("/person/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            person_id = client.get(f"/person/{res.json()['curie']}", headers=auth_headers).json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            assert fetched.json()["webpage"] == urls

    def test_create_person_with_institution_array(self, auth_headers):  # noqa
        with TestClient(app) as client:
            institutions = ["Caltech", "MIT", "Stanford"]
            payload = {
                "display_name": "Institution Person",
                "institution": institutions,
            }
            res = client.post("/person/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            person_id = client.get(f"/person/{res.json()['curie']}", headers=auth_headers).json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            assert fetched.json()["institution"] == institutions

    def test_patch_institution(self, auth_headers, test_person_id):  # noqa
        with TestClient(app) as client:
            institutions = ["Caltech"]
            res = client.patch(
                f"/person/{test_person_id}",
                json={"institution": institutions},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            fetched = client.get(f"/person/{test_person_id}", headers=auth_headers)
            assert fetched.json()["institution"] == institutions

    def test_create_person_with_active_status(self, auth_headers):  # noqa
        with TestClient(app) as client:
            payload = {
                "display_name": "Retired Person",
                "active_status": "retired",
            }
            res = client.post("/person/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            person_id = client.get(f"/person/{res.json()['curie']}", headers=auth_headers).json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            assert fetched.json()["active_status"] == "retired"

    def test_create_person_with_multiline_street_address(self, auth_headers):  # noqa
        with TestClient(app) as client:
            address = "123 Main St\nApt 4B\nBuilding C"
            payload = {
                "display_name": "Multiline Address Person",
                "street_address": address,
            }
            res = client.post("/person/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            person_id = client.get(f"/person/{res.json()['curie']}", headers=auth_headers).json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            assert fetched.json()["street_address"] == address

    def test_create_person_with_address_sets_timestamp(self, auth_headers):  # noqa
        with TestClient(app) as client:
            payload = {
                "display_name": "Address Timestamp Person",
                "city": "Boston",
            }
            res = client.post("/person/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            person_id = client.get(f"/person/{res.json()['curie']}", headers=auth_headers).json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            assert fetched.json()["address_last_updated"] is not None

    def test_create_person_without_address_no_timestamp(self, auth_headers):  # noqa
        with TestClient(app) as client:
            payload = {
                "display_name": "No Address Person",
            }
            res = client.post("/person/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            person_id = client.get(f"/person/{res.json()['curie']}", headers=auth_headers).json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            assert fetched.json()["address_last_updated"] is None

    def test_patch_webpage(self, auth_headers, test_person_id):  # noqa
        with TestClient(app) as client:
            urls = ["https://new-site.org"]
            res = client.patch(
                f"/person/{test_person_id}",
                json={"webpage": urls},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK

            fetched = client.get(f"/person/{test_person_id}", headers=auth_headers)
            assert fetched.json()["webpage"] == urls

    def test_patch_active_status(self, auth_headers, test_person_id):  # noqa
        with TestClient(app) as client:
            res = client.patch(
                f"/person/{test_person_id}",
                json={"active_status": "active"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK

            fetched = client.get(f"/person/{test_person_id}", headers=auth_headers)
            assert fetched.json()["active_status"] == "active"

    def test_patch_address_field_updates_timestamp(self, auth_headers, test_person_id):  # noqa
        with TestClient(app) as client:
            # Verify no timestamp initially
            fetched = client.get(f"/person/{test_person_id}", headers=auth_headers)
            assert fetched.json()["address_last_updated"] is None

            # Patch city
            res = client.patch(
                f"/person/{test_person_id}",
                json={"city": "New York"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK

            fetched = client.get(f"/person/{test_person_id}", headers=auth_headers)
            assert fetched.json()["city"] == "New York"
            assert fetched.json()["address_last_updated"] is not None

    def test_patch_multiple_address_fields(self, auth_headers, test_person_id):  # noqa
        with TestClient(app) as client:
            res = client.patch(
                f"/person/{test_person_id}",
                json={"city": "London", "state": "England", "country": "UK"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK

            fetched = client.get(f"/person/{test_person_id}", headers=auth_headers)
            body = fetched.json()
            assert body["city"] == "London"
            assert body["state"] == "England"
            assert body["country"] == "UK"
            assert body["address_last_updated"] is not None

    def test_patch_non_address_field_no_timestamp_change(self, auth_headers):  # noqa
        """Patching display_name should not update address_last_updated."""
        with TestClient(app) as client:
            # Create person without address
            res = client.post(
                "/person/",
                json={"display_name": "Timestamp Test"},
                headers=auth_headers,
            )
            person_id = client.get(f"/person/{res.json()['curie']}", headers=auth_headers).json()["person_id"]

            # Patch display_name only
            client.patch(
                f"/person/{person_id}",
                json={"display_name": "Timestamp Test Updated"},
                headers=auth_headers,
            )

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            assert fetched.json()["address_last_updated"] is None

    def test_show_person_includes_new_fields(self, auth_headers):  # noqa
        with TestClient(app) as client:
            payload = {
                "display_name": "Show Fields Person",
                "webpage": ["https://show.example.com"],
                "active_status": "deceased",
                "city": "Cambridge",
                "state": "MA",
                "postal_code": "02139",
                "country": "USA",
                "street_address": "77 Mass Ave",
                "biography_research_interest": "Studies ion channels.",
            }
            res = client.post("/person/", json=payload, headers=auth_headers)
            person_id = client.get(f"/person/{res.json()['curie']}", headers=auth_headers).json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            body = fetched.json()
            # All new fields should be present
            for field in ["webpage", "active_status", "city", "state",
                          "postal_code", "country", "street_address",
                          "address_last_updated", "biography_research_interest"]:
                assert field in body, f"Missing field: {field}"

    def test_create_person_with_biography(self, auth_headers):  # noqa
        with TestClient(app) as client:
            bio = "Research focuses on CRISPR gene editing in nematodes."
            res = client.post(
                "/person/",
                json={"display_name": "Bio Person", "biography_research_interest": bio},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_201_CREATED
            person_id = client.get(f"/person/{res.json()['curie']}", headers=auth_headers).json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            assert fetched.json()["biography_research_interest"] == bio

    def test_create_person_with_multiline_biography(self, auth_headers):  # noqa
        with TestClient(app) as client:
            bio = "Line one of biography.\nLine two.\n\nPara two starts here."
            res = client.post(
                "/person/",
                json={"display_name": "Multiline Bio Person", "biography_research_interest": bio},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_201_CREATED
            person_id = client.get(f"/person/{res.json()['curie']}", headers=auth_headers).json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            assert fetched.json()["biography_research_interest"] == bio

    def test_patch_biography(self, auth_headers, test_person_id):  # noqa
        with TestClient(app) as client:
            res = client.patch(
                f"/person/{test_person_id}",
                json={"biography_research_interest": "Updated biography."},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK

            fetched = client.get(f"/person/{test_person_id}", headers=auth_headers)
            assert fetched.json()["biography_research_interest"] == "Updated biography."

    def test_person_fields_default_null(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/person/",
                json={"display_name": "Minimal Person"},
                headers=auth_headers,
            )
            person_id = client.get(f"/person/{res.json()['curie']}", headers=auth_headers).json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            body = fetched.json()
            assert body["webpage"] is None
            # active_status is NOT NULL with default "active"
            assert body["active_status"] == "active"
            assert body["city"] is None
            assert body["state"] is None
            assert body["postal_code"] is None
            assert body["country"] is None
            assert body["street_address"] is None
            assert body["address_last_updated"] is None
            assert body["biography_research_interest"] is None

    def test_active_status_invalid_value_rejected(self, auth_headers):  # noqa
        """Pydantic Literal should reject values other than active/retired/deceased."""
        with TestClient(app) as client:
            res = client.post(
                "/person/",
                json={"display_name": "Bad Status", "active_status": "invalid_value"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_patch_invalid_active_status_rejected(self, auth_headers, test_person_id):  # noqa
        """PATCH with invalid active_status should be rejected at Pydantic layer."""
        with TestClient(app) as client:
            res = client.patch(
                f"/person/{test_person_id}",
                json={"active_status": "former"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_patch_clear_address_field_bumps_timestamp(self, auth_headers):  # noqa
        """PATCHing city=null should still bump address_last_updated."""
        with TestClient(app) as client:
            # Create person with city set
            res = client.post(
                "/person/",
                json={"display_name": "Clear Address Test", "city": "Boston"},
                headers=auth_headers,
            )
            person_id = client.get(f"/person/{res.json()['curie']}", headers=auth_headers).json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            original_timestamp = fetched.json()["address_last_updated"]
            assert original_timestamp is not None

            # PATCH city to null (clearing it)
            res = client.patch(
                f"/person/{person_id}",
                json={"city": None},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            body = fetched.json()
            assert body["city"] is None
            # timestamp should be bumped, not just non-null
            new_timestamp = body["address_last_updated"]
            assert new_timestamp is not None
            assert new_timestamp > original_timestamp

    def test_active_status_all_three_values(self, auth_headers):  # noqa
        """All three allowed active_status values should be accepted."""
        with TestClient(app) as client:
            for status_value in ["active", "retired", "deceased"]:
                res = client.post(
                    "/person/",
                    json={"display_name": f"Status {status_value}", "active_status": status_value},
                    headers=auth_headers,
                )
                assert res.status_code == status.HTTP_201_CREATED
                person_id = client.get(f"/person/{res.json()['curie']}", headers=auth_headers).json()["person_id"]
                fetched = client.get(f"/person/{person_id}", headers=auth_headers)
                assert fetched.json()["active_status"] == status_value

    def test_create_person_with_fields_and_inline_collections(self, auth_headers):  # noqa
        """POST /person/ with all person fields AND inline names, emails, cross_references, notes."""
        with TestClient(app) as client:
            multiline_note = "Initial note line one.\nLine two of the same note."
            payload = {
                "display_name": "Jane Comprehensive",
                "webpage": ["https://jane.example.com"],
                "active_status": "active",
                "city": "Seattle",
                "state": "WA",
                "postal_code": "98101",
                "country": "USA",
                "street_address": "1 Pine St",
                "biography_research_interest": "Studies zebrafish development.",
                "names": [
                    {"first_name": "Jane", "last_name": "Doe"},
                    {
                        "first_name": "Jane",
                        "middle_name": "C",
                        "last_name": "Comprehensive",
                        "is_primary": True,
                    },
                ],
                "emails": [
                    {"email_address": "jane.doe@example.com"},
                    {"email_address": "jane.c@example.org"},
                ],
                "cross_references": [
                    {"curie": "ORCID:0000-0005-1111-2222"},
                    {"curie": "WB:WBPerson77777"},
                ],
                "notes": [
                    {"note": "First note about Jane."},
                    {"note": multiline_note},
                ],
            }
            res = client.post("/person/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            person_id = client.get(f"/person/{res.json()['curie']}", headers=auth_headers).json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            assert fetched.status_code == status.HTTP_200_OK
            body = fetched.json()

            # Scalar fields persisted
            assert body["webpage"] == ["https://jane.example.com"]
            assert body["active_status"] == "active"
            assert body["city"] == "Seattle"
            assert body["state"] == "WA"
            assert body["postal_code"] == "98101"
            assert body["country"] == "USA"
            assert body["street_address"] == "1 Pine St"
            assert body["biography_research_interest"] == "Studies zebrafish development."
            # Address timestamp bumped because address fields were provided
            assert body["address_last_updated"] is not None

            # Names: 2 present, second is primary
            names = body.get("names") or []
            assert len(names) == 2
            primary_names = [n for n in names if n.get("is_primary") is True]
            assert len(primary_names) == 1
            assert primary_names[0]["last_name"] == "Comprehensive"

            # Emails: 2 present (no primary concept anymore)
            emails = body.get("emails") or []
            email_addresses = {e["email_address"] for e in emails}
            assert email_addresses == {"jane.doe@example.com", "jane.c@example.org"}

            # Cross-references: 2 present with derived curie_prefix
            xrefs = body.get("cross_references") or []
            curies = {x["curie"] for x in xrefs}
            assert curies == {"ORCID:0000-0005-1111-2222", "WB:WBPerson77777"}
            prefixes = {x["curie_prefix"] for x in xrefs}
            assert prefixes == {"ORCID", "WB"}

            # Notes: 2 present, multiline preserved
            notes = body.get("notes") or []
            note_texts = {n["note"] for n in notes}
            assert note_texts == {"First note about Jane.", multiline_note}


class TestPersonCurie:

    def test_create_person_returns_curie_string(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            payload = {"display_name": "Curie Assignment Person"}
            res = client.post("/person/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            curie = res.json()['curie']
            assert isinstance(curie, str)
            assert curie.startswith("AGRKB:103")
            assert len(curie) == len("AGRKB:103000000000001")

    def test_consecutive_persons_have_monotonic_curies(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            res1 = client.post(
                "/person/", json={"display_name": "Monotonic A"}, headers=auth_headers
            )
            res2 = client.post(
                "/person/", json={"display_name": "Monotonic B"}, headers=auth_headers
            )
            assert res1.status_code == status.HTTP_201_CREATED
            assert res2.status_code == status.HTTP_201_CREATED
            curie1 = res1.json()['curie']
            curie2 = res2.json()['curie']
            assert curie1.startswith("AGRKB:103")
            assert curie2.startswith("AGRKB:103")
            num1 = int(curie1[len("AGRKB:103"):])
            num2 = int(curie2[len("AGRKB:103"):])
            assert num2 == num1 + 1

    def test_create_person_rejects_caller_supplied_curie(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            payload = {
                "display_name": "Rejected Curie Person",
                "curie": "AGRKB:999000000000001",
            }
            res = client.post("/person/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_patch_person_rejects_curie_update(self, db, auth_headers, test_person_id):  # noqa
        with TestClient(app) as client:
            res = client.patch(
                f"/person/{test_person_id}",
                json={"curie": "AGRKB:999000000000002"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_get_person_by_curie_matches_get_by_person_id(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/person/",
                json={"display_name": "Lookup By Curie"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_201_CREATED
            curie = res.json()['curie']
            by_curie = client.get(f"/person/{curie}", headers=auth_headers)
            assert by_curie.status_code == status.HTTP_200_OK
            person_id = by_curie.json()["person_id"]
            by_id = client.get(f"/person/{person_id}", headers=auth_headers)
            assert by_id.status_code == status.HTTP_200_OK
            assert by_curie.json() == by_id.json()

    def test_get_person_unknown_identifier_returns_404(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            res_int = client.get("/person/99999999", headers=auth_headers)
            assert res_int.status_code == status.HTTP_404_NOT_FOUND
            res_curie = client.get(
                "/person/AGRKB:103999999999999", headers=auth_headers
            )
            assert res_curie.status_code == status.HTTP_404_NOT_FOUND

    def test_person_cross_reference_accepts_curie_in_path(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/person/",
                json={"display_name": "Xref By Curie"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_201_CREATED
            curie = res.json()['curie']
            post_xref = client.post(
                "/person_cross_reference/",
                json={"person_curie": curie, "curie": "ORCID:0000-0009-8888-7777"},
                headers=auth_headers,
            )
            assert post_xref.status_code == status.HTTP_201_CREATED
            listed = client.get(
                f"/person_cross_reference/person/{curie}", headers=auth_headers
            )
            assert listed.status_code == status.HTTP_200_OK
            xref_curies = {x["curie"] for x in listed.json()}
            assert "ORCID:0000-0009-8888-7777" in xref_curies


class TestPersonLookups:

    # ---- /person/by_email/{email} ----

    def test_by_email_found(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            curie = client.post(
                "/person/",
                json={"display_name": "Email Lookup Person"},
                headers=auth_headers,
            ).json()['curie']
            person_id = client.get(f"/person/{curie}", headers=auth_headers).json()["person_id"]
            email_post = client.post(
                f"/person_email/person/{person_id}",
                json={"email_address": "lookup@example.com"},
                headers=auth_headers,
            )
            assert email_post.status_code == status.HTTP_201_CREATED
            res = client.get("/person/by_email/lookup@example.com", headers=auth_headers)
            assert res.status_code == status.HTTP_200_OK
            assert res.json()["person_id"] == person_id

    def test_by_email_not_found_returns_204(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.get("/person/by_email/nobody@example.com", headers=auth_headers)
            assert res.status_code == status.HTTP_204_NO_CONTENT

    def test_old_by_slash_email_path_is_gone(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.get("/person/by/email/anything@example.com", headers=auth_headers)
            assert res.status_code == status.HTTP_404_NOT_FOUND

    # ---- /person/by_name (aggregates display_name + person_name) ----

    def test_by_name_matches_display_name(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            client.post(
                "/person/",
                json={"display_name": "Zelda Display Match"},
                headers=auth_headers,
            )
            res = client.get("/person/by_name", params={"name": "Zelda"}, headers=auth_headers)
            assert res.status_code == status.HTTP_200_OK
            assert any("Zelda" in p["display_name"] for p in res.json())

    def test_by_name_matches_person_name_first_name(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            curie = client.post(
                "/person/",
                json={"display_name": "Totally Unrelated"},
                headers=auth_headers,
            ).json()['curie']
            person_id = client.get(f"/person/{curie}", headers=auth_headers).json()["person_id"]
            client.post(
                f"/person_name/person/{person_id}",
                json={"first_name": "Borogove", "last_name": "Smith"},
                headers=auth_headers,
            )
            res = client.get("/person/by_name", params={"name": "Borogove"}, headers=auth_headers)
            assert res.status_code == status.HTTP_200_OK
            assert any(p["person_id"] == person_id for p in res.json())

    def test_by_name_matches_person_name_last_name(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            curie = client.post(
                "/person/",
                json={"display_name": "Totally Different Display"},
                headers=auth_headers,
            ).json()['curie']
            person_id = client.get(f"/person/{curie}", headers=auth_headers).json()["person_id"]
            client.post(
                f"/person_name/person/{person_id}",
                json={"first_name": "X", "last_name": "Slithytove"},
                headers=auth_headers,
            )
            res = client.get("/person/by_name", params={"name": "Slithytove"}, headers=auth_headers)
            assert any(p["person_id"] == person_id for p in res.json())

    def test_by_name_matches_person_name_middle_name(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            curie = client.post(
                "/person/",
                json={"display_name": "No Match Here"},
                headers=auth_headers,
            ).json()['curie']
            person_id = client.get(f"/person/{curie}", headers=auth_headers).json()["person_id"]
            client.post(
                f"/person_name/person/{person_id}",
                json={"first_name": "F", "middle_name": "Mimsy", "last_name": "L"},
                headers=auth_headers,
            )
            res = client.get("/person/by_name", params={"name": "Mimsy"}, headers=auth_headers)
            assert any(p["person_id"] == person_id for p in res.json())

    def test_by_name_dedupes_when_multiple_person_names_match(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            curie = client.post(
                "/person/",
                json={"display_name": "Dedupe Person"},
                headers=auth_headers,
            ).json()['curie']
            person_id = client.get(f"/person/{curie}", headers=auth_headers).json()["person_id"]
            for last in ["Jabberwocky", "Jabberwocky-Hyphenated"]:
                client.post(
                    f"/person_name/person/{person_id}",
                    json={"first_name": "F", "last_name": last},
                    headers=auth_headers,
                )
            res = client.get("/person/by_name", params={"name": "Jabberwocky"}, headers=auth_headers)
            matches = [p for p in res.json() if p["person_id"] == person_id]
            assert len(matches) == 1

    def test_by_name_no_match_returns_empty_list(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.get(
                "/person/by_name",
                params={"name": "ZZZ_DEFINITELY_NOT_PRESENT"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            assert res.json() == []

    def test_old_by_slash_name_path_is_gone(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.get("/person/by/name", params={"name": "X"}, headers=auth_headers)
            assert res.status_code == status.HTTP_404_NOT_FOUND

    # ---- /person/by_person_cross_reference/{curie_or_id} ----

    def test_by_person_cross_reference_found_by_curie(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            curie = client.post(
                "/person/",
                json={"display_name": "PCR Lookup Person"},
                headers=auth_headers,
            ).json()['curie']
            person_id = client.get(f"/person/{curie}", headers=auth_headers).json()["person_id"]
            client.post(
                "/person_cross_reference/",
                json={"person_curie": str(person_id), "curie": "ORCID:0000-0001-2345-6789"},
                headers=auth_headers,
            )
            res = client.get(
                "/person/by_person_cross_reference/ORCID:0000-0001-2345-6789",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            assert res.json()["person_id"] == person_id

    def test_by_person_cross_reference_found_by_id(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            curie = client.post(
                "/person/",
                json={"display_name": "PCR-by-id Person"},
                headers=auth_headers,
            ).json()['curie']
            person_id = client.get(f"/person/{curie}", headers=auth_headers).json()["person_id"]
            pcr = client.post(
                "/person_cross_reference/",
                json={"person_curie": str(person_id), "curie": "ORCID:0000-9999-9999-9999"},
                headers=auth_headers,
            ).json()
            res = client.get(
                f"/person/by_person_cross_reference/{pcr['person_cross_reference_id']}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            assert res.json()["person_id"] == person_id

    def test_by_person_cross_reference_not_found(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.get(
                "/person/by_person_cross_reference/ORCID:0000-0000-0000-0000",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_404_NOT_FOUND


class TestPersonEmailMixedCase:
    """
    Stored email_address values preserve the original casing the user typed.
    All uniqueness checks and lookups use lower() on both sides. The DB
    enforces this with the functional unique index
    uq_person_email_person_address_lower.
    """

    def test_email_preserves_case_in_storage(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            curie = client.post(
                "/person/",
                json={"display_name": "Mixed Case Storage"},
                headers=auth_headers,
            ).json()['curie']
            person_id = client.get(
                f"/person/{curie}", headers=auth_headers
            ).json()["person_id"]
            post = client.post(
                f"/person_email/person/{person_id}",
                json={"email_address": "Foo@Bar.com"},
                headers=auth_headers,
            )
            assert post.status_code == status.HTTP_201_CREATED
            assert post.json()["email_address"] == "Foo@Bar.com"

            listing = client.get(
                f"/person_email/person/{person_id}", headers=auth_headers
            ).json()
            assert [e["email_address"] for e in listing] == ["Foo@Bar.com"]

    def test_email_case_insensitive_duplicate_rejected(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            curie = client.post(
                "/person/",
                json={"display_name": "Case Insensitive Dup"},
                headers=auth_headers,
            ).json()['curie']
            person_id = client.get(
                f"/person/{curie}", headers=auth_headers
            ).json()["person_id"]

            first = client.post(
                f"/person_email/person/{person_id}",
                json={"email_address": "Foo@bar.com"},
                headers=auth_headers,
            )
            assert first.status_code == status.HTTP_201_CREATED

            second = client.post(
                f"/person_email/person/{person_id}",
                json={"email_address": "FOO@BAR.COM"},
                headers=auth_headers,
            )
            assert second.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_email_lookup_is_case_insensitive(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            curie = client.post(
                "/person/",
                json={"display_name": "Lookup Case Insensitive"},
                headers=auth_headers,
            ).json()['curie']
            person_id = client.get(
                f"/person/{curie}", headers=auth_headers
            ).json()["person_id"]
            client.post(
                f"/person_email/person/{person_id}",
                json={"email_address": "MixedCase@Example.com"},
                headers=auth_headers,
            )
            res = client.get(
                "/person/by_email/mixedcase@example.com",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            assert res.json()["person_id"] == person_id

    def test_patch_date_made_old_email(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            curie = client.post(
                "/person/",
                json={"display_name": "Mark Email Old"},
                headers=auth_headers,
            ).json()['curie']
            person_id = client.get(
                f"/person/{curie}", headers=auth_headers
            ).json()["person_id"]
            email_id = client.post(
                f"/person_email/person/{person_id}",
                json={"email_address": "Active@example.com"},
                headers=auth_headers,
            ).json()["person_email_id"]

            patch = client.patch(
                f"/person_email/{email_id}",
                json={"date_made_old_email": "2026-05-22T12:00:00"},
                headers=auth_headers,
            )
            assert patch.status_code == status.HTTP_200_OK

            shown = client.get(
                f"/person_email/{email_id}", headers=auth_headers
            ).json()
            assert shown["date_made_old_email"] is not None
            assert shown["date_made_old_email"].startswith("2026-05-22T12:00:00")


class TestGetMostCurrentEmailFunction:
    """
    The get_most_current_email(person_id) SQL function returns the
    most-recently-touched person_email row whose date_made_old_email IS NULL,
    or NULL when no active row exists.
    """

    def test_picks_most_recently_updated_active(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            curie = client.post(
                "/person/",
                json={"display_name": "Function Picks Recent"},
                headers=auth_headers,
            ).json()['curie']
            person_id = client.get(
                f"/person/{curie}", headers=auth_headers
            ).json()["person_id"]

            older_id = client.post(
                f"/person_email/person/{person_id}",
                json={"email_address": "older@example.com"},
                headers=auth_headers,
            ).json()["person_email_id"]
            newer_id = client.post(
                f"/person_email/person/{person_id}",
                json={"email_address": "newer@example.com"},
                headers=auth_headers,
            ).json()["person_email_id"]

            # Bump older row's date_updated so it becomes the most recent.
            patch = client.patch(
                f"/person_email/{older_id}",
                json={"email_address": "older@example.com"},
                headers=auth_headers,
            )
            assert patch.status_code == status.HTTP_200_OK

            result = db.execute(
                text("SELECT get_most_current_email(:pid)"),
                {"pid": person_id},
            ).scalar()
            assert result == "older@example.com"

            # Bump newer row to put it back on top.
            patch = client.patch(
                f"/person_email/{newer_id}",
                json={"email_address": "newer@example.com"},
                headers=auth_headers,
            )
            assert patch.status_code == status.HTTP_200_OK

            result = db.execute(
                text("SELECT get_most_current_email(:pid)"),
                {"pid": person_id},
            ).scalar()
            assert result == "newer@example.com"

    def test_skips_emails_marked_old(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            curie = client.post(
                "/person/",
                json={"display_name": "Function Skips Old"},
                headers=auth_headers,
            ).json()['curie']
            person_id = client.get(
                f"/person/{curie}", headers=auth_headers
            ).json()["person_id"]

            kept = client.post(
                f"/person_email/person/{person_id}",
                json={"email_address": "kept@example.com"},
                headers=auth_headers,
            ).json()["person_email_id"]
            retired = client.post(
                f"/person_email/person/{person_id}",
                json={"email_address": "retired@example.com"},
                headers=auth_headers,
            ).json()["person_email_id"]

            # Retire the second (which is the more-recent row).
            client.patch(
                f"/person_email/{retired}",
                json={"date_made_old_email": "2026-01-01T00:00:00"},
                headers=auth_headers,
            )

            result = db.execute(
                text("SELECT get_most_current_email(:pid)"),
                {"pid": person_id},
            ).scalar()
            assert result == "kept@example.com"
            assert kept != retired  # sanity

    def test_returns_null_when_no_active(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            curie = client.post(
                "/person/",
                json={"display_name": "All Retired"},
                headers=auth_headers,
            ).json()['curie']
            person_id = client.get(
                f"/person/{curie}", headers=auth_headers
            ).json()["person_id"]
            only_id = client.post(
                f"/person_email/person/{person_id}",
                json={"email_address": "soon-old@example.com"},
                headers=auth_headers,
            ).json()["person_email_id"]
            client.patch(
                f"/person_email/{only_id}",
                json={"date_made_old_email": "2026-01-01T00:00:00"},
                headers=auth_headers,
            )

            result = db.execute(
                text("SELECT get_most_current_email(:pid)"),
                {"pid": person_id},
            ).scalar()
            assert result is None


class TestPersonUnsubscribe:
    """
    `person.unsubscribe` is a boolean NOT NULL DEFAULT FALSE flag.
    Callers that send notifications are expected to check it separately
    (it is intentionally NOT enforced inside get_most_current_email).
    """

    def test_unsubscribe_defaults_to_false(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            curie = client.post(
                "/person/",
                json={"display_name": "Unsubscribe Default"},
                headers=auth_headers,
            ).json()['curie']
            body = client.get(f"/person/{curie}", headers=auth_headers).json()
            assert body["unsubscribe"] is False

    def test_unsubscribe_can_be_set_on_create(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            curie = client.post(
                "/person/",
                json={
                    "display_name": "Unsubscribe On Create",
                    "unsubscribe": True,
                },
                headers=auth_headers,
            ).json()['curie']
            body = client.get(f"/person/{curie}", headers=auth_headers).json()
            assert body["unsubscribe"] is True

    def test_unsubscribe_can_be_patched(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            curie = client.post(
                "/person/",
                json={"display_name": "Unsubscribe Patch"},
                headers=auth_headers,
            ).json()['curie']
            patch = client.patch(
                f"/person/{curie}",
                json={"unsubscribe": True},
                headers=auth_headers,
            )
            assert patch.status_code == status.HTTP_200_OK
            assert (
                client.get(f"/person/{curie}", headers=auth_headers).json()[
                    "unsubscribe"
                ]
                is True
            )

    def test_unsubscribe_null_in_patch_is_noop(self, db, auth_headers):  # noqa
        # Explicit null on a PATCH for unsubscribe must not 500 the way it
        # would if setattr(obj, "unsubscribe", None) reached the NOT NULL
        # column. CRUD treats null as "no change".
        with TestClient(app) as client:
            curie = client.post(
                "/person/",
                json={
                    "display_name": "Unsubscribe Null Patch",
                    "unsubscribe": True,
                },
                headers=auth_headers,
            ).json()['curie']
            patch = client.patch(
                f"/person/{curie}",
                json={"unsubscribe": None},
                headers=auth_headers,
            )
            assert patch.status_code == status.HTTP_200_OK
            # The previously-set True value must survive.
            assert (
                client.get(f"/person/{curie}", headers=auth_headers).json()[
                    "unsubscribe"
                ]
                is True
            )


class TestPersonCreateDuplicates:
    """
    Re-POSTing /person/ with the same cross_reference curies must return 422,
    not 500 from a leaked IntegrityError on the global uq_person_xref_curie
    constraint.
    """

    def test_post_person_with_duplicate_xref_curie_across_persons_returns_422(
        self, db, auth_headers
    ):  # noqa
        payload = {
            "display_name": "Dup XRef Person",
            "cross_references": [{"curie": "ORCID:0000-0005-1111-3333"}],
        }
        with TestClient(app) as client:
            first = client.post("/person/", json=payload, headers=auth_headers)
            assert first.status_code == status.HTTP_201_CREATED

            second = client.post("/person/", json=payload, headers=auth_headers)
            assert second.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
            assert "ORCID:0000-0005-1111-3333" in second.json()["detail"]

    def test_post_person_with_duplicate_curie_within_payload_returns_422(
        self, db, auth_headers
    ):  # noqa
        payload = {
            "display_name": "Same Curie Twice",
            "cross_references": [
                {"curie": "ORCID:0000-0006-2222-4444"},
                {"curie": "ORCID:0000-0006-2222-4444"},
            ],
        }
        with TestClient(app) as client:
            res = client.post("/person/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
            assert "duplicated in the request" in res.json()["detail"]

    def test_post_person_with_duplicate_prefix_within_payload_returns_422(
        self, db, auth_headers
    ):  # noqa
        # Two different curies with the same prefix violate
        # uq_person_xref_person_prefix on commit; check it surfaces as 422.
        payload = {
            "display_name": "Two Same-Prefix XRefs",
            "cross_references": [
                {"curie": "ORCID:0000-0007-3333-5555"},
                {"curie": "ORCID:0000-0007-3333-6666"},
            ],
        }
        with TestClient(app) as client:
            res = client.post("/person/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
            assert "at most one per prefix" in res.json()["detail"]

    def test_duplicate_xref_does_not_consume_mati_id(
        self, db, auth_headers, monkeypatch
    ):  # noqa
        """Regression guard for the validation reorder.

        The xref pre-check must run BEFORE get_next_person_curie() in
        person_crud.create(). If the order is reversed, this test fails.
        """
        payload = {
            "display_name": "MATI Skip Test",
            "cross_references": [{"curie": "ORCID:0000-0008-4444-7777"}],
        }
        with TestClient(app) as client:
            first = client.post("/person/", json=payload, headers=auth_headers)
            assert first.status_code == status.HTTP_201_CREATED

            spy = MagicMock()
            monkeypatch.setattr(person_crud, "get_next_person_curie", spy)

            second = client.post("/person/", json=payload, headers=auth_headers)
            assert second.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
            spy.assert_not_called()
