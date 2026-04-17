# flake8: noqa: F811
import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


@pytest.fixture
def test_person_id(db, auth_headers):  # noqa
    """Create a minimal person and return person_id."""
    with TestClient(app) as client:
        payload = {"display_name": "Field Test Person"}
        response = client.post("/person/", json=payload, headers=auth_headers)
        assert response.status_code == status.HTTP_201_CREATED
        yield response.json()["person_id"]


class TestPersonFields:

    def test_create_person_with_all_new_fields(self, auth_headers):  # noqa
        with TestClient(app) as client:
            payload = {
                "display_name": "Full Fields Person",
                "orcid": "0000-0001-2345-6789",
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
            person_id = res.json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            assert fetched.status_code == status.HTTP_200_OK
            body = fetched.json()
            assert body["orcid"] == "0000-0001-2345-6789"
            assert body["webpage"] == ["https://example.com", "https://lab.example.com"]
            assert body["active_status"] == "active"
            assert body["city"] == "Davis"
            assert body["state"] == "CA"
            assert body["postal_code"] == "95616"
            assert body["country"] == "USA"
            assert body["street_address"] == "123 Main St"
            assert body["address_last_updated"] is not None

    def test_create_person_with_orcid(self, auth_headers):  # noqa
        with TestClient(app) as client:
            payload = {
                "display_name": "ORCID Person",
                "orcid": "0000-0002-9999-8888",
            }
            res = client.post("/person/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            person_id = res.json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            assert fetched.json()["orcid"] == "0000-0002-9999-8888"

    def test_create_person_with_webpage_array(self, auth_headers):  # noqa
        with TestClient(app) as client:
            urls = ["https://one.com", "https://two.com", "https://three.com"]
            payload = {
                "display_name": "Webpage Person",
                "webpage": urls,
            }
            res = client.post("/person/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            person_id = res.json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            assert fetched.json()["webpage"] == urls

    def test_create_person_with_active_status(self, auth_headers):  # noqa
        with TestClient(app) as client:
            payload = {
                "display_name": "Retired Person",
                "active_status": "retired",
            }
            res = client.post("/person/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            person_id = res.json()["person_id"]

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
            person_id = res.json()["person_id"]

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
            person_id = res.json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            assert fetched.json()["address_last_updated"] is not None

    def test_create_person_without_address_no_timestamp(self, auth_headers):  # noqa
        with TestClient(app) as client:
            payload = {
                "display_name": "No Address Person",
                "orcid": "0000-0003-1111-2222",
            }
            res = client.post("/person/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            person_id = res.json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            assert fetched.json()["address_last_updated"] is None

    def test_patch_orcid(self, auth_headers, test_person_id):  # noqa
        with TestClient(app) as client:
            res = client.patch(
                f"/person/{test_person_id}",
                json={"orcid": "0000-0001-0000-0001"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_202_ACCEPTED

            fetched = client.get(f"/person/{test_person_id}", headers=auth_headers)
            assert fetched.json()["orcid"] == "0000-0001-0000-0001"

    def test_patch_webpage(self, auth_headers, test_person_id):  # noqa
        with TestClient(app) as client:
            urls = ["https://new-site.org"]
            res = client.patch(
                f"/person/{test_person_id}",
                json={"webpage": urls},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_202_ACCEPTED

            fetched = client.get(f"/person/{test_person_id}", headers=auth_headers)
            assert fetched.json()["webpage"] == urls

    def test_patch_active_status(self, auth_headers, test_person_id):  # noqa
        with TestClient(app) as client:
            res = client.patch(
                f"/person/{test_person_id}",
                json={"active_status": "active"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_202_ACCEPTED

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
            assert res.status_code == status.HTTP_202_ACCEPTED

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
            assert res.status_code == status.HTTP_202_ACCEPTED

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
            person_id = res.json()["person_id"]

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
                "orcid": "0000-0004-5678-1234",
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
            person_id = res.json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            body = fetched.json()
            # All new fields should be present
            for field in ["orcid", "webpage", "active_status", "city", "state",
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
            person_id = res.json()["person_id"]

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
            person_id = res.json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            assert fetched.json()["biography_research_interest"] == bio

    def test_patch_biography(self, auth_headers, test_person_id):  # noqa
        with TestClient(app) as client:
            res = client.patch(
                f"/person/{test_person_id}",
                json={"biography_research_interest": "Updated biography."},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_202_ACCEPTED

            fetched = client.get(f"/person/{test_person_id}", headers=auth_headers)
            assert fetched.json()["biography_research_interest"] == "Updated biography."

    def test_person_fields_default_null(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/person/",
                json={"display_name": "Minimal Person"},
                headers=auth_headers,
            )
            person_id = res.json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            body = fetched.json()
            assert body["orcid"] is None
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
            person_id = res.json()["person_id"]

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            original_timestamp = fetched.json()["address_last_updated"]
            assert original_timestamp is not None

            # PATCH city to null (clearing it)
            res = client.patch(
                f"/person/{person_id}",
                json={"city": None},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_202_ACCEPTED

            fetched = client.get(f"/person/{person_id}", headers=auth_headers)
            body = fetched.json()
            assert body["city"] is None
            # timestamp should be bumped, not just non-null
            new_timestamp = body["address_last_updated"]
            assert new_timestamp is not None
            assert new_timestamp >= original_timestamp

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
                person_id = res.json()["person_id"]
                fetched = client.get(f"/person/{person_id}", headers=auth_headers)
                assert fetched.json()["active_status"] == status_value
