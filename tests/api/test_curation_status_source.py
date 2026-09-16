"""Tests for curation status source attribution (SCRUM-6518).

The invariant under test throughout: associations are INDEPENDENT of the
curation_status row. Writing one never changes the base row, and a source may
report something the base row disagrees with.
"""
import pytest
from fastapi import status
from starlette.testclient import TestClient

from agr_literature_service.api.main import app
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from .test_curation_status import test_curation_status # noqa
from .test_mod import test_mod # noqa
from .test_reference import test_reference # noqa


@pytest.fixture
def loader_source(auth_headers, test_mod): # noqa
    """A non-ABC source, standing in for a loader."""
    with TestClient(app) as client:
        response = client.post(url="/tag_source", headers=auth_headers, json={
            "source_evidence_assertion": "ECO:0008025",
            "source_method": "a loader",
            "validation_type": None,
            "description": "a loader source",
            "data_provider": test_mod.new_mod_abbreviation,
            "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation,
        })
        yield response.json()["tag_source_id"]


class TestCurationStatusSourceAssociation:

    def test_add_source_association(self, test_curation_status, loader_source, auth_headers): # noqa
        with TestClient(app) as client:
            response = client.post(url="/curation_status/add_source_association",
                                   headers=auth_headers, json={
                                       "curation_status_id": test_curation_status.new_curation_status_id,
                                       "tag_source_id": loader_source,
                                       "curation_status": "ATP:curation_not_needed",
                                       "note": "loader says no",
                                   })
            assert response.status_code == status.HTTP_201_CREATED
            body = response.json()
            assert body["tag_source_id"] == loader_source
            assert body["curation_status"] == "ATP:curation_not_needed"
            assert body["source_method"] == "a loader"

    def test_it_never_auto_creates_the_anchor_row(self, loader_source, auth_headers): # noqa
        """404 when no curation_status row exists, and nothing is created."""
        with TestClient(app) as client:
            response = client.post(url="/curation_status/add_source_association",
                                   headers=auth_headers, json={
                                       "curation_status_id": 99999999,
                                       "tag_source_id": loader_source,
                                       "curation_status": "ATP:curation_not_needed",
                                   })
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_unknown_tag_source_404s(self, test_curation_status, auth_headers): # noqa
        with TestClient(app) as client:
            response = client.post(url="/curation_status/add_source_association",
                                   headers=auth_headers, json={
                                       "curation_status_id": test_curation_status.new_curation_status_id,
                                       "tag_source_id": 99999999,
                                   })
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_a_source_may_disagree_without_touching_the_base_row(self, test_curation_status,  # noqa
                                                                 loader_source, auth_headers):  # noqa
        """Nothing is reconciled at write time."""
        with TestClient(app) as client:
            curation_status_id = test_curation_status.new_curation_status_id
            client.post(url="/curation_status/add_source_association", headers=auth_headers, json={
                "curation_status_id": curation_status_id,
                "tag_source_id": loader_source,
                "curation_status": "ATP:curation_not_needed",
            })
            base = client.get(url=f"/curation_status/{curation_status_id}",
                              headers=auth_headers).json()
            assert base["curation_status"] == "ATP:curation_needed"

    def test_re_reporting_upserts_rather_than_duplicating(self, test_curation_status,  # noqa
                                                          loader_source, auth_headers):  # noqa
        with TestClient(app) as client:
            curation_status_id = test_curation_status.new_curation_status_id
            payload = {
                "curation_status_id": curation_status_id,
                "tag_source_id": loader_source,
                "curation_status": "ATP:curation_not_needed",
            }
            client.post(url="/curation_status/add_source_association",
                        headers=auth_headers, json=payload)
            payload["curation_status"] = "ATP:curation_needed"
            client.post(url="/curation_status/add_source_association",
                        headers=auth_headers, json=payload)
            associations = client.get(
                url=f"/curation_status/{curation_status_id}/source_associations",
                headers=auth_headers).json()
            assert len(associations) == 1
            assert associations[0]["curation_status"] == "ATP:curation_needed"

    def test_patch_and_delete_one_association(self, test_curation_status, loader_source,  # noqa
                                              auth_headers):  # noqa
        with TestClient(app) as client:
            curation_status_id = test_curation_status.new_curation_status_id
            created = client.post(url="/curation_status/add_source_association",
                                  headers=auth_headers, json={
                                      "curation_status_id": curation_status_id,
                                      "tag_source_id": loader_source,
                                      "curation_status": "ATP:curation_not_needed",
                                  }).json()
            association_id = created["curation_status_source_association_id"]

            response = client.patch(url=f"/curation_status/source_association/{association_id}",
                                    headers=auth_headers, json={"note": "revised"})
            assert response.status_code == status.HTTP_200_OK
            assert response.json()["note"] == "revised"
            # the patch touched only the note
            assert response.json()["curation_status"] == "ATP:curation_not_needed"

            response = client.delete(url=f"/curation_status/source_association/{association_id}",
                                     headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT
            associations = client.get(
                url=f"/curation_status/{curation_status_id}/source_associations",
                headers=auth_headers).json()
            assert associations == []

    def test_patch_nonexistent_association_404s(self, auth_headers): # noqa
        with TestClient(app) as client:
            response = client.patch(url="/curation_status/source_association/99999999",
                                    headers=auth_headers, json={"note": "anything"})
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_delete_every_association_for_one_source(self, test_curation_status, loader_source,  # noqa
                                                     auth_headers):  # noqa
        """Wipe-and-reload: the curation_status rows survive."""
        with TestClient(app) as client:
            curation_status_id = test_curation_status.new_curation_status_id
            client.post(url="/curation_status/add_source_association", headers=auth_headers, json={
                "curation_status_id": curation_status_id,
                "tag_source_id": loader_source,
                "curation_status": "ATP:curation_not_needed",
            })
            response = client.delete(url=f"/curation_status/source/{loader_source}",
                                     headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT
            associations = client.get(
                url=f"/curation_status/{curation_status_id}/source_associations",
                headers=auth_headers).json()
            assert associations == []
            base = client.get(url=f"/curation_status/{curation_status_id}",
                              headers=auth_headers).json()
            assert base["curation_status"] == "ATP:curation_needed"

    def test_source_associations_for_unknown_row_404s(self, auth_headers): # noqa
        with TestClient(app) as client:
            response = client.get(url="/curation_status/99999999/source_associations",
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_deleting_the_curation_status_removes_its_associations(self, test_curation_status,  # noqa
                                                                   loader_source,
                                                                   auth_headers):  # noqa
        """The FK cascade, so a deleted row leaves no orphan attributions."""
        with TestClient(app) as client:
            curation_status_id = test_curation_status.new_curation_status_id
            client.post(url="/curation_status/add_source_association", headers=auth_headers, json={
                "curation_status_id": curation_status_id,
                "tag_source_id": loader_source,
                "curation_status": "ATP:curation_not_needed",
            })
            response = client.delete(url=f"/curation_status/{curation_status_id}",
                                     headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT
            response = client.get(url=f"/curation_status/{curation_status_id}/source_associations",
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND
