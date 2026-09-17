from collections import namedtuple
from unittest.mock import patch

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
# from agr_literature_service.api.models import CurationStatusModel
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from .test_mod import test_mod # noqa
from .test_reference import test_reference # noqa

CurationStatusTestData = namedtuple('CurationStatusTestData', ['response', 'new_curation_status_id', 'new_reference_curie', 'new_mod_abbreviation'])


def patch_subset(topic=None, mod_abbr: str = ""):
    return [{"curie": "ATP:curation_test", "name": "curation test"}, {"curie": "ATP:topic1", "name": "Topic 1"},
            {"curie": "ATP:topic2", "name": "Topic 2"}, {"curie": "ATP:topic3", "name": "Topic 3"},
            {"curie": "ATP:0000002", "name": "Paper level curation status"}]


topic_curie_to_name = {"ATP:curation_test": "curation test", "ATP:topic1": "Topic 1", "ATP:topic2": "Topic 2",
                       "ATP:topic3": "Topic 3", "ATP:0000002": "Paper level curation status"}


def patch_map_curies_to_names(category, curies):
    return topic_curie_to_name


@pytest.fixture
def test_curation_status(db, auth_headers, test_reference, test_mod): # noqa
    print("***** Adding a test curation_status *****")
    with TestClient(app) as client:
        new_curation_status = {
            "mod_abbreviation": test_mod.new_mod_abbreviation,
            "reference_curie": test_reference.new_ref_curie,
            "topic": "ATP:curation_test",
            "curation_status": "ATP:curation_needed",
        }
        response = client.post(url="/curation_status/", json=new_curation_status, headers=auth_headers)
        yield CurationStatusTestData(response, response.json()['curation_status_id'], test_reference.new_ref_curie, test_mod.new_mod_abbreviation)


class TestCurationStatus:

    def test_create(self, test_curation_status, auth_headers): # noqa
        with TestClient(app):
            assert test_curation_status.response.status_code == status.HTTP_201_CREATED

    @patch("agr_literature_service.api.crud.curation_status_crud.search_topic_list", patch_subset)
    @patch("agr_literature_service.api.crud.curation_status_crud.map_curies_to_names", patch_map_curies_to_names)
    def test_show_aggregated_curation_status_and_tet_info(self, test_curation_status, auth_headers): # noqa
        with TestClient(app) as client:
            url = (f"/curation_status/aggregated_curation_status_and_tet_info/{test_curation_status.new_reference_curie}/"
                   f"{test_curation_status.new_mod_abbreviation}")
            response = client.get(url=url, headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            res = response.json()
            assert len(res) == 5
            assert any([res_obj["topic_curie"] == "ATP:topic1" for res_obj in res])
            paper_level_curation_status = {
                "mod_abbreviation": test_curation_status.new_mod_abbreviation,
                "reference_curie": test_curation_status.new_reference_curie,
                "topic": "ATP:0000002",
                "curation_status": "ATP:0000237",
            }
            client.post(url="/curation_status/", json=paper_level_curation_status, headers=auth_headers)
            response = client.get(url=url, headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            res = response.json()
            assert len(res) == 5
            assert any([res_obj["topic_curie"] == "ATP:0000002" for res_obj in res])
            for res_obj in res:
                if res_obj["topic_curie"] == "ATP:0000002":
                    assert res_obj["curst_curation_status"] == "ATP:0000237"

    def test_show(self, test_curation_status, auth_headers): # noqa
        with TestClient(app) as client:
            url = f"/curation_status/{test_curation_status.new_curation_status_id}"
            print(url)
            response = client.get(url=url, headers=auth_headers)
            print(response)
            assert response.status_code == status.HTTP_200_OK
            res = response.json()
            assert res["topic"] == "ATP:curation_test"

    def test_patch(self, test_curation_status, auth_headers): # noqa
        with TestClient(app) as client:
            patch_data = {
                "curation_tag": "ATP:curation_tag",
                "note": "some notes"
            }
            url = f"/curation_status/{test_curation_status.new_curation_status_id}"
            response = client.patch(url=url, headers=auth_headers, json=patch_data)
            assert response.status_code == status.HTTP_200_OK
            response = client.get(f"/curation_status/{test_curation_status.new_curation_status_id}",
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            resp_data = response.json()
            for key, value in patch_data.items():
                assert resp_data[key] == value
            assert resp_data["curation_status"] == "ATP:curation_needed"

    def _make_source(self, client, auth_headers, mod_abbreviation,  # noqa
                     assertion="ATP:0000036", method="abc_literature_system"):
        """A tag_source to attribute writes to."""
        return client.post(url="/tag_source", headers=auth_headers, json={
            "source_evidence_assertion": assertion,
            "source_method": method,
            "validation_type": "professional_biocurator",
            "description": "a test source",
            "data_provider": mod_abbreviation,
            "secondary_data_provider_abbreviation": mod_abbreviation,
        }).json()["tag_source_id"]

    def test_post_without_tag_source_id_creates_no_association(self, test_curation_status,
                                                               auth_headers):  # noqa
        """The API never attributes a source by default."""
        with TestClient(app) as client:
            url = f"/curation_status/{test_curation_status.new_curation_status_id}/source_associations"
            response = client.get(url=url, headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert response.json() == []

    def test_post_with_tag_source_id_records_the_association(self, test_curation_status,
                                                             auth_headers):  # noqa
        with TestClient(app) as client:
            tag_source_id = self._make_source(client, auth_headers,
                                              test_curation_status.new_mod_abbreviation)
            response = client.post(url="/curation_status/", headers=auth_headers, json={
                "mod_abbreviation": test_curation_status.new_mod_abbreviation,
                "reference_curie": test_curation_status.new_reference_curie,
                "topic": "ATP:topic1",
                "curation_status": "ATP:curation_needed",
                "note": "reported by a loader",
                "tag_source_id": tag_source_id,
            })
            assert response.status_code == status.HTTP_201_CREATED
            curation_status_id = response.json()["curation_status_id"]
            associations = client.get(
                url=f"/curation_status/{curation_status_id}/source_associations",
                headers=auth_headers).json()
            assert len(associations) == 1
            assert associations[0]["tag_source_id"] == tag_source_id
            assert associations[0]["curation_status"] == "ATP:curation_needed"
            assert associations[0]["note"] == "reported by a loader"
            # the source's own fields are flattened in, so no second round trip
            assert associations[0]["source_method"] == "abc_literature_system"

    def test_patch_mirrors_only_the_fields_actually_sent(self, test_curation_status,
                                                         auth_headers):  # noqa
        """Patching the note must not blank this source's reported status."""
        with TestClient(app) as client:
            tag_source_id = self._make_source(client, auth_headers,
                                              test_curation_status.new_mod_abbreviation)
            curation_status_id = test_curation_status.new_curation_status_id
            response = client.patch(url=f"/curation_status/{curation_status_id}",
                                    headers=auth_headers,
                                    json={"note": "curator note", "tag_source_id": tag_source_id})
            assert response.status_code == status.HTTP_200_OK
            associations = client.get(
                url=f"/curation_status/{curation_status_id}/source_associations",
                headers=auth_headers).json()
            assert len(associations) == 1
            assert associations[0]["note"] == "curator note"
            # curation_status was not in the patch payload, so this source makes
            # no claim about it
            assert associations[0]["curation_status"] is None

    def test_patch_without_tag_source_id_creates_no_association(self, test_curation_status,
                                                                auth_headers):  # noqa
        with TestClient(app) as client:
            curation_status_id = test_curation_status.new_curation_status_id
            response = client.patch(url=f"/curation_status/{curation_status_id}",
                                    headers=auth_headers, json={"note": "unattributed"})
            assert response.status_code == status.HTTP_200_OK
            associations = client.get(
                url=f"/curation_status/{curation_status_id}/source_associations",
                headers=auth_headers).json()
            assert associations == []

    def test_post_with_unknown_tag_source_id_404s_and_creates_nothing(self, test_curation_status,
                                                                      auth_headers):  # noqa
        """The base row and its attribution are one transaction.

        Regression: the attribution used to be validated after the base row was
        committed, so a typo'd tag_source_id returned 404 having created the
        row anyway - and the caller's retry then hit the (topic, reference_id,
        mod_id) unique constraint and got a confusing 422.
        """
        with TestClient(app) as client:
            payload = {
                "mod_abbreviation": test_curation_status.new_mod_abbreviation,
                "reference_curie": test_curation_status.new_reference_curie,
                "topic": "ATP:topic3",
                "curation_status": "ATP:curation_needed",
                "tag_source_id": 99999999,
            }
            response = client.post(url="/curation_status/", headers=auth_headers, json=payload)
            assert response.status_code == status.HTTP_404_NOT_FOUND
            # nothing was left behind: the same POST without the bad id must succeed
            del payload["tag_source_id"]
            retry = client.post(url="/curation_status/", headers=auth_headers, json=payload)
            assert retry.status_code == status.HTTP_201_CREATED

    def test_patch_with_unknown_tag_source_id_404s_and_changes_nothing(self, test_curation_status,
                                                                       auth_headers):  # noqa
        """Same transaction guarantee on the PATCH path."""
        with TestClient(app) as client:
            curation_status_id = test_curation_status.new_curation_status_id
            response = client.patch(url=f"/curation_status/{curation_status_id}",
                                    headers=auth_headers,
                                    json={"note": "should not stick",
                                          "tag_source_id": 99999999})
            assert response.status_code == status.HTTP_404_NOT_FOUND
            base = client.get(url=f"/curation_status/{curation_status_id}",
                              headers=auth_headers).json()
            assert base["note"] is None
