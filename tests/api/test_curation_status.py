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
        yield CurationStatusTestData(response, response.json(), test_reference.new_ref_curie, test_mod.new_mod_abbreviation)


class TestCurationStatus:

    def test_create(self, test_curation_status, auth_headers): # noqa
        with TestClient(app):
            assert test_curation_status.response.status_code == status.HTTP_201_CREATED

    @patch("agr_literature_service.api.crud.curation_status_crud.search_topic", patch_subset)
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
                "controlled_note": "ATP:cont_note",
                "note": "some notes"
            }
            url = f"/curation_status/{test_curation_status.new_curation_status_id}"
            response = client.patch(url=url, headers=auth_headers, json=patch_data)
            assert response.status_code == status.HTTP_202_ACCEPTED
            response = client.get(f"/curation_status/{test_curation_status.new_curation_status_id}")
            assert response.status_code == status.HTTP_200_OK
            resp_data = response.json()
            for key, value in patch_data.items():
                assert resp_data[key] == value
            assert resp_data["curation_status"] == "ATP:curation_needed"
