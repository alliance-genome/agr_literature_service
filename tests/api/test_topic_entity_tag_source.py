from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.lit_processing.utils.okta_utils import get_authentication_token
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from .test_mod import test_mod # noqa

TestSourceData = namedtuple('TestSourceData', ['response', 'new_source_id', 'new_source_name'])


@pytest.fixture
def test_topic_entity_tag_source(db, auth_headers, test_mod): # noqa
    print("***** Adding a test tag source *****")
    with TestClient(app) as client:
        new_source = {
            "source_name": "neural_network_phenotype",
            "evidence": "test_eco_code",
            "description": "a test source",
            "mod_abbreviation": test_mod.new_mod_abbreviation,
            "created_by": "somebody"
        }
        response = client.post(url="/topic_entity_tag/source", json=new_source, headers=auth_headers)
        yield TestSourceData(response, response.json(), new_source["source_name"])


class TestTopicEntityTagSource:

    def test_create_source(self, test_topic_entity_tag_source, test_mod, auth_headers): # noqa
        with TestClient(app):
            assert test_topic_entity_tag_source.response.status_code == status.HTTP_201_CREATED

    def test_show_source(self, test_topic_entity_tag_source, test_mod): # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/topic_entity_tag/source/{test_topic_entity_tag_source.new_source_id}")
            assert response.status_code == status.HTTP_200_OK
            res_obj = response.json()
            assert res_obj["source_name"] == "neural_network_phenotype"
            assert res_obj["evidence"] == "test_eco_code"
            assert res_obj["description"] == "a test source"
            assert res_obj["mod_abbreviation"] == test_mod.new_mod_abbreviation

    def test_patch_source(self, test_topic_entity_tag_source, auth_headers): # noqa
        with TestClient(app) as client:
            patch_data = {
                "source_name": "test_patch_name",
                "created_by": "me"
            }
            response = client.patch(url=f"/topic_entity_tag/source/{test_topic_entity_tag_source.new_source_id}",
                                    json=patch_data, headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED
            response = client.get(url=f"/topic_entity_tag/source/{test_topic_entity_tag_source.new_source_id}")
            assert response.json()["source_name"] == "test_patch_name"
            assert response.json()["created_by"] == "me"

    def test_destroy_source(self, test_topic_entity_tag_source, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.delete(f"/topic_entity_tag/source/{test_topic_entity_tag_source.new_source_id}",
                                     headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT
            response = client.get(f"/topic_entity_tag/source/{test_topic_entity_tag_source.new_source_id}")
            assert response.status_code == status.HTTP_404_NOT_FOUND