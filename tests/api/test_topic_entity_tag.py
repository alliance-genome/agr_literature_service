import copy
from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.lit_processing.utils.okta_utils import get_authentication_token
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from .test_reference import test_reference # noqa
from .test_mod import test_mod # noqa

test_reference2 = test_reference

TestTETData = namedtuple('TestTETData', ['response', 'new_tet_id', 'related_ref_curie'])


@pytest.fixture
def test_topic_entity_tag(db, auth_headers, test_reference, test_mod): # noqa
    print("***** Adding a test tag *****")
    with TestClient(app) as client:
        new_tet = {
            "reference_curie": test_reference.new_ref_curie,
            "topic": "ATP:0000122",
            "entity_type": "ATP:0000005",
            "entity": "WB:WBGene00003001",
            "entity_source": "alliance",
            "entity_published_as": "test",
            "species": "NCBITaxon:6239",
            "sources": [{
                "source": "WB_NN_1",
                "confidence_level": "high",
                "mod_abbreviation": test_mod.new_mod_abbreviation,
                "note": "test note"
            }]
        }
        response = client.post(url="/topic_entity_tag/", json=new_tet, headers=auth_headers)
        yield TestTETData(response, response.json(), test_reference.new_ref_curie)


class TestTopicEntityTag:

    def test_create(self, test_topic_entity_tag, test_mod, auth_headers): # noqa
        with TestClient(app) as client:
            assert test_topic_entity_tag.response.status_code == status.HTTP_201_CREATED

            topic_tag = {
                "reference_curie": test_topic_entity_tag.related_ref_curie,
                "topic": "Topic1",
                "sources": [{
                    "source": "WB_NN_1",
                    "confidence_level": "high",
                    "mod_abbreviation": test_mod.new_mod_abbreviation,
                    "note": "test note"
                }]
            }

            response = client.post(url="/topic_entity_tag/", json=topic_tag, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED

    def test_create_duplicate_different_source(self, test_topic_entity_tag, test_mod, auth_headers): # noqa
        with TestClient(app) as client:
            xml = {
                "reference_curie": test_topic_entity_tag.related_ref_curie,
                "topic": "ATP:0000122",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_source": "alliance",
                "entity_published_as": "test",
                "species": "NCBITaxon:6239",
                "sources": [{
                    "source": "WB_NN_1",
                    "confidence_level": "high",
                    "mod_abbreviation": test_mod.new_mod_abbreviation,
                    "note": "test note"
                }]
            }

            xml0 = copy.deepcopy(xml)
            xml0["sources"][0]["source"] = "WB_SVM"
            response = client.post(url="/topic_entity_tag/", json=xml0, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED

    def test_create_wrong(self, test_topic_entity_tag, test_mod, auth_headers):  # noqa
        with TestClient(app) as client:
            xml = {
                "reference_curie": test_topic_entity_tag.related_ref_curie,
                "topic": "ATP:0000122",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_source": "alliance",
                "species": "NCBITaxon:6239",
                "sources": [{
                    "source": "WB_NN_1",
                    "confidence_level": "high",
                    "mod_abbreviation": test_mod.new_mod_abbreviation,
                    "note": "test note"
                }]
            }

            # No sources
            xml1 = copy.deepcopy(xml)
            del xml1["sources"]
            response = client.post(url="/topic_entity_tag/", json=xml1, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            # No Entities
            xml2 = copy.deepcopy(xml)
            del xml2["entity"]
            response = client.post(url="/topic_entity_tag/", json=xml2, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            # No curie
            xml3 = copy.deepcopy(xml)
            del xml3["reference_curie"]
            response = client.post(url="/topic_entity_tag/", json=xml3, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            # Bad curie
            xml4 = copy.deepcopy(xml)
            xml4["reference_curie"] = "BADCURIE"
            response = client.post(url="/topic_entity_tag/", json=xml4, headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

            # Entity tag without species
            xml5 = copy.deepcopy(xml)
            del xml5["species"]
            response = client.post(url="/topic_entity_tag/", json=xml5, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            # Duplicate tag
            xml6 = copy.deepcopy(xml)
            xml6["topic"] = "ATP:0000122"
            xml6["entity_type"] = "ATP:0000005"
            xml6["entity"] = "WB:WBGene00003001"
            xml6["entity_source"] = "alliance"
            response = client.post(url="/topic_entity_tag/", json=xml6, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_show(self, test_topic_entity_tag):
        with TestClient(app) as client:

            # Test the show function
            response = client.get(f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}")
            assert response.status_code == status.HTTP_200_OK
            resp_data = response.json()
            expected_fields = {
                "topic_entity_tag_id": int(test_topic_entity_tag.new_tet_id),
                "reference_curie": test_topic_entity_tag.related_ref_curie,
                "topic": "ATP:0000122",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_source": "alliance",
                "entity_published_as": "test",
                "species": "NCBITaxon:6239"
            }
            for key, value in expected_fields.items():
                assert resp_data[key] == value

    def test_add_source_to_tag(self, test_topic_entity_tag, auth_headers, test_mod): # noqa
        with TestClient(app) as client:
            source_data = {
                "topic_entity_tag_id": test_topic_entity_tag.new_tet_id,
                "source": "SVM",
                "confidence_level": "high",
                "mod_abbreviation": test_mod.new_mod_abbreviation,
                "note": "test note"
            }
            response = client.post("/topic_entity_tag/add_source", json=source_data, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED

    def test_destroy_source(self, test_topic_entity_tag, auth_headers): # noqa
        with TestClient(app) as client:
            response = client.get(f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}")
            topic_entity_source_id = response.json()["sources"][0]["topic_entity_tag_source_id"]
            response = client.delete(f"/topic_entity_tag/delete_source/{topic_entity_source_id}", headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT
            response = client.get(f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}")
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_patch_source(self, test_topic_entity_tag, auth_headers): # noqa
        with TestClient(app) as client:
            source_patch = {
                "source": "SVM",
                "confidence_level": "low",
                "note": "new note"
            }
            response = client.get(f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}")
            source_id = response.json()["sources"][0]["topic_entity_tag_source_id"]
            response = client.patch(f"/topic_entity_tag/source/{source_id}", json=source_patch, headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED

    def test_get_all_reference_tags(self, auth_headers): # noqa
        with TestClient(app) as client:
            reference_data = {
                "category": "research_article",
                "abstract": "The Hippo (Hpo) pathway is a conserved tumor suppressor pathway",
                "date_published_start": "2022-10-01 00:00:01",
                "date_published_end": "2022-10-02T00:00:01",
                "title": "Some test 001 title",
                "authors": [
                    {
                        "order": 2,
                        "first_name": "S.",
                        "last_name": "Wu",
                        "name": "S. Wu",
                        "first_initial": "S"
                    },
                    {
                        "order": 1,
                        "first_name": "D.",
                        "last_name": "Wu",
                        "name": "D. Wu",
                        "first_initial": "D"
                    }
                ],
                "topic_entity_tags": [
                    {
                        "topic": "ATP:0000122",
                        "entity_type": "ATP:0000005",
                        "entity": "WB:WBGene00003001",
                        "entity_source": "alliance",
                        "species": "NCBITaxon:6239"
                    }
                ]
            }

            new_curie = client.post(url="/reference/", json=reference_data, headers=auth_headers).json()
            response = client.get(url=f"/topic_entity_tag/by_reference/{new_curie}").json()
            assert len(response) > 0

    @pytest.mark.webtest
    def test_get_map_entity_curie_to_name(self, test_topic_entity_tag, test_mod, auth_headers):
        with TestClient(app) as client:
            topic_tag = {
                "reference_curie": test_topic_entity_tag.related_ref_curie,
                "topic": "ATP:0000009",
                "sources": [{
                    "source": "WB_NN_1",
                    "confidence_level": "high",
                    "mod_abbreviation": test_mod.new_mod_abbreviation,
                    "note": "test note"
                }]
            }
            client.post(url="/topic_entity_tag/", json=topic_tag, headers=auth_headers)
            response = client.get(url="/topic_entity_tag/map_entity_curie_to_name/",
                                  params={"curie_or_reference_id": test_topic_entity_tag.related_ref_curie,
                                          "token": get_authentication_token()},
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert response.json() == {
                'ATP:0000005': 'gene',
                'ATP:0000009': 'phenotype',
                'ATP:0000122': 'entity type',
                'WB:WBGene00003001': 'lin-12'
            }
