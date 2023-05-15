import copy
from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from .test_reference import test_reference # noqa

test_reference2 = test_reference

TestTETData = namedtuple('TestTETData', ['response', 'new_tet_id', 'related_ref_curie'])


@pytest.fixture
def test_topic_entity_tag(db, auth_headers, test_reference): # noqa
    print("***** Adding a test tag *****")
    with TestClient(app) as client:
        new_tet = {
            "reference_curie": test_reference.new_ref_curie,
            "topic": "Topic1",
            "entity_type": "Gene",
            "entity": "Bob_gene_name",
            "entity_source": "alliance",
            "entity_published_as": "test",
            "species": "NCBITaxon:1234"
        }
        response = client.post(url="/topic_entity_tag/", json=new_tet, headers=auth_headers)
        yield TestTETData(response, response.json(), test_reference.new_ref_curie)


class TestTopicEntityTag:

    def test_create(self, test_topic_entity_tag, auth_headers): # noqa
        with TestClient(app) as client:

            # valid create
            assert test_topic_entity_tag.response.status_code == status.HTTP_201_CREATED

            # Invalid create cases
            xml = {
                "reference_curie": test_topic_entity_tag.related_ref_curie,
                "topic": "Topic1",
                "entity": "Gene",
                "entity_source": "alliance",
                "species": "NCBITaxon:1234"
            }

            # No Entities
            response = client.post(url="/topic_entity_tag/", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            # No curie
            xml2 = copy.deepcopy(xml)
            del xml2["reference_curie"]
            response = client.post(url="/topic_entity_tag/", json=xml2, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            # Bad curie
            xml3 = copy.deepcopy(xml2)
            xml3["reference_curie"] = "BADCURIE"
            response = client.post(url="/topic_entity_tag/", json=xml3, headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

            # No species
            xml4 = copy.deepcopy(xml3)
            del xml4["species"]
            xml4["reference_curie"] = test_topic_entity_tag.related_ref_curie
            response = client.post(url="/topic_entity_tag/", json=xml4, headers=auth_headers)
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
                "topic": "Topic1",
                "entity_type": "Gene",
                "entity": "Bob_gene_name",
                "entity_source": "alliance",
                "entity_published_as": "test",
                "species": "NCBITaxon:1234"
            }
            for key, value in expected_fields.items():
                assert resp_data[key] == value

    def test_patch(self, test_topic_entity_tag, auth_headers): # noqa
        with TestClient(app) as client:
            new_data = {
                "topic": "Topic_new",
                "entity": "Gene2",
                "entity_source": "mod"
            }
            response = client.patch(url=f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}", json=new_data,
                                    headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED
            response = client.get(f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}")
            resp_data = response.json()
            for key, value in new_data.items():
                assert resp_data[key] == value

    def test_destroy(self, test_topic_entity_tag, auth_headers): # noqa
        with TestClient(app) as client:
            response = client.delete(f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}", headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT
            response = client.get(f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}")
            assert response.status_code == status.HTTP_404_NOT_FOUND

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
                        "name": "S. Wu"
                    },
                    {
                        "order": 1,
                        "first_name": "D.",
                        "last_name": "Wu",
                        "name": "D. Wu"
                    }
                ],
                "topic_entity_tags": [
                    {
                        "topic": "string",
                        "entity_type": "string",
                        "entity": "string",
                        "entity_source": "alliance",
                        "species": "string"
                    }
                ]
            }

            new_curie = client.post(url="/reference/", json=reference_data, headers=auth_headers).json()
            response = client.get(url=f"/topic_entity_tag/by_reference/{new_curie}").json()
            assert len(response) > 0
