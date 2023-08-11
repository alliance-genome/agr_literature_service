from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import TopicEntityTagModel
from agr_literature_service.lit_processing.utils.okta_utils import get_authentication_token
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from .test_reference import test_reference # noqa
from .test_mod import test_mod # noqa
from .test_topic_entity_tag_source import test_topic_entity_tag_source # noqa

test_reference2 = test_reference

TestTETData = namedtuple('TestTETData', ['response', 'new_tet_id', 'related_ref_curie'])


@pytest.fixture
def test_topic_entity_tag(db, auth_headers, test_reference, test_topic_entity_tag_source, test_mod): # noqa
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
            "source_name": test_topic_entity_tag_source.new_source_name,
            "mod_abbreviation": test_mod.new_mod_abbreviation,
            "negated": False,
            "note": "test note",
            "created_by": "WBPerson1",
            "date_created": "2020-01-01"
        }
        response = client.post(url="/topic_entity_tag/", json=new_tet, headers=auth_headers)
        yield TestTETData(response, response.json(), test_reference.new_ref_curie)


class TestTopicEntityTag:

    def test_create(self, test_topic_entity_tag, auth_headers): # noqa
        with TestClient(app):
            assert test_topic_entity_tag.response.status_code == status.HTTP_201_CREATED

    def test_create_wrong_source(self, test_topic_entity_tag, auth_headers):  # noqa
        with TestClient(app) as client:
            new_tet = {
                "reference_curie": test_topic_entity_tag.related_ref_curie,
                "topic": "ATP:0000122",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_source": "alliance",
                "entity_published_as": "test",
                "species": "NCBITaxon:6239",
                "source_name": "not_there",
                "mod_abbreviation": "who_knows",
                "negated": False,
                "note": "test note",
                "created_by": "WBPerson1",
                "date_created": "2020-01-01"
            }
            response = client.post(url="/topic_entity_tag/", json=new_tet, headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_show(self, test_topic_entity_tag): # noqa
        with TestClient(app) as client:
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
                "display_tag": None,
                "species": "NCBITaxon:6239"
            }
            for key, value in expected_fields.items():
                assert resp_data[key] == value

    def test_patch(self, test_topic_entity_tag, auth_headers): # noqa
        with TestClient(app) as client:
            patch_data = {
                "topic": "new_topic",
                "entity_type": "new_type",
                "entity": "new_entity",
                "updated_by": "new_user"
            }
            response = client.patch(f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}", headers=auth_headers,
                                    json=patch_data)
            assert response.status_code == status.HTTP_202_ACCEPTED
            response = client.get(f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}")
            resp_data = response.json()
            for key, value in patch_data.items():
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

    def test_validation(self, test_topic_entity_tag, test_reference, test_mod, auth_headers, db): # noqa
        with TestClient(app) as client:
            author_source = {
                "source_name": "author_acknowledge",
                "evidence": "test_eco_code",
                "description": "author from acknowledge",
                "mod_abbreviation": test_mod.new_mod_abbreviation
            }
            client.post(url="/topic_entity_tag/source", json=author_source, headers=auth_headers)
            validating_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000122",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_source": "alliance",
                "species": "NCBITaxon:6239",
                "source_name": "author_acknowledge",
                "mod_abbreviation": test_mod.new_mod_abbreviation,
                "negated": True
            }
            client.post(url="/topic_entity_tag/", json=validating_tag, headers=auth_headers)
            response = client.get(f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}")
            assert response.status_code == status.HTTP_200_OK
            tag_obj: TopicEntityTagModel = db.query(TopicEntityTagModel).filter(
                TopicEntityTagModel.topic_entity_tag_id == test_topic_entity_tag.new_tet_id
            ).one()
            assert len(tag_obj.validated_by) > 0

    @pytest.mark.webtest
    def test_get_map_entity_curie_to_name(self, test_topic_entity_tag, test_topic_entity_tag_source, test_mod, # noqa
                                          auth_headers): # noqa
        with TestClient(app) as client:
            topic_tag = {
                "reference_curie": test_topic_entity_tag.related_ref_curie,
                "topic": "ATP:0000009",
                "confidence_level": "high",
                "source_name": test_topic_entity_tag_source.new_source_name,
                "mod_abbreviation": test_mod.new_mod_abbreviation
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
            alliance_topic_tag = {
                "reference_curie": test_topic_entity_tag.related_ref_curie,
                "topic": "ATP:0000009",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_source": "alliance",
                "species": "NCBITaxon:6239",
                "display_tag": "string",
                "source_name": test_topic_entity_tag_source.new_source_name,
                "mod_abbreviation": test_mod.new_mod_abbreviation
            }
            client.post(url="/topic_entity_tag/", json=alliance_topic_tag, headers=auth_headers)
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
            wormbase_topic_tag = {
                "reference_curie": test_topic_entity_tag.related_ref_curie,
                "topic": "ATP:0000009",
                "entity_type": "ATP:0000099",
                "entity": "WB:WBTransgene0001",
                "entity_source": "wormbase",
                "species": "NCBITaxon:6239",
                "display_tag": "string",
                "source_name": test_topic_entity_tag_source.new_source_name,
                "mod_abbreviation": test_mod.new_mod_abbreviation
            }
            client.post(url="/topic_entity_tag/", json=wormbase_topic_tag, headers=auth_headers)
            response = client.get(url="/topic_entity_tag/map_entity_curie_to_name/",
                                  params={"curie_or_reference_id": test_topic_entity_tag.related_ref_curie,
                                          "token": get_authentication_token()},
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert response.json() == {
                'ATP:0000005': 'gene',
                'ATP:0000009': 'phenotype',
                'ATP:0000099': 'existing transgenic construct',
                'ATP:0000122': 'entity type',
                'WB:WBGene00003001': 'lin-12'
            }
