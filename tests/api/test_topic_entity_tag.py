import copy
import datetime
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

TestSourceData = namedtuple('TestSourceData', ['response', 'new_source_id', 'new_source_name'])
TestTETData = namedtuple('TestTETData', ['response', 'new_tet_id', 'related_ref_curie'])


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

    def test_create_tag(self, test_topic_entity_tag, auth_headers): # noqa
        with TestClient(app) as client:
            assert test_topic_entity_tag.response.status_code == status.HTTP_201_CREATED

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
                "display_tag": None,
                "species": "NCBITaxon:6239"
            }
            for key, value in expected_fields.items():
                assert resp_data[key] == value
            assert resp_data["sources"][0]["validation_value_author"] is True

    def test_add_source_to_tag(self, test_topic_entity_tag, auth_headers, test_mod): # noqa
        with TestClient(app) as client:
            source_data = {
                "topic_entity_tag_id": test_topic_entity_tag.new_tet_id,
                "source": "SVM",
                "confidence_level": "high",
                "mod_abbreviation": test_mod.new_mod_abbreviation,
                "validation_value_author": True,
                "validation_value_curator": False,
                "validation_value_curation_tools": None,
                "note": "test note"
            }
            response = client.post("/topic_entity_tag/add_source", json=source_data, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED


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

    def test_validation(self, test_reference, test_mod, auth_headers): # noqa
        with TestClient(app) as client:
            topic_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",
                "sources": [
                    {
                        "source": "NN",
                        "negated": True,
                        "confidence_level": "NEG",
                        "mod_abbreviation": test_mod.new_mod_abbreviation,
                        "note": "test note"
                    },
                    {
                        "source": "author",
                        "negated": False,
                        "mod_abbreviation": test_mod.new_mod_abbreviation,
                        "note": "author said it's positive"
                    }
                ]
            }
            response = client.post(url="/topic_entity_tag/", json=topic_tag, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            new_topic_entity_tag_id = response.json()
            response = client.get(f"/topic_entity_tag/{new_topic_entity_tag_id}")
            assert response.status_code == status.HTTP_200_OK
            resp_data = response.json()
            assert any(source["validation_value_author"] is False for source in resp_data["sources"])

            curator_source_data = {
                "topic_entity_tag_id": new_topic_entity_tag_id,
                "source": "curator",
                "negated": True,
                "mod_abbreviation": test_mod.new_mod_abbreviation,
                "note": "curator said it's negative"
            }
            response = client.post("/topic_entity_tag/add_source", json=curator_source_data, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            curator_source_id = response.json()
            resp_data = client.get(f"/topic_entity_tag/{new_topic_entity_tag_id}").json()
            for source in resp_data["sources"]:
                if source["source"] == "NN":
                    assert source["validation_value_author"] is False and source["validation_value_curator"] is True
                elif source["source"] == "author":
                    assert source["validation_value_author"] is True and source["validation_value_curator"] is False
                elif source["source"] == "curator":
                    assert source["validation_value_author"] is False and source["validation_value_curator"] is True

            client.delete(f"/topic_entity_tag/delete_source/{curator_source_id}", headers=auth_headers)
            deleted_resp_data = client.get(f"/topic_entity_tag/{new_topic_entity_tag_id}").json()
            for source in deleted_resp_data["sources"]:
                if source["source"] == "NN":
                    assert source["validation_value_author"] is False and source["validation_value_curator"] is None
                elif source["source"] == "author":
                    assert source["validation_value_author"] is True and source["validation_value_curator"] is None

    @pytest.mark.webtest
    def test_get_map_entity_curie_to_name(self, test_topic_entity_tag, test_mod, auth_headers): # noqa
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
            alliance_topic_tag = {
                "reference_curie": test_topic_entity_tag.related_ref_curie,
                "topic": "ATP:0000009",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_source": "alliance",
                "species": "NCBITaxon:6239",
                "display_tag": "string",
                "sources": [{
                    "source": "WB_NN_1",
                    "confidence_level": "high",
                    "mod_abbreviation": test_mod.new_mod_abbreviation,
                    "note": "test note"
                }]
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
                "sources": [{
                    "source": "WB_NN_1",
                    "confidence_level": "high",
                    "mod_abbreviation": test_mod.new_mod_abbreviation,
                    "note": "test note"
                }]
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
