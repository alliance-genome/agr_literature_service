from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.crud.topic_entity_tag_utils import get_ancestors_or_descendants
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
            "topic_entity_tag_source_id": test_topic_entity_tag_source.new_source_id,
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
                "topic_entity_tag_source_id": -1,
                "negated": False,
                "note": "test note",
                "created_by": "WBPerson1",
                "date_created": "2020-01-01"
            }
            response = client.post(url="/topic_entity_tag/", json=new_tet, headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_create_empty_string(self, test_topic_entity_tag, test_topic_entity_tag_source, auth_headers): # noqa
        with TestClient(app) as client:
            new_tet = {
                "reference_curie": test_topic_entity_tag.related_ref_curie,
                "topic": "ATP:0000122",
                "entity_type": "",
                "entity": "WB:WBGene00003001",
                "entity_source": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": test_topic_entity_tag_source.new_source_id,
                "negated": False,
                "note": "test note",
                "created_by": "WBPerson1"
            }
            response = client.post(url="/topic_entity_tag/", json=new_tet, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

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

    @pytest.mark.webtest
    def test_validation(self, test_topic_entity_tag, test_reference, test_mod, auth_headers, db): # noqa
        with TestClient(app) as client:
            author_source_1 = {
                "source_type": "community curation",
                "source_method": "acknowledge",
                "validation_type": "author",
                "evidence": "test_eco_code",
                "description": "author from acknowledge",
                "mod_abbreviation": test_mod.new_mod_abbreviation
            }
            author_source_2 = {
                "source_type": "community curation",
                "source_method": "AFP",
                "validation_type": "author",
                "evidence": "test_eco_code",
                "description": "author from AFP",
                "mod_abbreviation": test_mod.new_mod_abbreviation
            }
            auth_source_1_resp = client.post(url="/topic_entity_tag/source", json=author_source_1, headers=auth_headers)
            auth_source_2_resp = client.post(url="/topic_entity_tag/source", json=author_source_2, headers=auth_headers)
            validating_tag_aut_1 = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000122",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_source": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": auth_source_1_resp.json(),
                "negated": True
            }
            validating_tag_aut_2 = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000122",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_source": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": auth_source_2_resp.json(),
                "negated": True
            }
            client.post(url="/topic_entity_tag/", json=validating_tag_aut_1, headers=auth_headers)
            client.post(url="/topic_entity_tag/", json=validating_tag_aut_2, headers=auth_headers)
            response = client.get(f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}")
            assert response.status_code == status.HTTP_200_OK
            tag_obj: TopicEntityTagModel = db.query(TopicEntityTagModel).filter(
                TopicEntityTagModel.topic_entity_tag_id == test_topic_entity_tag.new_tet_id
            ).one()
            assert len(tag_obj.validated_by) == 2
            response = client.get(f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}")
            assert response.json()["validation_value_author"] == "validated_wrong"

    @pytest.mark.webtest
    def test_validation_wrong(self, test_topic_entity_tag, test_reference, test_mod, auth_headers, db):  # noqa
        with TestClient(app) as client:
            curator_source = {
                "source_type": "curator",
                "source_method": "abc_literature_system",
                "validation_type": "curator",
                "evidence": "test_eco_code",
                "description": "curator from ABC",
                "mod_abbreviation": test_mod.new_mod_abbreviation
            }
            response = client.post(url="/topic_entity_tag/source", json=curator_source, headers=auth_headers)
            validating_tag_cur_1 = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000122",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_source": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": response.json(),
                "negated": True
            }
            validating_tag_cur_2 = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000122",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_source": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": response.json(),
                "negated": False
            }
            client.post(url="/topic_entity_tag/", json=validating_tag_cur_1, headers=auth_headers)
            cur_2_tag_id = client.post(url="/topic_entity_tag/", json=validating_tag_cur_2, headers=auth_headers).json()
            curation_tools_source = {
                "source_type": "curation",
                "source_method": "WB curation",
                "validation_type": "curation_tools",
                "evidence": "test_eco_code",
                "description": "curation from WB",
                "mod_abbreviation": test_mod.new_mod_abbreviation
            }
            response = client.post(url="/topic_entity_tag/source", json=curation_tools_source, headers=auth_headers)
            validating_tag_cur_tools_1 = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000122",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_source": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": response.json(),
                "negated": False
            }
            validating_tag_cur_tools_2 = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000122",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_source": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": response.json(),
                "negated": False
            }
            client.post(url="/topic_entity_tag/", json=validating_tag_cur_tools_1, headers=auth_headers)
            client.post(url="/topic_entity_tag/", json=validating_tag_cur_tools_2, headers=auth_headers)
            response = client.get(f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}")
            assert response.json()["validation_value_author"] == "not_validated"
            assert response.json()["validation_value_curator"] == "validation_conflict"
            assert response.json()["validation_value_curation_tools"] == "validated_right"
            response = client.get(f"/topic_entity_tag/{cur_2_tag_id}")
            assert response.json()["validation_value_author"] == "not_validated"
            assert response.json()["validation_value_curator"] == "validation_conflict"
            assert response.json()["validation_value_curation_tools"] == "validated_right"

    @pytest.mark.webtest
    def test_validate_generic_specific(self, test_topic_entity_tag, test_reference, test_mod, auth_headers, db): # noqa
        with TestClient(app) as client:
            author_source_1 = {
                "source_type": "community curation",
                "source_method": "acknowledge",
                "validation_type": "author",
                "evidence": "test_eco_code",
                "description": "author from acknowledge",
                "mod_abbreviation": test_mod.new_mod_abbreviation
            }
            author_source_2 = {
                "source_type": "manual_curation",
                "source_method": "abc_interface",
                "validation_type": "curator",
                "evidence": "test_eco_code",
                "description": "Curator using the ABC",
                "mod_abbreviation": test_mod.new_mod_abbreviation
            }
            auth_source_1_resp = client.post(url="/topic_entity_tag/source", json=author_source_1, headers=auth_headers)
            auth_source_2_resp = client.post(url="/topic_entity_tag/source", json=author_source_2, headers=auth_headers)
            more_generic_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",  # more generic topic
                "topic_entity_tag_source_id": auth_source_1_resp.json(),
                "negated": False
            }
            more_specific_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000084",  # made this more specific
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_source": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": auth_source_2_resp.json(),
                "negated": False
            }

            # add the new tags
            more_generic_tag_id = client.post(url="/topic_entity_tag/", json=more_generic_tag,
                                              headers=auth_headers).json()
            more_specific_tag_id = client.post(url="/topic_entity_tag/", json=more_specific_tag,
                                               headers=auth_headers).json()

            # next, we check if the validation process is correct. Supposed that your system recognizes more specific
            # tags validate more generic ones, so:
            generic_tag_obj: TopicEntityTagModel = db.query(TopicEntityTagModel).filter(
                TopicEntityTagModel.topic_entity_tag_id == more_generic_tag_id
            ).one()
            assert len(generic_tag_obj.validated_by) > 0
            assert int(more_specific_tag_id) in {tag.topic_entity_tag_id for tag in generic_tag_obj.validated_by}

            specific_tag_obj: TopicEntityTagModel = db.query(TopicEntityTagModel).filter(
                TopicEntityTagModel.topic_entity_tag_id == int(more_specific_tag_id)
            ).one()
            assert len(specific_tag_obj.validated_by) == 0  # nothing should validate the more specific tag

    @pytest.mark.webtest
    def test_get_map_entity_curie_to_name(self, test_topic_entity_tag, test_topic_entity_tag_source, test_mod, # noqa
                                          auth_headers): # noqa
        with TestClient(app) as client:
            topic_tag = {
                "reference_curie": test_topic_entity_tag.related_ref_curie,
                "topic": "ATP:0000009",
                "confidence_level": "high",
                "topic_entity_tag_source_id": test_topic_entity_tag_source.new_source_id,
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
                "topic_entity_tag_source_id": test_topic_entity_tag_source.new_source_id
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
                'WB:WBGene00003001': 'lin-12',
                'string': 'string'
            }
            wormbase_topic_tag = {
                "reference_curie": test_topic_entity_tag.related_ref_curie,
                "topic": "ATP:0000009",
                "entity_type": "ATP:0000099",
                "entity": "WB:WBTransgene0001",
                "entity_source": "wormbase",
                "species": "NCBITaxon:6239",
                "display_tag": "string",
                "topic_entity_tag_source_id": test_topic_entity_tag_source.new_source_id
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
                'WB:WBGene00003001': 'lin-12',
                'string': 'string'
            }

    @pytest.mark.webtest
    def test_get_ancestors(self, auth_headers):  # noqa
        onto_node = "ATP:0000079"
        ancestors = get_ancestors_or_descendants(onto_node)
        expected_ancestors = {"ATP:0000001", "ATP:0000002", "ATP:0000009"}
        assert [ancestor in expected_ancestors for ancestor in ancestors]

    @pytest.mark.webtest
    def test_get_descendants(self, auth_headers):  # noqa
        onto_node = "ATP:0000009"
        descendants = get_ancestors_or_descendants(onto_node, ancestors_or_descendants='descendants')
        expected_descendants = {'ATP:0000079', 'ATP:0000080', 'ATP:0000081', 'ATP:0000082', 'ATP:0000083',
                                'ATP:0000084', 'ATP:0000085', 'ATP:0000086', 'ATP:0000087', 'ATP:0000033',
                                'ATP:0000034', 'ATP:0000100'}
        assert [ancestor in expected_descendants for ancestor in descendants]

    @pytest.mark.webtest
    def test_get_ancestors_non_existent(self, auth_headers):  # noqa
        onto_node = "ATP:000007"
        ancestors = get_ancestors_or_descendants(onto_node)
        assert len(ancestors) == 0
