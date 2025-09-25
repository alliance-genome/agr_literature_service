from collections import namedtuple
from unittest.mock import patch

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.crud.topic_entity_tag_utils import get_ancestors, get_descendants
from agr_literature_service.api.main import app
from agr_literature_service.api.models import TopicEntityTagModel
from fastapi_okta.okta_utils import get_authentication_token
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from .test_reference import test_reference # noqa
from .test_mod import test_mod # noqa
from .test_topic_entity_tag_source import test_topic_entity_tag_source # noqa
from ..fixtures import load_name_to_atp_and_relationships_mock

test_reference2 = test_reference

TestTETData = namedtuple('TestTETData', ['response', 'new_tet_id', 'related_ref_curie'])


@pytest.fixture

def test_topic_entity_tag(db, auth_headers, test_reference, test_topic_entity_tag_source, test_mod): # noqa
    load_name_to_atp_and_relationships_mock()
    with TestClient(app) as client:
        new_tet = {
            "reference_curie": test_reference.new_ref_curie,
            "topic": "ATP:0000122",
            "entity_type": "ATP:0000005",
            "entity": "WB:WBGene00003001",
            "entity_id_validation": "alliance",
            "entity_published_as": "test",
            "species": "NCBITaxon:6239",
            "topic_entity_tag_source_id": test_topic_entity_tag_source.new_source_id,
            "negated": False,
            "data_novelty": "ATP:0000334",
            "note": "test note",
            "created_by": "WBPerson1",
            "date_created": "2020-01-01"
        }
        response = client.post(url="/topic_entity_tag/", json=new_tet, headers=auth_headers)
        yield TestTETData(response, response.json()['topic_entity_tag_id'], test_reference.new_ref_curie)


class TestTopicEntityTag:

    def test_create(self, test_topic_entity_tag, auth_headers): # noqa
        with TestClient(app) as client:
            assert test_topic_entity_tag.response.status_code == status.HTTP_201_CREATED
            response = client.get(f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}")
            assert response.json()["created_by"] == "WBPerson1"

    def test_create_wrong_source(self, test_topic_entity_tag, auth_headers):  # noqa
        with TestClient(app) as client:
            new_tet = {
                "reference_curie": test_topic_entity_tag.related_ref_curie,
                "topic": "ATP:0000122",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_id_validation": "alliance",
                "entity_published_as": "test",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": -1,
                "negated": False,
                "data_novelty": "ATP:0000334",
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
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": test_topic_entity_tag_source.new_source_id,
                "negated": False,
                "data_novelty": "ATP:0000334",
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
                "entity_id_validation": "alliance",
                "entity_published_as": "test",
                "display_tag": None,
                "data_novelty": "ATP:0000334",
                "negated": False,
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
                "entity_id_validation": "alliance",
                "entity_published_as": "test",
                "display_tag": None,
                "data_novelty": "ATP:0000334",
                "negated": False,
                "species": "NCBITaxon:6239"
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

    def test_create_tet_creates_mca_and_workflow(self, db, auth_headers, test_reference, test_topic_entity_tag_source, test_mod): # noqa
        load_name_to_atp_and_relationships_mock()
        with TestClient(app) as client:
            assert test_reference.response.status_code == status.HTTP_201_CREATED
            response = client.get(url=f"/reference/{test_reference.new_ref_curie}")
            assert response.status_code == status.HTTP_200_OK
            res = response.json()
            assert res['workflow_tags'] == []
            assert res['mod_corpus_associations'] is None
            new_tet = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000122",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_id_validation": "alliance",
                "entity_published_as": "test",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": test_topic_entity_tag_source.new_source_id,
                "negated": False,
                "data_novelty": "ATP:0000334",
                "note": "test note",
                "created_by": "WBPerson1",
                "date_created": "2020-01-01"
            }
            client.post(url="/topic_entity_tag/", json=new_tet, headers=auth_headers)
            new_response = client.get(url=f"/reference/{test_reference.new_ref_curie}")
            assert new_response.status_code == status.HTTP_200_OK
            res = new_response.json()
            assert res['workflow_tags'][0]['mod_abbreviation'] == test_mod.new_mod_abbreviation
            assert res['workflow_tags'][0]['workflow_tag_id'] == 'ATP:0000141'
            assert res['mod_corpus_associations'][0]['mod_abbreviation'] == test_mod.new_mod_abbreviation
            assert res['mod_corpus_associations'][0]['mod_corpus_sort_source'] == 'manual_creation'
            assert res['mod_corpus_associations'][0]['corpus'] is True

    def test_get_all_reference_tags(self, auth_headers, test_topic_entity_tag_source): # noqa
        with TestClient(app) as client, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_curie_to_name_from_all_tets") as \
                mock_get_curie_to_name_from_all_tets:
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
                        "entity_id_validation": "alliance",
                        "species": "NCBITaxon:6239",
                        "topic_entity_tag_source_id": test_topic_entity_tag_source.new_source_id
                    }
                ]
            }
            new_ref_req = client.post(url="/reference/", json=reference_data, headers=auth_headers)
            assert new_ref_req.status_code == status.HTTP_201_CREATED
            new_curie = new_ref_req.json()
            assert new_curie.startswith("AGRKB:")
            mock_get_curie_to_name_from_all_tets.return_value = {
                'ATP:0000009': 'phenotype', 'ATP:0000082': 'RNAi phenotype', 'ATP:0000122': 'ATP:0000122',
                'ATP:0000084': 'overexpression phenotype', 'ATP:0000079': 'genetic phenotype', 'ATP:0000005': 'gene',
                'WB:WBGene00003001': 'lin-12', 'NCBITaxon:6239': 'Caenorhabditis elegans'
            }
            response = client.get(url=f"/topic_entity_tag/by_reference/{new_curie}")
            assert response.status_code == status.HTTP_200_OK
            assert len(response.json()) > 0

    def test_validation(self, test_topic_entity_tag, test_reference, test_mod, auth_headers, db): # noqa
        load_name_to_atp_and_relationships_mock()
        with TestClient(app) as client:
            author_source_1 = {
                "source_evidence_assertion": "community curation",
                "source_method": "acknowledge",
                "validation_type": "author",
                "description": "author from acknowledge",
                "data_provider": "WB",
                "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation
            }
            author_source_2 = {
                "source_evidence_assertion": "community curation",
                "source_method": "AFP",
                "validation_type": "author",
                "description": "author from AFP",
                "data_provider": "WB",
                "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation
            }
            auth_source_1_resp = client.post(url="/topic_entity_tag/source", json=author_source_1, headers=auth_headers)
            auth_source_2_resp = client.post(url="/topic_entity_tag/source", json=author_source_2, headers=auth_headers)
            validating_tag_aut_1 = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000122",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": auth_source_1_resp.json(),
                "negated": True,
                "data_novelty": "ATP:0000334"
            }
            validating_tag_aut_2 = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000122",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": auth_source_2_resp.json(),
                "negated": True,
                "data_novelty": "ATP:0000334"
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
            assert response.json()["validation_by_author"] == "validated_wrong"

    def test_cannot_create_existing_similar_tag_with_negation(self, test_topic_entity_tag, test_reference, test_mod, auth_headers, db):  # noqa
        with TestClient(app) as client, \
                patch("agr_literature_service.api.crud.topic_entity_tag_utils.get_ancestors") as mock_get_ancestors, \
                patch("agr_literature_service.api.crud.topic_entity_tag_utils.get_descendants") as mock_get_descendants:
            mock_get_ancestors.return_value = []
            mock_get_descendants.return_value = []
            curator_source = {
                "source_evidence_assertion": "ATP:0000036",  # manual assertion by professional biocurator, ATP required because it will have an existing_tag in the crud
                "source_method": "abc_literature_system",
                "validation_type": "professional_biocurator",
                "description": "curator from ABC",
                "data_provider": "WB",
                "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation
            }
            response = client.post(url="/topic_entity_tag/source", json=curator_source, headers=auth_headers)
            validating_tag_cur_1 = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000122",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": response.json(),
                "negated": True,
                "data_novelty": "ATP:0000334"
            }
            validating_tag_cur_2 = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000122",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": response.json(),
                "negated": False,
                "data_novelty": "ATP:0000334",
            }
            client.post(url="/topic_entity_tag/", json=validating_tag_cur_1, headers=auth_headers)
#             response1 = client.post(url="/topic_entity_tag/", json=validating_tag_cur_1, headers=auth_headers)
#             print("Status code1:", response1.status_code)
#             print("Response body:", response1.text)
#             response1.raise_for_status()
#             response2 = client.post(url="/topic_entity_tag/", json=validating_tag_cur_2, headers=auth_headers)
#             print("Status code2:", response2.status_code)
#             print("Response body:", response2.text)
#             response2.raise_for_status()

#             cur_2_tag_id = client.post(url="/topic_entity_tag/", json=validating_tag_cur_2,
#                                        headers=auth_headers)
# TODO  this should return a 201 and check that status is exists
            response = client.post(url="/topic_entity_tag/", json=validating_tag_cur_2,
                                       headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            assert response.json()["status"] == "exists"

#             response = client.get(f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}")
#             assert response.json()["validation_by_author"] == "not_validated"
#             assert response.json()["validation_by_professional_biocurator"] == "validation_conflict"
#             response = client.get(f"/topic_entity_tag/{cur_2_tag_id}")
#             assert response.json()["validation_by_author"] == "not_validated"
#             assert response.json()["validation_by_professional_biocurator"] == "validation_conflict"

    def test_validation_wrong(self, test_topic_entity_tag, test_reference, test_mod, auth_headers, db):  # noqa
        with TestClient(app) as client, \
                patch("agr_literature_service.api.crud.topic_entity_tag_utils.get_ancestors") as mock_get_ancestors, \
                patch("agr_literature_service.api.crud.topic_entity_tag_utils.get_descendants") as mock_get_descendants:
            mock_get_ancestors.return_value = []
            mock_get_descendants.return_value = []
            curator_source = {
                "source_evidence_assertion": "curator",
                "source_method": "abc_literature_system",
                "validation_type": "professional_biocurator",
                "description": "curator from ABC",
                "data_provider": "WB",
                "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation
            }
            response = client.post(url="/topic_entity_tag/source", json=curator_source, headers=auth_headers)
            validating_tag_cur_1 = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000122",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": response.json(),
                "negated": True,
                "created_by": "curator1",
                "data_novelty": "ATP:0000334"
            }
            validating_tag_cur_2 = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000122",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": response.json(),
                "negated": False,
                "created_by": "curator2",
                "data_novelty": "ATP:0000334",
            }
            client.post(url="/topic_entity_tag/", json=validating_tag_cur_1, headers=auth_headers)
            cur_2_tag_id = client.post(url="/topic_entity_tag/", json=validating_tag_cur_2,
                                       headers=auth_headers).json()["topic_entity_tag_id"]
            response = client.get(f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}")
            assert response.json()["validation_by_author"] == "not_validated"
            assert response.json()["validation_by_professional_biocurator"] == "validation_conflict"
            response = client.get(f"/topic_entity_tag/{cur_2_tag_id}")
            assert response.json()["validation_by_author"] == "not_validated"
            assert response.json()["validation_by_professional_biocurator"] == "validation_conflict"


    def test_validate_generic_specific(self, test_topic_entity_tag, test_reference, test_mod, auth_headers, db): # noqa
        load_name_to_atp_and_relationships_mock()
        with TestClient(app) as client:
            author_source_1 = {
                "source_evidence_assertion": "community curation",
                "source_method": "acknowledge",
                "validation_type": "author",
                "description": "author from acknowledge",
                "data_provider": "WB",
                "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation
            }
            author_source_2 = {
                "source_evidence_assertion": "manual_curation",
                "source_method": "abc_interface",
                "validation_type": "professional_biocurator",
                "description": "Curator using the ABC",
                "data_provider": "WB",
                "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation
            }
            auth_source_1_resp = client.post(url="/topic_entity_tag/source", json=author_source_1, headers=auth_headers)
            auth_source_2_resp = client.post(url="/topic_entity_tag/source", json=author_source_2, headers=auth_headers)
            more_generic_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",  # more generic topic
                "topic_entity_tag_source_id": auth_source_1_resp.json(),
                "negated": False,
                "data_novelty": "ATP:0000321"
            }
            more_generic_tag_2 = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000068",  # more generic topic
                "topic_entity_tag_source_id": auth_source_1_resp.json(),
                "negated": False,
                "data_novelty": "ATP:0000321"
            }
            more_specific_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000084",  # made this more specific
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": auth_source_2_resp.json(),
                "negated": False,
                "data_novelty": "ATP:0000228"
            }
            more_specific_tag_2 = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000071",  # made this more specific
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": auth_source_2_resp.json(),
                "negated": False,
                "data_novelty": "ATP:0000228"
            }

            # add the new tags
            more_generic_tag_id = client.post(url="/topic_entity_tag/", json=more_generic_tag,
                                              headers=auth_headers).json()["topic_entity_tag_id"]
            more_specific_tag_id = client.post(url="/topic_entity_tag/", json=more_specific_tag,
                                               headers=auth_headers).json()["topic_entity_tag_id"]
            more_specific_tag_id_2 = client.post(url="/topic_entity_tag/", json=more_specific_tag_2,
                                                 headers=auth_headers).json()["topic_entity_tag_id"]
            more_generic_tag_id_2 = client.post(url="/topic_entity_tag/", json=more_generic_tag_2,
                                                headers=auth_headers).json()["topic_entity_tag_id"]

            # next, we check if the validation process is correct. Supposed that your system recognizes more specific
            # tags validate more generic ones, so:
            generic_tag_obj: TopicEntityTagModel = db.query(TopicEntityTagModel).filter(
                TopicEntityTagModel.topic_entity_tag_id == more_generic_tag_id
            ).one()
            assert len(generic_tag_obj.validated_by) > 0
            assert int(more_specific_tag_id) in {tag.topic_entity_tag_id for tag in generic_tag_obj.validated_by}

            generic_tag_obj_2: TopicEntityTagModel = db.query(TopicEntityTagModel).filter(
                TopicEntityTagModel.topic_entity_tag_id == more_generic_tag_id_2
            ).one()
            assert len(generic_tag_obj_2.validated_by) > 0
            assert int(more_specific_tag_id_2) in {tag.topic_entity_tag_id for tag in generic_tag_obj_2.validated_by}

            specific_tag_obj: TopicEntityTagModel = db.query(TopicEntityTagModel).filter(
                TopicEntityTagModel.topic_entity_tag_id == int(more_specific_tag_id)
            ).one()
            assert len(specific_tag_obj.validated_by) == 0  # nothing should validate the more specific tag

            specific_tag_obj_2: TopicEntityTagModel = db.query(TopicEntityTagModel).filter(
                TopicEntityTagModel.topic_entity_tag_id == int(more_specific_tag_id_2)
            ).one()
            assert len(specific_tag_obj_2.validated_by) == 0  # nothing should validate the more specific tag

    def test_validate_positive_with_pos_and_neg(self, test_topic_entity_tag, test_reference, test_mod,  # noqa
                                                auth_headers, db, test_topic_entity_tag_source):  # noqa
        with TestClient(app) as client, \
                patch("agr_literature_service.api.crud.topic_entity_tag_utils.get_ancestors") as mock_get_ancestors, \
                patch("agr_literature_service.api.crud.topic_entity_tag_utils.get_descendants") as mock_get_descendants:
            author_source = {
                "source_evidence_assertion": "manual",
                "source_method": "ACKnowledge",
                "validation_type": "author",
                "description": "author from acknowledge",
                "data_provider": "WB",
                "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation
            }
            curator_source = {
                "source_evidence_assertion": "manual",
                "source_method": "abc_interface",
                "validation_type": "professional_biocurator",
                "description": "Curator using the ABC",
                "data_provider": "WB",
                "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation
            }
            author_source_resp = client.post(url="/topic_entity_tag/source", json=author_source, headers=auth_headers)
            curator_source_resp = client.post(url="/topic_entity_tag/source", json=curator_source, headers=auth_headers)
            positive_tag_not_validating = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000079",  # genetic phenotype
                "topic_entity_tag_source_id": test_topic_entity_tag_source.new_source_id,  # automated tag
                "negated": False,
                "data_novelty": "ATP:0000321"
            }
            more_generic_positive_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",  # phenotype
                "topic_entity_tag_source_id": author_source_resp.json(),
                "negated": False,
                "data_novelty": "ATP:0000321"
            }
            more_generic_negative_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",  # phenotype
                "topic_entity_tag_source_id": author_source_resp.json(),
                "negated": True,
                "data_novelty": "ATP:0000321"
            }
            more_specific_positive_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000084",  # overexpression phenotype
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": author_source_resp.json(),
                "negated": False,
                "data_novelty": "ATP:0000321"
            }
            more_specific_negative_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000082",  # RNAi phenotype
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": curator_source_resp.json(),
                "negated": True,
                "data_novelty": "ATP:0000321"
            }
            mock_get_ancestors.return_value = {'ATP:0000001', 'ATP:0000002', 'ATP:0000009', 'ATP:0000079'}
            mock_get_descendants.return_value = {'ATP:0000079', 'ATP:0000082', 'ATP:0000083', 'ATP:0000084'}
            positive_tag_id = client.post(url="/topic_entity_tag/", json=positive_tag_not_validating,
                                          headers=auth_headers).json()["topic_entity_tag_id"]
            mock_get_ancestors.return_value = {'ATP:0000001', 'ATP:0000002', 'ATP:0000009'}
            mock_get_descendants.return_value = {'ATP:0000009', 'ATP:0000033', 'ATP:0000034', 'ATP:0000079',
                                                 'ATP:0000080', 'ATP:0000081', 'ATP:0000082', 'ATP:0000083',
                                                 'ATP:0000084', 'ATP:0000085', 'ATP:0000086', 'ATP:0000087',
                                                 'ATP:0000100'}
            client.post(url="/topic_entity_tag/", json=more_generic_positive_tag, headers=auth_headers).json()
            mock_get_ancestors.return_value = {'ATP:0000001', 'ATP:0000002', 'ATP:0000009'}
            mock_get_descendants.return_value = {'ATP:0000009', 'ATP:0000033', 'ATP:0000034', 'ATP:0000079',
                                                 'ATP:0000080', 'ATP:0000081', 'ATP:0000082', 'ATP:0000083',
                                                 'ATP:0000084', 'ATP:0000085', 'ATP:0000086', 'ATP:0000087',
                                                 'ATP:0000100'}
            more_generic_negative_tag_id = client.post(url="/topic_entity_tag/", json=more_generic_negative_tag,
                                                       headers=auth_headers).json()["topic_entity_tag_id"]
            mock_get_ancestors.return_value = {'ATP:0000001', 'ATP:0000002', 'ATP:0000009', 'ATP:0000079', 'ATP:0000082'}
            mock_get_descendants.return_value = {'ATP:0000082'}
            more_specific_positive_id = client.post(url="/topic_entity_tag/", json=more_specific_positive_tag,
                                                    headers=auth_headers).json()["topic_entity_tag_id"]
            mock_get_ancestors.return_value = {'ATP:0000001', 'ATP:0000002', 'ATP:0000009', 'ATP:0000079'}
            mock_get_descendants.return_value = {'ATP:0000079', 'ATP:0000082', 'ATP:0000083', 'ATP:0000084'}
            client.post(url="/topic_entity_tag/", json=more_specific_negative_tag, headers=auth_headers).json()
            positive_tag: TopicEntityTagModel = db.query(TopicEntityTagModel).filter(
                TopicEntityTagModel.topic_entity_tag_id == positive_tag_id).one()
            assert len(positive_tag.validated_by) == 2
            validating_tags = [int(validating_tag.topic_entity_tag_id) for validating_tag in positive_tag.validated_by]
            assert int(more_specific_positive_id) in validating_tags
            assert int(more_generic_negative_tag_id) in validating_tags

    def test_validate_negative_with_pos_and_neg(self, test_topic_entity_tag, test_reference, test_mod,  # noqa
                                                auth_headers, db, test_topic_entity_tag_source):  # noqa
        with TestClient(app) as client, \
                patch("agr_literature_service.api.crud.topic_entity_tag_utils.get_ancestors") as mock_get_ancestors, \
                patch("agr_literature_service.api.crud.topic_entity_tag_utils.get_descendants") as \
                mock_get_descendants, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_curie_to_name_from_all_tets") as \
                mock_get_curie_to_name_from_all_tets:
            author_source = {
                "source_evidence_assertion": "manual",
                "source_method": "ACKnowledge",
                "validation_type": "author",
                "description": "author from acknowledge",
                "data_provider": "WB",
                "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation
            }
            curator_source = {
                "source_evidence_assertion": "manual",
                "source_method": "abc_interface",
                "validation_type": "professional_biocurator",
                "description": "Curator using the ABC",
                "data_provider": "WB",
                "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation
            }
            author_source_resp = client.post(url="/topic_entity_tag/source", json=author_source,
                                             headers=auth_headers)
            curator_source_resp = client.post(url="/topic_entity_tag/source", json=curator_source,
                                              headers=auth_headers)
            negative_tag_not_validating = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000079",  # genetic phenotype
                "topic_entity_tag_source_id": test_topic_entity_tag_source.new_source_id,  # automated tag
                "negated": True,
                "data_novelty": "ATP:0000321"
            }
            more_generic_positive_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",  # phenotype
                "topic_entity_tag_source_id": author_source_resp.json(),
                "negated": False,
                "data_novelty": "ATP:0000321"
            }
            more_generic_negative_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",  # phenotype
                "topic_entity_tag_source_id": author_source_resp.json(),
                "negated": True,
                "data_novelty": "ATP:0000321"
            }
            more_specific_positive_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000084",  # overexpression phenotype
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": author_source_resp.json(),
                "negated": False,
                "data_novelty": "ATP:0000321"
            }
            more_specific_negative_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000082",  # RNAi phenotype
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": curator_source_resp.json(),
                "negated": True,
                "data_novelty": "ATP:0000321"
            }
            mock_get_ancestors.return_value = {'ATP:0000001', 'ATP:0000002', 'ATP:0000009', 'ATP:0000079'}
            mock_get_descendants.return_value = {'ATP:0000079', 'ATP:0000082', 'ATP:0000083', 'ATP:0000084'}
            negative_tag_id = client.post(url="/topic_entity_tag/", json=negative_tag_not_validating,
                                          headers=auth_headers).json()["topic_entity_tag_id"]
            mock_get_ancestors.return_value = {'ATP:0000001', 'ATP:0000002', 'ATP:0000009'}
            mock_get_descendants.return_value = {'ATP:0000009', 'ATP:0000033', 'ATP:0000034', 'ATP:0000079', 'ATP:0000080', 'ATP:0000081', 'ATP:0000082', 'ATP:0000083', 'ATP:0000084', 'ATP:0000085', 'ATP:0000086', 'ATP:0000087', 'ATP:0000100'}
            client.post(url="/topic_entity_tag/", json=more_generic_positive_tag, headers=auth_headers)
            mock_get_ancestors.return_value = {'ATP:0000001', 'ATP:0000002', 'ATP:0000009'}
            mock_get_descendants.return_value = {'ATP:0000009', 'ATP:0000033', 'ATP:0000034', 'ATP:0000079', 'ATP:0000080', 'ATP:0000081', 'ATP:0000082', 'ATP:0000083', 'ATP:0000084', 'ATP:0000085', 'ATP:0000086', 'ATP:0000087', 'ATP:0000100'}
            more_generic_negative_tag_id = client.post(url="/topic_entity_tag/", json=more_generic_negative_tag,
                                                       headers=auth_headers).json()["topic_entity_tag_id"]
            mock_get_ancestors.return_value = {'ATP:0000001', 'ATP:0000002', 'ATP:0000009', 'ATP:0000079', 'ATP:0000084'}
            mock_get_descendants.return_value = {'ATP:0000084'}
            more_specific_positive_tag_id = client.post(url="/topic_entity_tag/", json=more_specific_positive_tag,
                                                        headers=auth_headers).json()["topic_entity_tag_id"]
            mock_get_ancestors.return_value = {'ATP:0000001', 'ATP:0000002', 'ATP:0000009', 'ATP:0000079', 'ATP:0000082'}
            mock_get_descendants.return_value = {'ATP:0000082'}
            client.post(url="/topic_entity_tag/", json=more_specific_negative_tag, headers=auth_headers)
            negative_tag: TopicEntityTagModel = db.query(TopicEntityTagModel).filter(
                TopicEntityTagModel.topic_entity_tag_id == negative_tag_id).one()
            assert len(negative_tag.validated_by) == 2
            validating_tags = [int(validating_tag.topic_entity_tag_id) for validating_tag in negative_tag.validated_by]
            assert int(more_specific_positive_tag_id) in validating_tags
            assert int(more_generic_negative_tag_id) in validating_tags
            mock_get_curie_to_name_from_all_tets.return_value = {
                'ATP:0000009': 'phenotype', 'ATP:0000082': 'RNAi phenotype', 'ATP:0000122': 'ATP:0000122',
                'ATP:0000084': 'overexpression phenotype', 'ATP:0000079': 'genetic phenotype', 'ATP:0000005': 'gene',
                'WB:WBGene00003001': 'lin-12', 'NCBITaxon:6239': 'Caenorhabditis elegans'
            }
            all_tags_resp = client.get(url=f"/topic_entity_tag/by_reference/{test_reference.new_ref_curie}",
                                       headers=auth_headers)
            assert all_tags_resp.status_code == status.HTTP_200_OK
            all_tags = all_tags_resp.json()
            assert len(all_tags) == 6
            for tag in all_tags:
                if tag["topic"] == "ATP:0000079":
                    assert tag["validation_by_author"] == "validation_conflict"


    def test_validate_negated_null(self, test_topic_entity_tag, test_reference, test_mod, auth_headers, db):  # noqa
        with TestClient(app) as client, \
                patch("agr_literature_service.api.crud.topic_entity_tag_utils.map_curies_to_names") \
                as mock_map_curies_to_names, \
                patch("agr_literature_service.api.crud.topic_entity_tag_utils.get_ancestors") as mock_get_ancestors, \
                patch("agr_literature_service.api.crud.topic_entity_tag_utils.get_descendants") as mock_get_descendants:
            author_source_1 = {
                "source_evidence_assertion": "community curation",
                "source_method": "acknowledge",
                "validation_type": "author",
                "description": "author from acknowledge",
                "data_provider": "WB",
                "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation
            }
            auth_source_1_resp = client.post(url="/topic_entity_tag/source", json=author_source_1, headers=auth_headers)
            positive_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",
                "topic_entity_tag_source_id": auth_source_1_resp.json(),
                "negated": False,
                "data_novelty": "ATP:0000321"
            }
            null_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",
                "topic_entity_tag_source_id": auth_source_1_resp.json(),
                "negated": None,
                "data_novelty": "ATP:0000321"
            }
            # add the new tags
            mock_get_ancestors.return_value = {'ATP:0000001', 'ATP:0000002', 'ATP:0000009'}
            mock_get_descendants.return_value = {'ATP:0000009', 'ATP:0000033', 'ATP:0000034', 'ATP:0000079', 'ATP:0000080', 'ATP:0000081', 'ATP:0000082', 'ATP:0000083', 'ATP:0000084', 'ATP:0000085', 'ATP:0000086', 'ATP:0000087', 'ATP:0000100'}
            positive_tag_id = client.post(url="/topic_entity_tag/", json=positive_tag, headers=auth_headers).json()['topic_entity_tag_id']
            mock_map_curies_to_names.return_value = {
                'ATP:0000005': 'gene',
                'community curation': 'community curation',
                'ATP:0000009': 'phenotype',
                'ATP:0000122': 'some term'
            }
            mock_get_ancestors.return_value = {'ATP:0000001', 'ATP:0000002', 'ATP:0000009'}
            mock_get_descendants.return_value = {'ATP:0000009', 'ATP:0000033', 'ATP:0000034', 'ATP:0000079', 'ATP:0000080', 'ATP:0000081', 'ATP:0000082', 'ATP:0000083', 'ATP:0000084', 'ATP:0000085', 'ATP:0000086', 'ATP:0000087', 'ATP:0000100'}
            null_tag_id = client.post(url="/topic_entity_tag/", json=null_tag, headers=auth_headers).json()['topic_entity_tag_id']
            positive_tag_resp = client.get(url=f"/topic_entity_tag/{positive_tag_id}", headers=auth_headers)
            assert positive_tag_resp.json()["validation_by_author"] == "validated_right_self"
            null_tag_resp = client.get(url=f"/topic_entity_tag/{null_tag_id}", headers=auth_headers)
            assert null_tag_resp.json()["validation_by_author"] == "validated_right_self"

    @pytest.mark.webtest
    def test_get_curie_to_name_from_all_tets(self, test_topic_entity_tag, test_topic_entity_tag_source, test_mod, # noqa
                                             auth_headers): # noqa
        with TestClient(app) as client:
            topic_tag = {
                "reference_curie": test_topic_entity_tag.related_ref_curie,
                "topic": "ATP:0000009",
                "confidence_level": "high",
                "topic_entity_tag_source_id": test_topic_entity_tag_source.new_source_id,
            }
            client.post(url="/topic_entity_tag/", json=topic_tag, headers=auth_headers)
            response = client.get(url="/topic_entity_tag/get_curie_to_name_from_all_tets/",
                                  params={"curie_or_reference_id": test_topic_entity_tag.related_ref_curie,
                                          "token": get_authentication_token()},
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert response.json() == {
                'ATP:0000005': 'gene',
                'ATP:0000009': 'phenotype',
                'ATP:0000122': 'ATP:0000122',
                'WB:WBGene00003001': 'lin-12',
                'NCBITaxon:6239': 'Caenorhabditis elegans',
                'ECO:0008025': 'neural network method evidence used in automatic assertion'
            }
            alliance_topic_tag = {
                "reference_curie": test_topic_entity_tag.related_ref_curie,
                "topic": "ATP:0000009",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": test_topic_entity_tag_source.new_source_id
            }
            client.post(url="/topic_entity_tag/", json=alliance_topic_tag, headers=auth_headers)
            response = client.get(url="/topic_entity_tag/get_curie_to_name_from_all_tets/",
                                  params={"curie_or_reference_id": test_topic_entity_tag.related_ref_curie,
                                          "token": get_authentication_token()},
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert response.json() == {
                'ATP:0000005': 'gene',
                'ATP:0000009': 'phenotype',
                'ATP:0000122': 'ATP:0000122',
                'WB:WBGene00003001': 'lin-12',
                'NCBITaxon:6239': 'Caenorhabditis elegans',
                'ECO:0008025': 'neural network method evidence used in automatic assertion'
            }
            wormbase_topic_tag = {
                "reference_curie": test_topic_entity_tag.related_ref_curie,
                "topic": "ATP:0000009",
                "entity_type": "ATP:0000099",
                "entity": "WB:WBCnstr00007090",
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": test_topic_entity_tag_source.new_source_id
            }
            client.post(url="/topic_entity_tag/", json=wormbase_topic_tag, headers=auth_headers)
            response = client.get(url="/topic_entity_tag/get_curie_to_name_from_all_tets/",
                                  params={"curie_or_reference_id": test_topic_entity_tag.related_ref_curie,
                                          "token": get_authentication_token()},
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert response.json() == {
                'ATP:0000005': 'gene',
                'ATP:0000009': 'phenotype',
                'ATP:0000099': 'existing transgenic construct',
                'ATP:0000122': 'ATP:0000122',  # not present in the ontology
                'WB:WBGene00003001': 'lin-12',
                'WB:WBCnstr00007090': '[pCAM-1 deletion-S/T, rol-6(d)]',
                'NCBITaxon:6239': 'Caenorhabditis elegans',
                'ECO:0008025': 'neural network method evidence used in automatic assertion'
            }

    @pytest.mark.webtest
    def test_get_ancestors(self, auth_headers):  # noqa
        load_name_to_atp_and_relationships_mock()
        onto_node = "ATP:0000079"
        ancestors = get_ancestors(onto_node)
        expected_ancestors = {"ATP:0000001", "ATP:0000002", "ATP:0000009"}
        assert [ancestor in expected_ancestors for ancestor in ancestors]

    @pytest.mark.webtest
    def test_get_descendants(self, auth_headers):  # noqa
        load_name_to_atp_and_relationships_mock()
        onto_node = "ATP:0000009"
        descendants = get_descendants(onto_node)
        expected_descendants = {'ATP:0000079', 'ATP:0000080', 'ATP:0000081', 'ATP:0000082', 'ATP:0000083',
                                'ATP:0000084', 'ATP:0000085', 'ATP:0000086', 'ATP:0000087', 'ATP:0000033',
                                'ATP:0000034', 'ATP:0000100'}
        assert [ancestor in expected_descendants for ancestor in descendants]

    @pytest.mark.webtest
    def test_get_ancestors_non_existent(self, auth_headers):  # noqa
        load_name_to_atp_and_relationships_mock()
        onto_node = "ATP:000007"
        ancestors = get_ancestors(onto_node)
        assert len(ancestors) == 0

    def test_data_novelty_field(self, test_topic_entity_tag, test_reference, test_mod, auth_headers, db):  # noqa
        """Test that data_novelty field is properly handled."""
        with TestClient(app) as client:
            # Create a source with source_evidence_assertion = ATP:0000036
            source_with_atp36 = {
                "source_evidence_assertion": "ATP:0000036",
                "source_method": "test_method",
                "validation_type": None,
                "description": "test source for data novelty",
                "data_provider": "WB",
                "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation
            }
            source_resp = client.post(url="/topic_entity_tag/source", json=source_with_atp36, headers=auth_headers)
            assert source_resp.status_code == status.HTTP_201_CREATED

            # Create a tag with data_novelty="ATP:0000321" (novel data)
            new_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000122",
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003002",
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": source_resp.json(),
                "negated": False,
                "data_novelty": "ATP:0000321"
            }
            create_resp = client.post(url="/topic_entity_tag/", json=new_tag, headers=auth_headers)
            assert create_resp.status_code == status.HTTP_201_CREATED
            tag_id = create_resp.json()['topic_entity_tag_id']

            # Verify the tag returns both fields correctly
            get_resp = client.get(f"/topic_entity_tag/{tag_id}")
            assert get_resp.status_code == status.HTTP_200_OK
            tag_data = get_resp.json()
            # Check that data_novelty is set correctly
            assert tag_data["data_novelty"] == "ATP:0000321"

            # Test patch to update data_novelty
            patch_data = {"data_novelty": "ATP:0000334"}
            patch_resp = client.patch(f"/topic_entity_tag/{tag_id}", json=patch_data, headers=auth_headers)
            assert patch_resp.status_code == status.HTTP_202_ACCEPTED
            # Verify the update
            get_resp2 = client.get(f"/topic_entity_tag/{tag_id}")
            assert get_resp2.status_code == status.HTTP_200_OK
            tag_data2 = get_resp2.json()
            # Check that data_novelty is updated
            assert tag_data2["data_novelty"] == "ATP:0000334"

    def test_data_novelty_branch_separation(self):
        """Test that novel data and existing data branches are properly separated."""
        load_name_to_atp_and_relationships_mock()

        with patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_ancestors") as mock_get_ancestors:
            mock_get_ancestors.side_effect = lambda onto_node=None: {
                "ATP:0000321": {"ATP:0000335"},                     # novel data -> root
                "ATP:0000334": {"ATP:0000335"},                     # existing data -> root
                "ATP:0000228": {"ATP:0000321", "ATP:0000335"},      # novel to db -> novel data -> root
            }.get(onto_node, set())

            # Branch compatibility is now handled directly through hierarchy checks

    def test_data_novelty_validation_separation(self, test_reference, test_mod, auth_headers): # noqa
        """Test that novel data and existing data tags don't validate each other."""
        load_name_to_atp_and_relationships_mock()
        with TestClient(app) as client, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_ancestors") as mock_get_ancestors, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_descendants") as mock_get_descendants:

            # Mock ontology calls to ensure consistent behavior
            mock_get_ancestors.side_effect = lambda onto_node=None: {
                "ATP:0000321": {"ATP:0000335"},  # novel data -> data novelty root
                "ATP:0000334": {"ATP:0000335"},  # existing data -> data novelty root
                "ATP:0000228": {"ATP:0000321", "ATP:0000335"},  # novel to db -> novel data -> root
                "ATP:0000009": {"ATP:0000001", "ATP:0000002"},  # topic hierarchy
                "ATP:0000079": {"ATP:0000001", "ATP:0000002", "ATP:0000009"}
            }.get(onto_node, set())

            mock_get_descendants.side_effect = lambda onto_node=None: {
                "ATP:0000335": {"ATP:0000321", "ATP:0000334", "ATP:0000228", "ATP:0000229"},
                "ATP:0000321": {"ATP:0000228", "ATP:0000229"},  # novel data has specific subtypes
                "ATP:0000334": set(),  # existing data has no subtypes
                "ATP:0000009": {"ATP:0000079", "ATP:0000080", "ATP:0000081", "ATP:0000082", "ATP:0000083", "ATP:0000084"}
            }.get(onto_node, set())

            # Create curator source for validation
            curator_source = {
                "source_evidence_assertion": "ATP:0000036",
                "source_method": "abc_literature_system",
                "validation_type": "professional_biocurator",
                "description": "curator validation",
                "data_provider": "WB",
                "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation
            }
            curator_source_resp = client.post(url="/topic_entity_tag/source", json=curator_source, headers=auth_headers)

            # Create tag with existing data novelty
            existing_data_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",
                "topic_entity_tag_source_id": curator_source_resp.json(),
                "negated": False,
                "data_novelty": "ATP:0000334"  # existing data
            }
            existing_tag_resp = client.post(url="/topic_entity_tag/", json=existing_data_tag, headers=auth_headers)
            existing_tag_id = existing_tag_resp.json()["topic_entity_tag_id"]

            # Create tag with novel data novelty - should NOT validate existing data tag
            novel_data_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000079",  # more specific topic
                "topic_entity_tag_source_id": curator_source_resp.json(),
                "negated": False,
                "data_novelty": "ATP:0000321"  # novel data
            }
            novel_tag_resp = client.post(url="/topic_entity_tag/", json=novel_data_tag, headers=auth_headers)
            novel_tag_id = novel_tag_resp.json()["topic_entity_tag_id"]

            # Check validation status - should NOT be validated due to data novelty incompatibility
            existing_tag_data = client.get(f"/topic_entity_tag/{existing_tag_id}").json()
            novel_tag_data = client.get(f"/topic_entity_tag/{novel_tag_id}").json()

            # Neither tag should validate the other due to incompatible data novelty
            assert existing_tag_data["validation_by_professional_biocurator"] in ["not_validated",
                                                                                  "validated_right_self"]
            assert novel_tag_data["validation_by_professional_biocurator"] in ["not_validated", "validated_right_self"]

    def test_data_novelty_hierarchy_validation(self, test_reference, test_mod, auth_headers): # noqa
        """Test that data novelty hierarchy works correctly within the same branch."""
        load_name_to_atp_and_relationships_mock()
        with TestClient(app) as client, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_ancestors") as mock_get_ancestors, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_descendants") as mock_get_descendants:

            # Mock ontology hierarchy for data novelty
            mock_get_ancestors.side_effect = lambda onto_node=None: {
                "ATP:0000228": {"ATP:0000321", "ATP:0000335"},  # new to db -> new data -> root
                "ATP:0000321": {"ATP:0000335"},  # new data -> root
                "ATP:0000009": {"ATP:0000001", "ATP:0000002"},
                "ATP:0000079": {"ATP:0000001", "ATP:0000002", "ATP:0000009"}
            }.get(onto_node, set())

            mock_get_descendants.side_effect = lambda onto_node=None: {
                "ATP:0000321": {"ATP:0000228", "ATP:0000229"},  # new data -> specific types
                "ATP:0000009": {"ATP:0000079", "ATP:0000080", "ATP:0000081", "ATP:0000082", "ATP:0000083", "ATP:0000084"}
            }.get(onto_node, set())

            # Create curator source
            curator_source = {
                "source_evidence_assertion": "ATP:0000036",
                "source_method": "abc_literature_system",
                "validation_type": "professional_biocurator",
                "description": "curator validation",
                "data_provider": "WB",
                "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation
            }
            curator_source_resp = client.post(url="/topic_entity_tag/source", json=curator_source, headers=auth_headers)

            # Create tag with generic novel data novelty
            generic_novel_data_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",
                "topic_entity_tag_source_id": curator_source_resp.json(),
                "negated": False,
                "data_novelty": "ATP:0000321"  # generic novel data
            }
            generic_tag_resp = client.post(url="/topic_entity_tag/", json=generic_novel_data_tag, headers=auth_headers)
            generic_tag_id = generic_tag_resp.json()["topic_entity_tag_id"]

            # Create tag with more specific novel data novelty - should validate generic
            specific_novel_data_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000079",  # more specific topic
                "topic_entity_tag_source_id": curator_source_resp.json(),
                "negated": False,
                "data_novelty": "ATP:0000228"  # novel to database (more specific)
            }
            client.post(url="/topic_entity_tag/", json=specific_novel_data_tag, headers=auth_headers)

            # Check validation - generic tag should be validated by specific tag
            generic_tag_data = client.get(f"/topic_entity_tag/{generic_tag_id}").json()
            # specific_tag_data = client.get(f"/topic_entity_tag/{specific_tag_id}").json()

            # Generic tag should be validated as correct by the more specific tag
            assert generic_tag_data["validation_by_professional_biocurator"] == "validated_right"

    def test_comprehensive_topic_novelty_validation_matrix(self, test_topic_entity_tag, test_topic_entity_tag_source, # noqa
                                                           test_reference, test_mod, auth_headers, db): # noqa
        """Test all combinations of topic hierarchy and data novelty hierarchy validation."""
        load_name_to_atp_and_relationships_mock()
        with TestClient(app) as client, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_ancestors") as mock_get_ancestors, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_descendants") as mock_get_descendants:

            # Setup comprehensive ontology mock
            mock_get_ancestors.side_effect = lambda onto_node=None: {
                # Topic hierarchy
                "ATP:0000009": {"ATP:0000001", "ATP:0000002"},      # generic topic
                "ATP:0000079": {"ATP:0000001", "ATP:0000002", "ATP:0000009"},  # specific topic
                # Data novelty hierarchy
                "ATP:0000335": set(),                               # novelty root (no ancestors)
                "ATP:0000321": {"ATP:0000335"},                     # new data -> root
                "ATP:0000334": {"ATP:0000335"},                     # existing data -> root
                "ATP:0000228": {"ATP:0000321", "ATP:0000335"},      # new to db -> new data -> root
                "ATP:0000229": {"ATP:0000321", "ATP:0000335"},      # new to field -> new data -> root
            }.get(onto_node, set())

            mock_get_descendants.side_effect = lambda onto_node=None: {
                # Topic hierarchy
                "ATP:0000009": {"ATP:0000079", "ATP:0000080", "ATP:0000081"},  # generic has specific descendants
                "ATP:0000079": set(),                               # specific topic (no descendants)
                # Data novelty hierarchy
                "ATP:0000335": {"ATP:0000321", "ATP:0000334", "ATP:0000228", "ATP:0000229"},  # root has all descendants
                "ATP:0000321": {"ATP:0000228", "ATP:0000229"},      # new data has specific descendants
                "ATP:0000334": set(),                               # existing data (no descendants)
                "ATP:0000228": set(),                               # new to db (no descendants)
                "ATP:0000229": set(),                               # new to field (no descendants)
            }.get(onto_node, set())

            # Create curator source for validation
            curator_source = {
                "source_evidence_assertion": "ATP:0000036",
                "source_method": "abc_literature_system",
                "validation_type": "professional_biocurator",
                "description": "curator validation",
                "data_provider": "WB",
                "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation
            }
            curator_source_resp = client.post(url="/topic_entity_tag/source", json=curator_source, headers=auth_headers)
            curator_source_id = curator_source_resp.json()

            # Test Case 1: Positive specific topic + specific novelty validates positive generic topic + generic novelty
            generic_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",        # generic topic
                "topic_entity_tag_source_id": curator_source_id,
                "negated": False,
                "data_novelty": "ATP:0000321"  # generic new data
            }
            generic_resp = client.post(url="/topic_entity_tag/", json=generic_tag, headers=auth_headers)
            generic_id = generic_resp.json()["topic_entity_tag_id"]

            specific_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000079",        # specific topic
                "topic_entity_tag_source_id": curator_source_id,
                "negated": False,
                "data_novelty": "ATP:0000228"  # specific new data
            }
            specific_resp = client.post(url="/topic_entity_tag/", json=specific_tag, headers=auth_headers)
            specific_id = specific_resp.json()["topic_entity_tag_id"]

            # Generic tag should be validated as correct by more specific tag, both y curator
            generic_data = client.get(f"/topic_entity_tag/{generic_id}").json()
            assert generic_data["validation_by_professional_biocurator"] == "validated_right"

            # Test Case 2: Negative specific topic + specific novelty validates positive specific topic + specific novelty
            client.delete(f"/topic_entity_tag/{generic_id}", headers=auth_headers)  # Clean up
            client.delete(f"/topic_entity_tag/{specific_id}", headers=auth_headers)  # Clean up

            positive_specific = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000079",        # specific topic
                "topic_entity_tag_source_id": test_topic_entity_tag_source.new_source_id,
                "negated": False,
                "data_novelty": "ATP:0000228"  # specific novelty
            }
            pos_spec_resp = client.post(url="/topic_entity_tag/", json=positive_specific, headers=auth_headers)
            pos_spec_id = pos_spec_resp.json()["topic_entity_tag_id"]

            negative_specific = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000079",        # same specific topic
                "topic_entity_tag_source_id": curator_source_id,
                "negated": True,
                "data_novelty": "ATP:0000228"  # same specific novelty
            }
            client.post(url="/topic_entity_tag/", json=negative_specific, headers=auth_headers)

            # Positive specific should be validated as wrong by negative specific
            pos_spec_data = client.get(f"/topic_entity_tag/{pos_spec_id}").json()
            assert pos_spec_data["validation_by_professional_biocurator"] == "validated_wrong"

    def test_cross_branch_novelty_incompatibility(self, test_topic_entity_tag, test_reference, test_mod, # noqa
                                                  auth_headers, db): # noqa
        """Test that existing data and novel data branches don't validate each other."""
        load_name_to_atp_and_relationships_mock()
        with TestClient(app) as client, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_ancestors") as mock_get_ancestors, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_descendants") as mock_get_descendants:

            mock_get_ancestors.side_effect = lambda onto_node=None: {
                "ATP:0000321": {"ATP:0000335"},                     # novel data -> root
                "ATP:0000334": {"ATP:0000335"},                     # existing data -> root
                "ATP:0000228": {"ATP:0000321", "ATP:0000335"},      # novel to db -> novel data -> root
                "ATP:0000009": {"ATP:0000001", "ATP:0000002"},
                "ATP:0000079": {"ATP:0000001", "ATP:0000002", "ATP:0000009"}
            }.get(onto_node, set())

            mock_get_descendants.side_effect = lambda onto_node=None: {
                "ATP:0000335": {"ATP:0000321", "ATP:0000334", "ATP:0000228"},
                "ATP:0000321": {"ATP:0000228"},
                "ATP:0000334": set(),
                "ATP:0000009": {"ATP:0000079", "ATP:0000080", "ATP:0000081"}
            }.get(onto_node, set())

            curator_source = {
                "source_evidence_assertion": "ATP:0000036",
                "source_method": "abc_literature_system",
                "validation_type": "professional_biocurator",
                "description": "curator validation",
                "data_provider": "WB",
                "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation
            }
            source_resp = client.post(url="/topic_entity_tag/source", json=curator_source, headers=auth_headers)
            source_id = source_resp.json()

            # Create tag with existing data novelty
            existing_data_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",
                "topic_entity_tag_source_id": source_id,
                "negated": False,
                "data_novelty": "ATP:0000334"  # existing data branch
            }
            existing_resp = client.post(url="/topic_entity_tag/", json=existing_data_tag, headers=auth_headers)
            existing_id = existing_resp.json()["topic_entity_tag_id"]

            # Create tag with novel data novelty - should NOT validate existing data tag
            novel_data_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000079",        # more specific topic (normally would validate)
                "topic_entity_tag_source_id": source_id,
                "negated": False,
                "data_novelty": "ATP:0000228"  # novel data branch (specific)
            }
            client.post(url="/topic_entity_tag/", json=novel_data_tag, headers=auth_headers)

            # Existing data tag should NOT be validated due to incompatible novelty branches
            existing_data = client.get(f"/topic_entity_tag/{existing_id}").json()
            assert existing_data["validation_by_professional_biocurator"] == "validated_right_self"

    def test_mixed_hierarchy_validation_scenarios(self, test_reference, test_mod, auth_headers, db): # noqa
        """Test edge cases with mixed topic and novelty hierarchies."""
        load_name_to_atp_and_relationships_mock()
        with TestClient(app) as client, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_ancestors") as mock_get_ancestors, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_descendants") as mock_get_descendants:

            mock_get_ancestors.side_effect = lambda onto_node=None: {
                "ATP:0000335": set(),                               # novelty root
                "ATP:0000321": {"ATP:0000335"},                     # novel data -> root
                "ATP:0000228": {"ATP:0000321", "ATP:0000335"},      # novel to db -> novel data -> root
                "ATP:0000009": {"ATP:0000001", "ATP:0000002"},      # generic topic
                "ATP:0000079": {"ATP:0000001", "ATP:0000002", "ATP:0000009"}  # specific topic
            }.get(onto_node, set())

            mock_get_descendants.side_effect = lambda onto_node=None: {
                "ATP:0000335": {"ATP:0000321", "ATP:0000228"},      # root has descendants
                "ATP:0000321": {"ATP:0000228"},                     # novel data has specific descendants
                "ATP:0000009": {"ATP:0000079", "ATP:0000080"}       # generic topic has specific descendants
            }.get(onto_node, set())

            curator_source = {
                "source_evidence_assertion": "ATP:0000036",
                "source_method": "abc_literature_system",
                "validation_type": "professional_biocurator",
                "description": "curator validation",
                "data_provider": "WB",
                "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation
            }
            source_resp = client.post(url="/topic_entity_tag/source", json=curator_source, headers=auth_headers)
            source_id = source_resp.json()

            # Scenario: Generic topic + root novelty should be validated by specific topic + specific novelty
            root_novelty_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",        # generic topic
                "topic_entity_tag_source_id": source_id,
                "negated": False,
                "data_novelty": "ATP:0000335"  # root novelty (most generic)
            }
            root_resp = client.post(url="/topic_entity_tag/", json=root_novelty_tag, headers=auth_headers)
            root_id = root_resp.json()["topic_entity_tag_id"]

            specific_both_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000079",        # specific topic
                "topic_entity_tag_source_id": source_id,
                "negated": False,
                "data_novelty": "ATP:0000228"  # specific novelty
            }
            sbt_resp = client.post(url="/topic_entity_tag/", json=specific_both_tag, headers=auth_headers)
            sbt_id = sbt_resp.json()["topic_entity_tag_id"]

            # Root novelty tag should be validated as correct
            root_data = client.get(f"/topic_entity_tag/{root_id}").json()
            assert root_data["validation_by_professional_biocurator"] == "validated_right"

            # Clean up for next test
            client.delete(f"/topic_entity_tag/{root_id}", headers=auth_headers)
            client.delete(f"/topic_entity_tag/{sbt_id}", headers=auth_headers)

            # Scenario: Specific topic + generic novelty vs Generic topic + specific novelty
            # This tests hierarchy mismatch - should they validate?
            specific_topic_generic_novelty = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000079",        # specific topic
                "topic_entity_tag_source_id": source_id,
                "negated": False,
                "data_novelty": "ATP:0000321"  # generic novelty
            }
            st_gn_resp = client.post(url="/topic_entity_tag/", json=specific_topic_generic_novelty, headers=auth_headers)
            st_gn_id = st_gn_resp.json()["topic_entity_tag_id"]

            generic_topic_specific_novelty = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",        # generic topic
                "topic_entity_tag_source_id": source_id,
                "negated": False,
                "data_novelty": "ATP:0000228"  # specific novelty
            }
            gt_sn_resp = client.post(url="/topic_entity_tag/", json=generic_topic_specific_novelty, headers=auth_headers)
            gt_sn_id = gt_sn_resp.json()["topic_entity_tag_id"]

            # The specific topic + generic novelty should NOT be validated
            # because it's not more generic than generic topic + specific novelty in both dimensions
            # same but reverse for the generic topic + specific novelty
            st_gn_data = client.get(f"/topic_entity_tag/{st_gn_id}").json()
            assert st_gn_data["validation_by_professional_biocurator"] in ["not_validated", "validated_right_self"]
            gt_sn_data = client.get(f"/topic_entity_tag/{gt_sn_id}").json()
            assert gt_sn_data["validation_by_professional_biocurator"] in ["not_validated", "validated_right_self"]

    def test_negative_tag_hierarchy_validation(self, test_topic_entity_tag_source, test_reference, test_mod, # noqa
                                               auth_headers, db): # noqa
        """Test negative tag validation with both topic and novelty hierarchies."""
        load_name_to_atp_and_relationships_mock()
        with TestClient(app) as client, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_ancestors") as mock_get_ancestors, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_descendants") as mock_get_descendants:

            mock_get_ancestors.side_effect = lambda onto_node=None: {
                "ATP:0000321": {"ATP:0000335"},                     # novel data -> root
                "ATP:0000228": {"ATP:0000321", "ATP:0000335"},      # novel to db -> novel data -> root
                "ATP:0000009": {"ATP:0000001", "ATP:0000002"},      # generic topic
                "ATP:0000079": {"ATP:0000001", "ATP:0000002", "ATP:0000009"},  # specific topic
                "ATP:0000080": {"ATP:0000001", "ATP:0000002", "ATP:0000009", "ATP:0000079"}  # very specific topic
            }.get(onto_node, set())

            mock_get_descendants.side_effect = lambda onto_node=None: {
                "ATP:0000335": {"ATP:0000321", "ATP:0000228"},      # novelty root has descendants
                "ATP:0000321": {"ATP:0000228"},                     # novel data has specific descendants
                "ATP:0000009": {"ATP:0000079", "ATP:0000080"},      # generic topic has descendants
                "ATP:0000079": {"ATP:0000080"}                      # specific topic has descendant
            }.get(onto_node, set())

            curator_source = {
                "source_evidence_assertion": "ATP:0000036",
                "source_method": "abc_literature_system",
                "validation_type": "professional_biocurator",
                "description": "curator validation",
                "data_provider": "WB",
                "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation
            }
            source_resp = client.post(url="/topic_entity_tag/source", json=curator_source, headers=auth_headers)
            source_id = source_resp.json()

            # Create positive tag with specific topic and specific novelty
            positive_specific = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000080",        # very specific topic
                "topic_entity_tag_source_id": test_topic_entity_tag_source.new_source_id,
                "negated": False,
                "data_novelty": "ATP:0000228"  # specific novelty
            }
            pos_resp = client.post(url="/topic_entity_tag/", json=positive_specific, headers=auth_headers)
            pos_id = pos_resp.json()["topic_entity_tag_id"]

            # Create negative tag with less specific topic and less specific novelty
            # This should validate the positive tag as wrong
            negative_less_specific = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000079",        # less specific topic (ancestor of ATP:0000080)
                "topic_entity_tag_source_id": source_id,
                "negated": True,
                "data_novelty": "ATP:0000321"  # less specific novelty (ancestor of ATP:0000228 in novel data branch)
            }
            client.post(url="/topic_entity_tag/", json=negative_less_specific, headers=auth_headers)

            # Positive specific tag should be validated as wrong by negative less specific
            pos_data = client.get(f"/topic_entity_tag/{pos_id}").json()
            assert pos_data["validation_by_professional_biocurator"] == "validated_wrong"

    def test_comprehensive_novel_data_validation_combinations(self, test_topic_entity_tag, test_reference, # noqa
                                                              test_mod, auth_headers, db): # noqa
        """Test all combinations of novel data values in validation scenarios."""
        load_name_to_atp_and_relationships_mock()
        with TestClient(app) as client, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_ancestors") as mock_get_ancestors, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_descendants") as mock_get_descendants:

            # Complete novel data hierarchy
            mock_get_ancestors.side_effect = lambda onto_node=None: {
                "ATP:0000335": set(),                                           # root (no ancestors)
                "ATP:0000321": {"ATP:0000335"},                                 # novel data -> root
                "ATP:0000334": {"ATP:0000335"},                                 # existing data -> root
                "ATP:0000228": {"ATP:0000321", "ATP:0000335"},                  # novel to db -> novel data -> root
                "ATP:0000229": {"ATP:0000321", "ATP:0000335"},                  # novel to field -> novel data -> root
                "ATP:0000009": {"ATP:0000001", "ATP:0000002"},                  # generic topic
                "ATP:0000079": {"ATP:0000001", "ATP:0000002", "ATP:0000009"}    # specific topic
            }.get(onto_node, set())

            mock_get_descendants.side_effect = lambda onto_node=None: {
                "ATP:0000335": {"ATP:0000321", "ATP:0000334", "ATP:0000228", "ATP:0000229"},  # root has all
                "ATP:0000321": {"ATP:0000228", "ATP:0000229"},                  # novel data -> specific subtypes
                "ATP:0000334": set(),                                           # existing data (leaf)
                "ATP:0000228": set(),                                           # novel to db (leaf)
                "ATP:0000229": set(),                                           # novel to field (leaf)
                "ATP:0000009": {"ATP:0000079"}                                  # generic topic -> specific
            }.get(onto_node, set())

            curator_source = {
                "source_evidence_assertion": "ATP:0000036",
                "source_method": "abc_literature_system",
                "validation_type": "professional_biocurator",
                "description": "curator validation",
                "data_provider": "WB",
                "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation
            }
            source_resp = client.post(url="/topic_entity_tag/source", json=curator_source, headers=auth_headers)
            source_id = source_resp.json()

            # Test 1: Generic novel data validates by specific novel data
            generic_novel_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",
                "topic_entity_tag_source_id": source_id,
                "negated": False,
                "data_novelty": "ATP:0000321"  # generic novel data
            }
            generic_resp = client.post(url="/topic_entity_tag/", json=generic_novel_tag, headers=auth_headers)
            generic_id = generic_resp.json()["topic_entity_tag_id"]

            specific_novel_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000079",  # more specific topic
                "topic_entity_tag_source_id": source_id,
                "negated": False,
                "data_novelty": "ATP:0000228"  # more specific novel data
            }
            client.post(url="/topic_entity_tag/", json=specific_novel_tag, headers=auth_headers)

            generic_data = client.get(f"/topic_entity_tag/{generic_id}").json()
            assert generic_data["validation_by_professional_biocurator"] == "validated_right"

            # Clean up
            client.delete(f"/topic_entity_tag/{generic_id}", headers=auth_headers)

            # Test 2: Novel data root validates all novel subtypes
            root_novel_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",
                "topic_entity_tag_source_id": source_id,
                "negated": False,
                "data_novelty": "ATP:0000335"  # root novelty
            }
            root_resp = client.post(url="/topic_entity_tag/", json=root_novel_tag, headers=auth_headers)
            root_id = root_resp.json()["topic_entity_tag_id"]

            # This should validate the root tag
            root_data = client.get(f"/topic_entity_tag/{root_id}").json()
            assert root_data["validation_by_professional_biocurator"] == "validated_right"

            # Clean up
            client.delete(f"/topic_entity_tag/{root_id}", headers=auth_headers)

            # Test 3: Cross-branch validation should fail
            existing_novel_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",
                "topic_entity_tag_source_id": source_id,
                "negated": False,
                "data_novelty": "ATP:0000334"  # existing data branch
            }
            existing_resp = client.post(url="/topic_entity_tag/", json=existing_novel_tag, headers=auth_headers)
            existing_id = existing_resp.json()["topic_entity_tag_id"]

            # Should NOT validate existing data tag
            existing_data = client.get(f"/topic_entity_tag/{existing_id}").json()
            assert existing_data["validation_by_professional_biocurator"] in ["not_validated", "validated_right_self"]

    def test_entity_only_validation_with_novel_data(self, test_topic_entity_tag, test_reference, test_mod, # noqa
                                                    auth_headers, db): # noqa
        """Test entity-only tag validation with novel data considerations."""
        load_name_to_atp_and_relationships_mock()
        with TestClient(app) as client, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_ancestors") as mock_get_ancestors, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_descendants") as mock_get_descendants:

            mock_get_ancestors.side_effect = lambda onto_node=None: {
                "ATP:0000321": {"ATP:0000335"},
                "ATP:0000228": {"ATP:0000321", "ATP:0000335"},
                "ATP:0000009": {"ATP:0000001", "ATP:0000002"},
                "ATP:0000079": {"ATP:0000001", "ATP:0000002", "ATP:0000009"}
            }.get(onto_node, set())

            mock_get_descendants.side_effect = lambda onto_node=None: {
                "ATP:0000335": {"ATP:0000321", "ATP:0000228"},
                "ATP:0000321": {"ATP:0000228"},
                "ATP:0000009": {"ATP:0000079"}
            }.get(onto_node, set())

            curator_source = {
                "source_evidence_assertion": "ATP:0000036",
                "source_method": "abc_literature_system",
                "validation_type": "professional_biocurator",
                "description": "curator validation",
                "data_provider": "WB",
                "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation
            }
            source_resp = client.post(url="/topic_entity_tag/source", json=curator_source, headers=auth_headers)
            source_id = source_resp.json()

            # Test entity-only tag (topic == entity_type) with novel data
            entity_only_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000005",       # entity type (gene)
                "entity_type": "ATP:0000005",  # same as topic (pure entity-only)
                "entity": "WB:WBGene00003001",
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": source_id,
                "negated": False,
                "data_novelty": "ATP:0000321"  # generic novel data
            }
            entity_resp = client.post(url="/topic_entity_tag/", json=entity_only_tag, headers=auth_headers)
            entity_id = entity_resp.json()["topic_entity_tag_id"]

            # Mixed topic+entity tag that should validate the entity-only tag
            mixed_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000079",       # specific topic (different from entity_type)
                "entity_type": "ATP:0000005",  # same entity type
                "entity": "WB:WBGene00003001",  # same entity
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": source_id,
                "negated": False,
                "data_novelty": "ATP:0000228"  # more specific novel data
            }
            client.post(url="/topic_entity_tag/", json=mixed_tag, headers=auth_headers)

            # Entity-only tag should be validated by mixed tag
            entity_data = client.get(f"/topic_entity_tag/{entity_id}").json()
            assert entity_data["validation_by_professional_biocurator"] in ["validated_right", "validated_right_self"]

    def test_revalidation_on_delete(self, test_reference, test_mod, auth_headers, db): # noqa
        """
        Test that when a validating tag is deleted, the validated tag's validation status is correctly updated.

        Scenario:
        1. Create Tag A (more generic, will be validated)
        2. Create Tag B (more specific, validates Tag A as "validated_right")
        3. Create Tag C (more specific, also validates Tag A as "validated_right")
        4. Delete Tag B
        5. Verify Tag A is still validated by Tag C
        6. Delete Tag C
        7. Verify Tag A is no longer validated
        """
        load_name_to_atp_and_relationships_mock()
        with TestClient(app) as client, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_ancestors") as mock_get_ancestors, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_descendants") as mock_get_descendants, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_curie_to_name_from_all_tets") as \
                mock_get_curie_to_name_from_all_tets:

            # Setup hierarchy based on mock data structure
            def mock_ancestors_side_effect(onto_node):
                ancestors_map = {
                    "ATP:0000079": ["ATP:0000009", "ATP:0000002", "ATP:0000001"],
                    "ATP:0000082": ["ATP:0000079", "ATP:0000009", "ATP:0000002", "ATP:0000001"],
                    "ATP:0000083": ["ATP:0000079", "ATP:0000009", "ATP:0000002", "ATP:0000001"],
                    "ATP:0000084": ["ATP:0000079", "ATP:0000009", "ATP:0000002", "ATP:0000001"]
                }
                return ancestors_map.get(onto_node, [])
            mock_get_ancestors.side_effect = mock_ancestors_side_effect

            def mock_descendants_side_effect(onto_node):
                descendants_map = {
                    "ATP:0000009": ["ATP:0000079", "ATP:0000080", "ATP:0000081", "ATP:0000082", "ATP:0000083", "ATP:0000084"],
                    "ATP:0000079": ["ATP:0000082", "ATP:0000083", "ATP:0000084"]
                }
                return descendants_map.get(onto_node, [])
            mock_get_descendants.side_effect = mock_descendants_side_effect

            # Mock the ateam curies mapping to prevent DB connections
            mock_get_curie_to_name_from_all_tets.return_value = {
                'ATP:0000009': 'phenotype', 'ATP:0000082': 'RNAi phenotype', 'ATP:0000122': 'ATP:0000122',
                'ATP:0000084': 'overexpression phenotype', 'ATP:0000079': 'genetic phenotype', 'ATP:0000005': 'gene',
                'WB:WBGene00003001': 'lin-12', 'NCBITaxon:6239': 'Caenorhabditis elegans'
            }
            # Create a curator source with validation capability
            curator_source = {
                "source_evidence_assertion": "ATP:0000036",
                "source_method": "abc_literature_system",
                "validation_type": "professional_biocurator",
                "description": "curator from ABC",
                "data_provider": "WB",
                "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation
            }
            source_resp = client.post(url="/topic_entity_tag/source", json=curator_source, headers=auth_headers)
            source_id = source_resp.json()

            # Tag A: Generic tag that will be validated
            tag_a_generic = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",  # Generic topic (phenotype)
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": source_id,
                "negated": False,
                "data_novelty": "ATP:0000334"
            }
            tag_a_resp = client.post(url="/topic_entity_tag/", json=tag_a_generic, headers=auth_headers)
            tag_a_id = tag_a_resp.json()["topic_entity_tag_id"]

            # Verify Tag A starts as not validated
            tag_a_data = client.get(f"/topic_entity_tag/{tag_a_id}").json()
            assert tag_a_data["validation_by_professional_biocurator"] == "validated_right_self"

            # Tag B: Specific tag that validates Tag A
            tag_b_specific = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000079",  # More specific topic (genetic phenotype)
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": source_id,
                "negated": False,
                "data_novelty": "ATP:0000334"
            }
            tag_b_resp = client.post(url="/topic_entity_tag/", json=tag_b_specific, headers=auth_headers)
            tag_b_id = tag_b_resp.json()["topic_entity_tag_id"]

            # Verify Tag A is now validated by Tag B
            tag_a_data = client.get(f"/topic_entity_tag/{tag_a_id}").json()
            assert tag_a_data["validation_by_professional_biocurator"] == "validated_right"
            assert tag_b_id in tag_a_data.get("validating_tags", [])

            # Tag C: Another specific tag that also validates Tag A
            tag_c_specific = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000082",  # different specific topic (RNAi phenotype)
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": source_id,
                "negated": False,
                "data_novelty": "ATP:0000334",
                "note": "Additional validating tag",
            }
            tag_c_resp = client.post(url="/topic_entity_tag/", json=tag_c_specific, headers=auth_headers)
            tag_c_id = tag_c_resp.json()["topic_entity_tag_id"]

            # Verify Tag A is validated by both Tag B and Tag C
            tag_a_data = client.get(f"/topic_entity_tag/{tag_a_id}").json()
            assert tag_a_data["validation_by_professional_biocurator"] == "validated_right"
            validating_tags = tag_a_data.get("validating_tags", [])
            assert tag_b_id in validating_tags
            assert tag_c_id in validating_tags
            assert len(validating_tags) == 2

            # Delete Tag B
            delete_resp = client.delete(f"/topic_entity_tag/{tag_b_id}", headers=auth_headers)
            assert delete_resp.status_code == status.HTTP_204_NO_CONTENT

            # Verify Tag A is still validated, but only by Tag C
            tag_a_data = client.get(f"/topic_entity_tag/{tag_a_id}").json()
            assert tag_a_data["validation_by_professional_biocurator"] == "validated_right"
            validating_tags = tag_a_data.get("validating_tags", [])
            assert tag_b_id not in validating_tags
            assert tag_c_id in validating_tags
            assert len(validating_tags) == 1

            # Delete Tag C
            delete_resp = client.delete(f"/topic_entity_tag/{tag_c_id}", headers=auth_headers)
            assert delete_resp.status_code == status.HTTP_204_NO_CONTENT

            # Verify Tag A is no longer validated
            tag_a_data = client.get(f"/topic_entity_tag/{tag_a_id}").json()
            assert tag_a_data["validation_by_professional_biocurator"] == "validated_right_self"
            validating_tags = tag_a_data.get("validating_tags", [])
            assert len(validating_tags) == 0

    def test_revalidation_on_delete_with_conflicting_validations(self, test_reference, test_mod, auth_headers, db): # noqa
        """
        Test revalidation when deleting a tag that causes validation conflicts.

        Scenario:
        1. Create Tag A (generic, will be validated)
        2. Create Tag B (specific, negated=False, validates Tag A as "validated_right")
        3. Create Tag C (specific, negated=True, validates Tag A as "validated_wrong")
        4. Verify Tag A has validation_conflict
        5. Delete Tag B
        6. Verify Tag A is now "validated_wrong" (only Tag C remains)
        """
        load_name_to_atp_and_relationships_mock()
        with TestClient(app) as client, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_ancestors") as mock_get_ancestors, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_descendants") as mock_get_descendants, \
                patch("agr_literature_service.api.crud.topic_entity_tag_crud.get_curie_to_name_from_all_tets") as \
                mock_get_curie_to_name_from_all_tets:

            # Setup hierarchy based on mock data structure
            def mock_ancestors_side_effect(onto_node):
                ancestors_map = {
                    "ATP:0000079": ["ATP:0000009", "ATP:0000002", "ATP:0000001"],
                    "ATP:0000082": ["ATP:0000079", "ATP:0000009", "ATP:0000002", "ATP:0000001"],
                    "ATP:0000083": ["ATP:0000079", "ATP:0000009", "ATP:0000002", "ATP:0000001"],
                    "ATP:0000084": ["ATP:0000079", "ATP:0000009", "ATP:0000002", "ATP:0000001"]
                }
                return ancestors_map.get(onto_node, [])
            mock_get_ancestors.side_effect = mock_ancestors_side_effect

            def mock_descendants_side_effect(onto_node):
                descendants_map = {
                    "ATP:0000009": ["ATP:0000079", "ATP:0000080", "ATP:0000081", "ATP:0000082", "ATP:0000083", "ATP:0000084"],
                    "ATP:0000079": ["ATP:0000082", "ATP:0000083", "ATP:0000084"]
                }
                return descendants_map.get(onto_node, [])
            mock_get_descendants.side_effect = mock_descendants_side_effect

            # Mock the ateam curies mapping to prevent DB connections
            mock_get_curie_to_name_from_all_tets.return_value = {
                'ATP:0000009': 'phenotype', 'ATP:0000082': 'RNAi phenotype', 'ATP:0000122': 'ATP:0000122',
                'ATP:0000084': 'overexpression phenotype', 'ATP:0000079': 'genetic phenotype', 'ATP:0000005': 'gene',
                'WB:WBGene00003001': 'lin-12', 'NCBITaxon:6239': 'Caenorhabditis elegans'
            }

            # Create curator source
            curator_source = {
                "source_evidence_assertion": "ATP:0000036",
                "source_method": "abc_literature_system",
                "validation_type": "professional_biocurator",
                "description": "curator from ABC",
                "data_provider": "WB",
                "secondary_data_provider_abbreviation": test_mod.new_mod_abbreviation
            }
            source_resp = client.post(url="/topic_entity_tag/source", json=curator_source, headers=auth_headers)
            source_id = source_resp.json()

            # Tag A: Generic tag
            tag_a = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",  # Generic topic (phenotype)
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": source_id,
                "negated": False,
                "data_novelty": "ATP:0000334"
            }
            tag_a_resp = client.post(url="/topic_entity_tag/", json=tag_a, headers=auth_headers)
            tag_a_id = tag_a_resp.json()["topic_entity_tag_id"]

            # Tag B: Specific positive tag
            tag_b = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000079",  # Specific topic (genetic phenotype)
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": source_id,
                "negated": False,
                "data_novelty": "ATP:0000334",
                "created_by": "WBPerson1",
                "date_created": "2020-01-01"
            }
            tag_b_resp = client.post(url="/topic_entity_tag/", json=tag_b, headers=auth_headers)
            tag_b_id = tag_b_resp.json()["topic_entity_tag_id"]

            # Tag C: Specific negative tag
            tag_c = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000079",  # Specific topic
                "entity_type": "ATP:0000005",
                "entity": "WB:WBGene00003001",
                "entity_id_validation": "alliance",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": source_id,
                "negated": True,
                "data_novelty": "ATP:0000334",
                "created_by": "WBPerson2",
                "date_created": "2020-01-02"
            }
            client.post(url="/topic_entity_tag/", json=tag_c, headers=auth_headers)

            # Verify Tag A has validation conflict
            tag_a_data = client.get(f"/topic_entity_tag/{tag_a_id}").json()
            assert tag_a_data["validation_by_professional_biocurator"] == "validation_conflict"
            validating_tags = tag_a_data.get("validating_tags", [])
            assert tag_b_id in validating_tags

            # Delete Tag B (positive validation)
            delete_resp = client.delete(f"/topic_entity_tag/{tag_b_id}", headers=auth_headers)
            assert delete_resp.status_code == status.HTTP_204_NO_CONTENT

            # Verify Tag A is now validated_right_self (only negative validation on a more specific topic remains)
            tag_a_data = client.get(f"/topic_entity_tag/{tag_a_id}").json()
            assert tag_a_data["validation_by_professional_biocurator"] == "validated_right_self"
            validating_tags = tag_a_data.get("validating_tags", [])
            assert len(validating_tags) == 0
