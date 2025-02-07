from collections import namedtuple
from typing import Tuple, Dict
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
from agr_literature_service.api.crud.ateam_db_helpers import set_globals

test_reference2 = test_reference

TestTETData = namedtuple('TestTETData', ['response', 'new_tet_id', 'related_ref_curie'])

def mock_load_name_to_atp_and_relationships():
    workflow_children = {
        'ATP:0000177': ['ATP:0000172', 'ATP:0000140', 'ATP:0000165', 'ATP:0000161'],
        'ATP:0000172': ['ATP:0000175', 'ATP:0000174', 'ATP:0000173', 'ATP:0000178'],
        'ATP:0000140': ['ATP:0000141', 'ATP:0000135', 'ATP:0000139', 'ATP:0000134'],
        'ATP:0000165': ['ATP:0000168', 'ATP:0000167', 'ATP:0000170', 'ATP:0000171', 'ATP:0000169', 'ATP:0000166'],
        'ATP:0000161': ['ATP:0000164', 'ATP:0000163', 'ATP:0000162'],

        'ATP:fileupload': ['ATP:0000141', 'ATP:fileuploadinprogress', 'ATP:fileuploadcomplete', 'ATP:fileuploadfailed'],
        'ATP:0000166':  ['ATP:task1_needed', 'ATP:task2_needed', 'ATP:task3_needed'],
        'ATP:0000178': ['ATP:task1_in_progress', 'ATP:task2_in_progress', 'ATP:task3_in_progress'],
        'ATP:0000189': ['ATP:task1_failed', 'ATP:task2_failed', 'ATP:task3_failed'],
        'ATP:0000169': ['ATP:task1_complete', 'ATP:task2_complete', 'ATP:task3_complete']
    }
    workflow_parent = {
        'ATP:0000172': ['ATP:0000177'],
        'ATP:0000140': ['ATP:0000177'],
        'ATP:0000165': ['ATP:0000177'],
        'ATP:0000161': ['ATP:0000177'],
        'ATP:0000175': ['ATP:0000172'],
        'ATP:0000174': ['ATP:0000172'],
        'ATP:0000173': ['ATP:0000172'],
        'ATP:0000178': ['ATP:0000172'],
        'ATP:0000141': ['ATP:0000140'],
        'ATP:0000135': ['ATP:0000140'],
        'ATP:0000139': ['ATP:0000140'],
        'ATP:0000134': ['ATP:0000140'],
        'ATP:0000168': ['ATP:0000165'],
        'ATP:0000167': ['ATP:0000165'],
        'ATP:0000170': ['ATP:0000165'],
        'ATP:0000171': ['ATP:0000165'],
        'ATP:0000169': ['ATP:0000165'],
        'ATP:0000166': ['ATP:0000165'],
        'ATP:0000164': ['ATP:0000161'],
        'ATP:0000163': ['ATP:0000161'],
        'ATP:0000162': ['ATP:0000161']
    }
    atp_to_name = {
        'ATP:0000009': 'phenotype',
        'ATP:0000082': 'RNAi phenotype',
        'ATP:0000122': 'ATP:0000122',
        'ATP:0000084': 'overexpression phenotype',
        'ATP:0000079': 'genetic phenotype',
        'ATP:0000005': 'gene',
        'WB:WBGene00003001': 'lin-12',
        'NCBITaxon:6239': 'Caenorhabditis elegans'
    }
    name_to_atp = {
        'phenotype': 'ATP:0000009',
        'RNAi phenotype': 'ATP:0000082',
        'ATP:0000122': 'ATP:0000122',
        'overexpression phenotype': 'ATP:0000084',
        'genetic phenotype': 'ATP:0000079',
        'gene': 'ATP:0000005',
        'lin-12': 'WB:WBGene00003001',
        'Caenorhabditis elegans': 'NCBITaxon:6239'
    }
    for atp in workflow_children.keys():
        atp_to_name[atp] = atp
        name_to_atp[atp] = atp
        for atp2 in workflow_children[atp]:
            name_to_atp[atp2] = atp2
            atp_to_name[atp2] = atp2
    set_globals(atp_to_name, name_to_atp, workflow_children, workflow_parent)


@pytest.fixture
@patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
       mock_load_name_to_atp_and_relationships)
def test_topic_entity_tag(db, auth_headers, test_reference, test_topic_entity_tag_source, test_mod): # noqa
    print("***** Adding a test tag *****")
    with TestClient(app) as client:
        # mock_load_name_to_atp_and_relationships()
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
            "novel_topic_data": True,
            "note": "test note",
            "created_by": "WBPerson1",
            "date_created": "2020-01-01"
        }
        response = client.post(url="/topic_entity_tag/", json=new_tet, headers=auth_headers)
        print(f"TTTT: {response}")
        print(response.json)
        yield TestTETData(response, response.json()['topic_entity_tag_id'], test_reference.new_ref_curie)


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
                "entity_id_validation": "alliance",
                "entity_published_as": "test",
                "species": "NCBITaxon:6239",
                "topic_entity_tag_source_id": -1,
                "negated": False,
                "novel_topic_data": False,
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
                "novel_topic_data": True,
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
                "novel_topic_data": True,
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
                "novel_topic_data": True,
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
        with TestClient(app) as client, \
                patch("agr_literature_service.api.crud.topic_entity_tag_utils.get_ancestors") as mock_get_ancestors, \
                patch("agr_literature_service.api.crud.topic_entity_tag_utils.get_descendants") as mock_get_descendants:
            mock_get_ancestors.return_value = []
            mock_get_descendants.return_value = []
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
                "novel_topic_data": True
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
                "novel_topic_data": False
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
                "novel_topic_data": True
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
                "novel_topic_data": False,
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
        with TestClient(app) as client, \
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
                "novel_topic_data": True
            }
            more_generic_tag_2 = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000068",  # more generic topic
                "topic_entity_tag_source_id": auth_source_1_resp.json(),
                "negated": False,
                "novel_topic_data": True
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
                "novel_topic_data": False
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
                "novel_topic_data": True
            }

            # add the new tags
            mock_get_ancestors.return_value = {'ATP:0000001', 'ATP:0000002', 'ATP:0000009'}
            mock_get_descendants.return_value = {'ATP:0000009', 'ATP:0000033', 'ATP:0000034', 'ATP:0000079',
                                                 'ATP:0000080', 'ATP:0000081', 'ATP:0000082', 'ATP:0000083',
                                                 'ATP:0000084', 'ATP:0000085', 'ATP:0000086', 'ATP:0000087',
                                                 'ATP:0000100'}
            more_generic_tag_id = client.post(url="/topic_entity_tag/", json=more_generic_tag,
                                              headers=auth_headers).json()["topic_entity_tag_id"]
            mock_get_ancestors.return_value = {'ATP:0000001', 'ATP:0000002', 'ATP:0000009', 'ATP:0000079'}
            mock_get_descendants.return_value = {'ATP:0000068', 'ATP:0000071'}
            more_specific_tag_id = client.post(url="/topic_entity_tag/", json=more_specific_tag,
                                               headers=auth_headers).json()["topic_entity_tag_id"]
            mock_get_ancestors.return_value = {'ATP:0000001', 'ATP:0000002', 'ATP:0000015', 'ATP:0000068',
                                               'ATP:0000071'}
            mock_get_descendants.return_value = {'ATP:0000071'}
            more_specific_tag_id_2 = client.post(url="/topic_entity_tag/", json=more_specific_tag_2,
                                                 headers=auth_headers).json()["topic_entity_tag_id"]
            mock_get_ancestors.return_value = {'ATP:0000001', 'ATP:0000002', 'ATP:0000015', 'ATP:0000068'}
            mock_get_descendants.return_value = {'ATP:0000068', 'ATP:0000071'}
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
                "novel_topic_data": True
            }
            more_generic_positive_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",  # phenotype
                "topic_entity_tag_source_id": author_source_resp.json(),
                "negated": False,
                "novel_topic_data": True
            }
            more_generic_negative_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",  # phenotype
                "topic_entity_tag_source_id": author_source_resp.json(),
                "negated": True,
                "novel_topic_data": True
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
                "novel_topic_data": False
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
                "novel_topic_data": True
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
                "novel_topic_data": True
            }
            more_generic_positive_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",  # phenotype
                "topic_entity_tag_source_id": author_source_resp.json(),
                "negated": False,
                "novel_topic_data": True
            }
            more_generic_negative_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",  # phenotype
                "topic_entity_tag_source_id": author_source_resp.json(),
                "negated": True,
                "novel_topic_data": True
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
                "novel_topic_data": False
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
                "novel_topic_data": True
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
                "novel_topic_data": True
            }
            null_tag = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "ATP:0000009",
                "topic_entity_tag_source_id": auth_source_1_resp.json(),
                "negated": None,
                "novel_topic_data": True
            }
            # add the new tags
            mock_get_ancestors.return_value = {'ATP:0000001', 'ATP:0000002', 'ATP:0000009'}
            mock_get_descendants.return_value = {'ATP:0000009', 'ATP:0000033', 'ATP:0000034', 'ATP:0000079', 'ATP:0000080', 'ATP:0000081', 'ATP:0000082', 'ATP:0000083', 'ATP:0000084', 'ATP:0000085', 'ATP:0000086', 'ATP:0000087', 'ATP:0000100'}
            positive_tag_id = client.post(url="/topic_entity_tag/", json=positive_tag, headers=auth_headers).json()['topic_entity_tag_id']
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
        onto_node = "ATP:0000079"
        ancestors = get_ancestors(onto_node)
        expected_ancestors = {"ATP:0000001", "ATP:0000002", "ATP:0000009"}
        assert [ancestor in expected_ancestors for ancestor in ancestors]

    @pytest.mark.webtest
    def test_get_descendants(self, auth_headers):  # noqa
        onto_node = "ATP:0000009"
        descendants = get_descendants(onto_node)
        expected_descendants = {'ATP:0000079', 'ATP:0000080', 'ATP:0000081', 'ATP:0000082', 'ATP:0000083',
                                'ATP:0000084', 'ATP:0000085', 'ATP:0000086', 'ATP:0000087', 'ATP:0000033',
                                'ATP:0000034', 'ATP:0000100'}
        assert [ancestor in expected_descendants for ancestor in descendants]

    @pytest.mark.webtest
    def test_get_ancestors_non_existent(self, auth_headers):  # noqa
        onto_node = "ATP:000007"
        ancestors = get_ancestors(onto_node)
        assert len(ancestors) == 0
