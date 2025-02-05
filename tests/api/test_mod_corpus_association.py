from unittest.mock import patch

import pytest
from sqlalchemy import and_
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import ReferenceModel, CrossReferenceModel
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from ..fixtures import load_name_to_atp_and_relationships_mock, search_ancestors_or_descendants_mock
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from .test_reference import test_reference # noqa
from .test_mod import test_mod # noqa
from collections import namedtuple

test_reference2 = test_reference

TestMCAData = namedtuple('TestMCAData', ['response', 'new_mca_id', 'related_ref_curie'])


@pytest.fixture
def test_mca(monkeypatch, db, auth_headers, test_reference, test_mod): # noqa
    print("***** Adding a test mod-corpus association *****")
    monkeypatch.setattr("agr_literature_service.api.crud.workflow_tag_crud.load_name_to_atp_and_relationships_mock",
                        load_name_to_atp_and_relationships_mock)
    with TestClient(app) as client:
        mod_response = client.get(url=f"/mod/{test_mod.new_mod_abbreviation}")
        mod_abbreviation = mod_response.json()["abbreviation"]
        new_mca = {
            "mod_abbreviation": mod_abbreviation,
            "reference_curie": test_reference.new_ref_curie,
            "mod_corpus_sort_source": 'mod_pubmed_search'
        }
        response = client.post(url="/reference/mod_corpus_association/", json=new_mca, headers=auth_headers)
        yield TestMCAData(response, response.json(), test_reference.new_ref_curie)


class TestModCorpusAssociation:

    @patch("agr_literature_service.api.crud.workflow_tag_crud.load_name_to_atp_and_relationships_mock",
           load_name_to_atp_and_relationships_mock)
    def test_get_bad_mca(self, test_mca): # noqa
        with TestClient(app) as client:
            response = client.get(url="/reference/mod_corpus_association/-1")
            assert response.status_code == status.HTTP_404_NOT_FOUND

    @patch("agr_literature_service.api.crud.workflow_tag_crud.load_name_to_atp_and_relationships_mock",
           load_name_to_atp_and_relationships_mock)
    def test_create_mca(self, test_mca): # noqa
        assert test_mca.response.status_code == status.HTTP_201_CREATED

    def test_show_by_reference_mod_abbreviation(self, test_mca): # noqa
        with TestClient(app) as client:
            test_mca_response = client.get(url=f"/reference/mod_corpus_association/{test_mca.new_mca_id}")
            test_mca_abbreviation = test_mca_response.json()["mod_abbreviation"]
            response = client.get(url=f"/reference/mod_corpus_association/"
                                      f"reference/{test_mca.related_ref_curie}/"
                                      f"mod_abbreviation/{test_mca_abbreviation}")
            assert response.status_code == status.HTTP_200_OK

    @patch("agr_literature_service.api.crud.workflow_tag_crud.load_name_to_atp_and_relationships_mock",
           load_name_to_atp_and_relationships_mock)
    def test_patch_mca(self, test_mca, auth_headers): # noqa
        with TestClient(app) as client:
            patched_data = {"reference_curie": test_mca.related_ref_curie,
                            "mod_corpus_sort_source": "assigned_for_review"
                            }
            patch_response = client.patch(url=f"/reference/mod_corpus_association/{test_mca.new_mca_id}",
                                          json=patched_data, headers=auth_headers)
            assert patch_response.status_code == status.HTTP_202_ACCEPTED
            assert client.get(url=f"/reference/mod_corpus_association/"
                                  f"{test_mca.new_mca_id}").json()["mod_corpus_sort_source"] == "assigned_for_review"

            # add changeset tests
            response = client.get(url=f"/reference/mod_corpus_association/{test_mca.new_mca_id}/versions")
            transactions = response.json()
            print(transactions)
            assert transactions[1]['changeset']['mod_corpus_sort_source'][0] == 'mod_pubmed_search'
            assert transactions[1]['changeset']['mod_corpus_sort_source'][1] == 'assigned_for_review'

    @patch("agr_literature_service.api.crud.workflow_tag_crud.load_name_to_atp_and_relationships_mock",
           load_name_to_atp_and_relationships_mock)
    def test_change_reference_mca(self, test_mca, auth_headers, test_reference2):  # noqa
        with TestClient(app) as client:
            patched_data = {"reference_curie": test_reference2.new_ref_curie}
            patch_response = client.patch(url=f"/reference/mod_corpus_association/{test_mca.new_mca_id}",
                                          json=patched_data, headers=auth_headers)
            assert patch_response.status_code == status.HTTP_202_ACCEPTED
            test_mca_response = client.get(url=f"/reference/mod_corpus_association/{test_mca.new_mca_id}")
            assert test_mca_response.json()["reference_curie"] == test_reference2.new_ref_curie

    @patch("agr_literature_service.api.crud.workflow_tag_crud.load_name_to_atp_and_relationships_mock",
           load_name_to_atp_and_relationships_mock)
    def test_show_mca(self, test_mca): # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/reference/mod_corpus_association/{test_mca.new_mca_id}")
            assert response.status_code == status.HTTP_200_OK

    @patch("agr_literature_service.api.crud.workflow_tag_crud.load_name_to_atp_and_relationships_mock",
           load_name_to_atp_and_relationships_mock)
    def test_destroy_mca(self, test_mca, auth_headers): # noqa
        with TestClient(app) as client:
            response = client.delete(url=f"/reference/mod_corpus_association/{test_mca.new_mca_id}",
                                     headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT

            # it should now give an error on lookup.
            response = client.get(url=f"/reference/mod_corpus_association/{test_mca.new_mca_id}")
            assert response.status_code == status.HTTP_404_NOT_FOUND

            # deleting it again should give an error as the lookup will fail.
            response = client.delete(url=f"/reference/mod_corpus_association/{test_mca.new_mca_id}",
                                     headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    @patch("agr_literature_service.api.crud.workflow_tag_crud.load_name_to_atp_and_relationships_mock",
           load_name_to_atp_and_relationships_mock)
    def test_mca_modid_wb(self, db, test_reference, auth_headers): # noqa
        with TestClient(app) as client:
            populate_test_mods()

            new_mca = {
                "mod_abbreviation": "WB",
                "reference_curie": test_reference.new_ref_curie,
                "mod_corpus_sort_source": "assigned_for_review",
                "corpus": "false"
            }
            response_mca = client.post(url="/reference/mod_corpus_association/", json=new_mca, headers=auth_headers)
            assert response_mca.status_code == status.HTTP_201_CREATED

            reference_obj = db.query(ReferenceModel).filter(
                ReferenceModel.curie == test_reference.new_ref_curie).first()
            xref = db.query(CrossReferenceModel).filter(and_(CrossReferenceModel.reference_id == reference_obj.reference_id, CrossReferenceModel.curie_prefix == 'WB')).one_or_none()
            assert xref is None

            patch_mca = {
                "corpus": "true"
            }
            patch_response = client.patch(url=f"/reference/mod_corpus_association/{response_mca.text}",
                                          json=patch_mca, headers=auth_headers)
            assert patch_response.status_code == status.HTTP_202_ACCEPTED
            xref = db.query(CrossReferenceModel).filter(and_(CrossReferenceModel.reference_id == reference_obj.reference_id, CrossReferenceModel.curie_prefix == 'WB')).one_or_none()
            assert xref.curie == 'WB:WBPaper00000001'


    @patch("agr_literature_service.api.crud.ateam_db_helpers.search_ancestors_or_descendants",
           search_ancestors_or_descendants_mock)
    def test_mca_modid_wb_obsolete_xref(self, db, test_reference, auth_headers): # noqa
        # allow creating of xref via mca if xref already has mod + reference but is_obsolete
        with TestClient(app) as client:
            populate_test_mods()

            obs_cross_ref = {"curie": "WB:WBPaper00001234", "reference_curie": test_reference.new_ref_curie,
                             "is_obsolete": True}
            response = client.post(url="/cross_reference/", json=obs_cross_ref, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED

            new_mca = {
                "mod_abbreviation": "WB",
                "reference_curie": test_reference.new_ref_curie,
                "mod_corpus_sort_source": "manual_creation",
                "corpus": "true"
            }
            response_mca = client.post(url="/reference/mod_corpus_association/", json=new_mca, headers=auth_headers)
            assert response_mca.status_code == status.HTTP_201_CREATED

            xref_obs = db.query(CrossReferenceModel).filter(CrossReferenceModel.is_obsolete.is_(True)).one()
            assert xref_obs.curie == 'WB:WBPaper00001234'

            xref_obs_false = db.query(CrossReferenceModel).filter(CrossReferenceModel.is_obsolete.is_(False)).one()
            assert xref_obs_false.curie == 'WB:WBPaper00001235'
