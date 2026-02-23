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

MCATestData = namedtuple('MCATestData', ['response', 'new_mca_id', 'related_ref_curie'])


@pytest.fixture
def test_mca(monkeypatch, db, auth_headers, test_reference, test_mod): # noqa
    print("***** Adding a test mod-corpus association *****")
    monkeypatch.setattr("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
                        load_name_to_atp_and_relationships_mock)
    with TestClient(app) as client:
        mod_response = client.get(url=f"/mod/{test_mod.new_mod_abbreviation}", headers=auth_headers)
        mod_abbreviation = mod_response.json()["abbreviation"]
        new_mca = {
            "mod_abbreviation": mod_abbreviation,
            "reference_curie": test_reference.new_ref_curie,
            "mod_corpus_sort_source": 'mod_pubmed_search'
        }
        response = client.post(url="/reference/mod_corpus_association/", json=new_mca, headers=auth_headers)
        yield MCATestData(response, response.json(), test_reference.new_ref_curie)


class TestModCorpusAssociation:

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
           load_name_to_atp_and_relationships_mock)
    def test_get_bad_mca(self, test_mca, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get(url="/reference/mod_corpus_association/-1", headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
           load_name_to_atp_and_relationships_mock)
    def test_create_mca(self, test_mca): # noqa
        assert test_mca.response.status_code == status.HTTP_201_CREATED

    def test_show_by_reference_mod_abbreviation(self, test_mca, auth_headers):  # noqa
        with TestClient(app) as client:
            test_mca_response = client.get(url=f"/reference/mod_corpus_association/{test_mca.new_mca_id}",
                                           headers=auth_headers)
            test_mca_abbreviation = test_mca_response.json()["mod_abbreviation"]
            response = client.get(url=f"/reference/mod_corpus_association/"
                                      f"reference/{test_mca.related_ref_curie}/"
                                      f"mod_abbreviation/{test_mca_abbreviation}",
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
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
                                  f"{test_mca.new_mca_id}",
                                  headers=auth_headers).json()["mod_corpus_sort_source"] == "assigned_for_review"

            # add changeset tests
            response = client.get(url=f"/reference/mod_corpus_association/{test_mca.new_mca_id}/versions",
                                  headers=auth_headers)
            transactions = response.json()
            print(transactions)
            assert transactions[1]['changeset']['mod_corpus_sort_source'][0] == 'mod_pubmed_search'
            assert transactions[1]['changeset']['mod_corpus_sort_source'][1] == 'assigned_for_review'

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
           load_name_to_atp_and_relationships_mock)
    def test_change_reference_mca(self, test_mca, auth_headers, test_reference2):  # noqa
        with TestClient(app) as client:
            patched_data = {"reference_curie": test_reference2.new_ref_curie}
            patch_response = client.patch(url=f"/reference/mod_corpus_association/{test_mca.new_mca_id}",
                                          json=patched_data, headers=auth_headers)
            assert patch_response.status_code == status.HTTP_202_ACCEPTED
            test_mca_response = client.get(url=f"/reference/mod_corpus_association/{test_mca.new_mca_id}",
                                           headers=auth_headers)
            assert test_mca_response.json()["reference_curie"] == test_reference2.new_ref_curie

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
           load_name_to_atp_and_relationships_mock)
    def test_show_mca(self, test_mca, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/reference/mod_corpus_association/{test_mca.new_mca_id}",
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
           load_name_to_atp_and_relationships_mock)
    def test_destroy_mca(self, test_mca, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.delete(url=f"/reference/mod_corpus_association/{test_mca.new_mca_id}",
                                     headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT

            # it should now give an error on lookup.
            response = client.get(url=f"/reference/mod_corpus_association/{test_mca.new_mca_id}",
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

            # deleting it again should give an error as the lookup will fail.
            response = client.delete(url=f"/reference/mod_corpus_association/{test_mca.new_mca_id}",
                                     headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
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


class TestAllianceOnlyReference:
    """
    Tests for Alliance-only reference creation.

    Alliance references:
    - Can be created with only Alliance MOD corpus association (no other xrefs required)
    - Do not auto-generate MOD-specific IDs (unlike WB/SGD)
    - Only require an AGRKB curie (Alliance ID)
    """

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
           load_name_to_atp_and_relationships_mock)
    def test_create_alliance_only_reference(self, db, auth_headers):  # noqa
        """
        Test that a reference can be created with only Alliance MOD corpus association
        and no cross-references (no PMID, PMCID, or other MOD xrefs required).
        """
        with TestClient(app) as client:
            populate_test_mods()

            # Create a reference with only Alliance MOD corpus association
            new_reference = {
                "title": "Alliance-only test paper",
                "category": "research_article",
                "abstract": "This is a test paper for Alliance-only reference creation.",
                "mod_corpus_associations": [
                    {
                        "mod_abbreviation": "alliance",
                        "mod_corpus_sort_source": "manual_creation",
                        "corpus": True
                    }
                ]
            }
            response = client.post(url="/reference/", json=new_reference, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED

            ref_curie = response.json()
            assert ref_curie.startswith("AGRKB:")

            # Verify the reference was created
            ref_response = client.get(url=f"/reference/{ref_curie}", headers=auth_headers)
            assert ref_response.status_code == status.HTTP_200_OK
            ref_data = ref_response.json()
            assert ref_data["title"] == "Alliance-only test paper"

            # Verify Alliance MOD corpus association was created
            assert len(ref_data["mod_corpus_associations"]) == 1
            assert ref_data["mod_corpus_associations"][0]["mod_abbreviation"] == "alliance"
            assert ref_data["mod_corpus_associations"][0]["corpus"] is True

            # Verify no cross-references were auto-generated
            reference_obj = db.query(ReferenceModel).filter(
                ReferenceModel.curie == ref_curie).first()
            xrefs = db.query(CrossReferenceModel).filter(
                CrossReferenceModel.reference_id == reference_obj.reference_id).all()
            assert len(xrefs) == 0

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
           load_name_to_atp_and_relationships_mock)
    def test_alliance_mca_does_not_generate_mod_id(self, db, auth_headers):  # noqa
        """
        Test that setting corpus=true for Alliance MOD does NOT auto-generate
        a MOD-specific cross-reference (unlike WB/SGD which do auto-generate).
        """
        with TestClient(app) as client:
            populate_test_mods()

            # First create a reference without MOD corpus association
            new_reference = {
                "title": "Test paper for Alliance MCA",
                "category": "research_article"
            }
            response = client.post(url="/reference/", json=new_reference, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            ref_curie = response.json()

            # Now add Alliance MOD corpus association with corpus=true
            new_mca = {
                "mod_abbreviation": "alliance",
                "reference_curie": ref_curie,
                "mod_corpus_sort_source": "manual_creation",
                "corpus": True
            }
            mca_response = client.post(
                url="/reference/mod_corpus_association/",
                json=new_mca,
                headers=auth_headers
            )
            assert mca_response.status_code == status.HTTP_201_CREATED

            # Verify NO cross-reference was auto-generated for Alliance
            reference_obj = db.query(ReferenceModel).filter(
                ReferenceModel.curie == ref_curie).first()
            alliance_xref = db.query(CrossReferenceModel).filter(
                and_(
                    CrossReferenceModel.reference_id == reference_obj.reference_id,
                    CrossReferenceModel.curie_prefix == 'alliance'
                )
            ).one_or_none()
            assert alliance_xref is None

            # Also verify no other xrefs were created
            all_xrefs = db.query(CrossReferenceModel).filter(
                CrossReferenceModel.reference_id == reference_obj.reference_id).all()
            assert len(all_xrefs) == 0

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
           load_name_to_atp_and_relationships_mock)
    def test_alliance_reference_with_optional_pmid(self, db, auth_headers):  # noqa
        """
        Test that Alliance references CAN have optional xrefs like PMID if desired,
        but they are not required.
        """
        with TestClient(app) as client:
            populate_test_mods()

            # Create Alliance reference with optional PMID
            new_reference = {
                "title": "Alliance paper with PMID",
                "category": "research_article",
                "cross_references": [
                    {"curie": "PMID:12345678"}
                ],
                "mod_corpus_associations": [
                    {
                        "mod_abbreviation": "alliance",
                        "mod_corpus_sort_source": "manual_creation",
                        "corpus": True
                    }
                ]
            }
            response = client.post(url="/reference/", json=new_reference, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED

            ref_curie = response.json()

            # Verify the PMID xref was created
            reference_obj = db.query(ReferenceModel).filter(
                ReferenceModel.curie == ref_curie).first()
            pmid_xref = db.query(CrossReferenceModel).filter(
                and_(
                    CrossReferenceModel.reference_id == reference_obj.reference_id,
                    CrossReferenceModel.curie_prefix == 'PMID'
                )
            ).one_or_none()
            assert pmid_xref is not None
            assert pmid_xref.curie == "PMID:12345678"

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
           load_name_to_atp_and_relationships_mock)
    def test_alliance_only_reference_no_curator_mod(self, db, auth_headers):  # noqa
        """
        Test that creating an Alliance-only reference does not automatically
        add any other MOD associations (simulating the 'Alliance only' checkbox behavior).
        """
        with TestClient(app) as client:
            populate_test_mods()

            # Create reference with ONLY Alliance MOD
            new_reference = {
                "title": "Alliance-only paper without curator MOD",
                "category": "research_article",
                "mod_corpus_associations": [
                    {
                        "mod_abbreviation": "alliance",
                        "mod_corpus_sort_source": "manual_creation",
                        "corpus": True
                    }
                ]
            }
            response = client.post(url="/reference/", json=new_reference, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            ref_curie = response.json()

            # Verify ONLY Alliance MOD corpus association exists
            ref_response = client.get(url=f"/reference/{ref_curie}", headers=auth_headers)
            ref_data = ref_response.json()

            assert len(ref_data["mod_corpus_associations"]) == 1
            assert ref_data["mod_corpus_associations"][0]["mod_abbreviation"] == "alliance"

            # Verify no WB, SGD, FB, etc. MOD associations were auto-created
            for mca in ref_data["mod_corpus_associations"]:
                assert mca["mod_abbreviation"] == "alliance"

    def test_alliance_excluded_from_get_mod_abbreviations(self, db):  # noqa
        """
        Test that Alliance MOD is excluded from get_mod_abbreviations(),
        which is used by automated scripts to iterate over MODs.
        """
        from agr_literature_service.lit_processing.utils.db_read_utils import get_mod_abbreviations

        populate_test_mods()

        mod_abbreviations = get_mod_abbreviations(db)

        # Alliance should NOT be in the list
        assert 'alliance' not in mod_abbreviations

        # GO should also NOT be in the list
        assert 'GO' not in mod_abbreviations

        # Other MODs should be present
        assert 'WB' in mod_abbreviations
        assert 'SGD' in mod_abbreviations
        assert 'FB' in mod_abbreviations
        assert 'MGI' in mod_abbreviations
        assert 'RGD' in mod_abbreviations
        assert 'ZFIN' in mod_abbreviations

    def test_alliance_excluded_from_taxons(self, db):  # noqa
        """
        Test that Alliance MOD is excluded from mod_crud.taxons(),
        since Alliance is not organism-specific.
        """
        from agr_literature_service.api.crud.mod_crud import taxons

        populate_test_mods()

        # Use type='all' to avoid None taxon_ids issue in test data
        taxon_list = taxons(db, type='all')

        # Alliance should NOT be in the taxons list
        mod_abbrevs = [t['mod_abbreviation'] for t in taxon_list]
        assert 'alliance' not in mod_abbrevs
        assert 'GO' not in mod_abbrevs

        # Other MODs should be present
        assert 'WB' in mod_abbrevs
        assert 'SGD' in mod_abbrevs
