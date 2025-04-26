from unittest.mock import patch

from starlette.testclient import TestClient

from fastapi import status
from agr_literature_service.api.main import app
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from .test_mod_corpus_association import test_mca # noqa
from ..fixtures import load_name_to_atp_and_relationships_mock, search_ancestors_or_descendants_mock
from ..fixtures import db # noqa
from .test_reference import test_reference # noqa
from .test_mod import test_mod # noqa
from .fixtures import auth_headers # noqa


class TestSort:

    def test_sort_need_review(self, test_mca): # noqa
        with TestClient(app) as client:
            res = client.get(url="/sort/need_review", params={"mod_abbreviation": "0015_AtDB", "count": 10})
            assert res.status_code == status.HTTP_200_OK
            assert len(res.json()) > 0

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
           load_name_to_atp_and_relationships_mock)
    @patch("agr_literature_service.api.crud.ateam_db_helpers.search_ancestors_or_descendants",
           search_ancestors_or_descendants_mock)
    def test_sort_prepublication_pipeline(self, db, auth_headers): # noqa
        with TestClient(app) as client:
            populate_test_mods()

            reference_create_json = {
                "cross_references": [
                    {
                        "curie": "PMID:1110",
                        "is_obsolete": "false"
                    }
                ],
                "mod_corpus_associations": [
                    {
                        "mod_abbreviation": "WB",
                        "mod_corpus_sort_source": "prepublication_pipeline",
                        "corpus": "true"
                    }
                ],
                "title": "pp pmid wb",
                "prepublication_pipeline": "true"
            }
            response = client.post(url="/reference/", json=reference_create_json, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            new_curie = response.text
            if new_curie.startswith('"') and new_curie.endswith('"'):
                curie_pp_pmid_wb = new_curie[1:-1]
            curie_pp_pmid_wb_bool = False

            reference_create_json = {
                "mod_corpus_associations": [
                    {
                        "mod_abbreviation": "WB",
                        "mod_corpus_sort_source": "prepublication_pipeline",
                        "corpus": "true"
                    }
                ],
                "title": "pp no_pmid wb",
                "prepublication_pipeline": "true"
            }
            response = client.post(url="/reference/", json=reference_create_json, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            new_curie = response.text
            if new_curie.startswith('"') and new_curie.endswith('"'):
                curie_pp_nopmid_wb = new_curie[1:-1]
            curie_pp_nopmid_wb_bool = False
            reference_create_json = {
                "cross_references": [
                    {
                        "curie": "PMID:1112",
                        "is_obsolete": "false"
                    }
                ],
                "mod_corpus_associations": [
                    {
                        "mod_abbreviation": "WB",
                        "mod_corpus_sort_source": "prepublication_pipeline",
                        "corpus": "false"
                    }
                ],
                "title": "pp pmid wb outside",
                "prepublication_pipeline": "true"
            }
            response = client.post(url="/reference/", json=reference_create_json, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            new_curie = response.text
            if new_curie.startswith('"') and new_curie.endswith('"'):
                curie_pp_pmid_wb_outside = new_curie[1:-1]
            curie_pp_pmid_wb_outside_bool = False

            reference_create_json = {
                "cross_references": [
                    {
                        "curie": "PMID:1113",
                        "is_obsolete": "false"
                    }
                ],
                "mod_corpus_associations": [
                    {
                        "mod_abbreviation": "WB",
                        "mod_corpus_sort_source": "prepublication_pipeline",
                        "corpus": "true"
                    }
                ],
                "title": "pp pmid wb",
                "prepublication_pipeline": "false"
            }
            response = client.post(url="/reference/", json=reference_create_json, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            new_curie = response.text
            if new_curie.startswith('"') and new_curie.endswith('"'):
                curie_nopp_pmid_wb = new_curie[1:-1]
            curie_nopp_pmid_wb_bool = False

            reference_create_json = {
                "cross_references": [
                    {
                        "curie": "PMID:1114",
                        "is_obsolete": "false"
                    }
                ],
                "mod_corpus_associations": [
                    {
                        "mod_abbreviation": "WB",
                        "mod_corpus_sort_source": "dqm_files",
                        "corpus": "true"
                    }
                ],
                "title": "pp pmid wb dqm_files",
                "prepublication_pipeline": "true"
            }
            response = client.post(url="/reference/", json=reference_create_json, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            new_curie = response.text
            if new_curie.startswith('"') and new_curie.endswith('"'):
                curie_pp_pmid_wb_source = new_curie[1:-1]
            curie_pp_pmid_wb_source_bool = False

            reference_create_json = {
                "cross_references": [
                    {
                        "curie": "PMID:1115",
                        "is_obsolete": "false"
                    }
                ],
                "mod_corpus_associations": [
                    {
                        "mod_abbreviation": "WB",
                        "mod_corpus_sort_source": "prepublication_pipeline",
                        "corpus": "true"
                    }
                ],
                "title": "pp pmid wb",
                "prepublication_pipeline": "true"
            }
            response = client.post(url="/reference/", json=reference_create_json, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            new_curie = response.text
            if new_curie.startswith('"') and new_curie.endswith('"'):
                curie_pp_pmid_wb_2 = new_curie[1:-1]
            curie_pp_pmid_wb_2_bool = False

            reference_create_json = {
                "cross_references": [
                    {
                        "curie": "PMID:1116",
                        "is_obsolete": "false"
                    }
                ],
                "mod_corpus_associations": [
                    {
                        "mod_abbreviation": "WB",
                        "mod_corpus_sort_source": "prepublication_pipeline",
                        "corpus": "true"
                    }
                ],
                "workflow_tags": [
                    {
                        "workflow_tag_id": "ATP:0000103",
                        "mod_abbreviation": "WB"
                    }
                ],
                "title": "pp pmid wb",
                "prepublication_pipeline": "true"
            }
            response = client.post(url="/reference/", json=reference_create_json, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            new_curie = response.text
            if new_curie.startswith('"') and new_curie.endswith('"'):
                curie_sorted = new_curie[1:-1]
            curie_sorted_bool = False

            res = client.get(url="/sort/prepublication_pipeline", params={"mod_abbreviation": "WB", "count": 10})
            assert res.status_code == status.HTTP_200_OK
            for ref in res.json():
                if ref['curie'] == curie_pp_pmid_wb:
                    curie_pp_pmid_wb_bool = True
                if ref['curie'] == curie_pp_nopmid_wb:
                    curie_pp_nopmid_wb_bool = True
                if ref['curie'] == curie_pp_pmid_wb_outside:
                    curie_pp_pmid_wb_outside_bool = True
                if ref['curie'] == curie_nopp_pmid_wb:
                    curie_nopp_pmid_wb_bool = True
                if ref['curie'] == curie_pp_pmid_wb_source:
                    curie_pp_pmid_wb_source_bool = True
                if ref['curie'] == curie_pp_pmid_wb_2:
                    curie_pp_pmid_wb_2_bool = True
                if ref['curie'] == curie_sorted:
                    curie_sorted_bool = True
            assert curie_pp_pmid_wb_bool is True
            assert curie_pp_nopmid_wb_bool is False
            assert curie_pp_pmid_wb_outside_bool is True
            assert curie_nopp_pmid_wb_bool is False
            assert curie_pp_pmid_wb_source_bool is True
            assert curie_pp_pmid_wb_2_bool is True
            assert curie_sorted_bool is False

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
           load_name_to_atp_and_relationships_mock)
    @patch("agr_literature_service.api.crud.ateam_db_helpers.search_ancestors_or_descendants",
           search_ancestors_or_descendants_mock)
    def test_sort_prepublication_pipeline_simple(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            populate_test_mods()
            reference_create_json = {
                "cross_references": [
                    {
                        "curie": "PMID:1113",
                        "is_obsolete": "false"
                    }
                ],
                "mod_corpus_associations": [
                    {
                        "mod_abbreviation": "WB",
                        "mod_corpus_sort_source": "prepublication_pipeline",
                        "corpus": "true"
                    }
                ],
                "title": "pmid_fake",
                "prepublication_pipeline": "true"
            }
            response = client.post(url="/reference/", json=reference_create_json, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            res = client.get(url="/sort/prepublication_pipeline", params={"mod_abbreviation": "WB", "count": 10})
            assert res.status_code == status.HTTP_200_OK
            assert len(res.json()) > 0

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
           load_name_to_atp_and_relationships_mock)
    @patch("agr_literature_service.api.crud.ateam_db_helpers.search_ancestors_or_descendants",
           search_ancestors_or_descendants_mock)
    def test_sort_author_pubmed_publication_status(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            populate_test_mods()
            reference_create_json = {
                "cross_references": [
                    {
                        "curie": "PMID:1113",
                        "is_obsolete": "false"
                    }
                ],
                "mod_corpus_associations": [
                    {
                        "mod_abbreviation": "WB",
                        "mod_corpus_sort_source": "prepublication_pipeline",
                        "corpus": "true"
                    }
                ],
                "authors": [
                    {
                        "order": 2,
                        "first_name": "S.",
                        "last_name": "Wu",
                        "name": "S. Wu",
                    },
                    {
                        "order": 1,
                        "first_name": "D.",
                        "last_name": "Wu",
                        "name": "D. Wu",
                    }
                ],
                "pubmed_publication_status": "epublish",
                "title": "pmid_fake",
                "prepublication_pipeline": "true"
            }
            response = client.post(url="/reference/", json=reference_create_json, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            res = client.get(url="/sort/prepublication_pipeline", params={"mod_abbreviation": "WB", "count": 10})
            assert res.status_code == status.HTTP_200_OK
            assert len(res.json()) > 0
            assert res.json()[0]['pubmed_publication_status'] == "epublish"
            assert len(res.json()[0]['authors']) == 2
            for author in res.json()[0]['authors']:
                if author['name'] == 'D. Wu':
                    assert author['order'] == 1
                else:
                    assert author['order'] == 2
