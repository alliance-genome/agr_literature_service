from starlette.testclient import TestClient

from fastapi import status
from agr_literature_service.api.main import app
from .test_mod_corpus_association import test_mca # noqa
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

    def test_sort_prepublication_pipeline(self, db, auth_headers): # noqa
        with TestClient(app) as client:
            wb_mod = {
                "abbreviation": "WB",
                "short_name": "WB",
                "full_name": "WormBase"
            }
            response = client.post(url="/mod/", json=wb_mod, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            sgd_mod = {
                "abbreviation": "SGD",
                "short_name": "SGD",
                "full_name": "Saccharomyces Genome Database"
            }
            response = client.post(url="/mod/", json=sgd_mod, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED

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
                        "curie": "PMID:1111",
                        "is_obsolete": "false"
                    }
                ],
                "mod_corpus_associations": [
                    {
                        "mod_abbreviation": "SGD",
                        "mod_corpus_sort_source": "prepublication_pipeline",
                        "corpus": "true"
                    }
                ],
                "title": "pp pmid sgd",
                "prepublication_pipeline": "true"
            }
            response = client.post(url="/reference/", json=reference_create_json, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            new_curie = response.text
            if new_curie.startswith('"') and new_curie.endswith('"'):
                curie_pp_pmid_sgd = new_curie[1:-1]
            curie_pp_pmid_sgd_bool = False

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

            res = client.get(url="/sort/prepublication_pipeline", params={"mod_abbreviation": "WB", "count": 10})
            assert res.status_code == status.HTTP_200_OK
            for ref in res.json():
                if ref['curie'] == curie_pp_pmid_wb:
                    curie_pp_pmid_wb_bool = True
                if ref['curie'] == curie_pp_nopmid_wb:
                    curie_pp_nopmid_wb_bool = True
                if ref['curie'] == curie_pp_pmid_sgd:
                    curie_pp_pmid_sgd_bool = True
                if ref['curie'] == curie_pp_pmid_wb_outside:
                    curie_pp_pmid_wb_outside_bool = True
                if ref['curie'] == curie_nopp_pmid_wb:
                    curie_nopp_pmid_wb_bool = True
                if ref['curie'] == curie_pp_pmid_wb_2:
                    curie_pp_pmid_wb_2_bool = True
            assert curie_pp_pmid_wb_bool is True
            assert curie_pp_nopmid_wb_bool is False
            assert curie_pp_pmid_sgd_bool is False
            assert curie_pp_pmid_wb_outside_bool is True
            assert curie_nopp_pmid_wb_bool is False
            assert curie_pp_pmid_wb_2_bool is True


    def test_sort_prepublication_pipeline_simple(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            new_mod = {
                "abbreviation": "WB",
                "short_name": "WB",
                "full_name": "WormBase"
            }
            response = client.post(url="/mod/", json=new_mod, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
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
