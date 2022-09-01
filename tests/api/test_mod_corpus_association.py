import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from .fixtures import auth_headers, db # noqa
from .test_reference import test_reference # noqa
from .test_mod import test_mod # noqa
from collections import namedtuple

create_test_reference2 = test_reference

TestMCAData = namedtuple('TestMCAData', ['response', 'new_mca_id', 'related_ref_curie'])


@pytest.fixture
def test_mca(db, auth_headers, test_reference, test_mod): # noqa
    print("***** Adding a test mod-corpus association *****")
    with TestClient(app) as client:
        mod_response = client.get(url=f"/mod/{test_mod.new_mod_abbreviation}")
        mod_abbreviation = mod_response.json()["abbreviation"]
        new_mca = {
            "mod_abbreviation": mod_abbreviation,
            "reference_curie": test_reference.json(),
            "mod_corpus_sort_source": 'mod_pubmed_search'
        }
        response = client.post(url="/reference/mod_corpus_association/", json=new_mca, headers=auth_headers)
        yield TestMCAData(response, response.json(), test_reference.json())


class TestModCorpusAssociation:

    def test_get_bad_mca(self, test_mca): # noqa
        with TestClient(app) as client:
            response = client.get(url="/reference/mod_corpus_association/-1")
            assert response.status_code == status.HTTP_404_NOT_FOUND

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

    def test_patch_mca(self, test_mca, auth_headers): # noqa
        with TestClient(app) as client:
            test_mca_response = client.get(url=f"/reference/mod_corpus_association/{test_mca.new_mca_id}")
            test_mca_abbreviation = test_mca_response.json()["mod_abbreviation"]
            patched_data = {"reference_curie": test_mca.related_ref_curie,
                            "mod_abbreviation": test_mca_abbreviation,
                            "mod_corpus_sort_source": "assigned_for_review"
                            }
            patch_response = client.patch(url=f"/reference/mod_corpus_association/{test_mca.new_mca_id}",
                                          json=patched_data, headers=auth_headers)
            assert patch_response.status_code == status.HTTP_202_ACCEPTED
            assert client.get(url=f"/reference/mod_corpus_association/"
                                  f"{test_mca.new_mca_id}").json()["mod_corpus_sort_source"] == "assigned_for_review"

            # add changeset tests

    def test_show_mca(self, test_mca): # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/reference/mod_corpus_association/{test_mca.new_mca_id}")
            assert response.status_code == status.HTTP_200_OK

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
