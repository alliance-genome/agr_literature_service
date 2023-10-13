import pytest
from sqlalchemy import and_
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import ReferenceModel, CrossReferenceModel
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from .test_reference import test_reference # noqa
from .test_mod import test_mod # noqa
from collections import namedtuple

test_reference2 = test_reference

TestMCAData = namedtuple('TestMCAData', ['response', 'new_mca_id', 'related_ref_curie'])


@pytest.fixture
def test_mca(db, auth_headers, test_reference, test_mod): # noqa
    print("***** Adding a test mod-corpus association *****")
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

    def test_change_reference_mca(self, test_mca, auth_headers, test_reference2):  # noqa
        with TestClient(app) as client:
            patched_data = {"reference_curie": test_reference2.new_ref_curie}
            patch_response = client.patch(url=f"/reference/mod_corpus_association/{test_mca.new_mca_id}",
                                          json=patched_data, headers=auth_headers)
            assert patch_response.status_code == status.HTTP_202_ACCEPTED
            test_mca_response = client.get(url=f"/reference/mod_corpus_association/{test_mca.new_mca_id}")
            assert test_mca_response.json()["reference_curie"] == test_reference2.new_ref_curie

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

    def test_mca_modid_wb(self, db, test_reference, auth_headers): # noqa
        with TestClient(app) as client:
            new_mod = {
                "abbreviation": "WB",
                "short_name": "WB",
                "full_name": "WormBase"
            }
            response = client.post(url="/mod/", json=new_mod, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED

            # reference_obj = db.query(ReferenceModel).filter(
            #     ReferenceModel.curie == test_reference.new_ref_curie).first()
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
            xref = db.query(CrossReferenceModel).filter(and_(CrossReferenceModel.reference_id==reference_obj.reference_id, CrossReferenceModel.curie_prefix=='WB')).one_or_none()
            assert xref is None

            patch_mca = {
                "corpus": "true"
            }
            patch_response = client.patch(url=f"/reference/mod_corpus_association/{response_mca.text}",
                                          json=patch_mca, headers=auth_headers)
            assert patch_response.status_code == status.HTTP_202_ACCEPTED
            xref = db.query(CrossReferenceModel).filter(and_(CrossReferenceModel.reference_id==reference_obj.reference_id, CrossReferenceModel.curie_prefix=='WB')).one_or_none()
            assert xref.curie == 'WB:WBPaper00000001'

