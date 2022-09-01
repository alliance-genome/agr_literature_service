from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import ReferenceModel, ReferenceCommentAndCorrectionModel
from .fixtures import auth_headers, db # noqa
from .test_reference import test_reference # noqa

test_reference2 = test_reference

TestRefComAndCorData = namedtuple('TestRefComAndCorData', ['response', 'new_rcc_id', 'ref_curie_from', 'ref_curie_to'])


@pytest.fixture
def test_ref_cc(db, auth_headers, test_reference, test_reference2): # noqa
    print("***** Adding a test reference comment and correction *****")
    with TestClient(app) as client:
        ref1 = test_reference.new_ref_curie
        ref2 = test_reference2.new_ref_curie
        new_rcc = {"reference_curie_from": ref1,
                   "reference_curie_to": ref2,
                   "reference_comment_and_correction_type": "CommentOn"
                   }
        response = client.post(url="/reference_comment_and_correction/", json=new_rcc, headers=auth_headers)
        yield TestRefComAndCorData(response, response.json(), ref1, ref2)


class TestReferenceCommentAndCorrection:

    def test_get_bad_rcc(self):
        with TestClient(app) as client:
            response = client.get(url="/reference_comment_and_correction/-1")
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_bad_missing_args(self, test_ref_cc, auth_headers): # noqa
        with TestClient(app) as client:
            xml = {"reference_curie_from": test_ref_cc.ref_curie_from,
                   "reference_comment_and_correction_type": "CommentOn"
                   }
            response = client.post(url="/reference_comment_and_correction/", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            xml = {'reference_curie_to': test_ref_cc.ref_curie_to,
                   'reference_comment_and_correction_type': "CommentOn"
                   }
            response = client.post(url="/reference_comment_and_correction/", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            xml = {'reference_curie_from': test_ref_cc.ref_curie_from,
                   'reference_curie_to': test_ref_cc.ref_curie_to}
            response = client.post(url="/reference_comment_and_correction/", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_create_bad_same_curies_from_to(self, test_ref_cc, auth_headers): # noqa
        with TestClient(app) as client:
            same_as_test_obj = {"reference_curie_from": test_ref_cc.ref_curie_from,
                                "reference_curie_to": test_ref_cc.ref_curie_from,
                                "reference_comment_and_correction_type": "CommentOn"
                                }
            response = client.post(url="/reference_comment_and_correction/", json=same_as_test_obj,
                                   headers=auth_headers)
            assert 1 == 1
            # TODO uncomment this after adding this constraint to the models
            # assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_create_bad_duplicate(self, test_ref_cc, auth_headers): # noqa
        with TestClient(app) as client:
            xml = {"reference_curie_from": test_ref_cc.ref_curie_from,
                   "reference_curie_to": test_ref_cc.ref_curie_to,
                   "reference_comment_and_correction_type": "CommentOn"
                   }
            response = client.post(url="/reference_comment_and_correction/", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_409_CONFLICT

    def test_create_rcc(self, db, test_ref_cc): # noqa
        # check results in database
        rcc_obj = db.query(ReferenceCommentAndCorrectionModel).join(
            ReferenceModel, ReferenceCommentAndCorrectionModel.reference_id_from == ReferenceModel.reference_id).filter(
            ReferenceModel.curie == test_ref_cc.ref_curie_from).one()
        assert rcc_obj.reference_to.curie == test_ref_cc.ref_curie_to
        assert rcc_obj.reference_comment_and_correction_type == "CommentOn"

    def test_patch_rcc(self, db, test_ref_cc, auth_headers): # noqa
        with TestClient(app) as client:
            # swap to and from and change correction type
            xml = {'reference_curie_from': test_ref_cc.ref_curie_to,
                   'reference_curie_to': test_ref_cc.ref_curie_from,
                   'reference_comment_and_correction_type': "ReprintOf"
                   }
            response = client.patch(url=f"/reference_comment_and_correction/{test_ref_cc.new_rcc_id}",
                                    json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED

            rcc_obj: ReferenceCommentAndCorrectionModel = db.query(ReferenceCommentAndCorrectionModel). \
                filter(
                ReferenceCommentAndCorrectionModel.reference_comment_and_correction_id == test_ref_cc.new_rcc_id).one()
            assert rcc_obj.reference_to.curie == test_ref_cc.ref_curie_from
            assert rcc_obj.reference_from.curie == test_ref_cc.ref_curie_to
            assert rcc_obj.reference_comment_and_correction_type == "ReprintOf"

            response = client.get(url=f"/reference_comment_and_correction/{test_ref_cc.new_rcc_id}/versions")
            transactions = response.json()
            reference1_id = client.get(url=f"/reference/{test_ref_cc.ref_curie_from}").json()["reference_id"]
            reference2_id = client.get(url=f"/reference/{test_ref_cc.ref_curie_to}").json()["reference_id"]
            assert transactions[0]['changeset']['reference_id_from'][1] == reference1_id
            assert transactions[0]['changeset']['reference_id_to'][1] == reference2_id
            assert transactions[0]['changeset']['reference_comment_and_correction_type'][1] == "CommentOn"
            assert transactions[1]['changeset']['reference_id_from'][1] == reference2_id
            assert transactions[1]['changeset']['reference_id_to'][1] == reference1_id
            assert transactions[1]['changeset']['reference_comment_and_correction_type'][1] == "ReprintOf"

    def test_show_rcc(self, test_ref_cc): # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/reference_comment_and_correction/{test_ref_cc.new_rcc_id}")
            res = response.json()
            assert res['reference_curie_to'] == test_ref_cc.ref_curie_to
            assert res['reference_curie_from'] == test_ref_cc.ref_curie_from
            assert res['reference_comment_and_correction_type'] == "CommentOn"

    def test_destroy_rcc(self, test_ref_cc, auth_headers): # noqa
        with TestClient(app) as client:
            response = client.delete(url=f"/reference_comment_and_correction/{test_ref_cc.new_rcc_id}",
                                     headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT

            # It should now give an error on lookup.
            response = client.get(url=f"/reference_comment_and_correction/{test_ref_cc.new_rcc_id}")
            assert response.status_code == status.HTTP_404_NOT_FOUND

            # Deleting it again should give an error as the lookup will fail.
            response = client.delete(url=f"/reference_comment_and_correction/{test_ref_cc.new_rcc_id}",
                                     headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND
