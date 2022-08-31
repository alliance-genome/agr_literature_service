import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import ReferenceModel, ReferenceCommentAndCorrectionModel
from .fixtures import auth_headers, db # noqa
from .test_reference import create_test_reference # noqa

create_test_reference2 = create_test_reference


@pytest.fixture
def create_test_ref_cc(auth_headers, create_test_reference, create_test_reference2): # noqa
    print("***** Adding a test reference comment and correction *****")
    with TestClient(app) as client:
        ref1 = create_test_reference.json()
        ref2 = create_test_reference2.json()
        new_rcc = {"reference_curie_from": ref1,
                   "reference_curie_to": ref2,
                   "reference_comment_and_correction_type": "CommentOn"
                   }
        response = client.post(url="/reference_comment_and_correction/", json=new_rcc, headers=auth_headers)
        yield response, ref1, ref2


class TestReferenceCommentAndCorrection:

    def test_get_bad_rcc(self):
        with TestClient(app) as client:
            response = client.get(url="/reference_comment_and_correction/-1")
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_bad_missing_args(self, create_test_reference, auth_headers): # noqa
        with TestClient(app) as client:
            xml = {"reference_curie_from": create_test_reference.json(),
                   "reference_comment_and_correction_type": "CommentOn"
                   }
            response = client.post(url="/reference_comment_and_correction/", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            xml = {'reference_curie_to': create_test_reference.json(),
                   'reference_comment_and_correction_type': "CommentOn"
                   }
            response = client.post(url="/reference_comment_and_correction/", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            xml = {'reference_curie_from': create_test_reference.json(),
                   'reference_curie_to': create_test_reference.json()}
            response = client.post(url="/reference_comment_and_correction/", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_create_rcc(self, db, create_test_ref_cc): # noqa
        # check results in database
        rcc_obj = db.query(ReferenceCommentAndCorrectionModel).join(
            ReferenceModel, ReferenceCommentAndCorrectionModel.reference_id_from == ReferenceModel.reference_id).filter(
            ReferenceModel.curie == create_test_ref_cc[1]).one()
        assert rcc_obj.reference_to.curie == create_test_ref_cc[2]
        assert rcc_obj.reference_comment_and_correction_type == "CommentOn"

    def test_patch_rcc(self, db, create_test_ref_cc, auth_headers): # noqa
        with TestClient(app) as client:
            # swap to and from and change correction type
            xml = {'reference_curie_from': create_test_ref_cc[2],
                   'reference_curie_to': create_test_ref_cc[1],
                   'reference_comment_and_correction_type': "ReprintOf"
                   }
            response = client.patch(url=f"reference_comment_and_correction/{create_test_ref_cc[0].json()}",
                                    json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED

            rcc_obj: ReferenceCommentAndCorrectionModel = db.query(ReferenceCommentAndCorrectionModel). \
                filter(
                ReferenceCommentAndCorrectionModel.reference_comment_and_correction_id == create_test_ref_cc[0].
                json()).one()
            assert rcc_obj.reference_to.curie == create_test_ref_cc[1]
            assert rcc_obj.reference_from.curie == create_test_ref_cc[2]
            assert rcc_obj.reference_comment_and_correction_type == "ReprintOf"

            response = client.get(url=f"reference_comment_and_correction/{create_test_ref_cc[0].json()}/versions")
            transactions = response.json()
            reference1_id = client.get(url=f"reference/{create_test_ref_cc[1]}").json()["reference_id"]
            reference2_id = client.get(url=f"reference/{create_test_ref_cc[2]}").json()["reference_id"]
            assert transactions[0]['changeset']['reference_id_from'][1] == reference1_id
            assert transactions[0]['changeset']['reference_id_to'][1] == reference2_id
            assert transactions[0]['changeset']['reference_comment_and_correction_type'][1] == "CommentOn"
            assert transactions[1]['changeset']['reference_id_from'][1] == reference2_id
            assert transactions[1]['changeset']['reference_id_to'][1] == reference1_id
            assert transactions[1]['changeset']['reference_comment_and_correction_type'][1] == "ReprintOf"

    def test_show_rcc(self, db, create_test_ref_cc): # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/reference_comment_and_correction/{create_test_ref_cc[0].json()}")
            res = response.json()
            assert res['reference_curie_to'] == create_test_ref_cc[2]
            assert res['reference_curie_from'] == create_test_ref_cc[1]
            assert res['reference_comment_and_correction_type'] == "CommentOn"

    def test_destroy_rcc(self, create_test_ref_cc, auth_headers): # noqa
        with TestClient(app) as client:
            response = client.delete(url=f"/reference_comment_and_correction/{create_test_ref_cc[0].json()}",
                                     headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT

            # It should now give an error on lookup.
            response = client.get(url=f"/reference_comment_and_correction/{create_test_ref_cc[0].json()}")
            assert response.status_code == status.HTTP_404_NOT_FOUND

            # Deleting it again should give an error as the lookup will fail.
            response = client.delete(url=f"/reference_comment_and_correction/{create_test_ref_cc[0].json()}",
                                     headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND
