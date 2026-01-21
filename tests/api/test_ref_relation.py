from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import ReferenceModel, ReferenceRelationModel
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from .test_reference import test_reference # noqa

test_reference2 = test_reference
test_reference3 = test_reference
test_reference4 = test_reference

RefComAndCorTestData = namedtuple('RefComAndCorTestData', ['response', 'new_rcc_id', 'ref_curie_from', 'ref_curie_to'])


@pytest.fixture
def test_ref_cc(db, auth_headers, test_reference, test_reference2): # noqa
    print("***** Adding a test reference_relation *****")
    with TestClient(app) as client:
        ref1 = test_reference.new_ref_curie
        ref2 = test_reference2.new_ref_curie
        new_rcc = {"reference_curie_from": ref1,
                   "reference_curie_to": ref2,
                   "reference_relation_type": "CommentOn"
                   }
        response = client.post(url="/reference_relation/", json=new_rcc, headers=auth_headers)
        yield RefComAndCorTestData(response, response.json(), ref1, ref2)


class TestReferenceRelation:

    def test_get_bad_rcc(self, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get(url="/reference_relation/-1", headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_bad_missing_args(self, test_ref_cc, auth_headers): # noqa
        with TestClient(app) as client:
            xml = {"reference_curie_from": test_ref_cc.ref_curie_from,
                   "reference_relation_type": "CommentOn"
                   }
            response = client.post(url="/reference_relation/", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            xml = {'reference_curie_to': test_ref_cc.ref_curie_to,
                   'reference_relation_type': "CommentOn"
                   }
            response = client.post(url="/reference_relation/", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            xml = {'reference_curie_from': test_ref_cc.ref_curie_from,
                   'reference_curie_to': test_ref_cc.ref_curie_to}
            response = client.post(url="/reference_relation/", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_create_bad_same_curies_from_to(self, test_ref_cc, auth_headers): # noqa
        with TestClient(app) as client:
            same_as_test_obj = {"reference_curie_from": test_ref_cc.ref_curie_from,
                                "reference_curie_to": test_ref_cc.ref_curie_from,
                                "reference_relation_type": "CommentOn"
                                }
            response = client.post(url="/reference_relation/", json=same_as_test_obj,
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_409_CONFLICT

    def test_create_bad_duplicate(self, test_ref_cc, auth_headers): # noqa
        with TestClient(app) as client:
            xml = {"reference_curie_from": test_ref_cc.ref_curie_from,
                   "reference_curie_to": test_ref_cc.ref_curie_to,
                   "reference_relation_type": "CommentOn"
                   }
            response = client.post(url="/reference_relation/", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_409_CONFLICT

    def test_create_bad_duplicate_backward(self, test_ref_cc, auth_headers): # noqa
        with TestClient(app) as client:
            xml = {"reference_curie_from": test_ref_cc.ref_curie_to,
                   "reference_curie_to": test_ref_cc.ref_curie_from,
                   "reference_relation_type": "CommentOn"
                   }
            response = client.post(url="/reference_relation/", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_409_CONFLICT

    def test_create_rcc(self, db, test_ref_cc): # noqa
        # check results in database
        rcc_obj = db.query(ReferenceRelationModel).join(
            ReferenceModel, ReferenceRelationModel.reference_id_from == ReferenceModel.reference_id).filter(
            ReferenceModel.curie == test_ref_cc.ref_curie_from).one()
        assert rcc_obj.reference_to.curie == test_ref_cc.ref_curie_to
        assert rcc_obj.reference_relation_type == "CommentOn"

    def test_patch_rcc(self, db, test_ref_cc, auth_headers): # noqa
        with TestClient(app) as client:
            # swap to and from and change reference_relation type
            xml = {'reference_curie_from': test_ref_cc.ref_curie_to,
                   'reference_curie_to': test_ref_cc.ref_curie_from,
                   'reference_relation_type': "ReprintOf"
                   }
            response = client.patch(url=f"/reference_relation/{test_ref_cc.new_rcc_id}",
                                    json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED

            rcc_obj: ReferenceRelationModel = db.query(ReferenceRelationModel). \
                filter(
                ReferenceRelationModel.reference_relation_id == test_ref_cc.new_rcc_id).one()
            assert rcc_obj.reference_to.curie == test_ref_cc.ref_curie_from
            assert rcc_obj.reference_from.curie == test_ref_cc.ref_curie_to
            assert rcc_obj.reference_relation_type == "ReprintOf"

            response = client.get(url=f"/reference_relation/{test_ref_cc.new_rcc_id}/versions",
                                  headers=auth_headers)
            transactions = response.json()
            reference1_id = client.get(url=f"/reference/{test_ref_cc.ref_curie_from}",
                                       headers=auth_headers).json()["reference_id"]
            reference2_id = client.get(url=f"/reference/{test_ref_cc.ref_curie_to}",
                                       headers=auth_headers).json()["reference_id"]
            assert transactions[0]['changeset']['reference_id_from'][1] == reference1_id
            assert transactions[0]['changeset']['reference_id_to'][1] == reference2_id
            assert transactions[0]['changeset']['reference_relation_type'][1] == "CommentOn"
            assert transactions[1]['changeset']['reference_id_from'][1] == reference2_id
            assert transactions[1]['changeset']['reference_id_to'][1] == reference1_id
            assert transactions[1]['changeset']['reference_relation_type'][1] == "ReprintOf"

    def test_show_rcc(self, test_ref_cc, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/reference_relation/{test_ref_cc.new_rcc_id}", headers=auth_headers)
            res = response.json()
            assert res['reference_curie_to'] == test_ref_cc.ref_curie_to
            assert res['reference_curie_from'] == test_ref_cc.ref_curie_from
            assert res['reference_relation_type'] == "CommentOn"

    def test_destroy_rcc(self, test_ref_cc, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.delete(url=f"/reference_relation/{test_ref_cc.new_rcc_id}",
                                     headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT

            # It should now give an error on lookup.
            response = client.get(url=f"/reference_relation/{test_ref_cc.new_rcc_id}", headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

            # Deleting it again should give an error as the lookup will fail.
            response = client.delete(url=f"/reference_relation/{test_ref_cc.new_rcc_id}",
                                     headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_merge_references_rcc(self, test_reference, test_reference2, test_reference3, test_reference4, auth_headers): # noqa
        with TestClient(app) as client:
            ref1 = test_reference.new_ref_curie
            ref2 = test_reference2.new_ref_curie
            ref3 = test_reference3.new_ref_curie
            ref4 = test_reference4.new_ref_curie
            new_rcc = {"reference_curie_from": ref1,
                       "reference_curie_to": ref2,
                       "reference_relation_type": "CommentOn"
                       }
            response_rcc = client.post(url="/reference_relation/", json=new_rcc, headers=auth_headers)
            assert response_rcc.status_code == status.HTTP_201_CREATED
            new_rcc2 = {"reference_curie_from": ref3,
                        "reference_curie_to": ref4,
                        "reference_relation_type": "ChapterIn"
                        }
            response_rcc2 = client.post(url="/reference_relation/", json=new_rcc2, headers=auth_headers)
            assert response_rcc2.status_code == status.HTTP_201_CREATED
            # merge reference 1 into reference 3
            response_merge1 = client.post(url=f"/reference/merge/{ref1}/{ref3}",
                                          headers=auth_headers)
            assert response_merge1.status_code == status.HTTP_201_CREATED
            ref2_type_bool = False
            ref3_type_bool = False
            response_ref1 = client.get(url=f"/reference/{ref3}", headers=auth_headers)
            for rcc_to in response_ref1.json()['reference_relations']['to_references']:
                if rcc_to['reference_relation_type'] == 'CommentOn':
                    ref2_type_bool = True
                if rcc_to['reference_relation_type'] == 'ChapterIn':
                    ref3_type_bool = True
            assert ref2_type_bool is True
            assert ref3_type_bool is True

    def test_merge_references_rcc_merge_self(self, test_reference, test_reference2, test_reference3, auth_headers):  # noqa
        with TestClient(app) as client:
            ref1 = test_reference.new_ref_curie
            ref2 = test_reference2.new_ref_curie
            ref3 = test_reference3.new_ref_curie
            new_rcc = {"reference_curie_from": ref1,
                       "reference_curie_to": ref2,
                       "reference_relation_type": "CommentOn"
                       }
            response_rcc = client.post(url="/reference_relation/", json=new_rcc, headers=auth_headers)
            assert response_rcc.status_code == status.HTTP_201_CREATED
            new_rcc2 = {"reference_curie_from": ref1,
                        "reference_curie_to": ref3,
                        "reference_relation_type": "ChapterIn"
                        }
            response_rcc2 = client.post(url="/reference_relation/", json=new_rcc2, headers=auth_headers)
            assert response_rcc2.status_code == status.HTTP_201_CREATED

            # merge reference 1 into reference 3
            response_merge1 = client.post(url=f"/reference/merge/{ref1}/{ref3}",
                                          headers=auth_headers)
            assert response_merge1.status_code == status.HTTP_201_CREATED

            response_ref3 = client.get(url=f"/reference/{ref3}", headers=auth_headers)
            assert len(response_ref3.json()['reference_relations']['from_references']) == 0
            assert len(response_ref3.json()['reference_relations']['to_references']) == 1
            assert response_ref3.json()['reference_relations']['to_references'][0]['reference_curie_from'] is None
            assert response_ref3.json()['reference_relations']['to_references'][0]['reference_curie_to'] == ref2
            assert response_ref3.json()['reference_relations']['to_references'][0]['reference_relation_type'] == 'CommentOn'

            response_ref2 = client.get(url=f"/reference/{ref2}", headers=auth_headers)
            assert len(response_ref2.json()['reference_relations']['from_references']) == 1
            assert len(response_ref2.json()['reference_relations']['to_references']) == 0
            assert response_ref2.json()['reference_relations']['from_references'][0]['reference_curie_from'] == ref3
            assert response_ref2.json()['reference_relations']['from_references'][0]['reference_curie_to'] is None
            assert response_ref2.json()['reference_relations']['from_references'][0]['reference_relation_type'] == 'CommentOn'

    def test_merge_references_rcc_fail_constraint(self, test_reference, test_reference2, test_reference3, auth_headers): # noqa
        with TestClient(app) as client:
            ref1 = test_reference.new_ref_curie
            ref2 = test_reference2.new_ref_curie
            ref3 = test_reference3.new_ref_curie
            new_rcc = {"reference_curie_from": ref1,
                       "reference_curie_to": ref2,
                       "reference_relation_type": "CommentOn"
                       }
            response_rcc = client.post(url="/reference_relation/", json=new_rcc, headers=auth_headers)
            assert response_rcc.status_code == status.HTTP_201_CREATED
            new_rcc2 = {"reference_curie_from": ref1,
                        "reference_curie_to": ref3,
                        "reference_relation_type": "ChapterIn"
                        }
            response_rcc2 = client.post(url="/reference_relation/", json=new_rcc2, headers=auth_headers)
            assert response_rcc2.status_code == status.HTTP_201_CREATED
            # merge reference 2 into reference 3
            response_merge1 = client.post(url=f"/reference/merge/{ref2}/{ref3}",
                                          headers=auth_headers)
            assert response_merge1.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
            ref2_type_bool = False
            ref3_type_bool = False
            response_ref1 = client.get(url=f"/reference/{ref1}", headers=auth_headers)
            for rcc_to in response_ref1.json()['reference_relations']['to_references']:
                if rcc_to['reference_relation_type'] == 'CommentOn':
                    ref2_type_bool = True
                if rcc_to['reference_relation_type'] == 'ChapterIn':
                    ref3_type_bool = True
            assert ref2_type_bool is True
            assert ref3_type_bool is True
