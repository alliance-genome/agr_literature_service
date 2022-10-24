from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import CrossReferenceModel
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from .test_reference import test_reference # noqa
from .test_resource import test_resource # noqa

TestXrefData = namedtuple('TestXrefData', ['response', 'related_ref_curie'])


@pytest.fixture
def test_cross_reference(db, auth_headers, test_reference): # noqa
    print("***** Adding a test cross reference *****")
    with TestClient(app) as client:
        db.execute("INSERT INTO resource_descriptors (db_prefix, name, default_url) "
                   "VALUES ('XREF', 'Madeup', 'http://www.bob.com/[%s]')")
        db.commit()
        new_cross_ref = {
            "curie": "XREF:123456",
            "reference_curie": test_reference.new_ref_curie,
            "pages": ["reference"]
        }
        response = client.post(url="/cross_reference/", json=new_cross_ref, headers=auth_headers)
        yield TestXrefData(response, test_reference.new_ref_curie)


class TestCrossRef:
    def test_modify_curie(self, db, test_cross_reference): # noqa
        query_object = db.query(CrossReferenceModel).filter_by(curie='XREF:123456').one()
        query_object.curie = 'XREF:54321'
        db.commit()
        query_object_updated = db.query(CrossReferenceModel).filter_by(curie='XREF:54321').one()
        versions_updated = [version for version in query_object_updated.versions]
        versions = [version for version in query_object.versions]
        assert versions is not versions_updated
        # TODO: versions_updated should have a history of changes, after database remodeled to have a primary key.
        # Remove the above assertion and check that a single object has changed.

    def test_get_bad_xref(self):
        with TestClient(app) as client:
            response = client.get(url="/cross_reference/does_not_exist")
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_create_xref(self, db, auth_headers, test_cross_reference, test_resource): # noqa
        with TestClient(app) as client:
            assert test_cross_reference.response.status_code == status.HTTP_201_CREATED
            # check db for xref
            xref = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == "XREF:123456").one()
            assert xref.curie == "XREF:123456"
            assert xref.reference.curie == test_cross_reference.related_ref_curie
            # what has it stored for pages?
            assert xref.pages == ["reference"]

            # Now do a resource one
            new_cross_ref = {"curie": 'XREF:anoth', "resource_curie": test_resource.new_resource_curie}
            response = client.post(url="/cross_reference/", json=new_cross_ref, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED

            # check db for xref
            xref = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == "XREF:anoth").one()
            assert xref.curie == "XREF:anoth"
            assert not xref.reference
            assert xref.resource.curie == test_resource.new_resource_curie

            response = client.post(url="/cross_reference/", json={"curie": 'XREF:no_ref_res'}, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_create_again_bad(self, auth_headers, test_cross_reference): # noqa
        with TestClient(app) as client:
            response = client.post(url="/cross_reference/",
                                   json={"curie": 'XREF:123456',
                                         "reference_curie": test_cross_reference.related_ref_curie},
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_409_CONFLICT

    def test_show_xref(self, test_cross_reference): # noqa
        with TestClient(app) as client:
            response = client.get(url="/cross_reference/XREF:123456")
            assert response.status_code == status.HTTP_200_OK
            assert response.json()['curie'] == "XREF:123456"
            assert response.json()['reference_curie'] == test_cross_reference.related_ref_curie

    def test_patch_xref(self, db, test_cross_reference, auth_headers): # noqa
        with TestClient(app) as client:
            patched_xref = {
                "is_obsolete": True,
                "pages": ["different"],
                "reference_curie": test_cross_reference.related_ref_curie
            }
            response = client.patch(url="/cross_reference/XREF:123456", json=patched_xref, headers=auth_headers)
            assert response.json()['message'] == "updated"
            xref = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == "XREF:123456").one()
            assert xref.is_obsolete
            assert xref.pages == ["different"]

            res = client.get(url="/cross_reference/XREF:123456/versions").json()

            # Pages      : None -> reference -> different
            # is_obsolete: None -> False -> True
            for transaction in res:
                print(transaction)
                if not transaction['changeset']['pages'][0]:
                    assert transaction['changeset']['pages'][1] == ["reference"]
                    assert not transaction['changeset']['is_obsolete'][1]
                else:
                    assert transaction['changeset']['pages'][1] == ["different"]
                    assert transaction['changeset']['is_obsolete'][1]

    def test_destroy_xref(self, test_cross_reference, auth_headers): # noqa
        with TestClient(app) as client:
            response = client.delete(url="/cross_reference/XREF:123456", headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT

            # It should now give an error on lookup.
            response = client.get(url="/cross_reference/XREF:123456")
            assert response.status_code == status.HTTP_404_NOT_FOUND

            # Deleting it again should give an error as the lookup will fail.
            response = client.delete(url="/cross_reference/XREF:123456", headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_curie_prefix(self, db, test_cross_reference, auth_headers):
        new_cross_ref = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == "XREF:123456").one()
        assert new_cross_ref.curie_prefix == new_cross_ref.curie.split(":")[0]

        patched_xref = {
            "curie": "TESTXREF:1234"
        }
        with TestClient(app) as client:
            client.patch(url="/cross_reference/XREF:123456", json=patched_xref, headers=auth_headers)
            patched_cross_ref = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == "TESTXREF:1234").one()
            assert patched_cross_ref.curie_prefix == patched_cross_ref.curie.split(":")[0]
            assert new_cross_ref.cross_reference_id == patched_cross_ref.cross_reference_id

