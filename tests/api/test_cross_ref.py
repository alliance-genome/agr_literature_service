from typing import Tuple

import pytest
from requests import Response
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import CrossReferenceModel
from .fixtures import auth_headers, db # noqa
from .test_reference import create_test_reference # noqa
from .test_resource import create_test_resource # noqa


@pytest.fixture
def create_test_cross_reference(db, auth_headers, create_test_reference) -> Tuple[Response, str]: # noqa
    print("***** Adding a test cross reference *****")
    with TestClient(app) as client:
        db.execute("INSERT INTO resource_descriptors (db_prefix, name, default_url) "
                   "VALUES ('XREF', 'Madeup', 'http://www.bob.com/[%s]')")
        db.commit()
        new_cross_ref = {
            "curie": "XREF:123456",
            "reference_curie": create_test_reference.json(),
            "pages": ["reference"]
        }
        response = client.post(url="/cross_reference/", json=new_cross_ref, headers=auth_headers)
        yield response, create_test_reference.json()


class TestCrossRef:

    def test_get_bad_xref(self):
        with TestClient(app) as client:
            response = client.get(url="/cross_reference/does_not_exist")
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_create_xref(self, db, auth_headers, create_test_cross_reference, create_test_resource): # noqa
        with TestClient(app) as client:
            assert create_test_cross_reference[0].status_code == status.HTTP_201_CREATED
            # check db for xref
            xref = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == "XREF:123456").one()
            assert xref.curie == "XREF:123456"
            assert xref.reference.curie == create_test_cross_reference[1]
            # what has it stored for pages?
            assert xref.pages == ["reference"]

            # Now do a resource one
            new_cross_ref = {"curie": 'XREF:anoth', "resource_curie": create_test_resource.json()}
            response = client.post(url="/cross_reference/", json=new_cross_ref, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED

            # check db for xref
            xref = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == "XREF:anoth").one()
            assert xref.curie == "XREF:anoth"
            assert not xref.reference
            assert xref.resource.curie == create_test_resource.json()

            response = client.post(url="/cross_reference/", json={"curie": 'XREF:no_ref_res'}, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_create_again_bad(self, auth_headers, create_test_cross_reference): # noqa
        with TestClient(app) as client:
            response = client.post(url="/cross_reference/",
                                   json={"curie": 'XREF:123456',
                                         "reference_curie": create_test_cross_reference[1]},
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_409_CONFLICT

    def test_show_xref(self, create_test_cross_reference): # noqa
        with TestClient(app) as client:
            response = client.get(url="/cross_reference/XREF:123456")
            assert response.status_code == status.HTTP_200_OK
            assert response.json()['curie'] == "XREF:123456"
            assert response.json()['reference_curie'] == create_test_cross_reference[1]

    def test_patch_xref(self, db, create_test_cross_reference, auth_headers): # noqa
        with TestClient(app) as client:
            patched_xref = {
                "is_obsolete": True,
                "pages": ["different"],
                "reference_curie": create_test_cross_reference[1]
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

    def test_destroy_xref(self, create_test_cross_reference, auth_headers): # noqa
        with TestClient(app) as client:
            response = client.delete(url="/cross_reference/XREF:123456", headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT

            # It should now give an error on lookup.
            response = client.get(url="/cross_reference/XREF:123456")
            assert response.status_code == status.HTTP_404_NOT_FOUND

            # Deleting it again should give an error as the lookup will fail.
            response = client.delete(url="/cross_reference/XREF:123456", headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND
