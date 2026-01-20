from collections import namedtuple

import pytest
from starlette.testclient import TestClient

from agr_literature_service.api.main import app
from fastapi import status

from agr_literature_service.api.models import ResourceModel
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa

ResourceTestData = namedtuple('ResourceTestData', ['response', 'new_resource_curie'])


@pytest.fixture
def test_resource(db, auth_headers): # noqa
    print("***** Adding a test resource *****")
    with TestClient(app) as client:
        resource_data = {
            "title": "Bob", "abstract": "3", "open_access": True
        }
        response = client.post(url="/resource/", json=resource_data, headers=auth_headers)
        yield ResourceTestData(response, response.json())


class TestResource:

    def test_get_bad_resource(self, auth_headers):  # noqa
        with TestClient(app) as client:
            client.get(url="/resource/PMID:VQEVEQRVC", headers=auth_headers)

    def test_create_resource(self, auth_headers, test_resource): # noqa
        with TestClient(app) as client:
            assert test_resource.response.status_code == status.HTTP_201_CREATED
            new_resource = client.post(url="/resource/", json={"title": "Another Bob"}, headers=auth_headers)
            assert new_resource.status_code == status.HTTP_201_CREATED

            # create again with same title, category
            # Apparently not a problem!!
            new_resource = client.post(url="/resource/", json={"title": "Another Bob"}, headers=auth_headers)
            assert new_resource.status_code == status.HTTP_201_CREATED

            # No title
            # ResourceSchemaPost raises exception
            new_resource = client.post(url="/resource/", json={"title": None}, headers=auth_headers)
            assert new_resource.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            # blank title
            # ResourceSchemaPost raises exception
            new_resource = client.post(url="/resource/", json={"title": ""}, headers=auth_headers)
            assert new_resource.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_show_resource(self, auth_headers, test_resource):  # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/resource/{test_resource.new_resource_curie}", headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            resource = response.json()
            assert resource['title'] == "Bob"
            assert resource['abstract'] == '3'

            # Lookup 1 that does not exist
            response = client.get(url="/resource/does_not_exist", headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_update_resource(self, auth_headers, test_resource):  # noqa
        with TestClient(app) as client:
            response = client.patch(url=f"/resource/{test_resource.new_resource_curie}", json={"title": "new title"},
                                    headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED

            # fetch the new record.
            new_resource = client.get(url=f"/resource/{test_resource.new_resource_curie}",
                                      headers=auth_headers).json()

            # do we have the new title?
            assert new_resource['title'] == "new title"
            assert new_resource['abstract'] == "3"

    def test_resource_create_large(self, db, auth_headers): # noqa
        with TestClient(app) as client:
            xml = {
                "abbreviation_synonyms": ["Jackson, Mathews, Wickens, 1996"],
                "cross_references": [
                    {
                        "curie": "FB:FBrf0044885",
                        "pages": [
                            "something"
                        ]
                    }
                ],
                "editors": [
                    {
                        "order": 1,
                        "first_name": "R.J.",
                        "last_name": "Jackson",
                        "name": "R.J. Jackson"
                    },
                    {
                        "order": 2,
                        "first_name": "M.",
                        "last_name": "Mathews",
                        "name": "M. Mathews"
                    },
                    {
                        "order": 3,
                        "first_name": "M.P.",
                        "last_name": "Wickens",
                        "name": "M.P. Wickens"
                    }],
                "pages": "lxi + 351pp",
                "title": "Abstracts of papers presented at the 1996 meeting"
            }
            # process the resource
            response = client.post(url="/resource/", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            curie = response.json()

            # fetch the new record.
            response = client.get(url=f"/resource/{curie}", headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            new_resource = response.json()
            assert new_resource['cross_references'][0]['curie'] == "FB:FBrf0044885"

            # Not sure of order in array of the editors so:-
            assert len(new_resource['editors']) == 3
            for editor in new_resource['editors']:
                if editor['order'] == '1':
                    assert editor["first_name"] == "R.J."
                    assert editor["last_name"] == "Jackson"
                    assert editor["name"] == "R.J. Jackson"
                elif editor['order'] == '3':
                    assert editor["first_name"] == "Wickens"
                    assert editor["last_name"] == "Jackson"
                    assert editor["name"] == "M.P. Wickens"
            assert new_resource['title'] == "Abstracts of papers presented at the 1996 meeting"
            assert new_resource['pages'] == "lxi + 351pp"
            assert new_resource["abbreviation_synonyms"][0] == "Jackson, Mathews, Wickens, 1996"
            assert not new_resource['open_access']

            res = db.query(ResourceModel).filter(ResourceModel.curie == curie).one()
            assert res.title == "Abstracts of papers presented at the 1996 meeting"
            assert len(res.editor) == 3
            # open access defaults to False
            assert not res.open_access

            assert len(res.cross_reference) == 1


    def test_delete_resource(self, auth_headers, test_resource):  # noqa
        with TestClient(app) as client:
            response = client.delete(url=f"/resource/{test_resource.new_resource_curie}", headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT
            # It should now give an error on lookup.
            response = client.get(url=f"/resource/{test_resource.new_resource_curie}", headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

            # Deleting it again should give an error as the lookup will fail.
            response = client.delete(url=f"/resource/{test_resource.new_resource_curie}", headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND
