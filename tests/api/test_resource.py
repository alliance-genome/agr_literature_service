from collections import namedtuple

import pytest
from starlette.testclient import TestClient

from agr_literature_service.api.main import app
from fastapi import status

from agr_literature_service.api.models import ResourceModel
from agr_literature_service.api.models.copyright_license_model import CopyrightLicenseModel
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


    def test_show_all_resources(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            # Create two copyright licenses
            license_cc_by = CopyrightLicenseModel(
                name="CC BY 4.0",
                url="https://creativecommons.org/licenses/by/4.0/",
                description="Creative Commons Attribution 4.0",
                open_access=True
            )
            license_cc_nc = CopyrightLicenseModel(
                name="CC BY-NC 4.0",
                url="https://creativecommons.org/licenses/by-nc/4.0/",
                description="Creative Commons Attribution NonCommercial 4.0",
                open_access=False
            )
            db.add(license_cc_by)
            db.add(license_cc_nc)
            db.commit()

            # Resource 1: shared license (CC BY), with cross_ref and editor
            res1 = client.post(url="/resource/", json={
                "title": "Journal of Testing Alpha",
                "cross_references": [{"curie": "NLM:111111"}],
                "editors": [{"order": 1, "first_name": "Alice",
                             "last_name": "Smith", "name": "Alice Smith"}],
                "open_access": True
            }, headers=auth_headers)
            assert res1.status_code == status.HTTP_201_CREATED
            curie1 = res1.json()

            # Resource 2: shared license (CC BY), different cross_ref and editor
            res2 = client.post(url="/resource/", json={
                "title": "Journal of Testing Beta",
                "cross_references": [{"curie": "NLM:222222"}],
                "editors": [{"order": 1, "first_name": "Bob",
                             "last_name": "Jones", "name": "Bob Jones"}],
                "open_access": True
            }, headers=auth_headers)
            assert res2.status_code == status.HTTP_201_CREATED
            curie2 = res2.json()

            # Resource 3: different license (CC BY-NC), different cross_ref and editor
            res3 = client.post(url="/resource/", json={
                "title": "Journal of Testing Gamma",
                "cross_references": [{"curie": "NLM:333333"}],
                "editors": [{"order": 1, "first_name": "Carol",
                             "last_name": "White", "name": "Carol White"}],
                "open_access": False
            }, headers=auth_headers)
            assert res3.status_code == status.HTTP_201_CREATED
            curie3 = res3.json()

            # Resource 4: no license, different cross_ref, no editor
            res4 = client.post(url="/resource/", json={
                "title": "Journal of Testing Delta",
                "cross_references": [{"curie": "NLM:444444"}],
            }, headers=auth_headers)
            assert res4.status_code == status.HTTP_201_CREATED
            curie4 = res4.json()

            # Assign licenses directly via DB since ResourceSchemaUpdate
            # doesn't support copyright_license_id yet
            r1 = db.query(ResourceModel).filter(ResourceModel.curie == curie1).one()
            r2 = db.query(ResourceModel).filter(ResourceModel.curie == curie2).one()
            r3 = db.query(ResourceModel).filter(ResourceModel.curie == curie3).one()
            r1.copyright_license_id = license_cc_by.copyright_license_id
            r1.license_list = ["CC BY 4.0"]
            r1.license_start_year = 2020
            r2.copyright_license_id = license_cc_by.copyright_license_id
            r2.license_list = ["CC BY 4.0"]
            r2.license_start_year = 2021
            r3.copyright_license_id = license_cc_nc.copyright_license_id
            r3.license_list = ["CC BY-NC 4.0"]
            r3.license_start_year = 2019
            db.commit()

            # Call show_all
            response = client.get(url="/resource/show_all", headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            resources = response.json()

            # Build a lookup by curie
            by_curie = {r['curie']: r for r in resources}
            assert curie1 in by_curie
            assert curie2 in by_curie
            assert curie3 in by_curie
            assert curie4 in by_curie

            # Resource 1: CC BY license, cross_ref, editor
            r1_data = by_curie[curie1]
            assert r1_data['title'] == "Journal of Testing Alpha"
            assert r1_data['copyright_license_id'] == license_cc_by.copyright_license_id
            assert r1_data['copyright_license']['copyright_license_id'] == license_cc_by.copyright_license_id
            assert r1_data['copyright_license']['name'] == "CC BY 4.0"
            assert r1_data['copyright_license']['open_access'] is True
            assert r1_data['license_list'] == ["CC BY 4.0"]
            assert r1_data['license_start_year'] == 2020
            assert len(r1_data['cross_references']) == 1
            assert r1_data['cross_references'][0]['curie'] == "NLM:111111"
            assert len(r1_data['editors']) == 1
            assert r1_data['editors'][0]['first_name'] == "Alice"

            # Resource 2: same CC BY license as resource 1
            r2_data = by_curie[curie2]
            assert r2_data['title'] == "Journal of Testing Beta"
            assert r2_data['copyright_license_id'] == license_cc_by.copyright_license_id
            assert r2_data['copyright_license']['name'] == "CC BY 4.0"
            assert r2_data['license_list'] == ["CC BY 4.0"]
            assert r2_data['license_start_year'] == 2021
            assert r2_data['cross_references'][0]['curie'] == "NLM:222222"
            assert r2_data['editors'][0]['first_name'] == "Bob"

            # Resource 3: different CC BY-NC license
            r3_data = by_curie[curie3]
            assert r3_data['title'] == "Journal of Testing Gamma"
            assert r3_data['copyright_license_id'] == license_cc_nc.copyright_license_id
            assert r3_data['copyright_license']['name'] == "CC BY-NC 4.0"
            assert r3_data['copyright_license']['open_access'] is False
            assert r3_data['license_list'] == ["CC BY-NC 4.0"]
            assert r3_data['license_start_year'] == 2019
            assert r3_data['cross_references'][0]['curie'] == "NLM:333333"
            assert r3_data['editors'][0]['first_name'] == "Carol"

            # Resource 4: no license, no editor
            r4_data = by_curie[curie4]
            assert r4_data['title'] == "Journal of Testing Delta"
            assert r4_data['copyright_license_id'] is None
            assert r4_data.get('copyright_license') is None
            assert r4_data['license_list'] is None
            assert r4_data['license_start_year'] is None
            assert r4_data['cross_references'][0]['curie'] == "NLM:444444"
            assert r4_data['editors'] == []

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
