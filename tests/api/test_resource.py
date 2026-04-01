from collections import namedtuple

import pytest
from starlette.testclient import TestClient

from agr_literature_service.api.main import app
from fastapi import status

from agr_literature_service.api.models import ResourceModel
from agr_literature_service.api.models.copyright_license_model import CopyrightLicenseModel
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from unittest.mock import patch
from agr_literature_service.api.models import CrossReferenceModel  # noqa
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.resource_lookup import parse_nlm_catalog_xml

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
            assert 'copyright_license' in r4_data
            assert r4_data['copyright_license'] is None
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

    def test_external_lookup_nlm_exists_in_db(self, auth_headers, db):  # noqa
        with TestClient(app) as client:
            # Create a resource with an NLM cross-reference
            res = client.post(url="/resource/", json={
                "title": "Test NLM Journal",
                "cross_references": [{"curie": "NLM:9999999"}]
            }, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            resource_curie = res.json()

            response = client.get(
                url="/resource/external_lookup/NLM:9999999",
                headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data['exists_in_db'] is True
            assert data['resource_curies'] == [resource_curie]
            assert data['external_curie'] == 'NLM:9999999'
            assert data['external_curie_found'] is True
            assert data['title'] == 'Test NLM Journal'

    def test_external_lookup_issn_exists_in_db(self, auth_headers, db):  # noqa
        with TestClient(app) as client:
            res = client.post(url="/resource/", json={
                "title": "Test ISSN Journal",
                "print_issn": "1234-5678"
            }, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            resource_curie = res.json()

            response = client.get(
                url="/resource/external_lookup/ISSN:1234-5678",
                headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data['exists_in_db'] is True
            assert data['resource_curies'] == [resource_curie]
            assert data['external_curie'] == 'ISSN:1234-5678'
            assert data['external_curie_found'] is True
            assert data['title'] == 'Test ISSN Journal'

    @patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest.resource_lookup.fetch_nlm_catalog_xml")
    @patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest.resource_lookup.search_nlm_catalog")
    def test_external_lookup_nlm_found_at_nlm_catalog(self, mock_search, mock_fetch, auth_headers, db):  # noqa
        mock_search.return_value = "410462"
        mock_fetch.return_value = (
            '<?xml version="1.0" ?>'
            '<NLMCatalogRecordSet><NLMCatalogRecord>'
            '<NlmUniqueID>0410462</NlmUniqueID>'
            '<TitleMain><Title Sort="0">Nature.</Title></TitleMain>'
            '</NLMCatalogRecord></NLMCatalogRecordSet>'
        )
        with TestClient(app) as client:
            response = client.get(
                url="/resource/external_lookup/NLM:0410462",
                headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data['exists_in_db'] is False
            assert data['resource_curies'] is None
            assert data['external_curie'] == 'NLM:0410462'
            assert data['external_curie_found'] is True
            assert data['title'] == 'Nature.'

    @patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest.resource_lookup.fetch_nlm_catalog_xml")
    @patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest.resource_lookup.search_nlm_catalog")
    def test_external_lookup_issn_found_at_nlm_catalog(self, mock_search, mock_fetch, auth_headers, db):  # noqa
        mock_search.return_value = "410462"
        mock_fetch.return_value = (
            '<?xml version="1.0" ?>'
            '<NLMCatalogRecordSet><NLMCatalogRecord>'
            '<NlmUniqueID>0410462</NlmUniqueID>'
            '<TitleMain><Title Sort="0">Nature.</Title></TitleMain>'
            '</NLMCatalogRecord></NLMCatalogRecordSet>'
        )
        with TestClient(app) as client:
            response = client.get(
                url="/resource/external_lookup/ISSN:0028-0836",
                headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data['exists_in_db'] is False
            assert data['resource_curies'] is None
            assert data['external_curie'] == 'ISSN:0028-0836'
            assert data['external_curie_found'] is True
            assert data['title'] == 'Nature.'

    @patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest.resource_lookup.search_nlm_catalog")
    def test_external_lookup_nlm_not_found(self, mock_search, auth_headers, db):  # noqa
        mock_search.return_value = ""
        with TestClient(app) as client:
            response = client.get(
                url="/resource/external_lookup/NLM:0000000",
                headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data['exists_in_db'] is False
            assert data['resource_curies'] is None
            assert data['external_curie_found'] is False
            assert data['title'] == ''

    @patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest.resource_lookup.search_nlm_catalog")
    def test_external_lookup_nlm_api_failure(self, mock_search, auth_headers, db):  # noqa
        mock_search.side_effect = Exception("Connection refused")
        with TestClient(app) as client:
            response = client.get(
                url="/resource/external_lookup/NLM:0410462",
                headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data['exists_in_db'] is False
            assert data['external_curie_found'] is False
            assert data['title'] == ''

    def test_external_lookup_isbn_exists_in_db(self, auth_headers, db):  # noqa
        with TestClient(app) as client:
            res = client.post(url="/resource/", json={
                "title": "Test ISBN Book",
                "cross_references": [{"curie": "ISBN:978-0-12345"}]
            }, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            resource_curie = res.json()

            response = client.get(
                url="/resource/external_lookup/ISBN:978-0-12345",
                headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data['exists_in_db'] is True
            assert data['resource_curies'] == [resource_curie]
            assert data['external_curie_found'] is True
            assert data['title'] == 'Test ISBN Book'

    def test_external_lookup_isbn_not_supported(self, auth_headers, db):  # noqa
        with TestClient(app) as client:
            response = client.get(
                url="/resource/external_lookup/ISBN:000-0-00000",
                headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data['exists_in_db'] is False
            assert data['external_curie_found'] is False
            assert data['title'] == 'ISBN not supported yet'

    def test_external_lookup_unsupported_prefix(self, auth_headers, db):  # noqa
        with TestClient(app) as client:
            response = client.get(
                url="/resource/external_lookup/DOI:10.1234",
                headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    @patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest.resource_lookup.fetch_nlm_catalog_xml")
    @patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest.resource_lookup.search_nlm_catalog")
    def test_external_lookup_case_insensitive_prefix(self, mock_search, mock_fetch, auth_headers, db):  # noqa
        mock_search.return_value = "410462"
        mock_fetch.return_value = (
            '<?xml version="1.0" ?>'
            '<NLMCatalogRecordSet><NLMCatalogRecord>'
            '<NlmUniqueID>0410462</NlmUniqueID>'
            '<TitleMain><Title Sort="0">Nature.</Title></TitleMain>'
            '</NLMCatalogRecord></NLMCatalogRecordSet>'
        )
        with TestClient(app) as client:
            response = client.get(
                url="/resource/external_lookup/nlm:0410462",
                headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data['external_curie_found'] is True
            assert data['title'] == 'Nature.'

            response = client.get(
                url="/resource/external_lookup/NLMID:0410462",
                headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data['external_curie_found'] is True
            assert data['title'] == 'Nature.'

    def test_add_isbn_not_supported(self, auth_headers, db):  # noqa
        with TestClient(app) as client:
            response = client.post(
                url="/resource/add/",
                json={"curie": "ISBN:978-0-12345"},
                headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
            assert "ISBN not supported yet" in response.json()['detail']

    def test_add_unsupported_prefix(self, auth_headers, db):  # noqa
        with TestClient(app) as client:
            response = client.post(
                url="/resource/add/",
                json={"curie": "DOI:10.1234"},
                headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
            assert "You must enter an NLM, ISSN, or ISBN" in response.json()['detail']

    def test_add_missing_curie(self, auth_headers, db):  # noqa
        with TestClient(app) as client:
            response = client.post(
                url="/resource/add/",
                json={"curie": "nocolon"},
                headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    @patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest.resource_lookup.fetch_nlm_catalog_xml")
    @patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest.resource_lookup.search_nlm_catalog")
    def test_add_nlm_creates_resource(self, mock_search, mock_fetch, auth_headers, db):  # noqa
        mock_search.return_value = "410462"
        mock_fetch.return_value = (
            '<?xml version="1.0" ?>'
            '<NLMCatalogRecordSet><NLMCatalogRecord>'
            '<NlmUniqueID>0410462</NlmUniqueID>'
            '<TitleMain><Title Sort="0">Nature.</Title></TitleMain>'
            '<MedlineTA>Nature</MedlineTA>'
            '<ISSN ValidYN="Y" IssnType="Print">0028-0836</ISSN>'
            '<ISSN ValidYN="Y" IssnType="Electronic">1476-4687</ISSN>'
            '</NLMCatalogRecord></NLMCatalogRecordSet>'
        )
        with TestClient(app) as client:
            response = client.post(
                url="/resource/add/",
                json={"curie": "NLM:0410462"},
                headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            curies = response.json()
            assert len(curies) == 1
            assert curies[0].startswith("AGRKB:")

            # Verify the resource was created with correct data
            res = client.get(url=f"/resource/{curies[0]}", headers=auth_headers)
            assert res.status_code == status.HTTP_200_OK
            data = res.json()
            assert data['title'] == 'Nature.'
            assert data['medline_abbreviation'] == 'Nature'
            assert data['print_issn'] == '0028-0836'
            assert data['online_issn'] == '1476-4687'

    @patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest.resource_lookup.fetch_nlm_catalog_xml")
    @patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest.resource_lookup.search_nlm_catalog")
    def test_add_issn_creates_resource(self, mock_search, mock_fetch, auth_headers, db):  # noqa
        mock_search.return_value = "999999"
        mock_fetch.return_value = (
            '<?xml version="1.0" ?>'
            '<NLMCatalogRecordSet><NLMCatalogRecord>'
            '<NlmUniqueID>999999</NlmUniqueID>'
            '<TitleMain><Title Sort="0">Test ISSN Add Journal.</Title></TitleMain>'
            '<ISSN ValidYN="Y" IssnType="Print">9999-0001</ISSN>'
            '</NLMCatalogRecord></NLMCatalogRecordSet>'
        )
        with TestClient(app) as client:
            response = client.post(
                url="/resource/add/",
                json={"curie": "ISSN:9999-0001"},
                headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            curies = response.json()
            assert len(curies) == 1
            assert curies[0].startswith("AGRKB:")

    def test_add_nlm_already_exists(self, auth_headers, db):  # noqa
        with TestClient(app) as client:
            # Create a resource with an NLM cross-reference
            res = client.post(url="/resource/", json={
                "title": "Existing NLM Resource",
                "cross_references": [{"curie": "NLM:8888888"}]
            }, headers=auth_headers)
            assert res.status_code == status.HTTP_201_CREATED
            existing_curie = res.json()

            # Try to add via the same NLM — should return existing
            response = client.post(
                url="/resource/add/",
                json={"curie": "NLM:8888888"},
                headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            assert response.json() == [existing_curie]

    @patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest.resource_lookup.search_nlm_catalog")
    def test_add_nlm_not_found(self, mock_search, auth_headers, db):  # noqa
        mock_search.return_value = ""
        with TestClient(app) as client:
            response = client.post(
                url="/resource/add/",
                json={"curie": "NLM:0000000"},
                headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND


class TestParseNlmCatalogXml:

    def test_parse_full_xml(self):  # noqa
        xml = (
            '<?xml version="1.0" ?>'
            '<NLMCatalogRecordSet><NLMCatalogRecord>'
            '<NlmUniqueID>0410462</NlmUniqueID>'
            '<TitleMain><Title Sort="0">Nature.</Title></TitleMain>'
            '<MedlineTA>Nature</MedlineTA>'
            '<TitleAlternate Owner="NLM" TitleType="Other">'
            '<Title Sort="N">Nature (London)</Title></TitleAlternate>'
            '<TitleAlternate Owner="NLM" TitleType="Other">'
            '<Title Sort="N">Nature (Lond)</Title></TitleAlternate>'
            '<PublicationInfo>'
            '<Imprint ImprintType="Original" FunctionType="Publication">'
            '<Entity>Macmillan Journals ltd.</Entity></Imprint>'
            '<Imprint ImprintType="Current" FunctionType="Publication">'
            '<Entity>Nature Publishing Group</Entity></Imprint>'
            '</PublicationInfo>'
            '<ISSN ValidYN="Y" IssnType="Print">0028-0836</ISSN>'
            '<ISSN ValidYN="N" IssnType="Undetermined">0302-2889</ISSN>'
            '<ISSN ValidYN="Y" IssnType="Electronic">1476-4687</ISSN>'
            '</NLMCatalogRecord></NLMCatalogRecordSet>'
        )
        result = parse_nlm_catalog_xml(xml)
        assert result['primaryId'] == 'NLM:0410462'
        assert result['nlm'] == '0410462'
        assert result['title'] == 'Nature.'
        assert result['medlineAbbreviation'] == 'Nature'
        assert result['printISSN'] == '0028-0836'
        assert result['onlineISSN'] == '1476-4687'
        assert result['publisher'] == 'Nature Publishing Group'
        assert result['crossReferences'] == [{'id': 'NLM:0410462'}]
        assert 'Nature (London)' in result['titleSynonyms']
        assert 'Nature (Lond)' in result['titleSynonyms']
        assert 'Nature.' not in result['titleSynonyms']

    def test_parse_minimal_xml(self):  # noqa
        xml = (
            '<NLMCatalogRecordSet><NLMCatalogRecord>'
            '<NlmUniqueID>101528555</NlmUniqueID>'
            '<TitleMain><Title Sort="0">Nature communications.</Title></TitleMain>'
            '</NLMCatalogRecord></NLMCatalogRecordSet>'
        )
        result = parse_nlm_catalog_xml(xml)
        assert result['primaryId'] == 'NLM:101528555'
        assert result['title'] == 'Nature communications.'
        assert 'medlineAbbreviation' not in result
        assert 'printISSN' not in result
        assert 'onlineISSN' not in result
        assert 'titleSynonyms' not in result
        assert 'publisher' not in result

    def test_parse_no_nlm_id(self):  # noqa
        xml = '<NLMCatalogRecordSet><NLMCatalogRecord></NLMCatalogRecord></NLMCatalogRecordSet>'
        result = parse_nlm_catalog_xml(xml)
        assert result == {}

    def test_parse_issn_skips_invalid(self):  # noqa
        xml = (
            '<NLMCatalogRecordSet><NLMCatalogRecord>'
            '<NlmUniqueID>123</NlmUniqueID>'
            '<TitleMain><Title>Test</Title></TitleMain>'
            '<ISSN ValidYN="N" IssnType="Print">0000-0000</ISSN>'
            '<ISSN ValidYN="Y" IssnType="Electronic">1111-1111</ISSN>'
            '</NLMCatalogRecord></NLMCatalogRecordSet>'
        )
        result = parse_nlm_catalog_xml(xml)
        assert 'printISSN' not in result
        assert result['onlineISSN'] == '1111-1111'

    def test_parse_publisher_fallback_to_original(self):  # noqa
        xml = (
            '<NLMCatalogRecordSet><NLMCatalogRecord>'
            '<NlmUniqueID>123</NlmUniqueID>'
            '<TitleMain><Title>Test</Title></TitleMain>'
            '<PublicationInfo>'
            '<Imprint ImprintType="Original" FunctionType="Publication">'
            '<Entity>Old Publisher</Entity></Imprint>'
            '</PublicationInfo>'
            '</NLMCatalogRecord></NLMCatalogRecordSet>'
        )
        result = parse_nlm_catalog_xml(xml)
        assert result['publisher'] == 'Old Publisher'

    def test_parse_title_synonyms_deduplication(self):  # noqa
        xml = (
            '<NLMCatalogRecordSet><NLMCatalogRecord>'
            '<NlmUniqueID>123</NlmUniqueID>'
            '<TitleMain><Title>Test Journal.</Title></TitleMain>'
            '<TitleAlternate><Title>Dup Title</Title></TitleAlternate>'
            '<TitleAlternate><Title>Dup Title</Title></TitleAlternate>'
            '<TitleAlternate><Title>Test Journal.</Title></TitleAlternate>'
            '<TitleAlternate><Title>Unique Alt</Title></TitleAlternate>'
            '</NLMCatalogRecord></NLMCatalogRecordSet>'
        )
        result = parse_nlm_catalog_xml(xml)
        assert result['titleSynonyms'] == ['Dup Title', 'Unique Alt']
