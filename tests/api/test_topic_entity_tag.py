import copy
from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import TopicEntityTagModel, TopicEntityTagPropModel
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from .test_reference import test_reference # noqa

test_reference2 = test_reference

TestTETData = namedtuple('TestTETData', ['response', 'new_tet_id', 'related_ref_curie'])


@pytest.fixture
def test_topic_entity_tag(db, auth_headers, test_reference): # noqa
    print("***** Adding a test workflow tag *****")
    with TestClient(app) as client:
        new_tet = {
            "reference_curie": test_reference.new_ref_curie,
            "topic": "Topic1",
            "entity_type": "Gene",
            "alliance_entity": "Bob_gene_name",
            "taxon": "NCBITaxon:1234",
            "note": "Some Note",
            "props": [{"qualifier": "Quali1"},
                      {"qualifier": "Quali2"}]
        }
        response = client.post(url="/topic_entity_tag/", json=new_tet, headers=auth_headers)
        yield TestTETData(response, response.json(), test_reference.new_ref_curie)


class TestTopicEntityTag:

    def test_good_create_with_props(self, db, test_topic_entity_tag): # noqa
        with TestClient(app) as client:
            assert test_topic_entity_tag.response.status_code == status.HTTP_201_CREATED

            tet_obj = db.query(TopicEntityTagModel).filter(
                TopicEntityTagModel.topic_entity_tag_id == test_topic_entity_tag.new_tet_id).first()

            # assert tet_obj.reference_id == refs[0].reference_id
            assert tet_obj.topic == "Topic1"
            assert tet_obj.entity_type == "Gene"
            assert tet_obj.alliance_entity == "Bob_gene_name"
            assert tet_obj.taxon == "NCBITaxon:1234"
            assert tet_obj.note == "Some Note"

            props = db.query(TopicEntityTagPropModel).filter(
                TopicEntityTagPropModel.topic_entity_tag_id == test_topic_entity_tag.new_tet_id).all()

            count = 0
            for prop in props:
                if prop.qualifier in ["Quali1", "Quali2"]:
                    count += 1
                else:
                    assert "Diff qualifier" == prop.qualifier
            assert count == 2

            response = client.get(url=f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}")
            res = response.json()
            assert res["topic"] == "Topic1"
            assert res["props"][0]["qualifier"] == "Quali1"
            assert res["props"][1]["qualifier"] == "Quali2"

    def test_create_bad(self, test_topic_entity_tag, auth_headers): # noqa
        with TestClient(app) as client:
            xml = {
                "reference_curie": test_topic_entity_tag.related_ref_curie,
                "topic": "Topic1",
                "entity_type": "Gene",
                "taxon": "NCBITaxon:1234"
            }
            # No Entities
            response = client.post(url="/topic_entity_tag/", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            # More than one Entity
            xml2 = copy.deepcopy(xml)
            xml2["alliance_entity"] = "Bob_gene_name 1"
            xml2["mod_entity"] = "Bob_gene_name 2"
            response = client.post(url="/topic_entity_tag/", json=xml2, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            # No curie
            xml3 = copy.deepcopy(xml2)
            del xml3["mod_entity"]
            del xml3["reference_curie"]
            response = client.post(url="/topic_entity_tag/", json=xml3, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            # Bad curie
            xml4 = copy.deepcopy(xml3)
            xml4["reference_curie"] = "BADCURIE"
            response = client.post(url="/topic_entity_tag/", json=xml4, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            # No species
            xml5 = copy.deepcopy(xml4)
            del xml5["taxon"]
            xml5["reference_curie"] = test_topic_entity_tag.related_ref_curie
            response = client.post(url="/topic_entity_tag/", json=xml4, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_create_without_props(self, test_reference, auth_headers): # noqa
        with TestClient(app) as client:
            new_tet = {
                "reference_curie": test_reference.new_ref_curie,
                "topic": "Topic1",
                "entity_type": "Gene",
                "alliance_entity": "Bob_gene_name",
                "taxon": "NCBITaxon:1234",
                "note": "Some Note"
            }
            response = client.post(url="/topic_entity_tag/", json=new_tet, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED

    def test_patch_with_props(self, test_topic_entity_tag, test_reference2, auth_headers): # noqa
        with TestClient(app) as client:
            # change the reference
            patch_data = {
                "reference_curie": test_reference2.new_ref_curie,
                "props": [
                    {"qualifier": "NEW one"}
                ]
            }
            response = client.patch(url=f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}", json=patch_data,
                                    headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED
            res = client.get(url=f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}").json()
            assert res["reference_curie"] == test_reference2.new_ref_curie

            # Change the note
            patch_data2 = {
                "note": ""
            }
            response = client.patch(url=f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}", json=patch_data2,
                                    headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED
            res = client.get(url=f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}").json()
            assert res["note"] == ""

            # Change the note
            patch_data3 = {
                "note": None
            }
            response = client.patch(url=f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}", json=patch_data3,
                                    headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED
            res = client.get(url=f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}").json()
            assert not res["note"]
            # TODO we need to sort the props at the API level before testing them in specific order
            # assert res["props"][0]["qualifier"] == "Quali1"
            # assert res["props"][1]["qualifier"] == "Quali2"
            # assert res["props"][2]["qualifier"] == "NEW one"

            # change the prop?
            patch_data4 = {
                "props": [{"qualifier": "Quali3",
                           "topic_entity_tag_prop_id": res["props"][0]["topic_entity_tag_prop_id"]},
                          {"qualifier": "Quali4",
                           "topic_entity_tag_prop_id": res["props"][1]["topic_entity_tag_prop_id"]}]
            }
            response = client.patch(url=f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}", json=patch_data4,
                                    headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED
            res = client.get(url=f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}").json()
            # TODO we need to sort the props at the API level before testing them in specific order
            # assert res["props"][0]["qualifier"] == "Quali3"
            # assert res["props"][1]["qualifier"] == "Quali4"

    def test_delete_with_props(self, test_topic_entity_tag, auth_headers): # noqa
        with TestClient(app) as client:
            response = client.delete(url=f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}", headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT

            # Make sure it is no longer there
            response = client.get(url=f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}")
            assert response.status_code == status.HTTP_404_NOT_FOUND

            # Check the prop is no longer there.
            response = client.delete(url=f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}", headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_props(self, test_topic_entity_tag, auth_headers): # noqa
        with TestClient(app) as client:
            # Create the prop
            prop_data = {
                "qualifier": "New Q1",
                "topic_entity_tag_id": test_topic_entity_tag.new_tet_id
            }
            response = client.post(url="/topic_entity_tag_prop/", json=prop_data, headers=auth_headers)
            prop_id = response.json()
            assert response.status_code == status.HTTP_201_CREATED
            response = client.get(url=f"/topic_entity_tag/{test_topic_entity_tag.new_tet_id}")
            res = response.json()
            assert res["props"][2]["qualifier"] == "New Q1"
            assert res["props"][2]["topic_entity_tag_prop_id"] == int(prop_id)

            # show the prop
            response = client.get(url=f"/topic_entity_tag_prop/{prop_id}")
            res = response.json()
            assert res["qualifier"] == "New Q1"
            assert res["topic_entity_tag_prop_id"] == int(prop_id)

            # Update the prop
            patch_data = {
                "qualifier": "Another Q"
            }
            response = client.patch(url=f"/topic_entity_tag_prop/{prop_id}", json=patch_data, headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED
            response = client.get(url=f"/topic_entity_tag_prop/{prop_id}")
            res = response.json()
            assert res["qualifier"] == "Another Q"
            assert res["topic_entity_tag_prop_id"] == int(prop_id)

            # delete the prop
            response = client.delete(url=f"/topic_entity_tag_prop/{prop_id}", headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT

            # check it is not there
            response = client.get(url=f"/topic_entity_tag_prop/{prop_id}")
            assert response.status_code == status.HTTP_404_NOT_FOUND

            # try deleting again.
            response = client.delete(url=f"/topic_entity_tag_prop/{prop_id}", headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_get_all_reference_tags(self, auth_headers): # noqa
        with TestClient(app) as client:
            reference_data = {
                "category": "research_article",
                "abstract": "The Hippo (Hpo) pathway is a conserved tumor suppressor pathway",
                "date_published_start": "2022-10-01 00:00:01",
                "date_published_end": "2022-10-02T00:00:01",
                "title": "Some test 001 title",
                "authors": [
                    {
                        "order": 2,
                        "first_name": "S.",
                        "last_name": "Wu",
                        "name": "S. Wu",
                        "first_initial": "S"
                    },
                    {
                        "order": 1,
                        "first_name": "D.",
                        "last_name": "Wu",
                        "name": "D. Wu",
                        "first_initial": "D"
                    }
                ],
                "topic_entity_tags": [
                    {
                        "topic": "string",
                        "entity_type": "string",
                        "alliance_entity": "string",
                        "taxon": "string",
                        "note": "string",
                        "props": [{"qualifier": "Quali1"},
                                  {"qualifier": "Quali2"}]
                    }
                ]
            }

            new_curie = client.post(url="/reference/", json=reference_data, headers=auth_headers).json()
            response = client.get(url=f"/topic_entity_tag/by_reference/{new_curie}").json()
            assert len(response) > 0
            assert len(response[0]["props"]) == 2
