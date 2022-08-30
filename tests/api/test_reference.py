import pytest
from pydantic import ValidationError
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import ReferenceModel, AuthorModel, CrossReferenceModel
from agr_literature_service.api.schemas import ReferenceSchemaPost
from .fixtures import auth_headers, db # noqa


@pytest.fixture
def create_test_reference(auth_headers):
    print("***** Adding a test reference *****")
    with TestClient(app) as client:
        new_reference = {
            "title": "Bob",
            "category": "thesis",
            "abstract": "3",
            "language": "MadeUp"
        }
        response = client.post(url="/reference/", json=new_reference, headers=auth_headers)
        yield response


class TestReference:

    def test_create_reference(self, db, auth_headers, create_test_reference):
        with TestClient(app) as client:
            response = create_test_reference
            assert response.status_code == status.HTTP_201_CREATED
            db_obj = db.query(ReferenceModel).filter(ReferenceModel.curie == response.json()).one()
            assert db_obj.title == "Bob"
            assert db_obj.date_created is not None
            assert db_obj.date_updated is not None

            # create again with same title, category
            # Apparently not a problem!!
            new_reference = {
                "title": "Bob",
                "category": "thesis"
                }
            response = client.post(url="/reference/", json=new_reference, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            db_obj = db.query(ReferenceModel).filter(ReferenceModel.curie == response.json()).one()
            assert db_obj.title == "Bob"
            assert db_obj.date_created is not None
            assert db_obj.date_updated is not None

            # No title
            # ReferenceSchemaPost raises exception
            with pytest.raises(ValidationError):
                ReferenceSchemaPost(title=None, category="thesis")

            # blank title
            # ReferenceSchemaPost raises exception
            with pytest.raises(ValidationError):
                ReferenceSchemaPost(title="", category="thesis")

    def test_show_reference(self, db, auth_headers, create_test_reference):
        with TestClient(app) as client:
            get_response = client.get(url=f"/reference/{create_test_reference.json()}")
            added_ref = get_response.json()
            assert added_ref["title"] == "Bob"
            assert added_ref["category"] == 'thesis'
            assert added_ref["abstract"] == '3'

            # Lookup 1 that does not exist
            res = client.get(url="/reference/does_not_exist")
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_update_reference(self, db, auth_headers, create_test_reference):
        with TestClient(app) as client:

            # patch docs says it needs a ReferenceSchemaUpdate
            # but does not work with this.
            # with pytest.raises(AttributeError):
            created_ref_curie = create_test_reference.json()
            updated_fields = {"title": "new title", "category": "book", "language": "New"}
            response = client.patch(url=f"/reference/{created_ref_curie}", json=updated_fields, headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED
            response = client.post(url=f"/reference/citationupdate/{created_ref_curie}", headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            updated_ref = client.get(url=f"/reference/{created_ref_curie}").json()
            assert updated_ref["title"] == "new title"
            assert updated_ref["category"] == "book"
            assert updated_ref["language"] == "New"
            assert updated_ref["abstract"] == "3"
            # Do we have a new citation
            assert updated_ref["citation"] == ", () new title.   (): "

    def test_changesets(self, db, create_test_reference, auth_headers):
        with TestClient(app) as client:
            created_ref_curie = create_test_reference.json()
            # title            : None -> bob -> 'new title'
            # catergory        : None -> thesis -> book
            updated_fields = {"title": "new title", "category": "book", "language": "New"}
            client.patch(url=f"/reference/{created_ref_curie}", json=updated_fields, headers=auth_headers)
            client.post(url=f"/reference/citationupdate/{created_ref_curie}", headers=auth_headers)
            response = client.get(url=f"/reference/{created_ref_curie}/versions")
            transactions = response.json()
            assert transactions[0]['changeset']['curie'][1] == created_ref_curie
            assert transactions[0]['changeset']['title'][1] == "Bob"
            assert transactions[0]['changeset']['category'][1] == "thesis"
            assert transactions[1]['changeset']['title'][1] == "new title"
            assert transactions[1]['changeset']['category'][1] == "book"
            assert transactions[2]['changeset']['citation'][0] == ", () Bob.   (): "
            assert transactions[2]['changeset']['citation'][1] == ", () new title.   (): "

    def test_delete_reference(self, db, auth_headers, create_test_reference):
        with TestClient(app) as client:
            created_ref_curie = create_test_reference.json()
            delete_response = client.delete(url=f"/reference/{created_ref_curie}", headers=auth_headers)
            assert delete_response.status_code == status.HTTP_204_NO_CONTENT
            get_response = client.get(url=f"/reference/{created_ref_curie}")
            assert get_response.status_code == status.HTTP_404_NOT_FOUND
            delete_response = client.delete(url=f"/reference/{created_ref_curie}", headers=auth_headers)
            assert delete_response.status_code == status.HTTP_404_NOT_FOUND

    def test_reference_large(self, db, auth_headers):
        with TestClient(app) as client:
            full_xml = {
                "category": "research_article",
                "abstract": "The Hippo (Hpo) pathway is a conserved tumor suppressor pathway",
                "authors": [
                    {
                        "order": 2,
                        "first_name": "S.",
                        "last_name": "Wu",
                        "name": "S. Wu",
                        # "reference_id": "PMID:23524264"
                    },
                    {
                        "order": 1,
                        "first_name": "D.",
                        "last_name": "Wu",
                        "name": "D. Wu",
                        # "reference_id": "PMID:23524264"
                    }
                ],
                "mesh_terms": [
                    {
                        "heading_term": "hterm",
                        "qualifier_term": "qterm"
                    }
                ],
                "mod_reference_types": [
                    {
                        "reference_type": "mrt_rt",
                        "source": "mrt_s"
                    }
                ],
                "cross_references": [
                    {
                        "curie": "FB:FBrf0221304",
                        "pages": [
                            "reference"
                        ]
                    }
                ],
                "workflow_tags": [
                    {
                        "workflow_tag_id": "workflow_tag1",
                        "mod_abbreviation": "001_FB",
                        "created_by": "001_Bob"
                    },
                    {
                        "workflow_tag_id": "workflow_tag2",
                        "mod_abbreviation": "001_RGD",
                        "created_by": "001_Bob"
                    }
                ],
                "topic_entity_tags": [
                    {
                        "topic": "string",
                        "entity_type": "string",
                        "alliance_entity": "string",
                        "taxon": "string",
                        "note": "string"
                    }
                ],
                "issue_name": "4",
                "language": "English",
                "page_range": "538--541",
                "title": "Some test 001 title",
                "volume": "433",
                "open_access": True
            }

            new_curie = client.post(url="/reference/", json=full_xml, headers=auth_headers).json()
            # fetch the new record.
            res = client.get(url=f"/reference/{new_curie}").json()
            assert res['abstract'] == 'The Hippo (Hpo) pathway is a conserved tumor suppressor pathway'
            assert res['category'] == 'research_article'

            # Not sure of order in array of the authors so:-
            assert len(res['authors']) == 2
            for author in res['authors']:
                if author['first_name'] == 'D.':
                    assert author['name'] == 'D. Wu'
                    assert author['order'] == 1
                else:
                    assert author['name'] == 'S. Wu'
                    assert author['order'] == 2

            # Were authors created in the db?
            author = db.query(AuthorModel).filter(AuthorModel.name == "D. Wu").one()
            assert author.first_name == 'D.'
            author = db.query(AuthorModel).filter(AuthorModel.name == "S. Wu").one()
            assert author.first_name == 'S.'

            assert res['citation'] == "D. Wu; S. Wu, () Some test 001 title.  433 (): 538--541"

            assert res['cross_references'][0]['curie'] == 'FB:FBrf0221304'

            assert res['mod_reference_types'][0]['reference_type'] == "mrt_rt"

            assert res['mesh_terms'][0]['heading_term'] == "hterm"

            # cross references in the db?
            xref = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == "FB:FBrf0221304").one()
            assert xref.reference.curie == new_curie

            assert res["issue_name"] == "4"
            assert res["language"] == "English"
            assert res["page_range"] == "538--541"
            assert res["title"] == "Some test 001 title"
            assert res["volume"] == "433"
            assert res['open_access']

            print("BOB................")
            print(res)
            for ont in res["workflow_tags"]:
                if ont['mod_abbreviation'] == "001_RGD":
                    assert ont['workflow_tag_id'] == "workflow_tag2"
                elif ont['mod_abbreviation'] == "001_FB":
                    assert ont['workflow_tag_id'] == "workflow_tag1"
                else:
                    assert 1 == 0  # Not RGD or FB ?