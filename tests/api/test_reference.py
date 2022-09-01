import copy

import pytest
from pydantic import ValidationError
from sqlalchemy_continuum import Operation
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import ReferenceModel, AuthorModel, CrossReferenceModel
from agr_literature_service.api.schemas import ReferenceSchemaPost
from .fixtures import auth_headers, db # noqa
from .test_resource import test_resource # noqa


@pytest.fixture
def test_reference(db, auth_headers): # noqa
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

    def test_create_reference(self, db, auth_headers, test_reference): # noqa
        with TestClient(app) as client:
            response = test_reference
            assert response.status_code == status.HTTP_201_CREATED
            db_obj = db.query(ReferenceModel).filter(ReferenceModel.curie == response.json()).one()
            assert db_obj.title == "Bob"
            assert db_obj.date_created is not None
            assert db_obj.date_updated is not None
            response = client.get(url=f"/reference/{test_reference.json()}")
            assert response.status_code == status.HTTP_200_OK
            assert response.json()["title"] == "Bob"
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
            wrong_reference = {
                "title": None,
                "category": "thesis"
            }
            response = client.post(url="/reference/", json=wrong_reference, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            # blank title
            # ReferenceSchemaPost raises exception
            wrong_reference = {
                "title": "",
                "category": "thesis"
            }
            response = client.post(url="/reference/", json=wrong_reference, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_show_reference(self, auth_headers, test_reference): # noqa
        with TestClient(app) as client:
            get_response = client.get(url=f"/reference/{test_reference.json()}")
            added_ref = get_response.json()
            assert added_ref["title"] == "Bob"
            assert added_ref["category"] == 'thesis'
            assert added_ref["abstract"] == '3'

            # Lookup 1 that does not exist
            res = client.get(url="/reference/does_not_exist")
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_update_reference(self, auth_headers, test_reference): # noqa
        with TestClient(app) as client:

            # patch docs says it needs a ReferenceSchemaUpdate
            # but does not work with this.
            # with pytest.raises(AttributeError):
            created_ref_curie = test_reference.json()
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

    def test_changesets(self, test_reference, auth_headers): # noqa
        with TestClient(app) as client:
            created_ref_curie = test_reference.json()
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

    def test_delete_reference(self, auth_headers, test_reference): # noqa
        with TestClient(app) as client:
            created_ref_curie = test_reference.json()
            delete_response = client.delete(url=f"/reference/{created_ref_curie}", headers=auth_headers)
            assert delete_response.status_code == status.HTTP_204_NO_CONTENT
            get_response = client.get(url=f"/reference/{created_ref_curie}")
            assert get_response.status_code == status.HTTP_404_NOT_FOUND
            delete_response = client.delete(url=f"/reference/{created_ref_curie}", headers=auth_headers)
            assert delete_response.status_code == status.HTTP_404_NOT_FOUND

    def test_reference_large(self, db, auth_headers): # noqa
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

    def test_reference_merging(self, db, test_resource, auth_headers): # noqa
        with TestClient(app) as client:
            ref1_data = {
                "category": "research_article",
                "abstract": "013 - abs A",
                "authors": [
                    {
                        "orcid": 'ORCID:1234-1234-1234-123X'
                    },
                    {
                        "orcid": 'ORCID:1111-2222-3333-444X'  # New
                    }
                ],
                "resource": test_resource.json(),
                "title": "Another title",
                "volume": "013a",
                "open_access": True
            }
            response1 = client.post(url="/reference/", json=ref1_data, headers=auth_headers)

            ref2_data = copy.deepcopy(ref1_data)
            ref2_data['volume'] = '013b'
            ref2_data['abstract'] = "013 - abs B"
            response2 = client.post(url="/reference/", json=ref2_data, headers=auth_headers)

            ref3_data = copy.deepcopy(ref2_data)
            ref3_data['volume'] = '013c'
            ref3_data['abstract'] = "013 - abs C"
            response3 = client.post(url="/reference/", json=ref3_data, headers=auth_headers)

            # update ref_obj with a different category
            # This is just to test the transactions and versions
            xml = {"category": "other"}
            response_patch1 = client.patch(url=f"/reference/{response1.json()}", json=xml, headers=auth_headers)
            assert response_patch1.status_code == status.HTTP_202_ACCEPTED
            response_patch3 = client.patch(url=f"/reference/{response3.json()}", json=xml, headers=auth_headers)
            assert response_patch3.status_code == status.HTTP_202_ACCEPTED

            # fetch the new record.
            res = client.get(url=f"/reference/{response1.json()}").json()
            assert res['category'] == 'other'

            # merge 1 into 2
            response_merge1 = client.post(url=f"/reference/merge/{response1.json()}/{response2.json()}",
                                          headers=auth_headers)
            assert response_merge1.status_code == status.HTTP_201_CREATED
            # merge 2 into 3
            response_merge2 = client.post(url=f"/reference/merge/{response2.json()}/{response3.json()}",
                                          headers=auth_headers)
            assert response_merge2.status_code == status.HTTP_201_CREATED
            # So now if we look up ref_obj we should get ref3_obj
            # and if we lookup ref2_obj we should get ref3_obj
            response_ref1 = client.get(url=f"/reference/{response1.json()}")
            assert response_ref1.json()['curie'] == response3.json()
            response_ref2 = client.get(url=f"/reference/{response2.json()}")
            assert response_ref2.json()['curie'] == response3.json()

            ##########################################################################
            # The following are really examples of continuum and not testing the code
            ##########################################################################

            #####################################
            # 1) Manually examine the _version table
            #####################################
            sql = """SELECT transaction_id, operation_type, end_transaction_id, category, category_mod
                     FROM reference_version
                       WHERE curie = '{}'
                       ORDER BY transaction_id
                """.format(response1.json())

            rs = db.execute(sql)
            # (33, 1, 36, 'Research_Article', True)
            # (36, 1, 37, 'Other', True)
            # (37, 2, None, 'Other', True)

            print("insert: {}, update: {}, delete: {}".format(Operation.INSERT, Operation.UPDATE, Operation.DELETE))
            results = []
            for row in rs:
                print(row)
                results.append(row)

            # last transaction check.
            assert results[0][2] == results[1][0]

            # final Transaction_id is none
            assert results[2][2] is None

            # check category changed
            assert results[0][3] != results[1][3]

            ######################
            # 2) version traversal
            ######################
            ref = db.query(ReferenceModel).filter(ReferenceModel.curie == response3.json()).first()
            first_ver = ref.versions[0]
            # lower case now???
            assert first_ver.category == 'research_article'

            sec_ver = first_ver.next
            assert sec_ver.category == 'other'

            ########################################
            # 3) changesets, see test_001_reference.
            ########################################

