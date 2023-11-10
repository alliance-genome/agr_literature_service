import copy
from collections import namedtuple
import json

import pytest
from sqlalchemy_continuum import Operation
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import ReferenceModel, AuthorModel, CrossReferenceModel
from agr_literature_service.api.schemas import ReferencefileSchemaPost
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from ..fixtures import db, populate_test_mod_reference_types # noqa
from .fixtures import auth_headers # noqa
from .test_resource import test_resource # noqa
from .test_mod import test_mod # noqa
from .test_copyright_license import test_copyright_license # noqa
from .test_topic_entity_tag_source import test_topic_entity_tag_source # noqa

from agr_literature_service.api.crud.referencefile_crud import create_metadata

TestReferenceData = namedtuple('TestReferenceData', ['response', 'new_ref_curie'])


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
        yield TestReferenceData(response, response.json())


@pytest.fixture
def test_referencefile(db, auth_headers, test_reference): # noqa
    print("***** Adding a test referencefile *****")
    new_referencefile = {
        "display_name": "Bob",
        "reference_curie": test_reference.new_ref_curie,
        "file_class": "main",
        "file_publication_status": "final",
        "file_extension": "pdf",
        "pdf_type": "pdf",
        "md5sum": "1234567890"
    }
    yield create_metadata(db, ReferencefileSchemaPost(**new_referencefile))


class TestReference:

    def test_create_reference(self, db, auth_headers, test_reference): # noqa
        with TestClient(app) as client:
            assert test_reference.response.status_code == status.HTTP_201_CREATED
            db_obj = db.query(ReferenceModel).filter(ReferenceModel.curie == test_reference.new_ref_curie).one()
            assert db_obj.title == "Bob"
            assert db_obj.date_created is not None
            assert db_obj.date_updated is not None
            response = client.get(url=f"/reference/{test_reference.new_ref_curie}")
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
            # ReferenceSchemaPost no longer raises exception
            none_title_reference = {
                "title": None,
                "category": None,
                "volume": "string_volume"
            }
            response = client.post(url="/reference/", json=none_title_reference, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            db_obj = db.query(ReferenceModel).filter(ReferenceModel.curie == response.json()).one()
            assert db_obj.volume == "string_volume"

            # blank title
            # ReferenceSchemaPost no longer raises exception
            blank_title_reference = {
                "title": "",
                "category": "thesis"
            }
            response = client.post(url="/reference/", json=blank_title_reference, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            db_obj = db.query(ReferenceModel).filter(ReferenceModel.curie == response.json()).one()
            assert db_obj.title == ""

            # blank category
            # ReferenceSchemaPost raises exception
            blank_category_reference = {
                "title": "a title",
                "category": ""
            }
            response = client.post(url="/reference/", json=blank_category_reference, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


    def test_show_reference(self, auth_headers, test_reference): # noqa
        with TestClient(app) as client:
            get_response = client.get(url=f"/reference/{test_reference.new_ref_curie}")
            added_ref = get_response.json()
            assert added_ref["title"] == "Bob"
            assert added_ref["category"] == 'thesis'
            assert added_ref["abstract"] == '3'

            # Lookup 1 that does not exist
            res = client.get(url="/reference/does_not_exist")
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_update_reference(self, auth_headers, test_reference, test_resource): # noqa
        with TestClient(app) as client:
            # patch docs says it needs a ReferenceSchemaUpdate
            # but does not work with this.
            # with pytest.raises(AttributeError):
            updated_fields = {"title": "new title", "category": "book", "language": "New",
                              "date_published_start": "2022-10-01", "resource": test_resource.new_resource_curie}
            response = client.patch(url=f"/reference/{test_reference.new_ref_curie}", json=updated_fields,
                                    headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED

            updated_ref = client.get(url=f"/reference/{test_reference.new_ref_curie}").json()
            print(updated_ref)
            assert updated_ref["title"] == "new title"
            assert updated_ref["category"] == "book"
            assert updated_ref["language"] == "New"
            assert updated_ref["abstract"] == "3"
            assert updated_ref["date_published_start"] == "2022-10-01"
            # Do we have a new citation
            assert updated_ref["citation"] == ", () new title. Bob ():"
            assert updated_ref["copyright_license_id"] is None
            assert updated_ref["resource_id"]

    def test_changesets(self, test_reference, auth_headers): # noqa
        with TestClient(app) as client:
            # title            : None -> bob -> 'new title'
            # catergory        : None -> thesis -> book
            updated_fields = {"title": "new title", "category": "book", "language": "New"}
            client.patch(url=f"/reference/{test_reference.new_ref_curie}", json=updated_fields, headers=auth_headers)
            # client.post(url=f"/reference/citationupdate/{test_reference.new_ref_curie}", headers=auth_headers)
            response = client.get(url=f"/reference/{test_reference.new_ref_curie}/versions")
            transactions = response.json()
            assert transactions[0]['changeset']['curie'][1] == test_reference.new_ref_curie
            assert transactions[0]['changeset']['title'][1] == "Bob"
            assert transactions[0]['changeset']['category'][1] == "thesis"
            assert transactions[1]['changeset']['title'][1] == "new title"
            assert transactions[1]['changeset']['category'][1] == "book"
            # assert transactions[2]['changeset']['citation'][0] == ", () Bob.   (): "
            # assert transactions[2]['changeset']['citation'][1] == ", () new title.  ():"

    def test_delete_reference(self, auth_headers, test_reference): # noqa
        with TestClient(app) as client:
            delete_response = client.delete(url=f"/reference/{test_reference.new_ref_curie}", headers=auth_headers)
            assert delete_response.status_code == status.HTTP_204_NO_CONTENT
            get_response = client.get(url=f"/reference/{test_reference.new_ref_curie}")
            assert get_response.status_code == status.HTTP_404_NOT_FOUND
            delete_response = client.delete(url=f"/reference/{test_reference.new_ref_curie}", headers=auth_headers)
            assert delete_response.status_code == status.HTTP_404_NOT_FOUND

    def test_reference_mca_wb(self, db, auth_headers): # noqa
        with TestClient(app) as client:
            populate_test_mods()

            full_xml = {
                "category": "research_article",
                "mod_corpus_associations": [
                    {
                        "mod_abbreviation": "WB",
                        "mod_corpus_sort_source": "manual_creation",
                        "corpus": "true"
                    }
                ]
            }
            new_curie = client.post(url="/reference/", json=full_xml, headers=auth_headers).json()
            # fetch the new record.
            response = client.get(url=f"/reference/{new_curie}").json()
            assert response['category'] == 'research_article'
            reference_obj = db.query(ReferenceModel).filter(
                ReferenceModel.curie == new_curie).first()
            xref = db.query(CrossReferenceModel).filter_by(reference_id=reference_obj.reference_id).one()
            assert xref.curie == 'WB:WBPaper00000001'

    def test_reference_large(self, db, auth_headers, populate_test_mod_reference_types, test_mod, # noqa
                             test_topic_entity_tag_source): # noqa
        with TestClient(app) as client:
            full_xml = {
                "category": "research_article",
                "abstract": "The Hippo (Hpo) pathway is a conserved tumor suppressor pathway",
                "date_published_start": "2022-10-01",
                "date_published_end": "2022-10-02",
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
                        "reference_type": "Journal",
                        "source": "ZFIN"
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
                "prepublication_pipeline": True,
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
                        "entity": "string",
                        "entity_source": "string",
                        "species": "string",
                        "topic_entity_tag_source_id": test_topic_entity_tag_source.new_source_id,
                        "negated": False,
                        "note": "test"
                    }
                ],
                "mod_corpus_associations": [
                    {
                        "mod_abbreviation": test_mod.new_mod_abbreviation,
                        "mod_corpus_sort_source": "mod_pubmed_search",
                        "corpus": True
                    }
                ],
                "issue_name": "4",
                "language": "English",
                "page_range": "538--541",
                "title": "Some test 001 title",
                "volume": "433"
            }

            new_curie = client.post(url="/reference/", json=full_xml, headers=auth_headers).json()
            # fetch the new record.
            response = client.get(url=f"/reference/{new_curie}").json()
            assert response['abstract'] == 'The Hippo (Hpo) pathway is a conserved tumor suppressor pathway'
            assert response['category'] == 'research_article'
            assert response['date_published_start'] == '2022-10-01'
            assert response['date_published_end'] == '2022-10-02'
            assert response['prepublication_pipeline'] is True

            # Not sure of order in array of the authors so:-
            assert len(response['authors']) == 2
            for author in response['authors']:
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

            assert response['citation'] == "D. Wu; S. Wu, () Some test 001 title.  433(4):538--541"

            assert response['cross_references'][0]['curie'] == 'FB:FBrf0221304'

            assert response['mod_reference_types'][0]['reference_type'] == "Journal"

            assert response['mesh_terms'][0]['heading_term'] == "hterm"

            # cross references in the db?
            xref = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == "FB:FBrf0221304").one()
            assert xref.reference.curie == new_curie

            assert response["issue_name"] == "4"
            assert response["language"] == "English"
            assert response["page_range"] == "538--541"
            assert response["title"] == "Some test 001 title"
            assert response["volume"] == "433"

            print(response)
            for ont in response["workflow_tags"]:
                if ont['mod_abbreviation'] == "001_RGD":
                    assert ont['workflow_tag_id'] == "workflow_tag2"
                elif ont['mod_abbreviation'] == "001_FB":
                    assert ont['workflow_tag_id'] == "workflow_tag1"
                else:
                    assert 1 == 0  # Not RGD or FB ?

            delete_response = client.delete(url=f"/reference/{new_curie}", headers=auth_headers)
            assert delete_response.status_code == status.HTTP_204_NO_CONTENT

            assert response["mod_corpus_associations"]
            for ont in response["mod_corpus_associations"]:
                assert ont['mod_abbreviation'] == test_mod.new_mod_abbreviation

    def test_bad_mod(self, auth_headers): # noqa
        with TestClient(app) as client:
            new_reference = {
                "title": "Bob",
                "category": "thesis",
                "abstract": "3",
                "language": "MadeUp",
                "mod_corpus_associations": [
                    {
                        "mod_abbreviation": "Made up Mod",
                        "mod_corpus_sort_source": "mod_pubmed_search",
                        "corpus": True
                    }
                ]
            }
            response = client.post(url="/reference/", json=new_reference, headers=auth_headers)
            assert json.loads(response.content.decode('utf-8'))['detail'] == 'Mod with abbreviation Made up Mod does not exist'

    def test_reference_merging(self, db, test_resource, auth_headers): # noqa
        with TestClient(app) as client:
            ref1_data = {
                "category": "research_article",
                "abstract": "013 - abs A",
                "authors": [
                    {
                        "name": "S. K",
                        "order": 1
                        # "orcid": 'ORCID:1234-1234-1234-123X'
                    },
                    {
                        "name": "S. W",
                        "order": 2
                        # "orcid": 'ORCID:1111-2222-3333-444X'  # New
                    }
                ],
                "resource": test_resource.new_resource_curie,
                "title": "Another title",
                "volume": "013a"
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

    def test_show_mod_reference_types_by_mod(self, populate_test_mod_reference_types): # noqa
        with TestClient(app) as client:
            response = client.get(url="/reference/mod_reference_type/by_mod/WB")
            assert response.status_code == 200
            wb_ref_types = response.json()
            assert len(wb_ref_types) > 0
            assert 'Journal_article' in wb_ref_types
            assert 'Micropublication' in wb_ref_types

    def test_get_bib_info(self, test_reference, auth_headers, test_mod): # noqa
        with TestClient(app) as client:
            parameters = {
                'mod_abbreviation': test_mod.new_mod_abbreviation,
                'return_format': 'txt'
            }
            response = client.get(url=f"/reference/get_bib_info/{test_reference.new_ref_curie}", params=parameters,
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert response.json() == 'author|\n' \
                                      'accession| AGRKB:101000000000001\n' \
                                      'type|\n' \
                                      'title|Bob\n' \
                                      'journal|\n' \
                                      'citation|V: P: \n' \
                                      'year|\n' \
                                      'abstract|3\n'

    def test_get_textpresso_reference_list(self, test_reference, auth_headers, test_mod, db):  # noqa
        with TestClient(app) as client:
            new_referencefile_main_1 = {
                "display_name": "Bob1",
                "reference_curie": test_reference.new_ref_curie,
                "file_class": "main",
                "file_publication_status": "final",
                "file_extension": "pdf",
                "pdf_type": "pdf",
                "md5sum": "1234567890",
                "mod_abbreviation": test_mod.new_mod_abbreviation
            }
            new_referencefile_main_2 = {
                "display_name": "Bob2",
                "reference_curie": test_reference.new_ref_curie,
                "file_class": "main",
                "file_publication_status": "final",
                "file_extension": "pdf",
                "pdf_type": "pdf",
                "md5sum": "1234567891",
            }
            new_referencefile_sup_1 = {
                "display_name": "Sup1",
                "reference_curie": test_reference.new_ref_curie,
                "file_class": "supplement",
                "file_publication_status": "final",
                "file_extension": "pdf",
                "pdf_type": "pdf",
                "md5sum": "1234567892"
            }
            create_metadata(db, ReferencefileSchemaPost(**new_referencefile_main_1))
            reffile_id_main_2 = create_metadata(db, ReferencefileSchemaPost(**new_referencefile_main_2))
            reffile_id_sup_1 = create_metadata(db, ReferencefileSchemaPost(**new_referencefile_sup_1))

            new_mca = {
                "mod_abbreviation": test_mod.new_mod_abbreviation,
                "reference_curie": test_reference.new_ref_curie,
                "mod_corpus_sort_source": 'mod_pubmed_search',
                "corpus": True
            }
            client.post(url="/reference/mod_corpus_association/", json=new_mca, headers=auth_headers)

            new_referencefile_mod = {
                "referencefile_id": reffile_id_main_2,
                "mod_abbreviation": test_mod.new_mod_abbreviation
            }
            client.post(url="/reference/referencefile_mod/", json=new_referencefile_mod, headers=auth_headers)

            new_referencefile_mod = {
                "referencefile_id": reffile_id_sup_1,
                "mod_abbreviation": test_mod.new_mod_abbreviation
            }
            client.post(url="/reference/referencefile_mod/", json=new_referencefile_mod, headers=auth_headers)

            result = client.get(url=f"/reference/get_textpresso_reference_list/{test_mod.new_mod_abbreviation}",
                                headers=auth_headers)
            assert result.status_code == status.HTTP_200_OK
            assert result.json()

    def test_reference_licenses(self, auth_headers, test_reference, test_copyright_license): # noqa
        print(test_copyright_license)
        with TestClient(app) as client:
            response = client.post(url=f"/reference/add_license/{test_reference.new_ref_curie}/{test_copyright_license.new_license_name}",
                                   headers=auth_headers)
        print(response)
        response = client.get(url=f"/reference/{test_reference.new_ref_curie}")
        assert response.status_code == status.HTTP_200_OK
        print(response.json())
        assert response.json()["copyright_license_name"] == test_copyright_license.new_license_name
        assert response.json()["copyright_license_url"] == "test url"
        assert response.json()["copyright_license_description"] == "test description"
        assert response.json()["copyright_license_open_access"]

        # okay lets set it to blank
        with TestClient(app) as client:
            response = client.post(url=f"/reference/add_license/{test_reference.new_ref_curie}/No+license",
                                   headers=auth_headers)
        response = client.get(url=f"/reference/{test_reference.new_ref_curie}")
        assert response.status_code == status.HTTP_200_OK
        print(response.json())
        assert response.json()["copyright_license_name"] is None

        # okay test with a bad license name
        with TestClient(app) as client:
            response = client.post(url=f"/reference/add_license/{test_reference.new_ref_curie}/Made_Up_Name",
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

        # okay test with a bad reference name
        with TestClient(app) as client:
            response = client.post(url="/reference/add_license/MAdeUpRefCurie/l_name",
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_get_missing_files(self, auth_headers, test_reference, test_mod, test_referencefile): # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/reference/missing_files/{test_mod.new_mod_abbreviation}?filter=default&page=1&order_by=",
                                  headers=auth_headers)
            print(f"response.json -> {response.json()}")
            assert response.status_code == status.HTTP_200_OK
            print(response)
            assert response.json() == []

    @pytest.mark.webtest
    def test_add_pmid(self, auth_headers, test_mod, db): # noqa
        with TestClient(app) as client:
            new_mod_curie = test_mod.new_mod_abbreviation + ':test'
            new_pmid_add = {
                "pubmed_id": "12345",
                "mod_mca": test_mod.new_mod_abbreviation,
                "mod_curie": new_mod_curie
            }
            new_curie_response = client.post(url="/reference/add/", json=new_pmid_add, headers=auth_headers)
            # new_curie_response = client.post(url=f"/reference/add/12345/{test_mod.new_mod_abbreviation}:test/{test_mod.new_mod_abbreviation}/", headers=auth_headers)
            # new_curie_response = client.post(url="/reference/add/12345/0015_AtDB:test/0015_AtDB/", headers=auth_headers)
            new_curie = new_curie_response.text
            if new_curie.startswith('"') and new_curie.endswith('"'):
                new_curie = new_curie[1:-1]
            response = client.get(url=f"/reference/{new_curie}").json()
            assert response['mod_corpus_associations'][0]['mod_abbreviation'] == test_mod.new_mod_abbreviation
            xrefs_ok = 0
            for xref in response['cross_references']:
                if xref['curie'] == 'PMID:12345':
                    xrefs_ok = xrefs_ok + 1
                if xref['curie'] == test_mod.new_mod_abbreviation + ':test':
                    xrefs_ok = xrefs_ok + 1
            assert xrefs_ok == 2

    @pytest.mark.webtest
    def test_add_pmid_wb(self, auth_headers, test_mod, db): # noqa
        with TestClient(app) as client:
            new_mod = {
                "abbreviation": "WB",
                "short_name": "WB",
                "full_name": "WormBase"
            }
            response = client.post(url="/mod/", json=new_mod, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED

            new_pmid_add = {
                "pubmed_id": "12345",
                "mod_mca": "WB"
            }
            new_curie_response = client.post(url="/reference/add/", json=new_pmid_add, headers=auth_headers)
            new_curie = new_curie_response.text
            if new_curie.startswith('"') and new_curie.endswith('"'):
                new_curie = new_curie[1:-1]
            response = client.get(url=f"/reference/{new_curie}").json()
            assert response['mod_corpus_associations'][0]['mod_abbreviation'] == "WB"
            xrefs_ok = 0
            for xref in response['cross_references']:
                if xref['curie'] == 'PMID:12345':
                    xrefs_ok = xrefs_ok + 1
                if xref['curie'] == 'WB:WBPaper00000001':
                    xrefs_ok = xrefs_ok + 1
            assert xrefs_ok == 2
