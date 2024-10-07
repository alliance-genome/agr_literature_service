import copy
import datetime
import logging
from collections import namedtuple
import json
from typing import Dict, Tuple

import pytest
from sqlalchemy import text
from sqlalchemy_continuum import Operation
from starlette.testclient import TestClient
from fastapi import status
from unittest.mock import patch

from agr_literature_service.api.main import app
from agr_literature_service.api.models import ReferenceModel, AuthorModel, CrossReferenceModel
from agr_literature_service.api.schemas import ReferencefileSchemaPost
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from ..fixtures import load_workflow_parent_children_mock
from ..fixtures import db, populate_test_mod_reference_types # noqa
from .fixtures import auth_headers # noqa
from .test_resource import test_resource # noqa
from .test_mod import test_mod # noqa
from .test_copyright_license import test_copyright_license # noqa
from .test_topic_entity_tag_source import test_topic_entity_tag_source # noqa

from agr_literature_service.api.crud.referencefile_crud import create_metadata


logger = logging.getLogger(__name__)


CHECK_VALID_ATP_IDS_RETURN: Tuple[set, Dict[str, str]] = (
    {'ATP:0000005', 'ATP:0000009', 'ATP:0000068', 'ATP:0000071', 'ATP:0000079', 'ATP:0000082', 'ATP:0000084',
     'ATP:0000099', 'ATP:0000122', 'WB:WBGene00003001', 'NCBITaxon:6239'}, {})


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
            # get_response = client.get(url=f"/reference/{test_reference.new_ref_curie}")
            # assert get_response.status_code == status.HTTP_404_NOT_FOUND
            # delete_response = client.delete(url=f"/reference/{test_reference.new_ref_curie}", headers=auth_headers)
            # assert delete_response.status_code == status.HTTP_404_NOT_FOUND
