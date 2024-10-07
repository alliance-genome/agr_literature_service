from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import EditorModel
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from .test_resource import test_resource # noqa

TestEditorData = namedtuple('TestEditorData', ['response', 'new_editor_id', 'related_resource_curie'])


@pytest.fixture
def test_editor(db, auth_headers, test_resource): # noqa
    print("***** Adding a test editor *****")
    with TestClient(app) as client:
        new_editor = {
            "order": 1,
            "first_name": "string",
            "last_name": "string",
            "name": "003_TCU",
            "orcid": "ORCID:2345-2345-2345-234X",
            "resource_curie": test_resource.new_resource_curie
        }
        response = client.post(url="/editor/", json=new_editor, headers=auth_headers)
        yield TestEditorData(response, response.json(), test_resource.new_resource_curie)


class TestEditor:

    def test_get_bad_editor(self):
        with TestClient(app) as client:
            response = client.get(url="/editor/-1")
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_create_editor(self, db, test_editor): # noqa
        assert test_editor.response.status_code == status.HTTP_201_CREATED
        # check db for editor
        editor = db.query(EditorModel).filter(EditorModel.name == "003_TCU").one()
        assert editor.first_name == "string"

    def test_create_editor_for_ref_later(self, db, test_editor, auth_headers): # noqa
        with TestClient(app) as client:
            xml = {
                "order": 2,
                "first_name": "string2",
                "last_name": "string3",
                "name": "Name2",
                "orcid": "ORCID:3333-4444-5555-666X",
                "resource_curie": test_editor.related_resource_curie
            }
            response = client.post(url="/editor/", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            # check db for editor
            editor = db.query(EditorModel).filter(EditorModel.name == "Name2").one()
            assert editor.first_name == "string2"

    def test_patch_editor(self, db, test_editor, auth_headers): # noqa
        with TestClient(app) as client:
            xml = {'first_name': "003_TUA",
                   'orcid': "ORCID:5432-5432-5432-432X",
                   "resource_curie": test_editor.related_resource_curie
                   }
            response = client.patch(url=f"/editor/{test_editor.new_editor_id}", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED
            mod_editor = db.query(EditorModel).filter(EditorModel.first_name == "003_TUA").one()
            assert mod_editor.orcid == "ORCID:5432-5432-5432-432X"

            # test changeset
            response = client.get(url=f"/editor/{test_editor.new_editor_id}/versions")

            # Orcid changed from None -> ORCID:2345-2345-2345-234X -> ORCID:5432-5432-5432-432X
            transactions = response.json()
            assert transactions[0]['changeset']['orcid'][1] == 'ORCID:2345-2345-2345-234X'
            assert transactions[1]['changeset']['orcid'][0] == 'ORCID:2345-2345-2345-234X'
            assert transactions[1]['changeset']['orcid'][1] == 'ORCID:5432-5432-5432-432X'

    def test_show_editor(self, test_editor): # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/editor/{test_editor.new_editor_id}")
            assert response.status_code == status.HTTP_200_OK

            resp_data = response.json()
            assert "name" in resp_data
            assert resp_data['name'] == '003_TCU'
            assert resp_data['orcid'] == "ORCID:2345-2345-2345-234X"

    def test_destroy_editor(self, test_editor, auth_headers): # noqa
        with TestClient(app) as client:
            response = client.delete(url=f"/editor/{test_editor.new_editor_id}", headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT
            # It should now give an error on lookup.
            response = client.get(url=f"/editor/{test_editor.new_editor_id}")
            assert response.status_code == status.HTTP_404_NOT_FOUND

            # Deleting it again should give an error as the lookup will fail.
            response = client.delete(url=f"/editor/{test_editor.new_editor_id}", headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND
