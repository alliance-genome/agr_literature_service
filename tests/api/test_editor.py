import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import EditorModel
from .fixtures import auth_headers, db # noqa
from .test_reference import create_test_reference # noqa


@pytest.fixture
def create_test_editor(auth_headers, create_test_reference): # noqa
    print("***** Adding a test editor *****")
    with TestClient(app) as client:
        new_editor = {
            "order": 1,
            "first_name": "string",
            "last_name": "string",
            "name": "003_TCU",
            "orcid": "ORCID:2345-2345-2345-234X",
            "reference_curie": create_test_reference.json()
        }
        response = client.post(url="/editor/", json=new_editor, headers=auth_headers)
        yield response, create_test_reference.json()


class TestEditor:

    def test_get_bad_editor(self):
        with TestClient(app) as client:
            response = client.get(url="/editor/-1")
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_create_editor(self, db, create_test_editor): # noqa
        assert create_test_editor[0].status_code == status.HTTP_201_CREATED
        # check db for editor
        editor = db.query(EditorModel).filter(EditorModel.name == "003_TCU").one()
        assert editor.first_name == "string"

    def test_create_editor_for_ref_later(self, db, create_test_reference, auth_headers): # noqa
        with TestClient(app) as client:
            xml = {
                "order": 2,
                "first_name": "string2",
                "last_name": "string3",
                "name": "Name2",
                "orcid": "ORCID:3333-4444-5555-666X",
                "reference_curie": create_test_reference.json()
            }
            response = client.post(url="/editor/", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            # check db for editor
            editor = db.query(EditorModel).filter(EditorModel.name == "Name2").one()
            assert editor.first_name == "string2"

    def test_patch_editor(self, db, create_test_editor, auth_headers): # noqa
        with TestClient(app) as client:
            xml = {'first_name': "003_TUA",
                   'orcid': "ORCID:5432-5432-5432-432X",
                   "reference_curie": create_test_editor[1]
                   }
            response = client.patch(url=f"/editor/{create_test_editor[0].json()}", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED
            mod_editor = db.query(EditorModel).filter(EditorModel.first_name == "003_TUA").one()
            assert mod_editor.orcid == "ORCID:5432-5432-5432-432X"

            # test changeset
            response = client.get(url=f"/editor/{create_test_editor[0].json()}/versions")

            # Orcid changed from None -> ORCID:2345-2345-2345-234X -> ORCID:5432-5432-5432-432X
            for transaction in response.json():
                if not transaction['changeset']['orcid'][0]:
                    assert transaction['changeset']['orcid'][1] == 'ORCID:2345-2345-2345-234X'
                else:
                    assert transaction['changeset']['orcid'][0] == 'ORCID:2345-2345-2345-234X'
                    assert transaction['changeset']['orcid'][1] == 'ORCID:5432-5432-5432-432X'

    def test_show_editor(self, create_test_editor): # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/editor/{create_test_editor[0].json()}")
            assert response.json()['orcid'] == "ORCID:2345-2345-2345-234X"

    def test_destroy_editor(self, create_test_editor, auth_headers): # noqa
        with TestClient(app) as client:
            response = client.delete(url=f"/editor/{create_test_editor[0].json()}", headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT
            # It should now give an error on lookup.
            response = client.get(url=f"/editor/{create_test_editor[0].json()}")
            assert response.status_code == status.HTTP_404_NOT_FOUND

            # Deleting it again should give an error as the lookup will fail.
            response = client.delete(url=f"/editor/{create_test_editor[0].json()}", headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND
