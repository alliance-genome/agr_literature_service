from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import AuthorModel
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from .test_reference import test_reference # noqa

AuthorTestData = namedtuple('AuthorTestData', ['response', 'new_author_id', 'related_ref_curie'])


@pytest.fixture
def test_author(db, auth_headers, test_reference): # noqa
    print("***** Adding a test author *****")
    with TestClient(app) as client:
        new_author = {
            "order": 1,
            "first_name": "string",
            "last_name": "string",
            "first_initial": "FI",
            "name": "003_TCU",
            "orcid": "ORCID:1234-1234-1234-123X",
            "reference_curie": test_reference.new_ref_curie
        }
        response = client.post(url="/author/", json=new_author, headers=auth_headers)
        yield AuthorTestData(response, response.json(), test_reference.new_ref_curie)


class TestAuthor:

    def test_get_bad_author(self):
        with TestClient(app) as client:
            response = client.get(url=f"/author/{-1}")
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_create_author(self, db, test_author): # noqa
        assert test_author.response.status_code == status.HTTP_201_CREATED
        # check db for author
        author = db.query(AuthorModel).filter(AuthorModel.name == "003_TCU").one()
        assert author.first_name == "string"
        assert author.first_initial == "FI"
        assert author.reference.curie == test_author.related_ref_curie
        assert author.orcid == "ORCID:1234-1234-1234-123X"

    def test_update_author(self, db, auth_headers, test_author): # noqa
        with TestClient(app) as client:
            xml = {'first_name': "003_TUA",
                   'reference_curie': test_author.related_ref_curie,
                   'orcid': "ORCID:4321-4321-4321-321X"}
            author = db.query(AuthorModel).filter(AuthorModel.name == "003_TCU").one()
            response = client.patch(url=f"/author/{author.author_id}", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED
            mod_author = client.get(url=f"/author/{author.author_id}").json()
            assert author.author_id == mod_author["author_id"]
            assert mod_author["first_name"] == "003_TUA"
            assert mod_author["orcid"] == "ORCID:4321-4321-4321-321X"
            res = client.get(url=f"/author/{test_author.new_author_id}/versions").json()
            # Orcid changed from None -> ORCID:1234-1234-1234-123X -> ORCID:4321-4321-4321-321X
            for transaction in res:
                if not transaction['changeset']['orcid'][0]:
                    assert transaction['changeset']['orcid'][1] == 'ORCID:1234-1234-1234-123X'
                else:
                    assert transaction['changeset']['orcid'][0] == 'ORCID:1234-1234-1234-123X'
                    assert transaction['changeset']['orcid'][1] == 'ORCID:4321-4321-4321-321X'

    def test_show_author(self, test_author): # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/author/{test_author.new_author_id}")
            assert response.json()['orcid'] == "ORCID:1234-1234-1234-123X"

    def test_destroy_author(self, test_author, auth_headers): # noqa
        with TestClient(app) as client:
            response = client.delete(url=f"/author/{test_author.new_author_id}", headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT
            # It should now give an error on lookup.
            response = client.get(url=f"/author/{test_author.new_author_id}")
            assert response.status_code == status.HTTP_404_NOT_FOUND
            # Deleting it again should give an error as the lookup will fail.
            response = client.delete(url=f"/author/{test_author.new_author_id}", headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND
