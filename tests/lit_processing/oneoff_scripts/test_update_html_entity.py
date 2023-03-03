from ...fixtures import db # noqa
from tests.api.fixtures import auth_headers # noqa
from agr_literature_service.api.models import ReferenceModel, AuthorModel
from agr_literature_service.lit_processing.oneoff_scripts.update_reference_htmlentity import update_author_entity
from starlette.testclient import TestClient
from agr_literature_service.api.main import app
from fastapi import status


class TestUpdateHtmlEntity:

    def test_update_author_html_entity(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            new_reference = {
                "title": "Bob",
                "category": "thesis"
            }
            response = client.post(url="/reference/", json=new_reference, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            reference_db_obj = db.query(ReferenceModel).first()
            reference_id = reference_db_obj.reference_id
            new_author = {
                "order": 1,
                "first_name": "Nem&#x10d;ovi&#x10d;ov&#xe1;",
                "last_name": "in &amp;lt;i&amp;gt;Drosophila&amp;lt;/i&amp;gt;",
                "name": "tcf21<sup>+</sup>&#x3b1;-mannosidase<a href='/locus/S000004219'>Cdc42</a>",
                "orcid": "ORCID:1234-1234-1234-123X",
                "reference_curie": reference_db_obj.curie
            }
            response = client.post(url="/author/", json=new_author, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            update_author_entity()
            author_db_obj = db.query(AuthorModel).filter(
                AuthorModel.reference_id == reference_id).first()
            assert author_db_obj.first_name == "Nemčovičová"
            assert author_db_obj.last_name == "in &lt;i&gt;Drosophila&lt;/i&gt;"
            assert author_db_obj.name == "tcf21<sup>+</sup>α-mannosidase<a href='/locus/S000004219'>Cdc42</a>"
