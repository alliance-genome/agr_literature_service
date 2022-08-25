import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.main import app
from agr_literature_service.api.models import ReferenceModel
from agr_literature_service.api.schemas import ReferenceSchemaPost
from agr_literature_service.lit_processing.helper_post_to_api import get_authentication_token, generate_headers


@pytest.fixture(scope='module')
def db() -> Session:
    print("***** Initializing DB *****")
    engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
    db = sessionmaker(bind=engine, autoflush=True)()
    yield db
    print("***** Deleting DB connection *****")
    # this happens when tests are done


@pytest.fixture(scope="session")
def auth_headers():
    print("***** Generating Okta token *****")
    yield generate_headers(get_authentication_token())


class TestReference:

    def test_reference_create(self, db, auth_headers):
        with TestClient(app) as client:
            new_reference = {
                "title": "Bob",
                "category": "thesis",
                "abstract": "3",
                "language": "MadeUp"
                }
            response = client.post(url="/reference/", json=new_reference, headers=auth_headers)
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
