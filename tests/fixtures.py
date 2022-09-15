import pytest
from agr_literature_service.api.models import initialize
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from os import environ


def delete_all_table_content(engine):
    if environ.get('TEST_DATABASE_DELETE') == "true":
        for table in reversed(Base.metadata.sorted_tables):
            if table != "users":
                engine.execute(table.delete())


@pytest.fixture()
def db() -> Session:
    print("***** Creating DB connection *****")
    engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
    initialize()
    delete_all_table_content(engine)
    db = sessionmaker(bind=engine, autoflush=True)()
    yield db
    print("***** Deleting DB connection *****")
    delete_all_table_content(engine)
