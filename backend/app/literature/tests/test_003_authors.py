import pytest
from literature.crud.author_crud import create, show, patch
from sqlalchemy import create_engine
from sqlalchemy import MetaData

# from literature import models
from literature.models import (
    Base, AuthorModel
)

from literature.database.config import SQLALCHEMY_DATABASE_URL
from sqlalchemy.orm import sessionmaker
from fastapi import HTTPException
metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

# Add tables/schema if not already there.
Base.metadata.create_all(engine)

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)

db.execute('delete from cross_references')
db.execute('delete from authors')
db.execute('delete from "references"')


def test_get_bad_author():

    with pytest.raises(HTTPException):
        show(db, 99999)


def test_create_author():
    xml = {
        "order": 1,
        "first_name": "string",
        "last_name": "string",
        "name": "003_TCU",
        "orcid": "BOB",
        "reference_curie": "AGR:AGR-Reference-0000000001"
    }
    res = create(db, xml)
    assert res
    # check db for author
    author = db.query(AuthorModel).filter(AuthorModel.name == "003_TCU").one()
    assert author.first_name == "string"


def test_update_author():
    xml = {'first_name': "003_TUA",
           'reference_curie': 'AGR:AGR-Reference-0000000003'}
    author = db.query(AuthorModel).filter(AuthorModel.name == "003_TCU").one()
    res = patch(db, author.author_id, xml)
    assert res
    mod_author = db.query(AuthorModel).filter(AuthorModel.name == "003_TCU").one()
    assert author.author_id == mod_author.author_id
    assert mod_author.first_name == "003_TUA"
