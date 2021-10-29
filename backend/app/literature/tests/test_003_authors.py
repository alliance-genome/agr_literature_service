import pytest
from literature.crud.author_crud import create, show, patch, show_changesets, destroy
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


def test_get_bad_author():

    with pytest.raises(HTTPException):
        show(db, 99999)


def test_create_author():
    xml = {
        "order": 1,
        "first_name": "string",
        "last_name": "string",
        "name": "003_TCU",
        "orcid": "AUT:BOB",
        "reference_curie": "AGR:AGR-Reference-0000000001"
    }
    res = create(db, xml)
    assert res
    # check db for author
    author = db.query(AuthorModel).filter(AuthorModel.name == "003_TCU").one()
    assert author.first_name == "string"
    assert author.reference.curie == "AGR:AGR-Reference-0000000001"
    assert author.orcid == "AUT:BOB"
    assert author.orcid_cross_reference.curie == "AUT:BOB"


def test_update_author():
    xml = {'first_name': "003_TUA",
           'reference_curie': 'AGR:AGR-Reference-0000000003',
           'orcid': "AUT:JANE"}
    author = db.query(AuthorModel).filter(AuthorModel.name == "003_TCU").one()
    res = patch(db, author.author_id, xml)
    assert res
    mod_author = db.query(AuthorModel).filter(AuthorModel.name == "003_TCU").one()
    assert author.author_id == mod_author.author_id
    assert mod_author.first_name == "003_TUA"
    print(mod_author.orcid_cross_reference)
    assert mod_author.orcid_cross_reference.curie == "AUT:JANE"


def test_show_author():
    author = db.query(AuthorModel).filter(AuthorModel.name == "003_TCU").one()
    res = show(db, author.author_id)
    assert res['orcid'] == "AUT:JANE"

    res = show_changesets(db, author.author_id)

    print("BOB: {}".format(res[0]))
    # Orcid changed from None -> AUT:BOB -> AUT:JANE
    for transaction in res:
        if not transaction['changeset']['orcid'][0]:
            assert transaction['changeset']['orcid'][1] == 'AUT:BOB'
        else:
            assert transaction['changeset']['orcid'][0] == 'AUT:BOB'
            assert transaction['changeset']['orcid'][1] == 'AUT:JANE'


def test_destroy_author():
    author = db.query(AuthorModel).filter(AuthorModel.name == "003_TCU").one()
    destroy(db, author.author_id)

    # It should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, author.author_id)

    # Deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, author.author_id)
