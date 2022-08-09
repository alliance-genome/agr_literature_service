import pytest
from fastapi import HTTPException
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from agr_literature_service.api.crud.author_crud import (
    create, destroy, patch, show, show_changesets)
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models import AuthorModel
from agr_literature_service.api.tests import utils

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

# Add tables/schema if not already there.
Base.metadata.create_all(engine)

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)


(refs, ress, mods) = utils.initialise(db, '003')


def test_get_bad_author():

    with pytest.raises(HTTPException):
        show(db, 99999)


def test_create_author():
    global refs
    xml = {
        "order": 1,
        "first_name": "string",
        "last_name": "string",
        "name": "003_TCU",
        "orcid": "ORCID:1234-1234-1234-123X",
        "reference_curie": refs[0]
    }
    # auth_schema = AuthorSchemaPost(**xml)
    res = create(db, xml)
    assert res
    # check db for author
    author = db.query(AuthorModel).filter(AuthorModel.name == "003_TCU").one()
    assert author.first_name == "string"
    assert author.reference.curie == refs[0]
    assert author.orcid == "ORCID:1234-1234-1234-123X"
    assert author.orcid_cross_reference.curie == "ORCID:1234-1234-1234-123X"


def test_update_author():
    global refs
    xml = {'first_name': "003_TUA",
           'reference_curie': refs[1],
           'orcid': "ORCID:4321-4321-4321-321X"}
    author = db.query(AuthorModel).filter(AuthorModel.name == "003_TCU").one()
    res = patch(db, author.author_id, xml)
    assert res
    mod_author = db.query(AuthorModel).filter(AuthorModel.name == "003_TCU").one()
    assert author.author_id == mod_author.author_id
    assert mod_author.first_name == "003_TUA"
    print(mod_author.orcid_cross_reference)
    assert mod_author.orcid_cross_reference.curie == "ORCID:4321-4321-4321-321X"


def test_show_author():
    author = db.query(AuthorModel).filter(AuthorModel.name == "003_TCU").one()
    res = show(db, author.author_id)
    assert res['orcid']['curie'] == "ORCID:4321-4321-4321-321X"

    res = show_changesets(db, author.author_id)

    # Orcid changed from None -> ORCID:1234-1234-1234-123X -> ORCID:4321-4321-4321-321X
    for transaction in res:
        if not transaction['changeset']['orcid'][0]:
            assert transaction['changeset']['orcid'][1] == 'ORCID:1234-1234-1234-123X'
        else:
            assert transaction['changeset']['orcid'][0] == 'ORCID:1234-1234-1234-123X'
            assert transaction['changeset']['orcid'][1] == 'ORCID:4321-4321-4321-321X'


def test_destroy_author():
    author = db.query(AuthorModel).filter(AuthorModel.name == "003_TCU").one()
    destroy(db, author.author_id)

    # It should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, author.author_id)

    # Deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, author.author_id)
