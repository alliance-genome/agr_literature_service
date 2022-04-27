import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from literature.crud.reference_crud import (create, destroy, patch, show,
                                            show_changesets)
from literature.database.config import SQLALCHEMY_DATABASE_URL
# from literature import models
from literature.database.base import Base
from literature.models import AuthorModel, CrossReferenceModel
from literature.schemas import ReferenceSchemaPost, ReferenceSchemaUpdate

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
db.execute('delete from editors')
db.execute('delete from "references"')
db.execute('delete from resources')


def test_get_bad_reference():

    with pytest.raises(HTTPException):
        show(db, "PMID:VQEVEQRVC")


def test_create_reference():
    reference = ReferenceSchemaPost(title="Bob", category="thesis", abstract="3", language="MadeUp")
    res = create(db, reference)
    assert res == 'AGR:AGR-Reference-0000000001'

    reference = ReferenceSchemaPost(title="Another Bob", category="thesis")
    res = create(db, reference)
    assert res == 'AGR:AGR-Reference-0000000002'

    # create again with same title, category
    # Apparently not a problem!!
    reference = ReferenceSchemaPost(title="Bob", category="thesis")
    res = create(db, reference)
    assert res == 'AGR:AGR-Reference-0000000003'

    # No title
    # ReferenceSchemaPost raises exception
    with pytest.raises(ValidationError):
        ReferenceSchemaPost(title=None, category="thesis")

    # blank title
    # ReferenceSchemaPost raises exception
    with pytest.raises(ValidationError):
        ReferenceSchemaPost(title="", category="thesis")


def test_show_reference():
    """Test show for reference."""

    # Lookup 1 we created earlier
    res = show(db, 'AGR:AGR-Reference-0000000001')
    assert res['title'] == "Bob"
    assert res['category'] == 'thesis'
    assert res['abstract'] == '3'

    # Lookup 1 that does not exist
    with pytest.raises(HTTPException):
        show(db, 'Does not exist')


def test_update_reference():

    # patch docs says it needs a ReferenceSchemaUpdate
    # but does not work with this.
    # with pytest.raises(AttributeError):
    update_schema = ReferenceSchemaUpdate(title="new title", category="book", language="New")
    patch(db, 'AGR:AGR-Reference-0000000001', update_schema)

    # res = patch(db, 'AGR:AGR-Reference-0000000001', {'title': "new title", 'category': "book"})
    # assert res == {'message': 'updated'}

    # fetch the new record.
    res = show(db, 'AGR:AGR-Reference-0000000001')

    # do we have the new title?
    assert res['title'] == "new title"

    # do we have the new title?
    assert res['category'] == "book"

    # language changed
    assert res['language'] == "New"

    # NOTE: abstract set to None as it was not in the update and
    #       schemaupdate sets all items not listed to default values.
    #       In this case abstract is None
    assert res['abstract'] is None


def test_changesets():
    res = show_changesets(db, 'AGR:AGR-Reference-0000000001')

    # title            : None -> bob -> 'new title'
    # catergory        : None -> thesis -> book
    for transaction in res:
        print(transaction)
        if not transaction['changeset']['title'][0]:
            assert transaction['changeset']['reference_id'][1] == 1
            assert transaction['changeset']['title'][1] == "Bob"
            assert transaction['changeset']['category'][1] == "thesis"
        else:
            assert transaction['changeset']['title'][1] == "new title"
            assert transaction['changeset']['category'][1] == "book"


def test_delete_Reference():
    destroy(db, 'AGR:AGR-Reference-0000000002')

    # It should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, "AGR:AGR-Reference-0000000002")

    # Deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, 'AGR:AGR-Reference-0000000002')


def test_reference_large():
    full_xml = {
        "category": "research_article",
        "abstract": "The Hippo (Hpo) pathway is a conserved tumor suppressor pathway",
        "authors": [
            {
                "order": 2,
                "first_name": "S.",
                "last_name": "Wu",
                "name": "S. Wu",
                # "reference_id": "PMID:23524264"
            },
            {
                "order": 1,
                "first_name": "D.",
                "last_name": "Wu",
                "name": "D. Wu",
                # "reference_id": "PMID:23524264"
            }
        ],
        "citation": "Wu and Wu, 2013, Biochem. Biophys. Res. Commun. 433(4): 538--541",
        "cross_references": [
            {
                "curie": "FB:FBrf0221304",
                "pages": [
                    "reference"
                ]
            }
        ],
        "issue_name": "4",
        "language": "English",
        "pages": "538--541",
        # "primary_id": "PMID:23524264",
        "title": "A conserved serine residue regulates the stability of Drosophila Salvador",
        "volume": "433",
        "open_access": True
    }

    # process the reference.
    reference = ReferenceSchemaPost(**full_xml)
    res = create(db, reference)
    assert res == 'AGR:AGR-Reference-0000000004'

    # fetch the new record.
    res = show(db, 'AGR:AGR-Reference-0000000004')
    assert res['abstract'] == 'The Hippo (Hpo) pathway is a conserved tumor suppressor pathway'
    assert res['category'] == 'research_article'

    # Not sure of order in array of the authors so:-
    assert len(res['authors']) == 2
    for author in res['authors']:
        if author['first_name'] == 'D.':
            assert author['name'] == 'D. Wu'
            assert author['order'] == 1
        else:
            assert author['name'] == 'S. Wu'
            assert author['order'] == 2

    # Were authors created in the db?
    author = db.query(AuthorModel).filter(AuthorModel.name == "D. Wu").one()
    assert author.first_name == 'D.'
    author = db.query(AuthorModel).filter(AuthorModel.name == "S. Wu").one()
    assert author.first_name == 'S.'

    assert "citation" not in res

    assert res['cross_references'][0]['curie'] == 'FB:FBrf0221304'
    # cross references in the db?
    xref = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == "FB:FBrf0221304").one()
    assert xref.reference.curie == 'AGR:AGR-Reference-0000000004'

    assert res["issue_name"] == "4"
    assert res["language"] == "English"
    assert res["pages"] == "538--541"
    assert res["title"] == "A conserved serine residue regulates the stability of Drosophila Salvador"
    assert res["volume"] == "433"
    assert res['open_access']
