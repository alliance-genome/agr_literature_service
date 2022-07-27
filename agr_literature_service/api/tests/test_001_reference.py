import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from agr_literature_service.api.crud.reference_crud import (
    create, destroy, patch, show, show_changesets, update_citation)
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models import AuthorModel, CrossReferenceModel
from agr_literature_service.api.schemas import ReferenceSchemaPost, ReferenceSchemaUpdate

from agr_literature_service.api.crud.mod_crud import create as mod_create
from agr_literature_service.api.crud.user_crud import create as user_create

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

# Add tables/schema if not already there.
Base.metadata.create_all(engine)

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)

db.execute('delete from cross_reference')
db.execute('delete from author')
db.execute('delete from editor')
db.execute('delete from "reference"')
db.execute('delete from resource')


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

    # Update the citation
    update_citation(db, 'AGR:AGR-Reference-0000000001')

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

    # Do we have a new citation
    assert res['citation'] == ", () new title.   (): "


def test_changesets():
    res = show_changesets(db, 'AGR:AGR-Reference-0000000001')

    # title            : None -> bob -> 'new title'
    # catergory        : None -> thesis -> book
    for i, transaction in enumerate(res):
        print(transaction)
        if i == 0:
            assert transaction['changeset']['reference_id'][1] == 1
            assert transaction['changeset']['title'][1] == "Bob"
            assert transaction['changeset']['category'][1] == "thesis"
        elif i == 1:
            assert transaction['changeset']['title'][1] == "new title"
            assert transaction['changeset']['category'][1] == "book"
        else:
            assert transaction['changeset']['citation'][0] == ", () Bob.   (): "
            assert transaction['changeset']['citation'][1] == ", () new title.   (): "


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
        "mesh_terms": [
            {
                "heading_term": "hterm",
                "qualifier_term": "qterm"
            }
        ],
        "mod_reference_types": [
            {
                "reference_type": "mrt_rt",
                "source": "mrt_s"
            }
        ],
        "cross_references": [
            {
                "curie": "FB:FBrf0221304",
                "pages": [
                    "reference"
                ]
            }
        ],
        "ontologies": [
            {
                "workflow_tag_id": "workflow_tag1",
                "mod_abbreviation": "RGD_ont",
                "created_by": "Bob"
            },
            {
                "workflow_tag_id": "workflow_tag2",
                "mod_abbreviation": "FB_ont",
                "created_by": "Bob"
            }
        ],
        "issue_name": "4",
        "language": "English",
        "page_range": "538--541",
        "title": "Some test 001 title",
        "volume": "433",
        "open_access": True
    }
    # add User "Bob"
    user_create(db, "Bob")

    # add mods
    data = {
        "abbreviation": 'FB_ont',
        "short_name": "FlyBase",
        "full_name": "Test genome database ont1"
    }
    res = mod_create(db, data)

    data = {
        "abbreviation": 'RGD_ont',
        "short_name": "Rat",
        "full_name": "Test genome database ont2"
    }
    res = mod_create(db, data)

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

    assert res['citation'] == "D. Wu; S. Wu, () Some test 001 title.  433 (): 538--541"

    assert res['cross_references'][0]['curie'] == 'FB:FBrf0221304'

    assert res['mod_reference_types'][0]['reference_type'] == "mrt_rt"

    assert res['mesh_terms'][0]['heading_term'] == "hterm"

    # cross references in the db?
    xref = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == "FB:FBrf0221304").one()
    assert xref.reference.curie == 'AGR:AGR-Reference-0000000004'

    assert res["issue_name"] == "4"
    assert res["language"] == "English"
    assert res["page_range"] == "538--541"
    assert res["title"] == "Some test 001 title"
    assert res["volume"] == "433"
    assert res['open_access']

    print("BOB................")
    print(res)
    for ont in res["ontologies"]:
        if ont['mod_abbreviation'] == "RGD_ont":
            assert ont['workflow_tag_id'] == "workflow_tag1"
        elif ont['mod_abbreviation'] == "FB_ont":
            assert ont['workflow_tag_id'] == "workflow_tag2"
        else:
            assert 1 == 0  # Not RGD or FB ?
