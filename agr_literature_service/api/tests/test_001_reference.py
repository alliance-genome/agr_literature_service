import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from agr_literature_service.api.crud.reference_crud import (
    create, destroy, patch, show, show_changesets, update_citation)
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models import AuthorModel, CrossReferenceModel, ReferenceModel
from agr_literature_service.api.schemas import ReferenceSchemaPost, ReferenceSchemaUpdate

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


(refs, ress, mods) = utils.initialise(db, '001')


def test_get_bad_reference():

    with pytest.raises(HTTPException):
        show(db, "PMID:VQEVEQRVC")


def test_create_reference():
    reference = ReferenceSchemaPost(title="Bob", category="thesis", abstract="3", language="MadeUp")
    curie = create(db, reference)
    ref = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).one()
    assert ref.title == 'Bob'
    assert ref.date_created is not None
    # Okat so we are adding an update here on creation!!
    assert ref.date_updated is not None

    # create again with same title, category
    # Apparently not a problem!!
    reference = ReferenceSchemaPost(title="Bob", category="thesis")
    curie = create(db, reference)
    ref = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).one()
    assert ref.title == 'Bob'
    assert ref.date_created is not None
    assert ref.date_updated is not None

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
    global refs

    # Lookup 1 we created earlier
    res = show(db, refs[0])
    assert res['title'] == "Bob 001 1"
    assert res['category'] == 'thesis'
    assert res['abstract'] == '3'

    # Lookup 1 that does not exist
    with pytest.raises(HTTPException):
        show(db, 'Does not exist')


def test_update_reference():
    global refs

    # patch docs says it needs a ReferenceSchemaUpdate
    # but does not work with this.
    # with pytest.raises(AttributeError):
    update_schema = ReferenceSchemaUpdate(title="new title", category="book", language="New")
    patch(db, refs[0], update_schema)

    # Update the citation
    update_citation(db, refs[0])

    # fetch the new record.
    res = show(db, refs[0])

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
    res = show_changesets(db, refs[0])

    # title            : None -> bob -> 'new title'
    # catergory        : None -> thesis -> book
    for i, transaction in enumerate(res):
        print(transaction)
        if i == 0:
            assert transaction['changeset']['reference_id'][1] == 1
            assert transaction['changeset']['title'][1] == "Bob 001 1"
            assert transaction['changeset']['category'][1] == "thesis"
        elif i == 1:
            assert transaction['changeset']['title'][1] == "new title"
            assert transaction['changeset']['category'][1] == "book"
        else:
            assert transaction['changeset']['citation'][0] == ", () Bob 001 1.   (): "
            assert transaction['changeset']['citation'][1] == ", () new title.   (): "


def test_delete_Reference():
    global refs

    destroy(db, refs[1])

    # It should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, refs[1])

    # Deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, refs[1])


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
        "workflow_tags": [
            {
                "workflow_tag_id": "workflow_tag1",
                "mod_abbreviation": "001_FB",
                "created_by": "001_Bob"
            },
            {
                "workflow_tag_id": "workflow_tag2",
                "mod_abbreviation": "001_RGD",
                "created_by": "001_Bob"
            }
        ],
        "topic_entity_tags": [
            {
                "topic": "string",
                "entity_type": "string",
                "alliance_entity": "string",
                "taxon": "string",
                "note": "string"
            }
        ],
        "issue_name": "4",
        "language": "English",
        "page_range": "538--541",
        "title": "Some test 001 title",
        "volume": "433",
        "open_access": True
    }

    # process the reference.
    reference = ReferenceSchemaPost(**full_xml)
    curie = create(db, reference)

    # fetch the new record.
    res = show(db, curie)
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
    assert xref.reference.curie == curie

    assert res["issue_name"] == "4"
    assert res["language"] == "English"
    assert res["page_range"] == "538--541"
    assert res["title"] == "Some test 001 title"
    assert res["volume"] == "433"
    assert res['open_access']

    print("BOB................")
    print(res)
    for ont in res["workflow_tags"]:
        if ont['mod_abbreviation'] == "001_RGD":
            assert ont['workflow_tag_id'] == "workflow_tag2"
        elif ont['mod_abbreviation'] == "001_FB":
            assert ont['workflow_tag_id'] == "workflow_tag1"
        else:
            assert 1 == 0  # Not RGD or FB ?
