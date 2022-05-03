import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from literature.crud.resource_crud import create, destroy, patch, show
from literature.database.config import SQLALCHEMY_DATABASE_URL
from literature.models import ResourceModel
from literature.schemas import ResourceSchemaPost, ResourceSchemaUpdate

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)


def test_get_bad_Resource():

    with pytest.raises(HTTPException):
        show(db, "PMID:VQEVEQRVC")


def test_create_Resource():
    Resource = ResourceSchemaPost(title="Bob", abstract="3", open_access=True)
    res = create(db, Resource)
    assert res == 'AGR:AGR-Resource-0000000001'

    Resource = ResourceSchemaPost(title="Another Bob")
    res = create(db, Resource)
    assert res == 'AGR:AGR-Resource-0000000002'

    # create again with same title, category
    # Apparently not a problem!!
    Resource = ResourceSchemaPost(title="Bob")
    res = create(db, Resource)
    assert res == 'AGR:AGR-Resource-0000000003'

    # No title
    # ResourceSchemaPost raises exception
    with pytest.raises(ValidationError):
        ResourceSchemaPost(title=None)

    # blank title
    # ResourceSchemaPost raises exception
    with pytest.raises(ValidationError):
        ResourceSchemaPost(title="")


def test_show_Resource():
    """Test show for Resource."""

    # Lookup 1 we created earlier
    res = show(db, 'AGR:AGR-Resource-0000000001')
    assert res['title'] == "Bob"
    assert res['abstract'] == '3'

    # Lookup 1 that does not exist
    with pytest.raises(HTTPException):
        show(db, 'Does not exist')


def test_update_Resource():

    update_schema = ResourceSchemaUpdate(title="new title")
    res = patch(db, 'AGR:AGR-Resource-0000000001', update_schema)

    assert res == {'message': 'updated'}

    # fetch the new record.
    res = show(db, 'AGR:AGR-Resource-0000000001')

    # do we have the new title?
    assert res['title'] == "new title"

    # NOTE: abstract set to None as it was not in the update and
    #       schemaupdate sets all items not listed to default values.
    #       In this case abstract is None
    assert res['abstract'] is None


def test_resource_create_large():
    xml = {
        "abbreviation_synonyms": ["Jackson, Mathews, Wickens, 1996"],
        "cross_references": [
            {
                "curie": "FB:FBrf0044885",
                "pages": [
                    "something"
                ]
            }
        ],
        "editors": [
            {
                "order": 1,
                "first_name": "R.J.",
                "last_name": "Jackson",
                "name": "R.J. Jackson"
            },
            {
                "order": 2,
                "first_name": "M.",
                "last_name": "Mathews",
                "name": "M. Mathews"
            },
            {
                "order": 3,
                "first_name": "M.P.",
                "last_name": "Wickens",
                "name": "M.P. Wickens"
            }],
        "pages": "lxi + 351pp",
        "title": "Abstracts of papers presented at the 1996 meeting"
    }
    # process the resource
    resource = ResourceSchemaPost(**xml)
    res = create(db, resource)
    assert res == 'AGR:AGR-Resource-0000000004'

    # fetch the new record.
    res = show(db, 'AGR:AGR-Resource-0000000004')

    assert res['cross_references'][0]['curie'] == "FB:FBrf0044885"

    # Not sure of order in array of the editors so:-
    assert len(res['editors']) == 3
    for editor in res['editors']:
        if editor['order'] == '1':
            assert editor["first_name"] == "R.J."
            assert editor["last_name"] == "Jackson"
            assert editor["name"] == "R.J. Jackson"
        elif editor['order'] == '3':
            assert editor["first_name"] == "Wickens"
            assert editor["last_name"] == "Jackson"
            assert editor["name"] == "M.P. Wickens"
    assert res['title'] == "Abstracts of papers presented at the 1996 meeting"
    assert res['pages'] == "lxi + 351pp"
    assert res["abbreviation_synonyms"][0] == "Jackson, Mathews, Wickens, 1996"
    assert not res['open_access']

    res = db.query(ResourceModel).filter(ResourceModel.curie == 'AGR:AGR-Resource-0000000004').one()
    assert res.title == "Abstracts of papers presented at the 1996 meeting"
    assert len(res.editor) == 3
    # open access defaults to False
    assert not res.open_access

    assert len(res.cross_reference) == 1


def test_delete_Resource():
    destroy(db, 'AGR:AGR-Resource-0000000002')

    # It should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, "AGR:AGR-Resource-0000000002")

    # Deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, 'AGR:AGR-Resource-0000000002')
