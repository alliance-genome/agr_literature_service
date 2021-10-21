import pytest
from literature.crud.resource_crud import create, show, patch, destroy
from sqlalchemy import create_engine
from sqlalchemy import MetaData


from literature.database.config import SQLALCHEMY_DATABASE_URL
from literature.schemas import ResourceSchemaPost, ResourceSchemaUpdate
from sqlalchemy.orm import sessionmaker
from fastapi import HTTPException
from pydantic import ValidationError
metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)

db.execute('delete from "editors"')
db.execute('delete from "cross_references"')
db.execute('delete from "resources"')


def test_get_bad_Resource():

    with pytest.raises(HTTPException):
        show(db, "PMID:VQEVEQRVC")


def test_create_Resource():
    Resource = ResourceSchemaPost(title="Bob", abstract="3")
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

    # patch docs says it needs a ResourceSchemaUpdate
    # but does not work with this.
    with pytest.raises(AttributeError):
        update_schema = ResourceSchemaUpdate(title="Changed")
        patch(db, 'AGR:AGR-Resource-0000000001', update_schema)

    res = patch(db, 'AGR:AGR-Resource-0000000001', {'title': "new title"})
    assert res == {'message': 'updated'}

    # fetch the new record.
    res = show(db, 'AGR:AGR-Resource-0000000001')

    # do we have the new title?
    assert res['title'] == "new title"

    # abstract should still be there
    assert res['abstract'] == '3'


def test_delete_Resource():
    destroy(db, 'AGR:AGR-Resource-0000000002')

    # It should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, "AGR:AGR-Resource-0000000002")

    # Deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, 'AGR:AGR-Resource-0000000002')
