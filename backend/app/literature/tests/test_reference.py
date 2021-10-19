import pytest
from literature.crud.reference_crud import create, show, patch
from sqlalchemy import create_engine
from sqlalchemy import MetaData


from literature.database.config import SQLALCHEMY_DATABASE_URL
from literature.schemas import ReferenceSchemaPost, ReferenceSchemaUpdate
from sqlalchemy.orm import sessionmaker
from fastapi import HTTPException
from pydantic import ValidationError
metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

# Exit if this is not a test database, Exit.
if "-test-" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)

db.execute('delete from "references"')


def test_get_bad_reference():

    with pytest.raises(HTTPException):
        show(db, "PMID:VQEVEQRVC")


def test_create_reference():
    reference = ReferenceSchemaPost(title="Bob", category="thesis", abstract="3")
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
    with pytest.raises(AttributeError):
        update_schema = ReferenceSchemaUpdate(title="Changed", category="thesis")
        patch(db, 'AGR:AGR-Reference-0000000001', update_schema)

    res = patch(db, 'AGR:AGR-Reference-0000000001', {'title': "new title"})
    assert res == {'message': 'updated'}

    # fetch the new record.
    res = show(db, 'AGR:AGR-Reference-0000000001')

    # do we have the new title?
    assert res['title'] == "new title"

    # abstract should still be there
    assert res['abstract'] == '3'
