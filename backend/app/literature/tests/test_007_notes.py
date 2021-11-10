import pytest
from literature.crud.note_crud import create, show, patch, destroy, show_changesets
from sqlalchemy import create_engine
from sqlalchemy import MetaData

from literature.models import (
    Base, NoteModel
)
from literature.schemas import NoteSchemaPost
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


def test_get_bad_note():
    with pytest.raises(HTTPException):
        show(db, 99999)


def test_create_note():
    xml = {'reference_curie': "AGR:AGR-Reference-0000000001",
           'name': "Name1",
           'note': "Note1"}
    note_schema = NoteSchemaPost(**xml)
    res = create(db, note_schema)
    assert res == 1

    # add note for reference test later (0013)
    xml = {'reference_curie': "AGR:AGR-Reference-0000000001",
           'name': "Name for ref test",
           'note': "Note for ref test"}
    note_schema = NoteSchemaPost(**xml)
    res = create(db, note_schema)
    assert res == 2


def test_show_note():
    res = show(db, 1)
    assert res['name'] == "Name1"
    assert res['note'] == "Note1"
    assert res['reference_curie'] == "AGR:AGR-Reference-0000000001"

    # and in the db
    note_obj = db.query(NoteModel).filter(NoteModel.name == "Name1").one()
    assert note_obj.reference.curie == "AGR:AGR-Reference-0000000001"
    assert note_obj.note == "Note1"


def test_patch_note():
    xml = {'name': "Name2",
           'note': "Note2",
           'reference_curie': "AGR:AGR-Reference-0000000003"}

    res = patch(db, 1, xml)
    assert res == {"message": "updated"}
    note_obj = db.query(NoteModel).filter(NoteModel.note_id == 1).one()
    assert note_obj.name == "Name2"
    assert note_obj.note == "Note2"
    assert note_obj.reference.curie == "AGR:AGR-Reference-0000000003"


def test_changesets():
    res = show_changesets(db, 1)

    # reference_curie : None -> 1 -> 3
    # name            : None -> Name1 -> Name2
    # note            : None -> Note1 -> Note2
    for transaction in res:
        print(transaction)
        if not transaction['changeset']['reference_id'][0]:
            assert transaction['changeset']['reference_id'][1] == 1
            assert transaction['changeset']['name'][1] == "Name1"
            assert transaction['changeset']['note'][1] == "Note1"
        else:
            assert transaction['changeset']['reference_id'][1] == 3
            assert transaction['changeset']['name'][1] == "Name2"
            assert transaction['changeset']['note'][1] == "Note2"


def test_destroy_note():
    destroy(db, 1)

    # It should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, 1)

    # Deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, 1)
