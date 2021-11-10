import pytest
from literature.crud.editor_crud import create, show, patch, destroy, show_changesets
from sqlalchemy import create_engine
from sqlalchemy import MetaData

# from literature import models
from literature.models import (
    Base, EditorModel
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


def test_get_bad_editor():

    with pytest.raises(HTTPException):
        show(db, 99999)


def test_create_editor():
    xml = {
        "order": 1,
        "first_name": "string",
        "last_name": "string",
        "name": "003_TCU",
        "orcid": "ORCID:2345-2345-2345-234X",
        "reference_curie": "AGR:AGR-Reference-0000000001"
    }
    res = create(db, xml)
    assert res
    # check db for editor
    editor = db.query(EditorModel).filter(EditorModel.name == "003_TCU").one()
    assert editor.first_name == "string"


def test_create_editor_for_ref_later():
    xml = {
        "order": 2,
        "first_name": "string2",
        "last_name": "string3",
        "name": "Name2",
        "orcid": "ORCID:3333-4444-5555-666X",
        "reference_curie": "AGR:AGR-Reference-0000000001"
    }
    res = create(db, xml)
    assert res
    # check db for editor
    editor = db.query(EditorModel).filter(EditorModel.name == "Name2").one()
    assert editor.first_name == "string2"


def test_patch_editor():
    xml = {'first_name': "003_TUA",
           'orcid': "ORCID:5432-5432-5432-432X",
           'reference_curie': 'AGR:AGR-Reference-0000000003'}
    editor = db.query(EditorModel).filter(EditorModel.name == "003_TCU").one()
    res = patch(db, editor.editor_id, xml)
    assert res
    mod_editor = db.query(EditorModel).filter(EditorModel.name == "003_TCU").one()
    assert editor.editor_id == mod_editor.editor_id
    assert mod_editor.first_name == "003_TUA"


def test_show_editor():
    editor = db.query(EditorModel).filter(EditorModel.name == "003_TCU").one()
    edi = show(db, editor.editor_id)
    assert edi['orcid'] == "ORCID:5432-5432-5432-432X"


def test_changesets():
    editor = db.query(EditorModel).filter(EditorModel.name == "003_TCU").one()
    res = show_changesets(db, editor.editor_id)

    # Orcid changed from None -> ORCID:2345-2345-2345-234X -> ORCID:5432-5432-5432-432X
    for transaction in res:
        if not transaction['changeset']['orcid'][0]:
            assert transaction['changeset']['orcid'][1] == 'ORCID:2345-2345-2345-234X'
        else:
            assert transaction['changeset']['orcid'][0] == 'ORCID:2345-2345-2345-234X'
            assert transaction['changeset']['orcid'][1] == 'ORCID:5432-5432-5432-432X'


def test_destroy_editor():
    editor = db.query(EditorModel).filter(EditorModel.name == "003_TCU").one()
    destroy(db, editor.editor_id)

    # It should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, editor.editor_id)

    # Deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, editor.editor_id)
