import pytest
from fastapi import HTTPException
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from agr_literature_service.api.crud.editor_crud import (create, destroy, patch, show,
                                         show_changesets)
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
# from agr_literature_service.api import models
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models import EditorModel
from agr_literature_service.api.schemas import EditorSchemaPost

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

    ed_schem = EditorSchemaPost(**xml)
    res = patch(db, editor.editor_id, ed_schem)
    assert res
    mod_editor = db.query(EditorModel).filter(EditorModel.first_name == "003_TUA").one()
    assert editor.editor_id == mod_editor.editor_id
    assert mod_editor.orcid == "ORCID:5432-5432-5432-432X"


def test_show_editor():
    editor = db.query(EditorModel).filter(EditorModel.first_name == "003_TUA").one()
    edi = show(db, editor.editor_id)
    assert edi['orcid'] == "ORCID:5432-5432-5432-432X"


def test_changesets():
    editor = db.query(EditorModel).filter(EditorModel.first_name == "003_TUA").one()
    res = show_changesets(db, editor.editor_id)

    # Orcid changed from None -> ORCID:2345-2345-2345-234X -> ORCID:5432-5432-5432-432X
    for transaction in res:
        if not transaction['changeset']['orcid'][0]:
            assert transaction['changeset']['orcid'][1] == 'ORCID:2345-2345-2345-234X'
        else:
            assert transaction['changeset']['orcid'][0] == 'ORCID:2345-2345-2345-234X'
            assert transaction['changeset']['orcid'][1] == 'ORCID:5432-5432-5432-432X'


def test_destroy_editor():
    editor = db.query(EditorModel).filter(EditorModel.first_name == "003_TUA").one()
    destroy(db, editor.editor_id)

    # It should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, editor.editor_id)

    # Deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, editor.editor_id)
