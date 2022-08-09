import pytest
from fastapi import HTTPException
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from agr_literature_service.api.crud.editor_crud import (
    create, destroy, patch, show, show_changesets)
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models import EditorModel
from agr_literature_service.api.schemas import EditorSchemaPost
from agr_literature_service.api.schemas import ReferenceSchemaPost
from agr_literature_service.api.crud.mod_crud import create as mod_create
from agr_literature_service.api.crud.user_crud import create as user_create
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.api.crud.reference_crud import (
    create as reference_create)

fb_mod = None
refs = []

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

# Add tables/schema if not already there.
Base.metadata.create_all(engine)

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)


def test_initialise():
    global fb_mod
    global refs

    # add User "001_Bob"
    user = user_create(db, "004_Bob")
    # By adding set_global_user_id here we do not need to pass the
    # created_by and updated_by dict elements to the schema validators.
    set_global_user_id(db, user.id)

    # add mods
    data = {
        "abbreviation": '004_FB',
        "short_name": "004_FB",
        "full_name": "004_ont_1"
    }
    fb_mod = mod_create(db, data)

    data = {
        "abbreviation": '004_RGD',
        "short_name": "004_Rat",
        "full_name": "004_ont_2"
    }
    mod_create(db, data)

    # Add references.
    for title in ['Bob 004 1', 'Bob 004 2', 'Bob 004 3']:
        reference = ReferenceSchemaPost(title=title, category="thesis", abstract="3", language="MadeUp")
        res = reference_create(db, reference)
        refs.append(res)


def test_get_bad_editor():

    with pytest.raises(HTTPException):
        show(db, 99999)


def test_create_editor():
    global refs
    xml = {
        "order": 1,
        "first_name": "string",
        "last_name": "string",
        "name": "003_TCU",
        "orcid": "ORCID:2345-2345-2345-234X",
        "reference_curie": refs[0]
    }

    res = create(db, xml)
    assert res
    # check db for editor
    editor = db.query(EditorModel).filter(EditorModel.name == "003_TCU").one()
    assert editor.first_name == "string"


def test_create_editor_for_ref_later():
    global refs
    xml = {
        "order": 2,
        "first_name": "string2",
        "last_name": "string3",
        "name": "Name2",
        "orcid": "ORCID:3333-4444-5555-666X",
        "reference_curie": refs[0]
    }
    res = create(db, xml)
    assert res
    # check db for editor
    editor = db.query(EditorModel).filter(EditorModel.name == "Name2").one()
    assert editor.first_name == "string2"


def test_patch_editor():
    global refs
    xml = {'first_name': "003_TUA",
           'orcid': "ORCID:5432-5432-5432-432X",
           'reference_curie': refs[1]}
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
