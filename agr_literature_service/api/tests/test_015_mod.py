import pytest
from fastapi import HTTPException
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from agr_literature_service.api.crud.mod_crud import create, destroy, patch, show,\
    show_changesets
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models import ModModel

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

Base.metadata.create_all(engine)

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)


def test_get_bad_mod():

    with pytest.raises(HTTPException):
        show(db, "AtDB")


def test_create_mod():

    data = {
        "abbreviation": '0015_AtDB',
        "short_name": "AtDB",
        "full_name": "Test genome database"
    }

    res = create(db, data)
    assert res

    mod = db.query(ModModel).filter_by(abbreviation="0015_AtDB").one()
    assert mod.short_name == "AtDB"
    assert mod.full_name == "Test genome database"


def test_update_mod():

    data = {"abbreviation": "0015_AtDB",
            "short_name": "AtDB2",
            "full_name": "Test genome database2"}

    mod = db.query(ModModel).filter_by(abbreviation="0015_AtDB").one()
    res = patch(db, mod.mod_id, data)
    assert res

    mod2 = db.query(ModModel).filter_by(abbreviation="0015_AtDB").one()
    assert mod.mod_id == mod2.mod_id
    assert mod2.full_name == "Test genome database2"


def test_show_mod():

    mod = db.query(ModModel).filter_by(abbreviation="0015_AtDB").one()
    res = show(db, mod.abbreviation)

    assert res["full_name"] == "Test genome database2"


def test_changesets():

    mod = db.query(ModModel).filter_by(abbreviation="0015_AtDB").one()
    show_changesets(db, mod.mod_id)


def test_destroy_mod():

    mod = db.query(ModModel).filter_by(abbreviation="0015_AtDB").one()
    destroy(db, mod.mod_id)

    # it should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, mod.abbreviation)

    # deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, mod.mod_id)
