import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from literature.crud.mod_crud import create, destroy, patch, show,\
                                     show_changesets
from literature.database.config import SQLALCHEMY_DATABASE_URL
from literature.database.base import Base
from literature.models import ModModel

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
        "abbreviation": 'AtDB',
        "short_name": "AtDB",
        "full_name": "Test genome database"
    }
    
    res = create(db, data)
    assert res
    
    mod = db.query(ModModel).filter_by(abbreviation = "AtDB").one()
    assert mod.short_name == "AtDB"
    assert mod.full_name == "Test genome database"

def test_update_mod():
    
    data = { "abbreviation": "AtDB2",
             "short_name": "AtDB2",
             "full_name": "Test genome database2" }
    
    mod = db.query(ModModel).filter_by(abbreviation = "AtDB").one()
    res = patch(db, mod.mod_id, data)
    assert res
             
    mod2 = db.query(ModModel).filter_by(abbreviation = "AtDB2").one()
    assert mod.mod_id == mod2.mod_id
    assert mod2.full_name == "Test genome database2"
    
def test_show_mod():

    mod = db.query(ModModel).filter_by(abbreviation = "AtDB2").one()
    res = show(db, mod.abbreviation)
             
    assert res["short_name"] == "AtDB2"

    # mod_id = 88 that does not exist
    with pytest.raises(HTTPException):
        show(db, 88)

def test_changesets():
	
    mod = db.query(ModModel).filter_by(Abbreviation = "AtDB2").one()
    res = show_changesets(db, mod.mod_id)

    assert res["full_name"] == "Test genome database2"
             
def test_destroy_mod():
             
    mod = db.query(ModModel).filter_by(abbreviation = "AtDB2").one()
    destroy(db, mod.mod_id)

    # it should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, mod.abbreviation)

    # deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, mod.mod_id)



