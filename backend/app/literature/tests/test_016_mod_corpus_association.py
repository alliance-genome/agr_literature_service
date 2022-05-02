import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from literature.crud.mod_corpus_association_crud import create, destroy, patch,\
           show, show_by_reference_mod_abbreviation, show_changesets
from literature.database.config import SQLALCHEMY_DATABASE_URL
from literature.database.base import Base
from literature.models import ModCorpusAssociationModel

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

Base.metadata.create_all(engine)

test_ref_id = 641424
test_mod_id = 5
test_source = 'Mod_pubmed_search'
test_ref_id2 = 397259
test_mod_id2 = 6
test_source2 = 'Assigned_for_review'

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)

def test_get_bad_mca():

    with pytest.raises(HTTPException):
        show(db, 0)

def test_create_mca():

    data = {
        "mod_corpus_sort_source": test_source,
        "reference_id": test_ref_id,
        "mod_id": test_mod_id
    }
    
    res = create(db, data)
    assert res
    
    mca = db.query(ModCorpusAssociationModel).filter_by(reference_id=test_ref_id, mod_id=test_mod_id).one()
    assert mca.mod_corpus_sort_source == test_source

def test_show_by_reference_mod_abbreviation():

    ref_curie = "AGR:AGR-Reference-0000641424"
    mod_abbreviation = "MGI"

    res = show_mod_corpus_association(db, ref_curie, mod_abbreviation)

    assert res['reference_id'] == test_ref_id
    assert res['mod_id'] == test_mod_id
    assert res['mod_corpus_sort_source'] == test_source
    
def test_patch_mca():
    
    data = { "reference_id": test_ref_id2,
             "mod_id": test_mod_id2,
             "mod_corpus_sort_source": test_source2 }
    
    mca = db.query(ModCorpusAssociationModel).filter_by(reference_id=test_ref_id, mod_id=test_mod_id).one()
    res = patch(db, mca.mod_corpus_association_id, data)
    assert res
             
    mca2 = db.query(ModCorpusAssociationModel).filter_by(reference_id=test_ref_id2, mod_id=test_mod_id2).one()
    assert mca.mod_id == mca2.mod_id
    assert mca2.mod_corpus_sort_source == test_source2
    
def test_show_mca():

    mca = db.query(ModCorpusAssociationModel).filter_by(reference_id=test_ref_id2, mod_id=test_mod_id2).one()
    res = show(db, mca.mod_corpus_association_id)
             
    assert res["mod_corpus_sort_source"] == test_source2
    
def test_changesets():
	
    mca = db.query(ModCorpusAssociationModel).filter_by(reference_id=test_ref_id2, mod_id=test_mod_id2).one()
    res = show_changesets(db, mca.mod_corpus_association_id)

    assert res["mod_corpus_sort_source"] == test_source2

def test_destroy_mca():
             
    mca = db.query(ModCorpusAssociationModel).filter_by(reference_id=test_ref_id2, mod_id=test_mod_id2).one()
    destroy(db, mca.mod_corpus_association_id)

    # it should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, mca.mod_corpus_association_id)

    # deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, mca.mod_corpus_association_id)



