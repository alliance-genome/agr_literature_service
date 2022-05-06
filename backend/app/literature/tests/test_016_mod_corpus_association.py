import pytest
from fastapi import HTTPException
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from literature.crud.mod_corpus_association_crud import create, destroy, patch,\
    show, show_by_reference_mod_abbreviation, show_changesets
from literature.crud.mod_crud import create as mod_create
from literature.crud.reference_crud import create as ref_create
from literature.database.config import SQLALCHEMY_DATABASE_URL
from literature.database.base import Base
from literature.models import ModModel, ReferenceModel, ModCorpusAssociationModel
from literature.schemas import ReferenceSchemaPost

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

Base.metadata.create_all(engine)

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)

test_source = 'Mod_pubmed_search'
test_source2 = 'Assigned_for_review'
db.execute('delete from mod')
db.execute('delete from cross_reference')
db.execute('delete from author')
db.execute('delete from editor')
db.execute('delete from "reference"')
db.execute('delete from resource')


def get_ids():

    mod = db.query(ModModel).filter_by(abbreviation="AtDB").one()
    mod_id = mod.mod_id
    ref = db.query(ReferenceModel).filter_by(curie='AGR:AGR-Reference-0000000001').one()
    reference_id = ref.reference_id
    return (mod_id, reference_id)


def test_get_bad_mca():

    with pytest.raises(HTTPException):
        show(db, 0)


def test_create_mca():

    mod_data = {
        "abbreviation": 'AtDB',
        "short_name": "AtDB",
        "full_name": "Test genome database"
    }
    res = mod_create(db, mod_data)
    assert res

    reference = ReferenceSchemaPost(title="Bob", category="thesis", abstract="3", language="MadeUp")
    res = ref_create(db, reference)
    assert res

    data = {
        "mod_abbreviation": "AtDB",
        "reference_curie": "AGR:AGR-Reference-0000000001",
        "mod_corpus_sort_source": test_source
    }
    res = create(db, data)
    assert res


def test_show_by_reference_mod_abbreviation():

    ref_curie = "AGR:AGR-Reference-0000000001"
    mod_abbreviation = "AtDB"
    res = show_by_reference_mod_abbreviation(db, ref_curie, mod_abbreviation)
    assert res


def test_patch_mca():

    ref_curie = "AGR:AGR-Reference-0000000001"
    mod_abbreviation = "AtDB"
    data = {"reference_curie": ref_curie,
            "mod_abbreviation": mod_abbreviation,
            "mod_corpus_sort_source": test_source2}

    (mod_id, reference_id) = get_ids()

    mca = db.query(ModCorpusAssociationModel).filter_by(reference_id=reference_id, mod_id=mod_id).one()
    res = patch(db, mca.mod_corpus_association_id, data)
    assert res


def test_show_mca():

    (mod_id, reference_id) = get_ids()    
    mca = db.query(ModCorpusAssociationModel).filter_by(reference_id=reference_id, mod_id=mod_id).one()
    res = show(db, mca.mod_corpus_association_id)
    assert res


def test_changesets():

    (mod_id, reference_id) = get_ids()
    mca = db.query(ModCorpusAssociationModel).filter_by(reference_id=reference_id, mod_id=mod_id).one()
    res = show_changesets(db, mca.mod_corpus_association_id)
    assert res


def test_destroy_mca():

    (mod_id, reference_id) = get_ids()
    mca = db.query(ModCorpusAssociationModel).filter_by(reference_id=reference_id, mod_id=mod_id).one()
    destroy(db, mca.mod_corpus_association_id)

    # it should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, mca.mod_corpus_association_id)

    # deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, mca.mod_corpus_association_id)
