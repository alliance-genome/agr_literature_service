import pytest
from fastapi import HTTPException
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from agr_literature_service.api.crud.mod_corpus_association_crud import create, destroy, patch,\
    show, show_by_reference_mod_abbreviation, show_changesets
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models import ModModel, ReferenceModel, ModCorpusAssociationModel
from agr_literature_service.api.tests import utils

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

(refs, ress, mods, okta_user) = utils.initialise(db, '016')


def get_ids():
    global refs
    mod = db.query(ModModel).filter_by(abbreviation="016_FB").one()
    mod_id = mod.mod_id
    ref = db.query(ReferenceModel).filter_by(curie=refs[0]).one()
    reference_id = ref.reference_id
    return (mod_id, reference_id)


def test_get_bad_mca():

    with pytest.raises(HTTPException):
        show(db, 0)


def test_create_mca():
    global refs
    global fb_mod

    data = {
        "mod_abbreviation": "016_FB",
        "reference_curie": refs[0],
        "mod_corpus_sort_source": test_source
    }
    res = create(db, data)
    assert res


def test_show_by_reference_mod_abbreviation():
    global refs
    ref_curie = refs[0]
    mod_abbreviation = "016_FB"
    res = show_by_reference_mod_abbreviation(db, ref_curie, mod_abbreviation)
    assert res


def test_patch_mca():
    global refs
    ref_curie = refs[0]
    mod_abbreviation = "016_FB"
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
