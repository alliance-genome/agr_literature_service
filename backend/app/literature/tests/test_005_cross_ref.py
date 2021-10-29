import pytest
from literature.crud.cross_reference_crud import create, show, patch, destroy, show_changesets
from sqlalchemy import create_engine
from sqlalchemy import MetaData

# from literature import models
from literature.models import (
    Base, CrossReferenceModel
)
from literature.schemas import CrossReferenceSchemaPost, CrossReferenceSchemaUpdate
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


def test_get_bad_xref():

    with pytest.raises(HTTPException):
        show(db, "99999")


def test_create_xref():

    xml = {"curie": 'XREF:123456', "reference_curie": 'AGR:AGR-Reference-0000000001'}
    xref_schema = CrossReferenceSchemaPost(**xml)
    print("BOB: {}".format(xref_schema))
    res = create(db, xref_schema)
    assert res

    # check db for xref
    xref = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == "XREF:123456").one()
    assert xref.curie == "XREF:123456"
    assert xref.reference.curie == 'AGR:AGR-Reference-0000000001'

    # Now do a resource one
    xml = {"curie": 'XREF:anoth', "resource_curie": 'AGR:AGR-Resource-0000000001'}
    xref_schema = CrossReferenceSchemaPost(**xml)
    res = create(db, xref_schema)
    assert res

    # check db for xref
    xref = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == "XREF:anoth").one()
    assert xref.curie == "XREF:anoth"
    assert not xref.reference
    assert xref.resource.curie == 'AGR:AGR-Resource-0000000001'

    xml = {"curie": 'XREF:no_ref_res'}
    with pytest.raises(HTTPException):
        xref_schema = CrossReferenceSchemaPost(**xml)
        create(db, xref_schema)


def test_create_again_bad():
    with pytest.raises(HTTPException):
        xml = {"curie": 'XREF:123456', "reference_curie": 'AGR:AGR-Reference-0000000001'}
        create(db, xml)


def test_show_xref():
    res = show(db, "XREF:123456")
    assert res['curie'] == "XREF:123456"
    assert res['reference_curie'] == 'AGR:AGR-Reference-0000000001'

    # Causes a crash?? recursive error?
    # res = show(db, "XREF:anoth")
    # assert res['curie'] == "XREF:anoth"
    # assert res['resource_curie'] == 'AGR:AGR-Resource-0000000001'


def test_patch_xref():
    xref = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == "XREF:123456").one()
    # xref_schema = CrossReferenceSchemaUpdate(is_obsolete=True, reference_curie="AGR:AGR-Reference-0000000001")
    # print("xref schema: '{}'".format(xref_schema))
    res = patch(db, xref.curie, {'is_obsolete': True})
    assert res
