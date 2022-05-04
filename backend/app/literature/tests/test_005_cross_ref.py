import pytest
from fastapi import HTTPException
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from literature.crud.cross_reference_crud import (create, destroy, patch, show,
                                                  show_changesets)
from literature.database.config import SQLALCHEMY_DATABASE_URL
# from literature import models
from literature.database.base import Base
from literature.models import CrossReferenceModel
from literature.schemas import (CrossReferenceSchemaPost,
                                CrossReferenceSchemaUpdate)

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

    db.execute("INSERT INTO resource_descriptor  (db_prefix, name, default_url) VALUES ('XREF', 'Madeup', 'http://www.bob.com/[%s]')")
    db.commit()

    xml = {"curie": 'XREF:123456', "reference_curie": 'AGR:AGR-Reference-0000000001', "pages": ["reference"]}
    xref_schema = CrossReferenceSchemaPost(**xml)
    res = create(db, xref_schema)
    assert res

    # check db for xref
    xref = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == "XREF:123456").one()
    assert xref.curie == "XREF:123456"
    assert xref.reference.curie == 'AGR:AGR-Reference-0000000001'
    # what has it stored for pages?
    assert xref.pages == ["reference"]

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


def test_patch_xref():
    xref_schema = CrossReferenceSchemaUpdate(is_obsolete=True,
                                             pages=["different"],
                                             reference_curie="AGR:AGR-Reference-0000000001")
    res = patch(db, "XREF:123456", xref_schema)
    assert res['message'] == "updated"
    xref = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == "XREF:123456").one()
    assert xref.is_obsolete
    assert xref.pages == ["different"]


def test_changesets():
    res = show_changesets(db, "XREF:123456")

    # Pages      : None -> reference -> different
    # is_obsolete: None -> False -> True
    for transaction in res:
        print(transaction)
        if not transaction['changeset']['pages'][0]:
            assert transaction['changeset']['pages'][1] == ["reference"]
            assert not transaction['changeset']['is_obsolete'][1]
        else:
            assert transaction['changeset']['pages'][1] == ["different"]
            assert transaction['changeset']['is_obsolete'][1]


def test_destroy_xref():
    xref = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == "XREF:anoth").one()
    destroy(db, xref.curie)

    # It should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, xref.curie)

    # Deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, xref.curie)
