import pytest
from fastapi import HTTPException
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from literature.crud.mod_reference_type_crud import (create, destroy, patch,
                                                     show, show_changesets)
from literature.database.config import SQLALCHEMY_DATABASE_URL
from literature.database.base import Base
from literature.models import ModReferenceTypeModel
from literature.schemas import (ModReferenceTypeSchemaPost,
                                ModReferenceTypeSchemaUpdate)

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

# Add tables/schema if not already there.
Base.metadata.create_all(engine)

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)


def test_get_bad_mrt():
    with pytest.raises(HTTPException):
        show(db, 99999)


def test_create_mrt():
    res_sch = ModReferenceTypeSchemaPost(
        reference_curie="AGR:AGR-Reference-0000000001",
        reference_type="string1",
        source="string2")
    res = create(db, res_sch)
    assert res == 1
    # check db for mrt
    mrt = db.query(ModReferenceTypeModel).filter(ModReferenceTypeModel.mod_reference_type_id == 1).one()
    assert mrt.reference_type == "string1"
    assert mrt.reference.curie == "AGR:AGR-Reference-0000000001"
    assert mrt.source == "string2"


def test_patch_mrt():
    xml = {"reference_curie": "AGR:AGR-Reference-0000000003",
           "reference_type": "string3",
           "source": "string4"}
    schema = ModReferenceTypeSchemaUpdate(**xml)
    res = patch(db, 1, schema)
    assert res == {"message": "updated"}

    # check db for mrt
    mrt = db.query(ModReferenceTypeModel).filter(ModReferenceTypeModel.mod_reference_type_id == 1).one()
    assert mrt.reference_type == "string3"
    assert mrt.reference.curie == "AGR:AGR-Reference-0000000003"
    assert mrt.source == "string4"


# NOTE: BAD... recursion error. NEEDS fixing.
def test_show_mrt():
    with pytest.raises(RecursionError):
        res = show(db, 1)
        assert res == 0


def test_changesets():
    res = show_changesets(db, 1)

    # reference_id      : None -> 1 -> 3
    # reference_type    : None -> 'string1' -> 'string3'
    # source           : None -> 'string2' -> 'string4'
    for transaction in res:
        print(transaction)
        if not transaction['changeset']['reference_id'][0]:
            assert transaction['changeset']['reference_id'][1] == 1
            assert transaction['changeset']['reference_type'][1] == "string1"
            assert transaction['changeset']['source'][1] == "string2"
        else:
            assert transaction['changeset']['reference_id'][1] == 3
            assert transaction['changeset']['reference_type'][1] == "string3"
            assert transaction['changeset']['source'][1] == "string4"


def test_destroy_mrt():
    destroy(db, 1)

    # It should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, 1)

    # Deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, 1)
