import pytest
from fastapi import HTTPException
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from agr_literature_service.api.crud.mod_reference_type_crud import (create, destroy, patch,
                                                                     show, show_changesets)
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models import ModReferenceTypeModel, ReferenceModel
from agr_literature_service.api.schemas import (ModReferenceTypeSchemaPost,
                                                ModReferenceTypeSchemaUpdate,
                                                ReferenceSchemaPost)
from agr_literature_service.api.crud.mod_crud import create as mod_create
from agr_literature_service.api.crud.user_crud import create as user_create
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.api.crud.reference_crud import (
    create as reference_create)

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

# Add tables/schema if not already there.
Base.metadata.create_all(engine)

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)

mrt_id = None

fb_mod = None
refs = []


def test_initialise():
    global fb_mod
    global refs

    # add User "006_Bob"
    user = user_create(db, "009_Bob")
    # By adding set_global_user_id here we do not need to pass the
    # created_by and updated_by dict elements to the schema validators.
    set_global_user_id(db, user.id)

    # add mods
    data = {
        "abbreviation": '009_FB',
        "short_name": "009_FB",
        "full_name": "009_ont_1"
    }
    fb_mod = mod_create(db, data)

    data = {
        "abbreviation": '009_RGD',
        "short_name": "009_Rat",
        "full_name": "009_ont_2"
    }
    mod_create(db, data)

    # Add references.
    for title in ['Bob 009 1', 'Bob 009 2', 'Bob 009 3']:
        reference = ReferenceSchemaPost(title=title, category="thesis", abstract="3", language="MadeUp")
        res = reference_create(db, reference)
        refs.append(res)


def test_get_bad_mrt():
    with pytest.raises(HTTPException):
        show(db, 99999)


def test_create_mrt():
    global mrt_id
    global refs
    res_sch = ModReferenceTypeSchemaPost(
        reference_curie=refs[0],
        reference_type="string1",
        source="string2")
    res = create(db, res_sch)
    mrt_id = res
    assert res > 1
    # check db for mrt
    mrt = db.query(ModReferenceTypeModel).filter(ModReferenceTypeModel.mod_reference_type_id == mrt_id).one()
    assert mrt.reference_type == "string1"
    assert mrt.reference.curie == refs[0]
    assert mrt.source == "string2"


def test_patch_mrt():
    global mrt_id
    global refs
    xml = {"reference_curie": refs[1],
           "reference_type": "string3",
           "source": "string4"}
    schema = ModReferenceTypeSchemaUpdate(**xml)
    res = patch(db, mrt_id, schema)
    assert res == {"message": "updated"}

    # check db for mrt
    mrt = db.query(ModReferenceTypeModel).filter(ModReferenceTypeModel.mod_reference_type_id == mrt_id).one()
    assert mrt.reference_type == "string3"
    assert mrt.reference.curie == refs[1]
    assert mrt.source == "string4"


# NOTE: BAD... recursion error. NEEDS fixing.
def test_show_mrt():
    global mrt_id
    with pytest.raises(RecursionError):
        res = show(db, mrt_id)
        assert res == 0


def test_changesets():
    global mrt_id
    res = show_changesets(db, mrt_id)

    # reference_id      : None -> 1 -> 3
    # reference_type    : None -> 'string1' -> 'string3'
    # source           : None -> 'string2' -> 'string4'
    from_id = db.query(ReferenceModel).filter(ReferenceModel.curie == refs[0]).one().reference_id
    to_id = db.query(ReferenceModel).filter(ReferenceModel.curie == refs[1]).one().reference_id

    for transaction in res:
        print(transaction)
        if not transaction['changeset']['reference_id'][0]:
            assert transaction['changeset']['reference_id'][1] == from_id
            assert transaction['changeset']['reference_type'][1] == "string1"
            assert transaction['changeset']['source'][1] == "string2"
        else:
            assert transaction['changeset']['reference_id'][1] == to_id
            assert transaction['changeset']['reference_type'][1] == "string3"
            assert transaction['changeset']['source'][1] == "string4"


def test_destroy_mrt():
    global mrt_id
    destroy(db, mrt_id)

    # It should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, mrt_id)

    # Deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, mrt_id)
