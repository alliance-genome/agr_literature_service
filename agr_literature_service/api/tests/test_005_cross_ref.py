import pytest
from fastapi import HTTPException
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from agr_literature_service.api.crud.cross_reference_crud import (
    create, destroy, patch, show, show_changesets)
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
# from agr_literature_service.api import models
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models import CrossReferenceModel
from agr_literature_service.api.schemas import (CrossReferenceSchemaPost,
                                                CrossReferenceSchemaUpdate,
                                                ReferenceSchemaPost,
                                                ResourceSchemaPost)
from agr_literature_service.api.crud.mod_crud import create as mod_create
from agr_literature_service.api.crud.user_crud import create as user_create
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.api.crud.reference_crud import create as reference_create
from agr_literature_service.api.crud.resource_crud import create as resource_create

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

# Add tables/schema if not already there.
Base.metadata.create_all(engine)

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)

# (refs, ress, fb_mod) = initialise(db, '005')
refs = []
ress = []
fb_mod = []


def test_initialise():
    global fb_mod
    global refs

    # add User "005_Bob"
    user = user_create(db, "005_Bob")
    # By adding set_global_user_id here we do not need to pass the
    # created_by and updated_by dict elements to the schema validators.
    set_global_user_id(db, user.id)

    # add mods
    data = {
        "abbreviation": '005_FB',
        "short_name": "005_FB",
        "full_name": "005_ont_1"
    }
    fb_mod = mod_create(db, data)

    data = {
        "abbreviation": '005_RGD',
        "short_name": "005_Rat",
        "full_name": "005_ont_2"
    }
    mod_create(db, data)

    # Add references.
    for title in ['Bob 005 1', 'Bob 005 2', 'Bob 005 3']:
        reference = ReferenceSchemaPost(title=title, category="thesis", abstract="3", language="MadeUp")
        res = reference_create(db, reference)
        refs.append(res)

        Resource = ResourceSchemaPost(title=title, abstract="3", open_access=True)
        ress.append(resource_create(db, Resource))


def test_get_bad_xref():

    with pytest.raises(HTTPException):
        show(db, "99999")


def test_create_xref():
    global refs
    global ress

    db.execute("INSERT INTO resource_descriptors  (db_prefix, name, default_url) VALUES ('XREF', 'Madeup', 'http://www.bob.com/[%s]')")
    db.commit()

    xml = {"curie": 'XREF:123456', "reference_curie": refs[0], "pages": ["reference"]}
    xref_schema = CrossReferenceSchemaPost(**xml)
    res = create(db, xref_schema)
    assert res

    # check db for xref
    xref = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == "XREF:123456").one()
    assert xref.curie == "XREF:123456"
    assert xref.reference.curie == refs[0]
    # what has it stored for pages?
    assert xref.pages == ["reference"]

    # Now do a resource one
    xml = {"curie": 'XREF:anoth', "resource_curie": ress[0]}
    xref_schema = CrossReferenceSchemaPost(**xml)
    res = create(db, xref_schema)
    assert res

    # check db for xref
    xref = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == "XREF:anoth").one()
    assert xref.curie == "XREF:anoth"
    assert not xref.reference
    assert xref.resource.curie == ress[0]

    xml = {"curie": 'XREF:no_ref_res'}
    with pytest.raises(HTTPException):
        xref_schema = CrossReferenceSchemaPost(**xml)
        create(db, xref_schema)


def test_create_again_bad():
    global refs
    with pytest.raises(HTTPException):
        xml = {"curie": 'XREF:123456', "reference_curie": refs[0]}
        create(db, xml)


def test_show_xref():
    res = show(db, "XREF:123456")
    assert res['curie'] == "XREF:123456"
    assert res['reference_curie'] == refs[0]


def test_patch_xref():
    xref_schema = CrossReferenceSchemaUpdate(is_obsolete=True,
                                             pages=["different"],
                                             reference_curie=refs[0])
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
