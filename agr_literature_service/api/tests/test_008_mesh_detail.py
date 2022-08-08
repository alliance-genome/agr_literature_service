import pytest
from fastapi import HTTPException
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from agr_literature_service.api.crud.mesh_detail_crud import (create, destroy, patch, show,
                                                              show_changesets)
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models import MeshDetailModel, ReferenceModel
from agr_literature_service.api.schemas import (
    MeshDetailSchemaPost, MeshDetailSchemaUpdate, ReferenceSchemaPost)
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
mesh_id = None

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)

fb_mod = None
refs = []


def test_initialise():
    global fb_mod
    global refs

    # add User "006_Bob"
    user = user_create(db, "008_Bob")
    # By adding set_global_user_id here we do not need to pass the
    # created_by and updated_by dict elements to the schema validators.
    set_global_user_id(db, user.id)

    # add mods
    data = {
        "abbreviation": '008_FB',
        "short_name": "008_FB",
        "full_name": "008_ont_1"
    }
    fb_mod = mod_create(db, data)

    data = {
        "abbreviation": '008_RGD',
        "short_name": "008_Rat",
        "full_name": "008_ont_2"
    }
    mod_create(db, data)

    # Add references.
    for title in ['Bob 008 1', 'Bob 008 2', 'Bob 008 3']:
        reference = ReferenceSchemaPost(title=title, category="thesis", abstract="3", language="MadeUp")
        res = reference_create(db, reference)
        refs.append(res)


def test_get_bad_mesh_detail():
    with pytest.raises(HTTPException):
        show(db, 99999)


def test_create_mesh():
    global mesh_id
    global refs
    xml = {'reference_curie': refs[0],
           'heading_term': "Head1",
           'qualifier_term': "Qual1"}
    md_schema = MeshDetailSchemaPost(**xml)
    res = create(db, md_schema)
    mesh_id = res
    assert res > 0


def test_show_mesh():
    global mesh_id
    global refs
    res = show(db, mesh_id)
    assert res['heading_term'] == "Head1"
    assert res['qualifier_term'] == "Qual1"
    assert res['reference_curie'] == refs[0]

    # and in the db
    mesh_detail_obj = db.query(MeshDetailModel).filter(MeshDetailModel.heading_term == "Head1").one()
    assert mesh_detail_obj.reference.curie == refs[0]
    assert mesh_detail_obj.qualifier_term == "Qual1"


def test_patch_mesh():
    global mesh_id
    global refs
    xml = {'heading_term': "Head2",
           'qualifier_term': "Qual2",
           'reference_curie': refs[1]}
    schema = MeshDetailSchemaUpdate(**xml)
    res = patch(db, 2, schema)
    assert res == {"message": "updated"}
    mesh_detail_obj = db.query(MeshDetailModel).filter(MeshDetailModel.mesh_detail_id == mesh_id).one()
    assert mesh_detail_obj.heading_term == "Head2"
    assert mesh_detail_obj.reference.curie == refs[1]
    assert mesh_detail_obj.qualifier_term == "Qual2"


def test_changesets():
    global mesh_id
    res = show_changesets(db, mesh_id)

    # reference_curie : None -> 1 -> 3
    # reference_id_from      : None -> orig -> new
    from_id = db.query(ReferenceModel).filter(ReferenceModel.curie == refs[0]).one().reference_id
    # reference_id_to        : None -> new -> orig
    to_id = db.query(ReferenceModel).filter(ReferenceModel.curie == refs[1]).one().reference_id
    # heading_term            : None -> Head1 -> Head2
    # qualifier_term          : None -> Qual1 -> Qual2
    for transaction in res:
        print(transaction)
        if not transaction['changeset']['reference_id'][0]:
            assert transaction['changeset']['reference_id'][1] == from_id
            assert transaction['changeset']['heading_term'][1] == "Head1"
            assert transaction['changeset']['qualifier_term'][1] == "Qual1"
        else:
            assert transaction['changeset']['reference_id'][1] == to_id
            assert transaction['changeset']['heading_term'][1] == "Head2"
            assert transaction['changeset']['qualifier_term'][1] == "Qual2"


def test_destroy_mesh_detail():
    global mesh_id
    destroy(db, mesh_id)

    # It should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, mesh_id)

    # Deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, mesh_id)
