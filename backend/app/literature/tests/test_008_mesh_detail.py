import pytest
from fastapi import HTTPException
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from literature.crud.mesh_detail_crud import (create, destroy, patch, show,
                                              show_changesets)
from literature.database.config import SQLALCHEMY_DATABASE_URL
from literature.models import Base, MeshDetailModel
from literature.schemas import MeshDetailSchemaPost, MeshDetailSchemaUpdate

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

# Add tables/schema if not already there.
Base.metadata.create_all(engine)

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)


def test_get_bad_mesh_detail():
    with pytest.raises(HTTPException):
        show(db, 99999)


def test_create_mesh():
    xml = {'reference_curie': "AGR:AGR-Reference-0000000001",
           'heading_term': "Head1",
           'qualifier_term': "Qual1"}
    md_schema = MeshDetailSchemaPost(**xml)
    res = create(db, md_schema)
    assert res == 1


def test_show_mesh():
    res = show(db, 1)
    assert res['heading_term'] == "Head1"
    assert res['qualifier_term'] == "Qual1"
    assert res['reference_curie'] == "AGR:AGR-Reference-0000000001"

    # and in the db
    mesh_detail_obj = db.query(MeshDetailModel).filter(MeshDetailModel.heading_term == "Head1").one()
    assert mesh_detail_obj.reference.curie == "AGR:AGR-Reference-0000000001"
    assert mesh_detail_obj.qualifier_term == "Qual1"


def test_patch_mesh():
    xml = {'heading_term': "Head2",
           'qualifier_term': "Qual2",
           'reference_curie': "AGR:AGR-Reference-0000000003"}
    schema = MeshDetailSchemaUpdate(**xml)
    res = patch(db, 1, schema)
    assert res == {"message": "updated"}
    mesh_detail_obj = db.query(MeshDetailModel).filter(MeshDetailModel.mesh_detail_id == 1).one()
    assert mesh_detail_obj.heading_term == "Head2"
    assert mesh_detail_obj.reference.curie == "AGR:AGR-Reference-0000000003"
    assert mesh_detail_obj.qualifier_term == "Qual2"


def test_changesets():
    res = show_changesets(db, 1)

    # reference_curie : None -> 1 -> 3
    # heading_term            : None -> Head1 -> Head2
    # qualifier_term          : None -> Qual1 -> Qual2
    for transaction in res:
        print(transaction)
        if not transaction['changeset']['reference_id'][0]:
            assert transaction['changeset']['reference_id'][1] == 1
            assert transaction['changeset']['heading_term'][1] == "Head1"
            assert transaction['changeset']['qualifier_term'][1] == "Qual1"
        else:
            assert transaction['changeset']['reference_id'][1] == 3
            assert transaction['changeset']['heading_term'][1] == "Head2"
            assert transaction['changeset']['qualifier_term'][1] == "Qual2"


def test_destroy_mesh_detail():
    destroy(db, 1)

    # It should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, 1)

    # Deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, 1)
