import pytest
from fastapi import HTTPException
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from literature.crud.reference_automated_term_tag_crud import (create, destroy,
                                                               patch, show,
                                                               show_changesets)
from literature.database.config import SQLALCHEMY_DATABASE_URL
from literature.models import Base, ReferenceAutomatedTermTagModel
from literature.schemas import (ReferenceAutomatedTermTagSchemaPatch,
                                ReferenceAutomatedTermTagSchemaPost)

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

# Add tables/schema if not already there.
Base.metadata.create_all(engine)

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)


def test_get_bad_ratt():
    with pytest.raises(HTTPException):
        show(db, 99999)


def test_create_ratt():
    res_sch = ReferenceAutomatedTermTagSchemaPost(
        reference_curie="AGR:AGR-Reference-0000000001",
        ontology="Ont1",
        datatype="datatype1",
        term="term1",
        automated_system="auto1",
        confidence_score=99.9)
    res = create(db, res_sch)
    assert res == 1

    ratt = db.query(ReferenceAutomatedTermTagModel).\
        filter(ReferenceAutomatedTermTagModel.ontology == "Ont1").one()
    assert ratt.reference.curie == "AGR:AGR-Reference-0000000001"
    assert ratt.datatype == "datatype1"
    assert ratt.term == 'term1'
    assert ratt.automated_system == "auto1"
    assert ratt.confidence_score == 99.9


def test_show_ratt():
    res = show(db, 1)
    assert res['term'] == "term1"
    assert res['datatype'] == "datatype1"
    assert res['reference_curie'] == "AGR:AGR-Reference-0000000001"
    assert res['automated_system'] == "auto1"
    assert res['confidence_score'] == 99.9


def test_update_ratt():
    # NOTE: gibberish passes as we do no schema checking
    xml = {'term': "term2",
           'datatype': 'datatype2',
           'automated_system': 'auto2',
           'ontology': "ont2",
           'confidence_score': 75.2,
           'reference_curie': "AGR:AGR-Reference-0000000003"}
    schema = ReferenceAutomatedTermTagSchemaPatch(**xml)
    res = patch(db, 1, schema)
    assert res == {'message': 'updated'}

    ratt = db.query(ReferenceAutomatedTermTagModel).\
        filter(ReferenceAutomatedTermTagModel.ontology == "ont2").one()
    assert ratt.reference.curie == "AGR:AGR-Reference-0000000003"
    assert ratt.datatype == "datatype2"
    assert ratt.term == 'term2'
    assert ratt.automated_system == "auto2"
    assert ratt.confidence_score == 75.2


def test_changesets():
    res = show_changesets(db, 1)

    # datatype         : None -> datatype1 -> datatype2
    # term             : None -> 'term1' -> 'term2'
    # automated_system : None -> 'auto1' -> 'auto2'
    # confidence_score : None => 99.9 -> 75.2
    # reference_id  : None -> 1 -> 3
    for transaction in res:
        print(transaction)
        if not transaction['changeset']['term'][0]:
            assert transaction['changeset']['datatype'][1] == "datatype1"
            assert transaction['changeset']['term'][1] == "term1"
            assert transaction['changeset']['automated_system'][1] == "auto1"
            assert transaction['changeset']['confidence_score'][1] == 99.9
            assert transaction['changeset']['reference_id'][1] == 1
        else:
            assert transaction['changeset']['datatype'][1] == "datatype2"
            assert transaction['changeset']['term'][1] == "term2"
            assert transaction['changeset']['automated_system'][1] == "auto2"
            assert transaction['changeset']['confidence_score'][1] == 75.2
            assert transaction['changeset']['reference_id'][1] == 3


def test_destroy_mrt():
    destroy(db, 1)

    # It should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, 1)

    # Deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, 1)
