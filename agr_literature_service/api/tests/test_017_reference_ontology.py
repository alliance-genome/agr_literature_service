import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from agr_literature_service.api.crud.reference_ontology_crud import (
    create, destroy, patch, show, show_changesets)
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models import (ReferenceOntologyModel,
                                               ReferenceModel)
from agr_literature_service.api.schemas import (ReferenceOntologySchemaCreate,
                                                ReferenceOntologySchemaUpdate)
from agr_literature_service.api.crud.mod_crud import create as mod_create
from agr_literature_service.api.crud.user_crud import create as user_create

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

# Add tables/schema if not already there.
Base.metadata.create_all(engine)

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)

fb_mod = None

def test_initialise():
    global fb_mod
    # add User "017 Bob"
    user_create(db, "017_Bob")

    # add mods
    data = {
        "abbreviation": '017_FB',
        "short_name": "017_FB",
        "full_name": "017_ont_1"
    }
    fb_mod = mod_create(db, data)

    data = {
        "abbreviation": '017_RGD',
        "short_name": "017_Rat",
        "full_name": "017_ont_2"
    }
    mod_create(db, data)


def test_get_bad_ref_ont():
    with pytest.raises(HTTPException):
        show(db, 99999)


def test_bad_missing_args():
    xml = {'reference_curie': "AGR:AGR-Reference-0000000001",
           'ontology_id': "ont1",
           'created_by': "017_Bob"}
    with pytest.raises(ValidationError):  # mod_abbr missing
        ref_ont_schema = ReferenceOntologySchemaCreate(**xml)
        create(db, ref_ont_schema)

    xml = {'reference_curie': "AGR:AGR-Reference-0000000001",
           'mod_abbreviation': "017_FB",
           'created_by': "017_Bob"}
    with pytest.raises(ValidationError):  # ontology_id missing
        ref_ont_schema = ReferenceOntologySchemaCreate(**xml)
        create(db, ref_ont_schema)

    xml = {'mod_abbreviation': "017_FB",
           'ontology_id': "ont1",
           'created_by': "017_Bob"}
    with pytest.raises(ValidationError):  # ref_cur missing
        ref_ont_schema = ReferenceOntologySchemaCreate(**xml)
        create(db, ref_ont_schema)


def test_create_ref_ont():
    global fb_mod
    xml = {'reference_curie': "AGR:AGR-Reference-0000000001",
           'mod_abbreviation': "017_FB",
           'ontology_id': "ont1",
           'created_by': "017_Bob"}
    ref_ont_schema = ReferenceOntologySchemaCreate(**xml)
    create(db, ref_ont_schema)

    # check results in database
    ref_ont_obj = db.query(ReferenceOntologyModel).\
        join(ReferenceModel,
             ReferenceOntologyModel.reference_id == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == "AGR:AGR-Reference-0000000001").one()
    assert ref_ont_obj.ontology_id == "ont1"
    assert ref_ont_obj.created_by == "017_Bob"
    assert ref_ont_obj.mod_id == fb_mod


def test_patch_ref_ont():
    ref_ont_obj: ReferenceOntologyModel = db.query(ReferenceOntologyModel).\
        join(ReferenceModel,
             ReferenceOntologyModel.reference_id == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == "AGR:AGR-Reference-0000000001").one()

    # change ontology
    xml = {'ontology_id': 'ont test patch',
           'mod_abbreviation': "017_RGD"}

    res = patch(db, ref_ont_obj.reference_ontology_id, xml)
    assert res == {"message": "updated"}

    ref_ont_obj: ReferenceOntologyModel = db.query(ReferenceOntologyModel).\
        filter(ReferenceOntologyModel.reference_ontology_id == ref_ont_obj.reference_ontology_id).one()
    assert ref_ont_obj.reference.curie == "AGR:AGR-Reference-0000000001"
    assert ref_ont_obj.ontology_id == "ont test patch"


def test_show_ref_ont():
    ref_ont_obj: ReferenceOntologyModel = db.query(ReferenceOntologyModel).\
        join(ReferenceModel,
             ReferenceOntologyModel.reference_id == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == "AGR:AGR-Reference-0000000001").one()
    res = show(db, ref_ont_obj.reference_ontology_id)

    assert res['reference_curie'] == "AGR:AGR-Reference-0000000001"
    assert res['ontology_id'] == 'ont test patch'
    assert res['mod_abbreviation'] == '017_FB'
    assert res['created_by'] == '017_Bob'


def test_changesets():
    ref_ont_obj: ReferenceOntologyModel = db.query(ReferenceOntologyModel).\
        join(ReferenceModel,
             ReferenceOntologyModel.reference_id == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == "AGR:AGR-Reference-0000000001").one()
    res = show_changesets(db, ref_ont_obj.reference_ontology_id)

    for transaction in res:
        print("Test changesets 017: {}".format(transaction))
        if 'reference_ontology_id' in transaction['changeset']:
            assert transaction['changeset']['reference_ontology_id'][1] == 3
            assert transaction['changeset']['mod_id'][1] == 5
        else:
            assert transaction['changeset']['ontology_id'][0] == 'ont1'
            assert transaction['changeset']['ontology_id'][1] == 'ont test patch'


def test_destroy_ref_ont():
    ref_ont_obj: ReferenceOntologyModel = db.query(ReferenceOntologyModel).\
        join(ReferenceModel,
             ReferenceOntologyModel.reference_id == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == "AGR:AGR-Reference-0000000001").one()
    destroy(db, ref_ont_obj.reference_ontology_id)

    # It should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, ref_ont_obj.reference_ontology_id)

    # Deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, ref_ont_obj.reference_ontology_id)
