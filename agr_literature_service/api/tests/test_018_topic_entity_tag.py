import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from agr_literature_service.api.crud.topic_entity_tag_crud import (
    create, show, patch, destroy
)
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models import (TopicEntityTagModel,
                                               TopicEntityTagPropModel)
from agr_literature_service.api.schemas import (
    TopicEntityTagSchemaCreate,
    TopicEntityTagSchemaUpdate)
from agr_literature_service.api.crud.mod_crud import create as mod_create
from agr_literature_service.api.crud.user_crud import create as user_create
from agr_literature_service.api.crud.reference_crud import create as reference_create
from agr_literature_service.api.schemas import ReferenceSchemaPost

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
refs = []


def test_initialise():
    global fb_mod
    global refs
    # add User "017 Bob"
    user_create(db, "018_Bob")

    # add mods
    data = {
        "abbreviation": '018_FB',
        "short_name": "018_FB",
        "full_name": "018_ont_1"
    }
    fb_mod = mod_create(db, data)

    data = {
        "abbreviation": '018_RGD',
        "short_name": "018_Rat",
        "full_name": "018_ont_2"
    }
    mod_create(db, data)

    for title in ['Bob 018 1', 'Bob 018 2', 'Bob 018 3']:
        reference = ReferenceSchemaPost(title=title, category="thesis", abstract="3", language="MadeUp")
        res = reference_create(db, reference)
        refs.append(res)


def test_good_create_with_props():
    xml = {
        "reference_curie": refs[0],
        "topic": "Topic1",
        "entity_type": "Gene",
        "alliance_entity": "Bob_gene_name",
        "taxon": 1234,
        "note": "Some Note",
        "created_by": "018_Bob",
        "props": [{"qualifier": "Quali1"},
                  {"qualifier": "Quali2"}]
    }
    schema = TopicEntityTagSchemaCreate(**xml)

    tet_id = create(db, schema)

    tet_obj = db.query(TopicEntityTagModel).\
        filter(TopicEntityTagModel.topic_entity_tag_id == tet_id).first()

    # assert tet_obj.reference_id == refs[0].reference_id
    assert tet_obj.topic == "Topic1"
    assert tet_obj.entity_type == "Gene"
    assert tet_obj.alliance_entity == "Bob_gene_name"
    assert tet_obj.taxon == 1234
    assert tet_obj.note == "Some Note"

    props = db.query(TopicEntityTagPropModel).\
        filter(TopicEntityTagPropModel.topic_entity_tag_id == tet_id).all()

    count = 0
    for prop in props:
        if prop.qualifier == 'Quali1':
            count += 1
        elif prop.qualifier == "Quali2":
            count += 1
        else:
            assert "Diff qualifier" == prop.qualifier
    assert count == 2

    res = show(db, tet_id)
    assert res["topic"] == "Topic1"
    assert res["props"][0]["qualifier"] == 'Quali1'
    assert res["props"][0]["created_by"] == '018_Bob'
    assert res["props"][1]["qualifier"] == 'Quali2'


def test_create_bad():
    xml = {
        "reference_curie": refs[0],
        "topic": "Topic1",
        "entity_type": "Gene",
        "taxon": 1234,
        "created_by": "018_Bob",
    }
    # No Entitys
    with pytest.raises(HTTPException) as excinfo:
        schema = TopicEntityTagSchemaCreate(**xml)
        create(db, schema)
    assert "One of the XXXX_entity's MUST be set" in str(excinfo)

    # More than one Entity
    xml["alliance_entity"] = "Bob_gene_name 1"
    xml["mod_entity"] = "Bob_gene_name 2"
    with pytest.raises(HTTPException) as excinfo:
        schema = TopicEntityTagSchemaCreate(**xml)
        create(db, schema)
    assert "ONLY one of the XXXX_entity's MUST be set" in str(excinfo)

    # No curie
    del xml["mod_entity"]
    del xml["reference_curie"]
    with pytest.raises(ValidationError) as excinfo:
        schema = TopicEntityTagSchemaCreate(**xml)
        create(db, schema)
    assert "value_error.missing" in str(excinfo)

    # Bad curie
    xml["reference_curie"] = "BADCURIE"
    with pytest.raises(HTTPException) as excinfo:
        schema = TopicEntityTagSchemaCreate(**xml)
        create(db, schema)
    assert "Reference with curie BADCURIE does not exist" in str(excinfo)

    # No species
    del xml["taxon"]
    xml["reference_curie"] = refs[0]
    with pytest.raises(ValidationError) as excinfo:
        schema = TopicEntityTagSchemaCreate(**xml)
        create(db, schema)
    assert "value_error.missing" in str(excinfo)


def test_patch_with_props():
    xml = {
        "reference_curie": refs[1],
        "topic": "Topic2",
        "entity_type": "Gene2",
        "alliance_entity": "Bob_gene_name 2",
        "taxon": 2345,
        "note": "Some Note",
        "created_by": "018_Bob",
        "props": [{"qualifier": "Quali1"},
                  {"qualifier": "Quali2"}]
    }
    schema = TopicEntityTagSchemaCreate(**xml)

    tet_id = create(db, schema)
    res = show(db, tet_id)
    assert res["reference_curie"] == refs[1]

    # change the reference
    xml = {
        "reference_curie": refs[0],
    }
    schema = TopicEntityTagSchemaUpdate(**xml)
    patch(db, tet_id, schema)

    res = show(db, tet_id)
    assert res["reference_curie"] == refs[0]

    # Change the note
    xml = {
        "note": ""
    }
    schema = TopicEntityTagSchemaUpdate(**xml)
    patch(db, tet_id, schema)

    res = show(db, tet_id)
    assert res["note"] == ""

    # Change the note
    xml = {
        "note": None
    }
    schema = TopicEntityTagSchemaUpdate(**xml)
    patch(db, tet_id, schema)

    res = show(db, tet_id)
    assert res["note"] == None


def test_delete_with_props():
    xml = {
        "reference_curie": refs[2],
        "topic": "Topic3",
        "entity_type": "Gene3",
        "alliance_entity": "Bob_gene_name 3",
        "taxon": 3456,
        "note": "Some Note or other",
        "created_by": "018_Bob",
        "props": [{"qualifier": "Quali5"},
                  {"qualifier": "Quali6"}]
    }
    schema = TopicEntityTagSchemaCreate(**xml)
    tet_id = create(db, schema)
    res = show(db, tet_id)

    assert res["props"][0]["qualifier"] == "Quali5"
    p1_id = res["props"][0]["topic_entity_tag_prop_id"]

    # Delete the topic entity tag
    destroy(db, tet_id)

    # Make sure it is no longer there
    with pytest.raises(NoResultFound):
        db.query(TopicEntityTagModel).filter(TopicEntityTagModel.topic_entity_tag_id == tet_id).one()

    # Check the prop is no longer there.
    with pytest.raises(NoResultFound):
        db.query(TopicEntityTagPropModel).filter(TopicEntityTagPropModel.topic_entity_tag_prop_id == p1_id).one()
