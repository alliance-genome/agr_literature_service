import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from agr_literature_service.api.crud.topic_entity_tag_crud import (
    create
    # create, destroy, patch, show, show_changesets)
)
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models import (TopicEntityTagModel,
                                               TopicEntityTagPropModel,
                                               ReferenceModel)
from agr_literature_service.api.schemas import TopicEntityTagSchemaCreate
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

    reference = ReferenceSchemaPost(title="Bob 018 1", category="thesis", abstract="3", language="MadeUp")
    res = reference_create(db, reference)
    refs.append(res)
    reference = ReferenceSchemaPost(title="Bob 018 2", category="thesis", abstract="3", language="MadeUp")
    res = reference_create(db, reference)
    refs.append(res)


def test_good_with_props():
    xml = {
        "reference_curie": refs[0],
        "topic": "Topic1",
        "entity_type": "Gene",
        "alliance_entity": "Bob_gene_name",
        "species_id": 1234,
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
    assert tet_obj.species_id == 1234
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
