import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from agr_literature_service.api.crud.workflow_tag_crud import (
    create, destroy, patch, show, show_changesets)
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models import (WorkflowTagModel,
                                               ReferenceModel)
from agr_literature_service.api.schemas import WorkflowTagSchemaCreate
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

    reference = ReferenceSchemaPost(title="Bob1", category="thesis", abstract="3", language="MadeUp")
    res = reference_create(db, reference)
    refs.append(res)
    reference = ReferenceSchemaPost(title="Bob2", category="thesis", abstract="3", language="MadeUp")
    res = reference_create(db, reference)
    refs.append(res)


def test_get_bad_ref_ont():
    with pytest.raises(HTTPException):
        show(db, 99999)


def test_bad_missing_args():
    global refs

    xml = {'reference_curie': refs[0],
           'workflow_tag_id': "ont1",
           'created_by': "017_Bob"}
    with pytest.raises(ValidationError):  # mod_abbr missing
        ref_ont_schema = WorkflowTagSchemaCreate(**xml)
        create(db, ref_ont_schema)

    xml = {'reference_curie': refs[0],
           'mod_abbreviation': "017_FB",
           'created_by': "017_Bob"}
    with pytest.raises(ValidationError):  # workflow_tag_id missing
        ref_ont_schema = WorkflowTagSchemaCreate(**xml)
        create(db, ref_ont_schema)

    xml = {'mod_abbreviation': "017_FB",
           'workflow_tag_id': "ont1",
           'created_by': "017_Bob"}
    with pytest.raises(ValidationError):  # ref_cur missing
        ref_ont_schema = WorkflowTagSchemaCreate(**xml)
        create(db, ref_ont_schema)


def test_good_blank_args():
    global refs
    xml = {'mod_abbreviation': "",
           'workflow_tag_id': "ont tgma",
           'reference_curie': refs[1],
           'created_by': "017_Bob"}
    ref_ont_schema = WorkflowTagSchemaCreate(**xml)
    create(db, ref_ont_schema)

    # check results in database
    ref_ont_obj = db.query(WorkflowTagModel).\
        join(ReferenceModel,
             WorkflowTagModel.reference_id == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == refs[1]).one()
    assert ref_ont_obj.workflow_tag_id == "ont tgma"
    assert ref_ont_obj.created_by == "017_Bob"
    assert ref_ont_obj.mod_id == None

    res = show(db, ref_ont_obj.reference_workflow_tag_id)
    assert res["workflow_tag_id"] == "ont tgma"
    assert res["created_by"] == "017_Bob"
    assert res["mod_abbreviation"] == ""


def test_create_ref_ont():
    global fb_mod
    global refs
    xml = {'reference_curie': refs[0],
           'mod_abbreviation': "017_FB",
           'workflow_tag_id': "ont1",
           'created_by': "017_Bob"}
    ref_ont_schema = WorkflowTagSchemaCreate(**xml)
    create(db, ref_ont_schema)

    # check results in database
    ref_ont_obj = db.query(WorkflowTagModel).\
        join(ReferenceModel,
             WorkflowTagModel.reference_id == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == refs[0]).one()
    assert ref_ont_obj.workflow_tag_id == "ont1"
    assert ref_ont_obj.created_by == "017_Bob"
    assert ref_ont_obj.mod_id == fb_mod


def test_patch_ref_ont():
    global refs
    ref_ont_obj: WorkflowTagModel = db.query(WorkflowTagModel).\
        join(ReferenceModel,
             WorkflowTagModel.reference_id == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == refs[0]).one()

    # change workflow_tag
    xml = {'workflow_tag_id': 'ont test patch',
           'updated_by': '017_Bob',
           'mod_abbreviation': "017_RGD"}

    res = patch(db, ref_ont_obj.reference_workflow_tag_id, xml)
    assert res == {"message": "updated"}

    ref_ont_obj: WorkflowTagModel = db.query(WorkflowTagModel).\
        filter(WorkflowTagModel.workflow_tag_id == ref_ont_obj.workflow_tag_id).one()
    assert ref_ont_obj.reference.curie == refs[0]
    assert ref_ont_obj.workflow_tag_id == "ont test patch"


def test_show_ref_ont():
    ref_ont_obj: WorkflowTagModel = db.query(WorkflowTagModel).\
        join(ReferenceModel,
             WorkflowTagModel.reference_id == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == refs[0]).one()
    res = show(db, ref_ont_obj.reference_workflow_tag_id)

    assert res['reference_curie'] == refs[0]
    assert res['workflow_tag_id'] == 'ont test patch'
    assert res['mod_abbreviation'] == '017_FB'
    assert res['created_by'] == '017_Bob'


def test_changesets():
    ref_ont_obj: WorkflowTagModel = db.query(WorkflowTagModel).\
        join(ReferenceModel,
             WorkflowTagModel.reference_id == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == refs[0]).one()
    res = show_changesets(db, ref_ont_obj.reference_workflow_tag_id)

    for transaction in res:
        print("Test changesets 017: {}".format(transaction))
        if 'reference_workflow_tag_id' in transaction['changeset']:
            assert transaction['changeset']['workflow_tag_id'][1] == 'ont1'
            assert transaction['changeset']['mod_id'][0] == None
        else:
            assert transaction['changeset']['workflow_tag_id'][0] == 'ont1'
            assert transaction['changeset']['workflow_tag_id'][1] == 'ont test patch'


def test_destroy_ref_ont():
    ref_ont_obj: WorkflowTagModel = db.query(WorkflowTagModel).\
        join(ReferenceModel,
             WorkflowTagModel.reference_id == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == refs[0]).one()
    destroy(db, ref_ont_obj.reference_workflow_tag_id)

    # It should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, ref_ont_obj.reference_workflow_tag_id)

    # Deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, ref_ont_obj.reference_workflow_tag_id)
