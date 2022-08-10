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
from agr_literature_service.api.tests import utils
from agr_literature_service.api.user import get_global_user_id

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

# Add tables/schema if not already there.
Base.metadata.create_all(engine)

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)


(refs, ress, mods, okta_user) = utils.initialise(db, '017')
print(okta_user)
print("okta is now {}".format(get_global_user_id()))


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
    global okta_user
    print(okta_user)
    print("okta is now {}".format(get_global_user_id()))
    xml = {'mod_abbreviation': "",
           'workflow_tag_id': "ont tgba",
           'reference_curie': refs[2]}
    ref_ont_schema = WorkflowTagSchemaCreate(**xml)
    rwt_id = create(db, ref_ont_schema)

    # check results in database
    ref_ont_obj = db.query(WorkflowTagModel).\
        filter(WorkflowTagModel.reference_workflow_tag_id == rwt_id).one()
    assert ref_ont_obj.workflow_tag_id == "ont tgba"
    # check okta users are being added by default
    print(okta_user)
    print("okta is now {}".format(get_global_user_id()))
    assert ref_ont_obj.created_by is not None  # == okta_user
    assert not ref_ont_obj.mod_id

    res = show(db, ref_ont_obj.reference_workflow_tag_id)
    assert res["workflow_tag_id"] == "ont tgba"
    # This needs investigating........ as okta_user changes???
    # assert res["created_by"] == okta_user
    assert res["mod_abbreviation"] == ""


def test_create_ref_ont():
    global mods
    global refs
    xml = {'reference_curie': refs[0],
           'mod_abbreviation': mods[0],
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


def test_patch_ref_ont():
    global refs
    global mods
    ref_ont_obj: WorkflowTagModel = db.query(WorkflowTagModel).\
        join(ReferenceModel,
             WorkflowTagModel.reference_id == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == refs[0]).one()

    # change workflow_tag
    xml = {'workflow_tag_id': 'ont test patch',
           'updated_by': '017_Bob',
           'mod_abbreviation': mods[1]}

    res = patch(db, ref_ont_obj.reference_workflow_tag_id, xml)
    assert res == {"message": "updated"}

    ref_ont_obj: WorkflowTagModel = db.query(WorkflowTagModel).\
        filter(WorkflowTagModel.workflow_tag_id == ref_ont_obj.workflow_tag_id).one()
    assert ref_ont_obj.reference.curie == refs[0]
    assert ref_ont_obj.workflow_tag_id == "ont test patch"


def test_show_ref_ont():
    global refs
    global mods
    ref_ont_obj: WorkflowTagModel = db.query(WorkflowTagModel).\
        join(ReferenceModel,
             WorkflowTagModel.reference_id == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == refs[0]).one()
    res = show(db, ref_ont_obj.reference_workflow_tag_id)

    assert res['reference_curie'] == refs[0]
    assert res['workflow_tag_id'] == 'ont test patch'
    assert res['mod_abbreviation'] == mods[0]
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
            assert not transaction['changeset']['mod_id'][0]
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
