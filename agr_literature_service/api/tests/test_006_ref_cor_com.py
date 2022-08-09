import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from agr_literature_service.api.crud.reference_comment_and_correction_crud import (
    create, destroy, patch, show, show_changesets)
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models import (ReferenceCommentAndCorrectionModel,
                                               ReferenceModel)
from agr_literature_service.api.schemas import (ReferenceCommentAndCorrectionSchemaPost)
from agr_literature_service.api.tests import utils

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

# Add tables/schema if not already there.
Base.metadata.create_all(engine)

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)

(refs, ress, mods) = utils.initialise(db, '006')


def test_get_bad_rcc():
    with pytest.raises(HTTPException):
        show(db, 99999)


def test_bad_missing_args():
    global refs
    xml = {'reference_curie_from': refs[0],
           'reference_comment_and_correction_type': "CommentOn"}
    with pytest.raises(ValidationError):  # ref_cur_to missing
        rcc_schema = ReferenceCommentAndCorrectionSchemaPost(**xml)
        create(db, rcc_schema)

    xml = {'reference_curie_to': refs[0],
           'reference_comment_and_correction_type': "CommentOn"}
    with pytest.raises(ValidationError):  # ref_cur_to missing
        rcc_schema = ReferenceCommentAndCorrectionSchemaPost(**xml)
        create(db, rcc_schema)

    xml = {'reference_curie_from': refs[0],
           'reference_curie_to': refs[1]}
    with pytest.raises(ValidationError):  # ref_cur_to missing
        rcc_schema = ReferenceCommentAndCorrectionSchemaPost(**xml)
        create(db, rcc_schema)


def test_create_rcc():
    global refs
    xml = {'reference_curie_from': refs[0],
           'reference_curie_to': refs[1],
           'reference_comment_and_correction_type': "CommentOn"}
    rcc_schema = ReferenceCommentAndCorrectionSchemaPost(**xml)
    res = create(db, rcc_schema)
    assert res == 1

    # check results in database
    rcc_obj = db.query(ReferenceCommentAndCorrectionModel).\
        join(ReferenceModel,
             ReferenceCommentAndCorrectionModel.reference_id_from == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == refs[0]).one()
    assert rcc_obj.reference_to.curie == refs[1]
    assert rcc_obj.reference_comment_and_correction_type == "CommentOn"


def test_patch_rcc():
    global refs
    rcc_obj: ReferenceCommentAndCorrectionModel = db.query(ReferenceCommentAndCorrectionModel).\
        join(ReferenceModel,
             ReferenceCommentAndCorrectionModel.reference_id_from == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == refs[0]).one()

    # swap to and from and change correction type
    xml = {'reference_curie_from': refs[1],
           'reference_curie_to': refs[0],
           'reference_comment_and_correction_type': "ReprintOf"}

    res = patch(db, rcc_obj.reference_comment_and_correction_id, xml)
    assert res == {"message": "updated"}

    rcc_obj: ReferenceCommentAndCorrectionModel = db.query(ReferenceCommentAndCorrectionModel).\
        filter(ReferenceCommentAndCorrectionModel.reference_comment_and_correction_id == rcc_obj.reference_comment_and_correction_id).one()
    assert rcc_obj.reference_to.curie == refs[0]
    assert rcc_obj.reference_from.curie == refs[1]
    assert rcc_obj.reference_comment_and_correction_type == "ReprintOf"


def test_show_rcc():
    global refs
    rcc_obj: ReferenceCommentAndCorrectionModel = db.query(ReferenceCommentAndCorrectionModel).\
        join(ReferenceModel,
             ReferenceCommentAndCorrectionModel.reference_id_from == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == refs[1]).one()
    res = show(db, rcc_obj.reference_comment_and_correction_id)

    assert res['reference_curie_to'] == refs[0]
    assert res['reference_curie_from'] == refs[1]
    assert res['reference_comment_and_correction_type'] == "ReprintOf"


def test_changesets():
    global refs
    rcc_obj: ReferenceCommentAndCorrectionModel = db.query(ReferenceCommentAndCorrectionModel).\
        join(ReferenceModel,
             ReferenceCommentAndCorrectionModel.reference_id_from == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == refs[1]).one()
    res = show_changesets(db, rcc_obj.reference_comment_and_correction_id)

    # reference_id_from      : None -> orig -> new
    from_id = db.query(ReferenceModel).filter(ReferenceModel.curie == refs[0]).one().reference_id
    # reference_id_to        : None -> new -> orig
    to_id = db.query(ReferenceModel).filter(ReferenceModel.curie == refs[1]).one().reference_id
    for transaction in res:
        print(transaction)
        print("from {}, to {}".format(from_id, to_id))
        if not transaction['changeset']['reference_id_from'][0]:
            assert transaction['changeset']['reference_id_from'][1] == from_id
            assert transaction['changeset']['reference_id_to'][1] == to_id
            assert transaction['changeset']['reference_comment_and_correction_type'][1] == "CommentOn"
        else:
            assert transaction['changeset']['reference_id_from'][1] == to_id
            assert transaction['changeset']['reference_id_to'][1] == from_id
            assert transaction['changeset']['reference_comment_and_correction_type'][1] == "ReprintOf"


def test_destroy_rcc():
    global refs
    rcc_obj: ReferenceCommentAndCorrectionModel = db.query(ReferenceCommentAndCorrectionModel).\
        join(ReferenceModel,
             ReferenceCommentAndCorrectionModel.reference_id_from == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == refs[1]).one()
    destroy(db, rcc_obj.reference_comment_and_correction_id)

    # It should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, rcc_obj.reference_comment_and_correction_id)

    # Deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, rcc_obj.reference_comment_and_correction_id)
