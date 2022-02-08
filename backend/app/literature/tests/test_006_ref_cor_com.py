import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from literature.crud.reference_comment_and_correction_crud import (
    create, destroy, patch, show, show_changesets)
from literature.database.config import SQLALCHEMY_DATABASE_URL
from literature.models import (Base, ReferenceCommentAndCorrectionModel,
                               ReferenceModel)
from literature.schemas import ReferenceCommentAndCorrectionSchemaPost

metadata = MetaData()

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
SessionLocal = sessionmaker(bind=engine, autoflush=True)
db = SessionLocal()

# Add tables/schema if not already there.
Base.metadata.create_all(engine)

# Exit if this is not a test database, Exit.
if "literature-test" not in SQLALCHEMY_DATABASE_URL:
    exit(-1)


def test_get_bad_rcc():
    with pytest.raises(HTTPException):
        show(db, 99999)


def test_bad_missing_args():
    xml = {'reference_curie_from': "AGR:AGR-Reference-0000000001",
           'reference_comment_and_correction_type': "CommentOn"}
    with pytest.raises(HTTPException):  # ref_cur_to missing
        rcc_schema = ReferenceCommentAndCorrectionSchemaPost(**xml)
        create(db, rcc_schema)

    xml = {'reference_curie_to': "AGR:AGR-Reference-0000000001",
           'reference_comment_and_correction_type': "CommentOn"}
    with pytest.raises(HTTPException):  # ref_cur_to missing
        rcc_schema = ReferenceCommentAndCorrectionSchemaPost(**xml)
        create(db, rcc_schema)

    xml = {'reference_curie_from': "AGR:AGR-Reference-0000000001",
           'reference_curie_to': "AGR:AGR-Reference-0000000003"}
    with pytest.raises(ValidationError):  # ref_cur_to missing
        rcc_schema = ReferenceCommentAndCorrectionSchemaPost(**xml)
        create(db, rcc_schema)


def test_create_rcc():
    xml = {'reference_curie_from': "AGR:AGR-Reference-0000000001",
           'reference_curie_to': "AGR:AGR-Reference-0000000003",
           'reference_comment_and_correction_type': "CommentOn"}
    rcc_schema = ReferenceCommentAndCorrectionSchemaPost(**xml)
    res = create(db, rcc_schema)
    assert res == 1

    # check results in database
    rcc_obj = db.query(ReferenceCommentAndCorrectionModel).\
        join(ReferenceModel,
             ReferenceCommentAndCorrectionModel.reference_id_from == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == "AGR:AGR-Reference-0000000001").one()
    assert rcc_obj.reference_to.curie == "AGR:AGR-Reference-0000000003"
    assert rcc_obj.reference_comment_and_correction_type == "CommentOn"


def test_patch_rcc():
    rcc_obj: ReferenceCommentAndCorrectionModel = db.query(ReferenceCommentAndCorrectionModel).\
        join(ReferenceModel,
             ReferenceCommentAndCorrectionModel.reference_id_from == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == "AGR:AGR-Reference-0000000001").one()

    # swap to and from and change correction type
    xml = {'reference_curie_from': "AGR:AGR-Reference-0000000003",
           'reference_curie_to': "AGR:AGR-Reference-0000000001",
           'reference_comment_and_correction_type': "ReprintOf"}

    res = patch(db, rcc_obj.reference_comment_and_correction_id, xml)
    assert res == {"message": "updated"}

    rcc_obj: ReferenceCommentAndCorrectionModel = db.query(ReferenceCommentAndCorrectionModel).\
        filter(ReferenceCommentAndCorrectionModel.reference_comment_and_correction_id == rcc_obj.reference_comment_and_correction_id).one()
    assert rcc_obj.reference_to.curie == "AGR:AGR-Reference-0000000001"
    assert rcc_obj.reference_from.curie == "AGR:AGR-Reference-0000000003"
    assert rcc_obj.reference_comment_and_correction_type == "ReprintOf"


def test_show_rcc():
    rcc_obj: ReferenceCommentAndCorrectionModel = db.query(ReferenceCommentAndCorrectionModel).\
        join(ReferenceModel,
             ReferenceCommentAndCorrectionModel.reference_id_from == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == "AGR:AGR-Reference-0000000003").one()
    res = show(db, rcc_obj.reference_comment_and_correction_id)

    assert res['reference_curie_to'] == "AGR:AGR-Reference-0000000001"
    assert res['reference_curie_from'] == 'AGR:AGR-Reference-0000000003'
    assert res['reference_comment_and_correction_type'] == "ReprintOf"


def test_changesets():
    rcc_obj: ReferenceCommentAndCorrectionModel = db.query(ReferenceCommentAndCorrectionModel).\
        join(ReferenceModel,
             ReferenceCommentAndCorrectionModel.reference_id_from == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == "AGR:AGR-Reference-0000000003").one()
    res = show_changesets(db, rcc_obj.reference_comment_and_correction_id)

    # reference_id_from      : None -> 1 -> 3
    # reference_id_to        : None -> 3 -> 1

    for transaction in res:
        print(transaction)
        if not transaction['changeset']['reference_id_from'][0]:
            assert transaction['changeset']['reference_id_from'][1] == 1
            assert transaction['changeset']['reference_id_to'][1] == 3
            assert transaction['changeset']['reference_comment_and_correction_type'][1] == "CommentOn"
        else:
            assert transaction['changeset']['reference_id_from'][1] == 3
            assert transaction['changeset']['reference_id_to'][1] == 1
            assert transaction['changeset']['reference_comment_and_correction_type'][1] == "ReprintOf"


def test_destroy_rcc():
    rcc_obj: ReferenceCommentAndCorrectionModel = db.query(ReferenceCommentAndCorrectionModel).\
        join(ReferenceModel,
             ReferenceCommentAndCorrectionModel.reference_id_from == ReferenceModel.reference_id).\
        filter(ReferenceModel.curie == "AGR:AGR-Reference-0000000003").one()
    destroy(db, rcc_obj.reference_comment_and_correction_id)

    # It should now give an error on lookup.
    with pytest.raises(HTTPException):
        show(db, rcc_obj.reference_comment_and_correction_id)

    # Deleting it again should give an error as the lookup will fail.
    with pytest.raises(HTTPException):
        destroy(db, rcc_obj.reference_comment_and_correction_id)
