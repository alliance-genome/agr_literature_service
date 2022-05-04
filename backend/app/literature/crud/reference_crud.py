"""
reference_crud.py
=================
"""
import logging
from datetime import datetime
from typing import Any, Dict, List

import sqlalchemy
from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import ARRAY, Boolean, String, func
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import cast

from literature.crud import (cross_reference_crud,
                             reference_comment_and_correction_crud)
from literature.crud.reference_resource import create_obj
from literature.models import (AuthorModel, CrossReferenceModel,
                               MeshDetailModel, ModReferenceTypeModel,
                               ReferenceModel,
                               ResourceModel)
from literature.schemas import ReferenceSchemaPost, ReferenceSchemaUpdate
from literature.crud.mod_corpus_association_crud import create as create_mod_corpus_association


logger = logging.getLogger(__name__)


def create_next_curie(curie) -> str:
    """

    :param curie:
    :return:
    """

    curie_parts = curie.rsplit("-", 1)
    number_part = curie_parts[1]
    number = int(number_part) + 1

    return "-".join([curie_parts[0], str(number).rjust(10, "0")])


def create(db: Session, reference: ReferenceSchemaPost): # noqa
    """

    :param db:
    :param reference:
    :return:
    """

    logger.debug("creating reference")
    logger.debug(reference)
    add_separately_fields = ["mod_corpus_associations"]
    list_fields = ["authors", "mod_reference_types", "tags", "mesh_terms", "cross_references"]
    remap = {'authors': 'author',
             'mesh_terms': 'mesh_term',
             'cross_references': 'cross_reference',
             'mod_reference_types': 'mod_reference_type'}
    reference_data = {}  # type: Dict[str, Any]

    if reference.cross_references:
        for cross_reference in reference.cross_references:
            if db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == cross_reference.curie).first():
                raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                                    detail=f"CrossReference with id {cross_reference.curie} already exists")
    logger.debug("done x ref")
    last_curie = db.query(ReferenceModel.curie).order_by(sqlalchemy.desc(ReferenceModel.curie)).first()

    if not last_curie:
        last_curie = "AGR:AGR-Reference-0000000000"
    else:
        last_curie = last_curie[0]
    logger.debug("done last curie")

    curie = create_next_curie(last_curie)
    reference_data["curie"] = curie

    for field, value in vars(reference).items():
        if value is None:
            continue
        logger.debug("processing {} {}".format(field, value))
        if field in list_fields:
            db_objs = []
            for obj in value:
                obj_data = jsonable_encoder(obj)
                db_obj = None
                if field in ["authors"]:
                    if obj_data["orcid"]:
                        cross_reference_obj = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == obj_data["orcid"]).first()
                        if not cross_reference_obj:
                            cross_reference_obj = CrossReferenceModel(curie=obj_data["orcid"])
                            db.add(cross_reference_obj)

                        obj_data["orcid_cross_reference"] = cross_reference_obj
                    del obj_data["orcid"]
                    db_obj = create_obj(db, AuthorModel, obj_data, non_fatal=True)
                elif field == "mod_reference_types":
                    db_obj = ModReferenceTypeModel(**obj_data)
                elif field == "mesh_terms":
                    db_obj = MeshDetailModel(**obj_data)
                elif field == "cross_references":
                    db_obj = CrossReferenceModel(**obj_data)
                db.add(db_obj)
                db_objs.append(db_obj)
            if field in remap:
                reference_data[remap[field]] = db_objs
            else:
                reference_data[field] = db_objs
        elif field == "resource":
            resource = db.query(ResourceModel).filter(ResourceModel.curie == value).first()
            if not resource:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail=f"Resource with curie {value} does not exist")
            reference_data["resource"] = resource
        elif field == "merged_into_reference_curie":
            merged_into_obj = db.query(ReferenceModel).filter(ReferenceModel.curie == value).first()
            if not merged_into_obj:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail=f"Merged_into Reference with curie {value} does not exist")
            reference_data["merged_into_reference"] = merged_into_obj
        elif field in add_separately_fields:
            continue
        else:
            reference_data[field] = value
        logger.debug("finished processing {} {}".format(field, value))

    logger.debug("add reference")
    reference_db_obj = ReferenceModel(**reference_data)
    logger.debug("have model, save to db")
    db.add(reference_db_obj)
    logger.debug("saved")
    db.commit()

    for field, value in vars(reference).items():
        logger.debug("Porcessing mod corpus asso")
        if field == "mod_corpus_associations":
            if value is not None:
                for obj in value:
                    obj_data = jsonable_encoder(obj)
                    obj_data["reference_curie"] = curie
                    try:
                        create_mod_corpus_association(db, obj_data)
                    except HTTPException:
                        logger.warning("skipping mod corpus association to a mod that is already associated to "
                                       "the reference")
    logger.debug("returning successfully?")
    return curie


def destroy(db: Session, curie: str):
    """

    :param db:
    :param curie:
    :return:
    """

    reference = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with curie {curie} not found")
    db.delete(reference)
    db.commit()

    return None


def patch(db: Session, curie: str, reference_update: ReferenceSchemaUpdate) -> dict:
    """

    :param db:
    :param curie:
    :param reference_update:
    :return:
    """

    reference_data = jsonable_encoder(reference_update)
    reference_db_obj = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).first()

    if not reference_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with curie {curie} not found")

    for field, value in reference_data.items():
        if field == "resource" and value:
            resource_curie = value
            resource = db.query(ResourceModel).filter(ResourceModel.curie == resource_curie).first()
            if not resource:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail=f"Resource with curie {resource_curie} does not exist")
            reference_db_obj.resource = resource
        elif field == "merged_into_reference_curie" and value:
            merged_into_obj = db.query(ReferenceModel).filter(ReferenceModel.curie == value).first()
            if not merged_into_obj:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail=f"Merged_into Reference with curie {value} does not exist")
            reference_db_obj.merged_into_reference = merged_into_obj
        elif field == "merged_into_reference_curie":
            reference_db_obj.merged_into_reference = None
        else:
            setattr(reference_db_obj, field, value)

    reference_db_obj.dateUpdated = datetime.utcnow()
    db.commit()

    return {"message": "updated"}


def show_all_references_external_ids(db: Session):
    """

    :param db:
    :return:
    """

    references_query = db.query(ReferenceModel.curie,
                                cast(func.array_agg(CrossReferenceModel.curie),
                                     ARRAY(String)),
                                cast(func.array_agg(CrossReferenceModel.is_obsolete),
                                     ARRAY(Boolean))) \
        .outerjoin(ReferenceModel.cross_reference) \
        .group_by(ReferenceModel.curie)

    return [{"curie": reference[0],
             "cross_references": [{"curie": reference[1][idx],
                                   "is_obsolete": reference[2][idx]}
                                  for idx in range(len(reference[1]))]}
            for reference in references_query.all()]


def show(db: Session, curie: str, http_request=True):  # noqa
    """

    :param db:
    :param curie:
    :param http_request:
    :return:
    """

    logger.debug("BOB: fetching reference '{}'".format(curie))
    try:
        reference = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).one_or_none()
    except Exception as e:
        logger.debug("BOB: lookup failed '{}' raising http exception instead".format(e))
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the id {curie} is not available")
        return None

    logger.debug("BOB: Reference is {}".format(reference))
    if not reference:
        if http_request:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"Reference with the id {curie} is not available")
        else:
            return None

    reference_data = jsonable_encoder(reference)
    logger.debug("BOB: Post encoding:- {}".format(reference_data))
    if reference.resource_id:
        reference_data["resource_curie"] = db.query(ResourceModel.curie).filter(ResourceModel.resource_id == reference.resource_id).first()[0]
        reference_data["resource_title"] = db.query(ResourceModel.title).filter(ResourceModel.resource_id == reference.resource_id).first()[0]

    if reference.cross_reference:
        cross_references = []
        for cross_reference in reference.cross_reference:
            cross_reference_show = jsonable_encoder(cross_reference_crud.show(db, cross_reference.curie))
            del cross_reference_show["reference_curie"]
            cross_references.append(cross_reference_show)
        reference_data["cross_references"] = cross_references
        # del reference_data["cross_reference"]

    if reference.mod_reference_type:
        mrt = []
        for mod_reference_type in reference_data["mod_reference_type"]:
            del mod_reference_type["reference_id"]
            mrt.append(mod_reference_type)
        reference_data['mod_reference_types'] = mrt
        # del reference_data['mod_reference_type']

    if reference.mod_corpus_association:
        for i in range(len(reference_data["mod_corpus_association"])):
            del reference_data["mod_corpus_association"][i]["reference_id"]
            reference_data["mod_corpus_association"][i]["mod_abbreviation"] = reference_data[
                "mod_corpus_association"][i]["mod"]["abbreviation"]
            del reference_data["mod_corpus_association"][i]["mod"]
            del reference_data["mod_corpus_association"][i]["mod_id"]
        reference_data["mod_corpus_associations"] = reference_data["mod_corpus_association"]
        del reference_data["mod_corpus_association"]

    if reference.mesh_term:
        for mesh_term in reference_data["mesh_term"]:
            del mesh_term["reference_id"]
            # add mesh_terms
        reference_data['mesh_terms'] = reference_data['mesh_term']

    if reference.author:
        authors = []
        for author in reference_data["author"]:
            if author["orcid"]:
                author["orcid"] = jsonable_encoder(cross_reference_crud.show(db, author["orcid"]))
            del author["orcid_cross_reference"]
            del author["reference_id"]
            authors.append(author)
        reference_data['authors'] = authors
        del reference_data['author']

    if reference.merged_into_id:
        reference_data["merged_into_reference_curie"] = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == reference_data["merged_into_id"]).first()[0]

    if reference.mergee_references:
        reference_data["merged_reference_curies"] = [mergee.curie for mergee in reference.mergee_references]

    comment_and_corrections_data = {"to_references": [], "from_references": []}  # type: Dict[str, List[str]]
    for comment_and_correction in reference.comment_and_corrections_out:
        comment_and_correction_data = reference_comment_and_correction_crud.show(db, comment_and_correction.reference_comment_and_correction_id)
        del comment_and_correction_data["reference_curie_from"]
        comment_and_corrections_data["to_references"].append(comment_and_correction_data)
    for comment_and_correction in reference.comment_and_corrections_in:
        comment_and_correction_data = reference_comment_and_correction_crud.show(db, comment_and_correction.reference_comment_and_correction_id)
        del comment_and_correction_data["reference_curie_to"]
        comment_and_corrections_data["from_references"].append(comment_and_correction_data)

    reference_data["comment_and_corrections"] = comment_and_corrections_data
    logger.debug("returning {}".format(reference_data))
    return reference_data


def show_changesets(db: Session, curie: str):
    """

    :param db:
    :param curie:
    :return:
    """

    reference = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the id {curie} is not available")
    history = []
    for version in reference.versions:
        tx = version.transaction
        history.append({"transaction": {"id": tx.id,
                                        "issued_at": tx.issued_at,
                                        "user_id": tx.user_id},
                        "changeset": version.changeset})

    return history
