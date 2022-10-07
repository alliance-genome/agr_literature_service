"""
reference_crud.py
=================
"""
import logging
import math
from datetime import datetime
from typing import Any, Dict, List
import re


import sqlalchemy
from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import ARRAY, Boolean, String, func
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import cast
from sqlalchemy.orm.exc import NoResultFound

from agr_literature_service.api.crud import (cross_reference_crud,
                                             reference_comment_and_correction_crud)
from agr_literature_service.api.crud.reference_resource import create_obj
from agr_literature_service.api.models import (AuthorModel, CrossReferenceModel,
                                               MeshDetailModel,
                                               ObsoleteReferenceModel,
                                               ReferenceCommentAndCorrectionModel,
                                               ReferenceModel,
                                               ResourceModel, ModModel, ReferenceTypeModel,
                                               ModReferenceTypeAssociationModel,
                                               ReferenceModReferenceTypeAssociationModel)
from agr_literature_service.api.schemas import ReferenceSchemaPost, ModReferenceTypeSchemaCreate
from agr_literature_service.api.crud.mod_corpus_association_crud import create as create_mod_corpus_association
from agr_literature_service.api.crud.workflow_tag_crud import (
    create as create_workflow_tag,
    patch as update_workflow_tag,
    show as show_workflow_tag
)
from agr_literature_service.api.crud.topic_entity_tag_crud import (
    show as show_topic_entity_tag,
    patch as update_topic_entity_tag,
    create as create_topic_entity_tag
)

logger = logging.getLogger(__name__)


def insert_mod_reference_type(db_session, pubmed_types, mod_abbreviation, referencetype_label, reference_id):
    mod = db_session.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).one_or_none()
    ref_type = db_session.query(ReferenceTypeModel).filter(ReferenceTypeModel.label ==
                                                           referencetype_label).one_or_none()
    mrt = db_session.query(ModReferenceTypeAssociationModel).filter(
        ModReferenceTypeAssociationModel.mod == mod,
        ModReferenceTypeAssociationModel.referencetype == ref_type).one_or_none()
    if (ref_type is None or mrt is None) and mod.abbreviation == "SGD":
        if referencetype_label in set(pubmed_types):
            if ref_type is None:
                ref_type = ReferenceTypeModel(label=referencetype_label)
            max_display_order = max((mod_ref_type.display_order for mod_ref_type in mod.referencetypes),
                                    default=0)
            mrt = ModReferenceTypeAssociationModel(
                mod=mod, referencetype=ref_type,
                display_order=math.ceil(max_display_order / 10) * 10)
    rmrt = ReferenceModReferenceTypeAssociationModel(reference_id=reference_id, mod_referencetype=mrt)
    db_session.add(rmrt)


def get_next_curie(db: Session) -> str:
    """

    :param db:
    :return:
    """
    last_curie = db.query(ReferenceModel.curie).order_by(sqlalchemy.desc(ReferenceModel.curie)).first()

    if not last_curie:
        last_curie = "AGR:AGR-Reference-0000000000"
    else:
        last_curie = last_curie[0]

    curie_parts = last_curie.rsplit("-", 1)
    number_part = curie_parts[1]
    number = int(number_part)

    # So we need to check that a later one was not obsoleted as we
    # do not want to use that curie then.
    checked = False
    new_curie = ''
    while not checked:
        number += 1
        new_curie = "-".join([curie_parts[0], str(number).rjust(10, "0")])
        try:
            db.query(ObsoleteReferenceModel).filter(ObsoleteReferenceModel.curie == new_curie).one()
        except NoResultFound:
            checked = True
    logger.debug("created new curie {new_curie}")

    return new_curie


def create(db: Session, reference: ReferenceSchemaPost):  # noqa
    """

    :param db:
    :param reference:
    :return:
    """

    logger.debug("creating reference")
    logger.debug(reference)
    add_separately_fields = ["mod_corpus_associations", "workflow_tags", "topic_entity_tags", "mod_reference_types"]
    list_fields = ["authors", "tags", "mesh_terms", "cross_references"]
    remap = {'authors': 'author',
             'mesh_terms': 'mesh_term',
             'cross_references': 'cross_reference',
             'mod_reference_types': 'mod_reference_type'}
    reference_data = {}  # type: Dict[str, Any]
    author_names_order = []

    if reference.cross_references:
        for cross_reference in reference.cross_references:
            if db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == cross_reference.curie).first():
                raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                                    detail=f"CrossReference with id {cross_reference.curie} already exists")
    logger.debug("done x ref")
    curie = get_next_curie(db)
    reference_data["curie"] = curie

    for field, value in vars(reference).items():
        if value is None:
            continue
        logger.debug("processing {field} {value}")
        if field in list_fields:
            db_objs = []
            for obj in value:
                obj_data = jsonable_encoder(obj)
                db_obj = None
                if field in ["authors"]:
                    if obj_data["orcid"]:
                        cross_reference_obj = db.query(CrossReferenceModel).filter(
                            CrossReferenceModel.curie == obj_data["orcid"]).first()
                        if not cross_reference_obj:
                            cross_reference_obj = CrossReferenceModel(curie=obj_data["orcid"])
                            db.add(cross_reference_obj)

                        obj_data["orcid_cross_reference"] = cross_reference_obj
                    del obj_data["orcid"]
                    db_obj = create_obj(db, AuthorModel, obj_data, non_fatal=True)
                    if db_obj.name:
                        author_names_order.append((db_obj.name, db_obj.order))
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
    reference_data['citation'] = citation_from_data(reference_data,
                                                    "; ".join([x[0] for x in sorted(author_names_order,
                                                                                    key=lambda x: x[1])]))
    reference_db_obj = ReferenceModel(**reference_data)
    logger.debug("have model, save to db")
    db.add(reference_db_obj)
    logger.debug("saved")
    db.commit()
    db.refresh(reference_db_obj)
    for field, value in vars(reference).items():
        logger.debug("Processing mod corpus asso")
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
        elif field == "workflow_tags":
            if value is not None:
                for obj in value:
                    obj_data = jsonable_encoder(obj)
                    obj_data["reference_curie"] = curie
                    try:
                        if "reference_workflow_tag_id" in obj_data and obj_data["reference_workflow_tag_id"]:
                            update_workflow_tag(db, obj_data["reference_workflow_tag_id"], obj_data)
                        else:
                            create_workflow_tag(db, obj_data)
                    except HTTPException:
                        logger.warning("skipping workflow_tag to a mod that is already associated to "
                                       "the reference")
        elif field == "topic_entity_tags":
            if value is not None:
                for obj in value:
                    obj_data = jsonable_encoder(obj)
                    obj_data["reference_curie"] = curie
                    try:
                        if "reference_topic_entity_tag_id" in obj_data and obj_data["reference_topic_entity_tag_id"]:
                            update_topic_entity_tag(db, obj_data["reference_topic_entity_tag_id"], obj_data)
                        else:
                            create_topic_entity_tag(db, obj_data)
                    except HTTPException:
                        logger.warning("skipping topic_entity_tag as that is already associated to "
                                       "the reference")
        elif field == "mod_reference_types":
            if value is not None:
                for obj in value:
                    insert_mod_reference_type(db, reference.pubmed_types, obj.source, obj.reference_type,
                                              reference_db_obj.reference_id)
                db.commit()
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


def patch(db: Session, curie: str, reference_update) -> dict:
    """

    :param db:
    :param curie:
    :param reference_update:
    :return:
    """

    reference_data = jsonable_encoder(reference_update)
    logger.debug("reference_data = {}".format(reference_data))
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
        else:
            setattr(reference_db_obj, field, value)

    # currently do not update citation on patches. code will call update_citation seperately when all done
    # reference_db_obj.citation = get_citation_from_obj(db, reference_db_obj)
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


def get_merged(db: Session, curie):
    logger.debug("Looking up if '{}' is a merged entry".format(curie))
    # Is the curie in the merged set
    try:
        obs_ref_cur: ObsoleteReferenceModel = db.query(ObsoleteReferenceModel).filter(
            ObsoleteReferenceModel.curie == curie).one_or_none()
    except Exception:
        logger.debug("No merge data found so give error message")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the id {curie} is not available")

    # If found in merge then get new reference.
    if obs_ref_cur:
        logger.debug("Merge found looking up the id '{}' instead now".format(obs_ref_cur.new_id))
    try:
        reference = db.query(ReferenceModel).filter(ReferenceModel.reference_id == obs_ref_cur.new_id).one_or_none()
    except Exception:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the id {curie} is not available")
    return reference


def show(db: Session, curie: str, http_request=True):  # noqa
    """

    :param db:
    :param curie:
    :param http_request:
    :return:
    """
    try:
        reference = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).one()
    except Exception:
        reference = get_merged(db, curie)
        logger.debug("Found from merged '{}'".format(reference))

    if not reference:
        logger.warning("Reference not found for {}?".format(curie))
        if http_request:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"Reference with the id {curie} is not available")
        else:
            return None

    reference_data = jsonable_encoder(reference)
    if reference.resource_id:
        reference_data["resource_curie"] = \
            db.query(ResourceModel.curie).filter(ResourceModel.resource_id == reference.resource_id).first()[0]
        reference_data["resource_title"] = \
            db.query(ResourceModel.title).filter(ResourceModel.resource_id == reference.resource_id).first()[0]

    if reference.cross_reference:
        cross_references = []
        for cross_reference in reference.cross_reference:
            cross_reference_show = jsonable_encoder(cross_reference_crud.show(db, cross_reference.curie))
            del cross_reference_show["reference_curie"]
            cross_references.append(cross_reference_show)
        reference_data["cross_references"] = cross_references

    if reference.mod_referencetypes:
        reference_data["mod_referencetypes"] = []
        for mod_referencetype in reference.mod_referencetypes:
            reference_data["mod_referencetypes"].append(
                ModReferenceTypeSchemaCreate(reference_type=mod_referencetype.referencetype.label,
                                             source=mod_referencetype.mod.abbreviation)
            )
    reference_data["obsolete_references"] = [obs_reference["curie"] for obs_reference in
                                             reference_data["obsolete_reference"]]
    del reference_data["obsolete_reference"]

    # So thisis wierd, we check reference.mod_corpus_association BUT
    # use reference_data["mod_corpus_association"]
    if reference.mod_corpus_association:
        for i in range(len(reference_data["mod_corpus_association"])):
            del reference_data["mod_corpus_association"][i]["reference_id"]
            reference_data["mod_corpus_association"][i]["mod_abbreviation"] = reference_data[
                "mod_corpus_association"][i]["mod"]["abbreviation"]
            del reference_data["mod_corpus_association"][i]["mod"]
            del reference_data["mod_corpus_association"][i]["mod_id"]
        reference_data["mod_corpus_associations"] = reference_data["mod_corpus_association"]
        del reference_data["mod_corpus_association"]

    reference_data['workflow_tags'] = []
    if reference.workflow_tag:
        for ont in reference.workflow_tag:
            ont_json = show_workflow_tag(db, ont.reference_workflow_tag_id)

            reference_data["workflow_tags"].append(ont_json)

    reference_data["topic_entity_tags"] = []
    if reference.topic_entity_tags:
        for tet in reference.topic_entity_tags:
            tet_json = show_topic_entity_tag(db, tet.topic_entity_tag_id)

            reference_data["topic_entity_tags"].append(tet_json)

    if reference.mesh_term:
        for mesh_term in reference_data["mesh_term"]:
            del mesh_term["reference_id"]
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

    comment_and_corrections_data = {"to_references": [], "from_references": []}  # type: Dict[str, List[str]]
    for comment_and_correction in reference.comment_and_corrections_out:
        comment_and_correction_data = reference_comment_and_correction_crud.show(db,
                                                                                 comment_and_correction.reference_comment_and_correction_id)
        del comment_and_correction_data["reference_curie_from"]
        comment_and_corrections_data["to_references"].append(comment_and_correction_data)
    for comment_and_correction in reference.comment_and_corrections_in:
        comment_and_correction_data = reference_comment_and_correction_crud.show(db,
                                                                                 comment_and_correction.reference_comment_and_correction_id)
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


def merge_references(db: Session,
                     old_curie: str,
                     new_curie: str):
    """
    :param db:
    :param old_curie:
    :param new_curie:
    :return:

    Add merge details to obsolete_reference_curie table.
    Then delete old_curie.
    """

    # Lookup both curies
    old_ref = db.query(ReferenceModel).filter(ReferenceModel.curie == old_curie).first()
    new_ref = db.query(ReferenceModel).filter(ReferenceModel.curie == new_curie).first()

    merge_comments_and_corrections(db, old_ref.reference_id, new_ref.reference_id,
                                   old_curie, new_curie)

    # Check if old_curie is already in the obsolete table (It may have been merged itself)
    # by looking for it in the new_id column.
    # If so then we also want to update that to the new_id.
    prev_obs_ref_cur = db.query(ObsoleteReferenceModel).filter(
        ObsoleteReferenceModel.new_id == old_ref.reference_id).all()
    for old in prev_obs_ref_cur:
        old.new_id = new_ref.reference_id
    obs_ref_cur_data = {'new_id': new_ref.reference_id,
                        'curie': old_ref.curie}
    # Add old_curie and new_id into the obsolete_reference_curie table.
    obs_ref_cur_db_obj = ObsoleteReferenceModel(**obs_ref_cur_data)
    db.add(obs_ref_cur_db_obj)
    # Commit remapping in obsolete_reference_curie to avoid deleting them when deleting old_ref
    db.commit()

    # Delete the old_curie object
    db.delete(old_ref)
    db.commit()
    return new_curie


def merge_comments_and_corrections(db, old_reference_id, new_reference_id, old_curie, new_curie):

    try:
        for x in db.query(ReferenceCommentAndCorrectionModel).filter_by(reference_id_from=old_reference_id).all():
            y = db.query(ReferenceCommentAndCorrectionModel).filter_by(reference_id_from=new_reference_id, reference_id_to=x.reference_id_to, reference_comment_and_correction_type=x.reference_comment_and_correction_type).one_or_none()
            if y is None:
                x.reference_id_from = new_reference_id
                db.add(x)
            else:
                db.delete(x)
        for x in db.query(ReferenceCommentAndCorrectionModel).filter_by(reference_id_to=old_reference_id).all():
            y = db.query(ReferenceCommentAndCorrectionModel).filter_by(reference_id_from=x.reference_id_from, reference_id_to=new_reference_id, reference_comment_and_correction_type=x.reference_comment_and_correction_type).one_or_none()
            if y is None:
                x.reference_id_to = new_reference_id
                db.add(x)
            else:
                db.delete(x)
    except Exception as e:
        logger.warning("An error occurred when tranferring the comments/corrections from " + old_curie + " to " + new_curie + " : " + str(e))


def get_citation_from_args(authorNames, year, title, journal, volume, issue, page_range):

    if type(authorNames) == list:
        authorNames = "; ".join(authorNames)

    if year is not None and not str(year).isdigit():
        year_re_result = re.search(r"(\d{4})", year)
        if year_re_result:
            year = year_re_result.group(1)

    # Create the citation from the args given.
    citation = "{}, ({}) {} {} {} ({}): {}".\
        format(authorNames, year, title,
               journal, volume, issue, page_range)
    return citation


def author_order_sort(author: AuthorModel):
    return author.order


def citation_from_data(reference_data, authorNames):
    if authorNames.endswith("; "):
        authorNames = authorNames[:-2]  # remove last '; '
    year = ''
    issue = ''
    volume = ''
    journal = ''
    page_range = ''
    title = ''
    if 'resource' in reference_data and reference_data["resource"].title:
        journal = reference_data["resource"].title
    if 'published_date' in reference_data:
        year = re.search(r"(\d{4})", reference_data['date_published'])
        if not year:
            year = ''
    if 'issue' in reference_data and reference_data['issue']:
        issue = reference_data['issue']
    if 'page_range' in reference_data and reference_data['page_range']:
        page_range = reference_data['page_range']
    if 'title' in reference_data and reference_data['title']:
        title = reference_data['title']
        if not re.search('[.]$', title):
            title = title + '.'
    if 'volume' in reference_data and reference_data['volume']:
        volume = reference_data['volume']
    return get_citation_from_args(authorNames, year, title, journal, volume, issue, page_range)


def get_citation_from_obj(db: Session, ref_db_obj: ReferenceModel):

    # Authors, (year) title.   Journal  volume (issue): page_range
    year = ''
    if ref_db_obj.date_published:
        year_re_result = re.search(r"(\d{4})", ref_db_obj.date_published)
        if year_re_result:
            year = year_re_result.group(1)

    title = ref_db_obj.title or ''
    if not re.search('[.]$', title):
        title = title + '.'

    authorNames = ''
    for author in db.query(AuthorModel).filter_by(reference_id=ref_db_obj.reference_id).order_by(AuthorModel.order).all():
        if author.name:
            authorNames += author.name + "; "
    authorNames = authorNames[:-2]  # remove last ';'

    journal = ''
    if ref_db_obj.resource and ref_db_obj.resource.title:
        journal = ref_db_obj.resource.title

    citation = get_citation_from_args(authorNames, year, title, journal,
                                      ref_db_obj.volume or '',
                                      ref_db_obj.issue_name or '',
                                      ref_db_obj.page_range or '')
    return citation


def update_citation(db: Session, curie: str):  # noqa
    """
    :param db:
    :param curie:
    :param http_request:
    :return:
    """
    try:
        reference = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).one()
    except Exception:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the id {curie} is not available")

    new_citation = get_citation_from_obj(db, reference)
    if new_citation != reference.citation:
        reference.citation = new_citation
        db.commit()
