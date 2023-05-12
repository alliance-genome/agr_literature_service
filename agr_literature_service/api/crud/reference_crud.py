"""
reference_crud.py
=================
"""
import logging
import re
from datetime import datetime
from typing import Any, Dict, List

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import ARRAY, Boolean, String, func
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import cast, or_

from agr_literature_service.api.crud import (cross_reference_crud,
                                             reference_comment_and_correction_crud)
from agr_literature_service.api.crud.cross_reference_crud import set_curie_prefix
from agr_literature_service.api.crud.mod_reference_type_crud import insert_mod_reference_type_into_db
from agr_literature_service.api.crud.reference_resource import create_obj
from agr_literature_service.api.crud.reference_utils import get_reference
from agr_literature_service.api.models import (AuthorModel, CrossReferenceModel,
                                               MeshDetailModel,
                                               ObsoleteReferenceModel,
                                               ReferenceCommentAndCorrectionModel,
                                               ReferenceModel,
                                               ResourceModel,
                                               CopyrightLicenseModel,
                                               CitationModel)
from agr_literature_service.api.schemas import ReferenceSchemaPost, ModReferenceTypeSchemaRelated
from agr_literature_service.api.crud.mod_corpus_association_crud import create as create_mod_corpus_association
from agr_literature_service.api.crud.workflow_tag_crud import (
    create as create_workflow_tag,
    patch as update_workflow_tag,
    show as show_workflow_tag
)
from agr_literature_service.api.crud.topic_entity_tag_crud import (
    patch as update_topic_entity_tag,
    create as create_topic_entity_tag
)
from agr_literature_service.global_utils import get_next_reference_curie
from agr_literature_service.api.crud.referencefile_crud import destroy as destroy_referencefile

logger = logging.getLogger(__name__)


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
    curie = get_next_reference_curie(db)
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
                    db_obj = create_obj(db, AuthorModel, obj_data, non_fatal=True)
                    if db_obj.name:
                        author_names_order.append((db_obj.name, db_obj.order))
                elif field == "mesh_terms":
                    db_obj = MeshDetailModel(**obj_data)
                elif field == "cross_references":
                    db_obj = CrossReferenceModel(**obj_data)
                    set_curie_prefix(db_obj)
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
    # reference_data['citation'] = citation_from_data(reference_data,
    #                                                "; ".join([x[0] for x in sorted(author_names_order,
    #                                                                                key=lambda x: x[1])]))
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
            for obj in value or []:
                insert_mod_reference_type_into_db(db, reference.pubmed_types, obj.source, obj.reference_type,
                                                  reference_db_obj.reference_id)
    return curie


def destroy(db: Session, curie_or_reference_id: str):
    """

    :param db:
    :param curie_or_reference_id:
    :return:
    """
    reference_id = int(curie_or_reference_id) if curie_or_reference_id.isdigit() else None
    reference = db.query(ReferenceModel).filter(or_(
        ReferenceModel.curie == curie_or_reference_id,
        ReferenceModel.reference_id == reference_id)).one_or_none()

    if not reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with curie or reference_id {curie_or_reference_id} not found")
    for referencefile in reference.referencefiles:
        destroy_referencefile(db, referencefile.referencefile_id)
    db.delete(reference)
    db.commit()

    return None


def patch(db: Session, curie_or_reference_id: str, reference_update) -> dict:
    """

    :param db:
    :param curie_or_reference_id:
    :param reference_update:
    :return:
    """

    reference_data = jsonable_encoder(reference_update)
    logger.debug("reference_data = {}".format(reference_data))
    reference_id = int(curie_or_reference_id) if curie_or_reference_id.isdigit() else None
    reference_db_obj = db.query(ReferenceModel).filter(or_(
        ReferenceModel.curie == curie_or_reference_id,
        ReferenceModel.reference_id == reference_id)).one_or_none()

    if not reference_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with curie or reference_id {curie_or_reference_id} not found")

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

    # currently do not update citation on patches. code will call update_citation separately when all done
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


def show(db: Session, curie_or_reference_id: str):  # noqa
    """

    :param db:
    :param curie_or_reference_id:
    :param http_request:
    :return:
    """
    reference = get_reference(db, curie_or_reference_id, load_authors=True, load_mod_corpus_associations=True,
                              load_mesh_terms=True, load_obsolete_references=True)
    reference_data = jsonable_encoder(reference)
    if reference.resource_id:
        reference_data["resource_curie"] = \
            db.query(ResourceModel.curie).filter(ResourceModel.resource_id == reference.resource_id).first()[0]
        reference_data["resource_title"] = \
            db.query(ResourceModel.title).filter(ResourceModel.resource_id == reference.resource_id).first()[0]

    if reference.copyright_license_id:
        crl = db.query(CopyrightLicenseModel).filter_by(
            copyright_license_id=reference.copyright_license_id).one_or_none()
        if crl:
            reference_data["copyright_license_name"] = crl.name
            reference_data["copyright_license_url"] = crl.url
            reference_data["copyright_license_description"] = crl.description
            reference_data["copyright_license_open_access"] = crl.open_access
            rows = db.execute(f"SELECT rv.updated_by, u.email "
                              f"FROM reference_version rv, users u "
                              f"WHERE curie = '{reference_data['curie']}' "
                              f"AND copyright_license_id_mod IS true "
                              f"AND rv.updated_by = u.id "
                              f"ORDER BY rv.date_updated DESC LIMIT 1").fetchall()
            if len(rows) == 1:
                if rows[0]['email']:
                    reference_data["copyright_license_last_updated_by"] = rows[0]['email']
                else:
                    reference_data["copyright_license_last_updated_by"] = rows[0]['updated_by']

    if reference.citation_id:
        cit = db.query(CitationModel).filter_by(
            citation_id=reference.citation_id).one_or_none()
        if cit:
            reference_data["citation"] = cit.citation
            reference_data["citation_short"] = cit.short_citation
        else:
            logger.warning(f"ref: {reference} has no citation, id is {reference.citation_id}")
            reference_data["citation"] = f'No citation lookup failed for ref:{reference.curie} cit_id:{reference.citation_id}'
            reference_data["citation_short"] = 'Problem No short citation'
    else:
        reference_data["citation"] = f'No citation_id for ref:{reference.curie}'
        reference_data["citation_short"] = f'No citation_id for ref:{reference.curie}'

    bad_cross_ref_ids = []
    if reference.cross_reference:
        cross_references = []
        for cross_reference in reference.cross_reference:
            cross_reference_show = jsonable_encoder(cross_reference_crud.show(db, cross_reference.curie))
            del cross_reference_show["reference_curie"]
            cross_references.append(cross_reference_show)
        reference_data["cross_references"] = cross_references
        for x in cross_references:
            pieces = x['curie'].split(":")
            if len(pieces) > 2 and pieces[0] != 'DOI':
                ## will pick up something like 'FB:FB:FBrf0221304'
                bad_cross_ref_ids.append(x['curie'])
            elif pieces[1] == "":
                ## will pick up something like 'FB:'
                bad_cross_ref_ids.append(x['curie'])
    reference_data["invalid_cross_reference_ids"] = bad_cross_ref_ids

    if reference.mod_referencetypes:
        reference_data["mod_reference_types"] = []
        for ref_mod_referencetype in reference.mod_referencetypes:
            reference_data["mod_reference_types"].append(
                jsonable_encoder(ModReferenceTypeSchemaRelated(
                    mod_reference_type_id=ref_mod_referencetype.reference_mod_referencetype_id,
                    reference_type=ref_mod_referencetype.mod_referencetype.referencetype.label,
                    source=ref_mod_referencetype.mod_referencetype.mod.abbreviation)))
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

    if reference.mesh_term:
        for mesh_term in reference_data["mesh_term"]:
            del mesh_term["reference_id"]
        reference_data['mesh_terms'] = reference_data['mesh_term']

    if reference.author:
        authors = []
        for author in reference_data["author"]:
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


def show_changesets(db: Session, curie_or_reference_id: str):
    """

    :param db:
    :param curie_or_reference_id:
    :return:
    """
    reference_id = int(curie_or_reference_id) if curie_or_reference_id.isdigit() else None
    reference = db.query(ReferenceModel).filter(or_(
        ReferenceModel.curie == curie_or_reference_id, ReferenceModel.reference_id == reference_id)).one_or_none()
    if not reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the reference id or curie {curie_or_reference_id} is not available")
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
        db.commit()
    except Exception as e:
        logger.warning("An error occurred when transferring the comments/corrections from " + old_curie + " to " + new_curie + " : " + str(e))


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


def add_license(db: Session, curie: str, license: str):  # noqa
    """
    :param db:
    :param curie:
    :param license:
    :return:
    """
    try:
        reference = db.query(ReferenceModel).filter_by(curie=curie).one()
    except Exception:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the id '{curie}' is not in the database.")

    license = license.replace('+', ' ')
    if license == 'No license':
        license = ''
    copyright_license_id = None
    if license != '':
        try:
            copyrightLicense = db.query(CopyrightLicenseModel).filter_by(name=license).one()
        except Exception:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"Copyright_license with the name '{license}' is not in the database.")
        copyright_license_id = copyrightLicense.copyright_license_id
    try:
        reference.copyright_license_id = copyright_license_id
        db.commit()
    except Exception:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Error adding license '{license}'")
    return {"message": "Update Success!"}


def missing_files(db: Session, mod_abbreviation: str):
    try:
        query = f"""SELECT reference.curie, short_citation, reference.date_created, MAINCOUNT, SUPCOUNT, ref_pmid.curie as PMID, ref_mod.curie AS mod_curie
                    FROM reference, citation,
                        (SELECT b.reference_id, COUNT(1) FILTER (WHERE c.file_class = 'main') AS MAINCOUNT,
                        COUNT(1) FILTER (WHERE c.file_class = 'supplement') AS SUPCOUNT
                        FROM mod_corpus_association AS b
                        JOIN mod ON b.mod_id = mod.mod_id
                        LEFT JOIN referencefile AS c ON b.reference_id = c.reference_id
                        LEFT JOIN workflow_tag AS d ON b.reference_id = d.reference_id
                        WHERE mod.abbreviation = '{mod_abbreviation}'
                        AND corpus=true
                        GROUP BY b.reference_id
                        HAVING (COUNT(1) FILTER (WHERE c.file_class = 'main') < 1
                        OR COUNT(1) FILTER (WHERE c.file_class = 'supplement') < 1)
                        AND COUNT(1) FILTER (WHERE d.workflow_tag_id = 'ATP:0000134') < 1
                        AND COUNT(1) FILTER (WHERE d.workflow_tag_id = 'ATP:0000135') < 1
                        LIMIT 25)
                        AS sub_select,
                        (SELECT cross_reference.curie, reference_id FROM cross_reference where curie_prefix='PMID') as ref_pmid,
                        (SELECT cross_reference.curie, reference_id FROM cross_reference where curie_prefix='{mod_abbreviation}') as ref_mod
                    WHERE sub_select.reference_id=reference.reference_id
                    AND sub_select.reference_id=ref_pmid.reference_id
                    AND sub_select.reference_id=ref_mod.reference_id
                    AND reference.citation_id=citation.citation_id
                """
        rs = db.execute(query)
        rows = rs.fetchall()
        data = jsonable_encoder(rows)
    except Exception:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Cant search missing files.")
    return data
