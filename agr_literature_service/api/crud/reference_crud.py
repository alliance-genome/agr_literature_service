"""
reference_crud.py
=================
"""
import logging
import re
from collections import defaultdict
from typing import Any, Dict, List, Optional
from os import getcwd
from datetime import datetime, date, timedelta

from fastapi.responses import FileResponse
from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from starlette.background import BackgroundTask
from sqlalchemy import ARRAY, Boolean, String, func, and_, text, TextClause
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import cast, or_
from sqlalchemy.exc import SQLAlchemyError

from agr_literature_service.api.crud import (cross_reference_crud,
                                             reference_relation_crud)
from agr_literature_service.api.crud.cross_reference_crud import set_curie_prefix
from agr_literature_service.api.crud.referencefile_crud import cleanup
from agr_literature_service.api.crud.mod_reference_type_crud import insert_mod_reference_type_into_db
from agr_literature_service.api.crud.reference_resource import create_obj
from agr_literature_service.api.crud.reference_utils import get_reference, BibInfo, Citation
from agr_literature_service.api.models import (AuthorModel, CrossReferenceModel,
                                               MeshDetailModel,
                                               ModModel,
                                               ModCorpusAssociationModel,
                                               ObsoleteReferenceModel,
                                               ReferenceRelationModel,
                                               ReferenceModel,
                                               ResourceModel,
                                               CopyrightLicenseModel,
                                               CitationModel, TopicEntityTagModel)
from agr_literature_service.api.routers.okta_utils import OktaAccess
from agr_literature_service.api.schemas import ReferenceSchemaPost, ModReferenceTypeSchemaRelated, \
    TopicEntityTagSchemaPost
from agr_literature_service.api.crud.mod_corpus_association_crud import create as create_mod_corpus_association
from agr_literature_service.api.crud.workflow_tag_crud import (
    create as create_workflow_tag,
    patch as update_workflow_tag,
    show as show_workflow_tag
)
from agr_literature_service.api.crud.topic_entity_tag_crud import create_tag, revalidate_all_tags
from agr_literature_service.global_utils import get_next_reference_curie
from agr_literature_service.api.crud.referencefile_crud import destroy as destroy_referencefile
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_references_single_mod import \
    update_data
from agr_literature_service.api.crud.cross_reference_crud import check_xref_and_generate_mod_id
from agr_literature_service.api.crud.workflow_tag_crud import transition_to_workflow_status, \
    get_current_workflow_status
from agr_literature_service.lit_processing.utils.db_read_utils import \
    get_cross_reference_data_for_ref_ids, get_author_data_for_ref_ids, \
    get_mesh_term_data_for_ref_ids, get_mod_corpus_association_data_for_ref_ids, \
    get_mod_reference_type_data_for_ref_ids, get_all_reference_relation_data, \
    get_journal_by_resource_id
from agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json import \
    get_meta_data, get_reference_col_names, generate_json_data
from agr_literature_service.lit_processing.utils.report_utils import send_report

logger = logging.getLogger(__name__)

file_needed_tag_atp_id = "ATP:0000141"  # file needed


def create(db: Session, reference: ReferenceSchemaPost):  # noqa
    """

    :param db:
    :param reference:
    :return:
    """

    logger.debug("creating reference")
    # logger.debug(reference)
    add_separately_fields = ["mod_corpus_associations", "workflow_tags", "topic_entity_tags", "mod_reference_types"]
    list_fields = ["authors", "tags", "mesh_terms", "cross_references"]
    remap = {
        'authors': 'author',
        'mesh_terms': 'mesh_term',
        'cross_references': 'cross_reference',
        'mod_reference_types': 'mod_reference_type'
    }
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
        # mod_corpus_associations now triggers creating mod xref for WB, so must happen after xref creation here.
        if field == "mod_corpus_associations":
            if value is not None:
                for obj in value:
                    obj_data = jsonable_encoder(obj)
                    obj_data["reference_curie"] = curie
                    try:
                        create_mod_corpus_association(db, obj_data)
                    except HTTPException as e:
                        # We have several reasons why this could have failed
                        # 1) Mod does not exist , this is a problem.
                        if e.detail.startswith('Mod with abbreviation') and e.detail.endswith('does not exist'):
                            logger.error(e.detail)
                            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                                detail=e.detail)
                        # 2) Reference does not exist, this is a problem
                        elif e.detail.startswith('Reference with curie') and e.detail.endswith('does not exist'):
                            logger.error(e.detail)
                            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                                detail=e.detail)
                        # 3) It already exists, not really a problem
                        elif e.detail.startswith('ModCorpusAssociation with the reference_curie') and e.detail.endswith(
                                'create duplicate record.'):
                            logger.warning(e.detail)
                        # We do not know what error this is so flag it
                        else:
                            raise
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
                    obj_data = obj.dict(exclude_unset=True)
                    obj_data["reference_curie"] = curie
                    obj_data["force_insertion"] = True
                    try:
                        create_tag(db, obj_data, validate_on_insert=False)
                    except HTTPException:
                        logger.warning("skipping topic_entity_tag as that is already associated to "
                                       "the reference")
                db.commit()
                revalidate_all_tags(curie_or_reference_id=str(reference_db_obj.reference_id))
        elif field == "mod_reference_types":
            for obj in value or []:
                insert_mod_reference_type_into_db(db, reference.pubmed_types, obj.mod_abbreviation, obj.reference_type,
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
        destroy_referencefile(db, referencefile.referencefile_id, OktaAccess.ALL_ACCESS)
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
    # logger.debug("reference_data = {}".format(reference_data))
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
    db.add(reference_db_obj)
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
                                     ARRAY(Boolean))).outerjoin(ReferenceModel.cross_reference).group_by(
        ReferenceModel.curie)

    return [
        {
            "curie": reference[0],
            "cross_references": [
                {
                    "curie": reference[1][idx],
                    "is_obsolete": reference[2][idx]
                } for idx in range(len(reference[1]))
            ]
        } for reference in references_query.all()
    ]


def show(db: Session, curie_or_reference_id: str):  # noqa
    """

    :param db:
    :param curie_or_reference_id:
    :param http_request:
    :return:
    """
    logger.info("Show reference called")
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
            rows = db.execute(text(f"SELECT rv.updated_by, u.email "
                                   f"FROM reference_version rv, users u "
                                   f"WHERE curie = '{reference_data['curie']}' "
                                   f"AND copyright_license_id_mod IS true "
                                   f"AND rv.updated_by = u.id "
                                   f"ORDER BY rv.date_updated DESC LIMIT 1")).mappings().fetchall()
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
            reference_data[
                "citation"] = f'No citation lookup failed for ref:{reference.curie} cit_id:{reference.citation_id}'
            reference_data["citation_short"] = 'Problem No short citation'
    else:
        reference_data["citation"] = f'No citation_id for ref:{reference.curie}'
        reference_data["citation_short"] = f'No citation_id for ref:{reference.curie}'

    bad_cross_ref_ids = []
    pmid = None
    if reference.cross_reference:
        cross_references = []
        for cross_reference in reference.cross_reference:
            cross_reference_show = jsonable_encoder(
                cross_reference_crud.show(db, str(cross_reference.cross_reference_id)))
            del cross_reference_show["reference_curie"]
            cross_references.append(cross_reference_show)
            if cross_reference_show["curie_prefix"] == 'PMID' and not cross_reference_show["is_obsolete"]:
                pmid = cross_reference_show["curie"]
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

    if pmid:
        pmid_no_prefix = pmid.replace('PMID:', '')
        resource_links = [
            {
                "display_name": "Ontomate",
                "link_url": f"https://ontomate.rgd.mcw.edu/QueryBuilder/getResult/?qSource=pubmed&qPMID={pmid.lower()}"
            },
            {
                "display_name": "Pubtator",
                "link_url": f"https://www.ncbi.nlm.nih.gov/research/pubtator3/publication/{pmid_no_prefix}?text={pmid_no_prefix}"
            },
            {
                "display_name": "EuropePMC",
                "link_url": f"https://europepmc.org/article/MED/{pmid_no_prefix}"
            }
        ]
        ## generate links to Textpresso
        sql_query = f"""
        SELECT rfm.mod_id
        FROM referencefile rf
        JOIN referencefile_mod rfm ON rf.referencefile_id = rfm.referencefile_id
        WHERE rf.reference_id = {reference.reference_id}
        AND rf.file_class = 'main'
        AND rf.pdf_type = 'pdf'
        AND rf.date_created <= NOW() - INTERVAL '7 days'
        """
        rows = db.execute(text(sql_query)).mappings().fetchall()
        pdf_eligible_mod_ids = [row['mod_id'] for row in rows if row['mod_id']]
        is_pmc = any(row['mod_id'] is None for row in rows)
        if is_pmc:
            pdf_eligible_mod_ids = [1, 2, 3, 4, 5, 6, 7]

        sql_query = f"""
        SELECT m.mod_id, m.abbreviation
        FROM mod m
        JOIN mod_corpus_association mca ON m.mod_id = mca.mod_id
        WHERE mca.reference_id = {reference.reference_id}
        AND mca.corpus = True
        """
        rows = db.execute(text(sql_query)).mappings().fetchall()
        mod_list = set()
        for row in rows:
            if row['mod_id'] in pdf_eligible_mod_ids:
                mod_list.add(row['abbreviation'])
        sorted_mod_list = sorted(mod_list)
        for mod in sorted_mod_list:
            if mod not in ["RGD", "XB"]:
                resource_links.append({
                    "display_name": f"{mod} Textpresso",
                    "link_url": f"https://www.alliancegenome.org/textpresso/{mod.lower()}/tpc/search?accession={pmid}&keyword="
                })
        reference_data["resources_for_curation"] = resource_links

    if reference.mod_referencetypes:
        reference_data["mod_reference_types"] = []
        for ref_mod_referencetype in reference.mod_referencetypes:
            reference_data["mod_reference_types"].append(
                jsonable_encoder(ModReferenceTypeSchemaRelated(
                    mod_reference_type_id=ref_mod_referencetype.reference_mod_referencetype_id,
                    reference_type=ref_mod_referencetype.mod_referencetype.referencetype.label,
                    mod_abbreviation=ref_mod_referencetype.mod_referencetype.mod.abbreviation)))
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

    reference_relations_data = {"to_references": [], "from_references": []}  # type: Dict[str, List[str]]
    for reference_relation in reference.reference_relation_out:
        reference_relation_data = reference_relation_crud.show(db,
                                                               reference_relation.reference_relation_id)
        del reference_relation_data["reference_curie_from"]
        reference_relations_data["to_references"].append(reference_relation_data)
    for reference_relation in reference.reference_relation_in:
        reference_relation_data = reference_relation_crud.show(db,
                                                               reference_relation.reference_relation_id)
        del reference_relation_data["reference_curie_to"]
        reference_relations_data["from_references"].append(reference_relation_data)

    reference_data["reference_relations"] = reference_relations_data
    # logger.debug("returning {}".format(reference_data))
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
        history.append(
            {
                "transaction": {
                    "id": tx.id,
                    "issued_at": tx.issued_at,
                    "user_id": tx.user_id
                },
                "changeset": version.changeset
            }
        )

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
    logger.info("Merging references started")
    old_ref = get_reference(db=db, curie_or_reference_id=old_curie)
    new_ref = get_reference(db=db, curie_or_reference_id=new_curie)

    if old_ref.prepublication_pipeline or new_ref.prepublication_pipeline:
        new_ref.prepublication_pipeline = True
        db.commit()

    merge_reference_relations(db, old_ref.reference_id, new_ref.reference_id,
                              old_curie, new_curie)

    old_ref_tets = db.query(TopicEntityTagModel).filter(TopicEntityTagModel.reference_id == old_ref.reference_id).all()

    for old_tet in old_ref_tets:
        new_tet_data = {
            "topic": old_tet.topic,
            "entity_type": old_tet.entity_type,
            "entity": old_tet.entity,
            "entity_id_validation": old_tet.entity_id_validation,
            "entity_published_as": old_tet.entity_published_as,
            "species": old_tet.species,
            "display_tag": old_tet.display_tag,
            "topic_entity_tag_source_id": old_tet.topic_entity_tag_source_id,
            "negated": old_tet.negated,
            "novel_topic_data": old_tet.novel_topic_data,
            "confidence_level": old_tet.confidence_level,
            "note": old_tet.note,
            "reference_curie": new_ref.curie,
            "force_insertion": True,
            "created_by": str(old_tet.created_by),
            "updated_by": str(old_tet.updated_by),
            "date_created": str(old_tet.date_created),
            "date_updated": str(old_tet.date_updated)
        }
        new_tet = TopicEntityTagSchemaPost(**new_tet_data)
        create_tag(db, new_tet, validate_on_insert=False)
    db.commit()

    revalidate_all_tags(curie_or_reference_id=new_ref.curie)

    # Check if old_curie is already in the obsolete table (It may have been merged itself)
    # by looking for it in the new_id column.
    # If so then we also want to update that to the new_id.
    try:
        prev_obs_ref_cur = db.query(ObsoleteReferenceModel).filter(
            ObsoleteReferenceModel.new_id == old_ref.reference_id).all()
        for old in prev_obs_ref_cur:
            old.new_id = new_ref.reference_id
        obs_ref_cur_data = {
            'new_id': new_ref.reference_id,
            'curie': old_ref.curie
        }
        # Add old_curie and new_id into the obsolete_reference_curie table.
        obs_ref_cur_db_obj = ObsoleteReferenceModel(**obs_ref_cur_data)
        db.add(obs_ref_cur_db_obj)
        # Commit remapping in obsolete_reference_curie to avoid deleting them when deleting old_ref
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Cannot merge these two references. {e}")

    # Delete the old_curie object
    db.delete(old_ref)
    db.commit()

    # find which mods reference this paper
    mods = db.query(ModModel).join(ModCorpusAssociationModel). \
        filter(ModCorpusAssociationModel.reference_id == new_ref.reference_id,
               ModCorpusAssociationModel.mod_id == ModModel.mod_id).all()

    # send report of merge
    mod_list = []
    for mod in mods:
        mod_list.append(mod.abbreviation)
    message = f"{old_ref.curie} has been merged into {new_ref.curie} for {mod_list}"
    send_report("Reference Merge Report", message)

    # if winning reference after merge has a valid pmid, update data from pubmed
    cross_reference = db.query(CrossReferenceModel).filter(
        and_(CrossReferenceModel.reference_id == new_ref.reference_id,
             CrossReferenceModel.is_obsolete.is_(False),
             CrossReferenceModel.curie_prefix == 'PMID')).order_by(
        CrossReferenceModel.is_obsolete).first()
    if cross_reference is not None:
        pmid_number = cross_reference.curie.split(":")[1]
        update_data(None, pmid_number)

    return new_curie


def merge_reference_relations(db, old_reference_id, new_reference_id, old_curie, new_curie):
    all_ref_relations = db.query(
        ReferenceRelationModel.reference_id_from,
        ReferenceRelationModel.reference_id_to).filter(
        or_(
            ReferenceRelationModel.reference_id_from == old_reference_id,
            ReferenceRelationModel.reference_id_to == old_reference_id,
            ReferenceRelationModel.reference_id_from == new_reference_id,
            ReferenceRelationModel.reference_id_to == new_reference_id
        )
    ).all()
    all_ref_relations_with_new_ids = [(new_reference_id if rel[0] == old_reference_id else rel[0],
                                       new_reference_id if rel[1] == old_reference_id else rel[1]) for rel in
                                      all_ref_relations]
    if len(set([(min(rel[0], rel[1]), max(rel[0], rel[1])) for rel in all_ref_relations_with_new_ids])) < len(
            all_ref_relations):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="Cannot merge these two references as they have duplicate reference relations")
    try:
        for x in db.query(ReferenceRelationModel).filter_by(reference_id_from=old_reference_id).all():
            y = db.query(ReferenceRelationModel).filter_by(reference_id_from=new_reference_id,
                                                           reference_id_to=x.reference_id_to,
                                                           reference_relation_type=x.reference_relation_type).one_or_none()
            if y is None:
                if x.reference_id_to != new_reference_id:
                    x.reference_id_from = new_reference_id
                    db.add(x)
                else:
                    db.delete(x)
            else:
                db.delete(x)
        for x in db.query(ReferenceRelationModel).filter_by(reference_id_to=old_reference_id).all():
            y = db.query(ReferenceRelationModel).filter_by(reference_id_from=x.reference_id_from,
                                                           reference_id_to=new_reference_id,
                                                           reference_relation_type=x.reference_relation_type).one_or_none()
            if y is None:
                if x.reference_id_from != new_reference_id:
                    x.reference_id_to = new_reference_id
                    db.add(x)
                else:
                    db.delete(x)
            else:
                db.delete(x)
        db.commit()
    except Exception as e:
        db.rollback()
        logger.warning(
            "An error occurred when transferring the reference_relations from " + old_curie + " to " + new_curie + " : " + str(
                e))
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Cannot merge these two references. {e}")


def author_order_sort(author: AuthorModel):
    return author.order


# Not used anymore?
# Adding log error incase it is.
# Used by alembic update but likelyhood of this being used again is very small
# So if we do not see any error messages after a while, we can delete this.
def get_citation_from_args(authorNames, year, title, journal, volume, issue, page_range):  # pragma: no cover
    logger.error("get_citation_from_args scheduled to be removed."
                 " Please notify blueteam to remove docs about removal.")
    if type(authorNames) == list:
        authorNames = "; ".join(authorNames)

    if year is not None and not str(year).isdigit():
        year_re_result = re.search(r"(\d{4})", year)
        if year_re_result:
            year = year_re_result.group(1)

    # Create the citation from the args given.
    citation = "{}, ({}) {} {} {} ({}): {}". \
        format(authorNames, year, title,
               journal, volume, issue, page_range)
    return citation


# Not used anymore?
# Adding log error incase it is.
# Used by alembic update but likelyhood of this being used again is very small
# So if we do not see any error messages after a while, we can delete this.
def citation_from_data(reference_data, authorNames):  # pragma: no cover
    logger.error("citation_from_data scheduled to be removed."
                 " Please notify blueteam to remove docs about removal.")
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


# Not used anymore? Done by psql trigger.
# Adding log error incase it is.
# Used by alembic update but likelyhood of this being used again is very small
# So if we do not see any error messages after a while, we can delete this.
def get_citation_from_obj(db: Session, ref_db_obj: ReferenceModel):  # pragma: no cover
    logger.error("get_citation_from_obj scheduled to be removed."
                 " Please notify blueteam to remove docs about removal.")
    # Authors, (year) title.   Journal  volume (issue): page_range
    year = ''
    if ref_db_obj.date_published:
        date_published_value = ref_db_obj.date_published
        if isinstance(date_published_value, str):
            year_re_result = re.search(r"(\d{4})", date_published_value)
            if year_re_result:
                year = year_re_result.group(1)

    title = getattr(ref_db_obj, 'title', '') or ''
    if not re.search(r'[.]$', str(title)):
        title = title + '.'

    authorNames = ''
    for author in db.query(AuthorModel).filter_by(reference_id=ref_db_obj.reference_id).order_by(
            AuthorModel.order).all():
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


def sql_query_for_workflow_files(db: Session, mod_abbreviation: str, order_by: str, filter: str, offset: Optional[int] = None, limit: Optional[int] = None):

    curie_prefix = 'Xenbase' if mod_abbreviation == 'XB' else mod_abbreviation
    workflow_tag_id_clause: Optional[TextClause] = None

    if filter == 'default':
        workflow_tag_id_clause = text("""WHERE (d.workflow_tag_id = 'ATP:0000139' OR d.workflow_tag_id = 'ATP:0000141')""")
    elif filter in ['ATP:0000134', 'ATP:0000135']:
        workflow_tag_id_clause = text("""WHERE workflow_tag_id = :filter""")
    query_str = f"""
        SELECT reference.reference_id, reference.curie, short_citation, reference.date_created, MAX(ref_pmid.curie) AS PMID, MAX(ref_doi.curie) AS DOI, MAX(ref_mod.curie) AS mod_curie
        FROM reference
        JOIN citation ON reference.citation_id = citation.citation_id
        JOIN
          (
            SELECT b.reference_id
            FROM mod_corpus_association AS b
            JOIN mod ON b.mod_id = mod.mod_id
            LEFT JOIN workflow_tag AS d ON b.reference_id = d.reference_id
            {workflow_tag_id_clause}
            AND mod.abbreviation = :mod_abbreviation
            AND b.corpus = TRUE
          ) AS sub_select ON sub_select.reference_id = reference.reference_id
        LEFT JOIN
          (
            SELECT curie, reference_id
            FROM cross_reference
            WHERE curie_prefix IN ('PMID', 'DOI', :mod_abbreviation)
          ) AS cross_ref ON cross_ref.reference_id = reference.reference_id
        LEFT JOIN cross_reference AS ref_pmid ON ref_pmid.reference_id = reference.reference_id AND ref_pmid.curie_prefix = 'PMID'
        LEFT JOIN cross_reference AS ref_doi ON ref_doi.reference_id = reference.reference_id AND ref_doi.curie_prefix = 'DOI'
        LEFT JOIN cross_reference AS ref_mod ON ref_mod.reference_id = reference.reference_id AND ref_mod.curie_prefix = :mod_abbreviation
        GROUP BY reference.reference_id, reference.curie, short_citation, reference.date_created
        ORDER BY reference.date_created DESC
    """

    # Conditionally add limit and offset only if they are provided
    if limit is not None:
        query_str += " LIMIT :limit"
    if offset is not None:
        query_str += " OFFSET :offset"

    query = text(query_str)

    # Bind the necessary parameters, including limit and offset if present
    params = {
        'mod_abbreviation': mod_abbreviation,
        'curie_prefix': curie_prefix,
    }
    if filter != 'default':
        params['filter'] = filter
    if limit is not None:
        params['limit'] = str(limit)
    if offset is not None:
        params['offset'] = str(offset)

    rows = db.execute(query, params).mappings().fetchall()
    ref_data = jsonable_encoder(rows)
    if not ref_data:
        return ref_data

    reference_ids = [item['reference_id'] for item in ref_data if 'reference_id' in item]
    reffile_query_str = """
        SELECT reference_id,
                   COUNT(1) FILTER (WHERE file_class = 'main') AS maincount,
                   COUNT(1) FILTER (WHERE file_class = 'supplement') AS supcount
        FROM referencefile
        WHERE reference_id = ANY(:reference_ids)
        GROUP BY reference_id
    """
    rows_reffile = db.execute(text(reffile_query_str), {'reference_ids': reference_ids}).mappings().fetchall()
    reffile_data = jsonable_encoder(rows_reffile)
    reffile_dict = {
        entry['reference_id']: (entry['maincount'], entry['supcount'])
        for entry in reffile_data
    }
    for item in ref_data:
        item_reference_id = item['reference_id']
        if item_reference_id in reffile_dict:
            item['maincount'] = reffile_dict[item_reference_id][0]
            item['supcount'] = reffile_dict[item_reference_id][1]
        else:
            item['maincount'] = 0
            item['supcount'] = 0
    return ref_data


def missing_files(db: Session, mod_abbreviation: str, order_by: str, page: int, filter: str):

    if order_by is None:
        order_by = 'desc'
    elif order_by.lower() not in ['asc', 'desc']:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Invalid order_by value: {order_by}")
    if filter is None:
        filter = 'default'
    if filter not in ['default', 'ATP:0000134', 'ATP:0000135']:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Invalid filter: {filter}")
    try:
        limit = 25
        offset = (page - 1) * limit
        data = sql_query_for_workflow_files(db, mod_abbreviation, order_by, filter, offset, limit)
        if not data:
            return []
    except SQLAlchemyError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Can't search workflow files: {str(e)}")
    return data


def download_tracker_table(db: Session, mod_abbreviation: str, order_by: str, filter: str):
    try:
        query = sql_query_for_workflow_files(db, mod_abbreviation, order_by, filter)
        rows = db.execute(query).fetchall()
        tag = {
            'default': 'needed',
            'ATP:0000134': 'uploaded',
            'ATP:0000135': 'unobtainable'
        }.get(filter, 'needed')

        tmp_file = f"{mod_abbreviation}_file_{tag}.tsv"
        workflowtag = f"file {tag}"
        tmp_file_with_path = f"{getcwd()}/{tmp_file}"
        fw = open(tmp_file_with_path, "w")
        fw.write("Curie\tMOD Curie\tPMID\tDOI\tCitation\tWorkflow Tag\tMain File Count\tSuppl File Count\tDate Created\n")
        for x in rows:
            date_created = str(x[2]).split(' ')[0]
            main_file_count = x[3]
            suppl_file_count = x[4]
            fw.write(f"{x[0]}\t{x[5]}\t{x[6]}\t{x[7]}\t{x[1]}\t"
                     f"{workflowtag}\t{main_file_count}\t{suppl_file_count}\t{date_created}\n")

        fw.close()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"The download file for the tracker table can not be created. {e}")

    # return FileResponse(path=tmp_file_with_path, filename=tmp_file, media_type='application/plain')
    return FileResponse(path=tmp_file_with_path, filename=tmp_file, media_type='application/plain',
                        background=BackgroundTask(cleanup, tmp_file_with_path))


def get_bib_info(db, curie, mod_abbreviation: str, return_format: str = 'txt'):
    logger.info("Get biblio info")
    bib_info = BibInfo()
    reference: ReferenceModel = get_reference(db, curie, load_authors=True)
    author: AuthorModel
    for author in sorted(reference.author, key=lambda a: a.order):
        last_name = str(author.last_name or '')
        first_initial = str(author.first_initial or '')
        full_name = str(author.name or '')
        bib_info.add_author(last_name, first_initial, full_name)
    all_mods_abbreviations = [mod.abbreviation if mod.abbreviation != "XB" else mod.short_name for mod in
                              db.query(ModModel).all()]

    bib_info.cross_references = [xref.curie for xref in reference.cross_reference if not xref.is_obsolete
                                 and (xref.curie_prefix not in all_mods_abbreviations
                                      or xref.curie_prefix == mod_abbreviation)]
    pubmed_types = getattr(reference, 'pubmed_types', None)
    if pubmed_types:
        bib_info.pubmed_types = [str(pub_type).replace("_", " ") for pub_type in pubmed_types]
    else:
        bib_info.pubmed_types = []

    bib_info.title = str(reference.title or '')
    if reference.resource is not None:
        bib_info.journal = str(reference.resource.title or '')
    bib_info.citation = Citation(volume=reference.volume, pages=reference.page_range)
    if reference.date_published:
        bib_info.year = str(reference.date_published)
    bib_info.abstract = str(reference.abstract or '')
    bib_info.reference_curie = str(reference.curie)
    return bib_info.get_formatted_bib(format_type=return_format)


def get_textpresso_reference_list(db, mod_abbreviation, files_updated_from_date=None, reference_type=None,
                                  species: str = None, from_reference_id: int = None, page_size: int = 1000):
    if reference_type and reference_type not in ['Experimental', 'Not_experimental', 'Meeting_abstract']:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="The reference_type passed in is not a valid reference_type.")

    mod = db.query(ModModel).filter_by(abbreviation=mod_abbreviation).one_or_none()
    if not mod:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"The mod_abbreviation: {mod_abbreviation} is not in the database.")

    mod_id = mod.mod_id

    """
    Caenorhabditis elegans   NCBITaxon:6239
    Caenorhabditis brenneri  NCBITaxon:135651
    Caenorhabditis briggsae  NCBITaxon:6238
    Caenorhabditis japonica  NCBITaxon:281687
    Caenorhabditis remanei   NCBITaxon:31234
    Brugia malayi            NCBITaxon:6279
    Onchocerca volvulus      NCBITaxon:6282
    Pristionchus pacificus   NCBITaxon:54126
    Strongyloides ratti      NCBITaxon:34506
    Trichuris muris          NCBITaxon:70415
    """

    wb_textpresso_species_list = (
        'NCBITaxon:6239', 'NCBITaxon:135651', 'NCBITaxon:6238', 'NCBITaxon:281687',
        'NCBITaxon:31234', 'NCBITaxon:6279', 'NCBITaxon:6282', 'NCBITaxon:54126',
        'NCBITaxon:34506', 'NCBITaxon:70415'
    )

    # Start building the query string
    query_str = """
        SELECT r.curie, r.reference_id, rf.referencefile_id, rf.md5sum, rfm.mod_id, rf.date_created
        FROM reference r
        JOIN mod_corpus_association mca on r.reference_id = mca.reference_id
        JOIN referencefile rf ON rf.reference_id = r.reference_id
        JOIN referencefile_mod rfm ON rf.referencefile_id = rfm.referencefile_id
        WHERE mca.corpus is True
        AND mca.mod_id = :mod_id
        AND rf.file_class = 'main'
        AND rf.pdf_type = 'pdf'
        AND (rfm.mod_id is NULL OR rfm.mod_id = :mod_id)
    """

    query_params = {'mod_id': mod_id}

    # Add condition for reference_type if provided
    if reference_type:
        query_str += """
        AND r.reference_id IN (
            SELECT rmrt.reference_id
            FROM reference_mod_referencetype rmrt
            JOIN mod_referencetype mrt ON mrt.mod_referencetype_id = rmrt.mod_referencetype_id
            JOIN referencetype rt ON rt.referencetype_id = mrt.referencetype_id
            WHERE rt.label = :reference_type
            AND mrt.mod_id = :mod_id
        )
        """
        query_params['reference_type'] = reference_type

    # Add species filter if provided
    if species:
        query_str += " AND r.reference_id IN (SELECT reference_id FROM topic_entity_tag WHERE entity = :species)"
        query_params['species'] = species

    # Add the WB species list if the mod is WB and no species filter is provided
    elif mod_abbreviation == 'WB':
        query_str += " AND r.reference_id IN (SELECT reference_id FROM topic_entity_tag WHERE entity in :wb_species_list)"
        query_params['wb_species_list'] = wb_textpresso_species_list

    # Add condition for `from_reference_id` if provided
    if from_reference_id:
        query_str += " AND r.reference_id > :from_reference_id"
        query_params['from_reference_id'] = from_reference_id

    # Add condition for files updated from a specific date if provided
    if files_updated_from_date:
        query_str += " AND rf.date_updated >= :files_updated_from_date"
        query_params['files_updated_from_date'] = files_updated_from_date

    # Add limit for pagination
    query_str += " ORDER BY r.reference_id LIMIT :page_size"
    query_params['page_size'] = page_size

    textpresso_referencefiles = db.execute(text(query_str).bindparams(**query_params)).mappings().fetchall()

    # Aggregate reference files for each reference
    """
    aggregated_reffiles = defaultdict(set)
    for reffile in textpresso_referencefiles:
        aggregated_reffiles[(reffile['reference_id'], reffile['curie'])].add(
            (reffile['referencefile_id'], reffile['md5sum'], reffile['mod_id'] is None, reffile['date_created'])
        )
    """
    aggregated_reffiles = defaultdict(set)
    for reffile in textpresso_referencefiles:
        aggregated_reffiles[(reffile.reference_id, reffile.curie)].add(
            (reffile.referencefile_id, reffile.md5sum, reffile.mod_id is None, reffile.date_created))

    # Return the aggregated results
    return [
        {
            "reference_curie": reference_curie,
            "reference_id": reference_id,
            "main_referencefiles": [
                {
                    "referencefile_id": reffile_data[0],
                    "md5sum": reffile_data[1],
                    "source_is_pmc": reffile_data[2],
                    "date_created": reffile_data[3]
                } for reffile_data in reffiles_data
            ]
        } for (reference_id, reference_curie), reffiles_data in aggregated_reffiles.items()
    ]


def add_to_corpus(db: Session, mod_abbreviation: str, reference_curie: str):  # noqa

    reference = db.query(ReferenceModel).filter_by(curie=reference_curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Reference with curie {reference_curie} does not exist")

    mod = db.query(ModModel).filter_by(abbreviation=mod_abbreviation).first()
    if not mod:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Mod with abbreviation {mod_abbreviation} does not exist")

    mca = db.query(ModCorpusAssociationModel).filter_by(
        reference_id=reference.reference_id, mod_id=mod.mod_id).first()
    try:
        newly_added_mca = False
        if mca:
            if mca.corpus is not True:
                mca.corpus = True
                db.add(mca)
                db.commit()
                newly_added_mca = True
        else:
            mca = ModCorpusAssociationModel(reference_id=reference.reference_id,
                                            mod_id=mod.mod_id,
                                            corpus=True,
                                            mod_corpus_sort_source='manual_creation')
            db.add(mca)
            db.commit()
            newly_added_mca = True
        if newly_added_mca:
            check_xref_and_generate_mod_id(db, reference, mod_abbreviation)
            if get_current_workflow_status(db, reference_curie, "ATP:0000140",
                                           mod_abbreviation) is None:
                transition_to_workflow_status(db, reference_curie, mod_abbreviation, file_needed_tag_atp_id)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Error adding {reference_curie} to {mod_abbreviation} corpus: {e}")


def get_recently_sorted_references(db: Session, mod_abbreviation, days):

    datestamp = str(date.today()).replace("-", "")
    metaData = get_meta_data(mod_abbreviation, datestamp)

    now = datetime.now().date()
    start_date = now - timedelta(days=days)
    end_date = now + timedelta(days=1)  # to cover timezone issue

    refColNmList = ", ".join(get_reference_col_names())

    sql_query = text(
        f"SELECT {refColNmList} "
        "FROM reference "
        "WHERE reference_id IN ("
        "    SELECT reference_id "
        "    FROM mod_corpus_association "
        "    WHERE mod_id = ("
        "        SELECT mod_id "
        "        FROM mod "
        "        WHERE abbreviation = :mod_abbreviation"
        "    ) "
        "    AND corpus = :corpus "
        "    AND date_updated >= :start_date "
        "    AND date_updated < :end_date"
        ") "
        "ORDER BY reference_id"
    )

    rows = db.execute(sql_query, {
        "mod_abbreviation": mod_abbreviation,
        "corpus": True,
        "start_date": start_date,
        "end_date": end_date
    }).fetchall()

    if len(rows) == 0:
        return {
            "metaData": metaData,
            "data": []
        }

    reference_id_list = []
    for x in rows:
        reference_id_list.append(x[0])

    ref_ids = ", ".join([str(x) for x in reference_id_list])

    reference_id_to_xrefs = get_cross_reference_data_for_ref_ids(db, ref_ids)
    reference_id_to_authors = get_author_data_for_ref_ids(db, ref_ids)
    reference_id_to_mesh_terms = get_mesh_term_data_for_ref_ids(db, ref_ids)
    reference_id_to_mod_corpus_data = get_mod_corpus_association_data_for_ref_ids(db, ref_ids)
    reference_id_to_mod_reference_types = get_mod_reference_type_data_for_ref_ids(db, ref_ids)
    reference_id_to_reference_relation_data = get_all_reference_relation_data(db)
    resource_id_to_journal = get_journal_by_resource_id(db)

    data: List[Dict[str, Any]] = []
    generate_json_data(rows, reference_id_to_xrefs, reference_id_to_authors,
                       reference_id_to_reference_relation_data,
                       reference_id_to_mod_reference_types,
                       reference_id_to_mesh_terms,
                       reference_id_to_mod_corpus_data,
                       resource_id_to_journal, data)
    return {
        "metaData": metaData,
        "data": data
    }
