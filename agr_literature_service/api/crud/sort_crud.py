import logging
from sqlalchemy.orm import Session
from sqlalchemy import text

from agr_literature_service.api.models import ReferenceModel, WorkflowTagModel, CrossReferenceModel,\
    ModCorpusAssociationModel, ModModel, ResourceDescriptorModel, ReferencefileModAssociationModel
from agr_literature_service.api.schemas import ReferenceSchemaNeedReviewShow, \
    CrossReferenceSchemaShow, ReferencefileSchemaRelated, ReferencefileModSchemaShow
from agr_literature_service.api.crud.reference_crud import get_past_to_present_date_range

logger = logging.getLogger(__name__)


def convert_xref_curie_to_url(curie, resource_descriptor_default_urls):
    db_prefix, local_id = curie.split(":", 1)
    if db_prefix in resource_descriptor_default_urls:
        return resource_descriptor_default_urls[db_prefix].replace("[%s]", local_id)
    return None


def show_need_review(mod_abbreviation, count, db: Session):
    references_query = db.query(
        ReferenceModel
    ).join(
        ReferenceModel.mod_corpus_association
    ).filter(
        ModCorpusAssociationModel.corpus == None # noqa
    ).join(
        ModCorpusAssociationModel.mod
    ).filter(
        ModModel.abbreviation == mod_abbreviation
    ).outerjoin(
        ReferenceModel.copyright_license
    ).order_by(
        ReferenceModel.curie.desc()
    ).limit(count)
    references = references_query.all()
    return show_sort_result(references, mod_abbreviation, db)


def show_sort_result(references, mod_abbreviation, db):
    resource_descriptor_default_urls = db.query(ResourceDescriptorModel).all()
    resource_descriptor_default_urls_dict = {
        resource_descriptor_default_url.db_prefix: resource_descriptor_default_url.default_url
        for resource_descriptor_default_url in resource_descriptor_default_urls}

    mod_id_to_mod = dict([(x.mod_id, x.abbreviation) for x in db.query(ModModel).all()])

    return [
        ReferenceSchemaNeedReviewShow(
            curie=reference.curie,
            title=reference.title,
            abstract=reference.abstract,
            category=reference.category,
            copyright_license_name=reference.copyright_license.name if reference.copyright_license else "",
            copyright_license_url=reference.copyright_license.url if reference.copyright_license else "",
            copyright_license_description=reference.copyright_license.description if reference.copyright_license else "",
            copyright_license_open_access=reference.copyright_license.open_access if reference.copyright_license else "",
            cross_references=[CrossReferenceSchemaShow(
                cross_reference_id=xref.cross_reference_id, curie=xref.curie, curie_prefix=xref.curie_prefix,
                url=convert_xref_curie_to_url(xref.curie, resource_descriptor_default_urls_dict),
                is_obsolete=xref.is_obsolete, pages=xref.pages) for xref in reference.cross_reference],
            mod_corpus_association_corpus=[mca.corpus for mca in reference.mod_corpus_association if
                                           mca.mod.abbreviation == mod_abbreviation][0],
            mod_corpus_association_id=[mca.mod_corpus_association_id for mca in reference.mod_corpus_association if
                                       mca.mod.abbreviation == mod_abbreviation][0],
            prepublication_pipeline=reference.prepublication_pipeline,
            pubmed_publication_status=reference.pubmed_publication_status if reference.pubmed_publication_status else None,
            resource_title=reference.resource.title if reference.resource else "",
            referencefiles=[ReferencefileSchemaRelated(
                referencefile_id=rf.referencefile_id, display_name=rf.display_name,
                file_class=rf.file_class, file_publication_status=rf.file_publication_status,
                file_extension=rf.file_extension,
                md5sum=rf.md5sum, is_annotation=rf.is_annotation,
                referencefile_mods=get_referencefile_mod(rf.referencefile_id, db)) for rf in reference.referencefiles],
            authors=[{"order": author.order,
                      "author_id": author.author_id,
                      "name": author.name} for author in reference.author],
            workflow_tags=[{"reference_workflow_tag_id": wft.reference_workflow_tag_id,
                            "workflow_tag_id": wft.workflow_tag_id,
                            "mod_abbreviation": mod_id_to_mod.get(wft.mod_id, '')} for wft in reference.workflow_tag])
        for reference in references]


def show_prepublication_pipeline(mod_abbreviation, count, db: Session):
    query_ref_mod_curatability = db.query(
        WorkflowTagModel
    ).filter(
        WorkflowTagModel.workflow_tag_id.in_(['ATP:0000103', 'ATP:0000104', 'ATP:0000106'])
    ).join(
        ModCorpusAssociationModel.mod
    ).filter(
        ModModel.abbreviation == mod_abbreviation
    )
    wfts = query_ref_mod_curatability.all()
    references_with_curatability = []
    for wft in wfts:
        references_with_curatability.append(wft.reference_id)

    query_prepub_references = db.query(
        ReferenceModel
    ).filter_by(
        prepublication_pipeline=True
    )
    pp_refs = query_prepub_references.all()
    references_with_prepublication = []
    for pp_ref in pp_refs:
        references_with_prepublication.append(pp_ref.reference_id)

    # filtering by prepublication_pipeline and then sorting at the end takes 2.5 seconds,
    # doing a separate query for prepub references and filtering by that takes 1 second.
    references_query = db.query(
        ReferenceModel
    ).filter(
        ReferenceModel.reference_id.in_(references_with_prepublication)
    ).join(
        ReferenceModel.mod_corpus_association
    ).join(
        ModCorpusAssociationModel.mod
    ).filter(
        ModModel.abbreviation == mod_abbreviation
    ).join(
        CrossReferenceModel, CrossReferenceModel.reference_id == ReferenceModel.reference_id
    ).filter(
        CrossReferenceModel.curie_prefix == 'PMID'  # noqa
    ).outerjoin(
        ReferenceModel.copyright_license
    ).filter(
        ReferenceModel.reference_id.notin_(references_with_curatability)
    ).order_by(
        ReferenceModel.curie.desc()
    ).limit(count)
    references = references_query.all()
    return show_sort_result(references, mod_abbreviation, db)


def get_referencefile_mod(referencefile_id, db: Session):
    referencefile_mod_query = db.query(
        ReferencefileModAssociationModel
    ).filter(
        ReferencefileModAssociationModel.referencefile_id == referencefile_id
    )
    referencefile_mod = referencefile_mod_query.all()
    mod_id_to_mod = dict([(x.mod_id, x.abbreviation) for x in db.query(ModModel).all()])
    return [
        ReferencefileModSchemaShow(
            referencefile_id=rfm.referencefile_id, referencefile_mod_id=rfm.referencefile_mod_id,
            mod_abbreviation=mod_id_to_mod.get(rfm.mod_id, '')) for rfm in referencefile_mod]


def get_mod_curators(db: Session, mod_abbreviation):

    _, one_month_ago, _ = get_past_to_present_date_range(30)

    sql_query_str = """
        SELECT u.id, u.email
        FROM users u
        INNER JOIN mod_corpus_association mca ON mca.updated_by = u.id
        INNER JOIN mod m ON mca.mod_id = m.mod_id
        WHERE mca.corpus IS NOT NULL
        AND m.abbreviation = :mod_abbreviation
        AND u.email is NOT NULL
        AND mca.date_updated >= :one_month_ago
    """
    sql_query = text(sql_query_str)
    result = db.execute(sql_query, {
        'mod_abbreviation': mod_abbreviation,
        'one_month_ago': one_month_ago
    })
    return {row[1]: row[0] for row in result}


def get_recently_sorted_reference_ids(db: Session, mod_abbreviation, count, curator_okta_id, day):

    _, start_date, end_date = get_past_to_present_date_range(day)

    sql_query_str = """
        SELECT DISTINCT mcav.reference_id, mcav.date_updated
        FROM mod_corpus_association_version mcav
        INNER JOIN mod m ON m.mod_id = mcav.mod_id
        WHERE m.abbreviation = :mod_abbreviation
          AND (mcav.corpus = TRUE or mcav.corpus = FALSE)
          AND mcav.corpus_mod = TRUE
          AND mcav.operation_type IN (0, 1)
          AND mcav.date_updated >= :start_date
          AND mcav.date_updated < :end_date
    """

    params = {
        "mod_abbreviation": mod_abbreviation,
        "start_date": start_date,
        "end_date": end_date
    }

    if curator_okta_id is not None:
        sql_query_str += " AND mcav.updated_by = :curator_okta_id"
        params["curator_okta_id"] = curator_okta_id

    sql_query_str += " ORDER BY mcav.date_updated DESC"

    if count is not None:
        sql_query_str += " LIMIT :result_limit"
        params["result_limit"] = count

    sql_query = text(sql_query_str)
    rows = db.execute(sql_query, params)
    reference_ids = [row[0] for row in rows]
    return reference_ids


def show_recently_sorted(db: Session, mod_abbreviation, count, curator, day):

    email_to_okta_id_mapping = get_mod_curators(db, mod_abbreviation)

    reference_ids = get_recently_sorted_reference_ids(db, mod_abbreviation, count, curator, day)

    references_query = (
        db.query(ReferenceModel)
        .join(ReferenceModel.mod_corpus_association)
        .filter(ModCorpusAssociationModel.corpus.in_([True, False]))
        .join(ModCorpusAssociationModel.mod)
        .filter(ModModel.abbreviation == mod_abbreviation)
        .outerjoin(ReferenceModel.copyright_license)
        .filter(ReferenceModel.reference_id.in_(reference_ids))
        .order_by(ModCorpusAssociationModel.date_updated)
    )
    references = references_query.all()
    data = show_sort_result(references, mod_abbreviation, db)
    return {
        "curator_data": email_to_okta_id_mapping,
        "data": data
    }
