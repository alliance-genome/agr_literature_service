import logging


from agr_literature_service.api.models import ReferenceModel, WorkflowTagModel, CrossReferenceModel
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ModCorpusAssociationModel, ModModel, ResourceDescriptorModel, ReferencefileModAssociationModel
from agr_literature_service.api.schemas import ReferenceSchemaNeedReviewShow, CrossReferenceSchemaShow, ReferencefileSchemaRelated, ReferencefileModSchemaShow

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
    ).outerjoin(ReferenceModel.copyright_license
    ).order_by(ReferenceModel.curie.desc()).limit(count)
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
            resource_title=reference.resource.title if reference.resource else "",
            referencefiles=[ReferencefileSchemaRelated(
                referencefile_id=rf.referencefile_id,  display_name=rf.display_name,
                file_class=rf.file_class, file_publication_status=rf.file_publication_status,
                file_extension=rf.file_extension,
                md5sum=rf.md5sum, is_annotation=rf.is_annotation,
                referencefile_mods=get_referencefile_mod(rf.referencefile_id, db)) for rf in reference.referencefiles],
            workflow_tags=[{"reference_workflow_tag_id": wft.reference_workflow_tag_id,
                            "workflow_tag_id": wft.workflow_tag_id,
                            "mod_abbreviation": mod_id_to_mod.get(wft.mod_id, '')} for wft in reference.workflow_tag])
        for reference in references]


def show_prepublication_pipeline(mod_abbreviation, count, db: Session):
    query_ref_mod_curatability = db.query(
        WorkflowTagModel
    ).filter(
        WorkflowTagModel.workflow_tag_id.in_(['ATP:0000103', 'ATP:0000104', 'ATP:0000106'])
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
                referencefile_id=rfm.referencefile_id,  referencefile_mod_id=rfm.referencefile_mod_id,
                mod_abbreviation=mod_id_to_mod.get(rfm.mod_id, '')) for rfm in referencefile_mod]
