from typing import List

from elasticsearch import Elasticsearch
from literature.config import config
from literature.models import ReferenceModel
from sqlalchemy.orm import Session

from literature.models import ModCorpusAssociationModel, ModModel, CrossReferenceModel, ResourceDescriptorModel
from literature.schemas import ReferenceSchemaNeedReviewShow, CrossReferenceSchemaShow


def search_references(query):
    es_host = config.ELASTICSEARCH_HOST
    es = Elasticsearch(hosts=es_host + ":" + config.ELASTICSEARCH_PORT)
    res = es.search(index="references_index",
                    body={
                        "query": {
                            "match": {
                                "title": query
                            }
                        }
                    })
    return [{"curie": ref["_source"]["curie"], "title": ref["_source"]["title"]} for ref in res["hits"]["hits"]]


def show_need_review(mod_abbreviation, db: Session):
    references_query = db.query(
        ReferenceModel.curie,
        ReferenceModel.title,
        ReferenceModel.abstract,
        CrossReferenceModel.curie.label("cross_reference_curie")
    ).join(
        ReferenceModel.cross_references
    ).join(
        ReferenceModel.mod_corpus_association
    ).filter(
        ModCorpusAssociationModel.corpus == None # noqa
    ).join(
        ModCorpusAssociationModel.mod
    ).filter(
        ModModel.abbreviation == mod_abbreviation
    )
    references_xref_joined = references_query.all()
    resource_descriptor_default_urls = db.query(ResourceDescriptorModel).all()
    resource_descriptor_default_urls_dict = {
        resource_descriptor_default_url.db_prefix: resource_descriptor_default_url.default_url
        for resource_descriptor_default_url in resource_descriptor_default_urls}
    ref_curie_xref_set = set()
    references = []
    for reference_xref_joined in references_xref_joined:
        if reference_xref_joined.curie not in ref_curie_xref_set:
            cross_references: List[CrossReferenceSchemaShow] = []
            references.append(ReferenceSchemaNeedReviewShow(
                curie=reference_xref_joined.curie,
                title=reference_xref_joined.title,
                abstract=reference_xref_joined.abstract,
                cross_references=cross_references))
            ref_curie_xref_set.add(reference_xref_joined.curie)
        db_prefix, local_id = reference_xref_joined.cross_reference_curie.split(":", 1)
        if resource_descriptor_default_urls:
            xref_url = resource_descriptor_default_urls_dict[db_prefix].replace("[%s]", local_id)
            if references[-1].cross_references is not None:
                references[-1].cross_references.append(CrossReferenceSchemaShow(
                    curie=reference_xref_joined.cross_reference_curie, url=xref_url))
    return references
