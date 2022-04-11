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
    ).order_by(ReferenceModel.curie.desc()).limit(count)
    references = references_query.all()
    resource_descriptor_default_urls = db.query(ResourceDescriptorModel).all()
    resource_descriptor_default_urls_dict = {
        resource_descriptor_default_url.db_prefix: resource_descriptor_default_url.default_url
        for resource_descriptor_default_url in resource_descriptor_default_urls}

    return [
        ReferenceSchemaNeedReviewShow(
            curie=reference.curie,
            title=reference.title,
            abstract=reference.abstract,
            cross_references=[CrossReferenceSchemaShow(
                curie=xref.curie, url=convert_xref_curie_to_url(xref.curie, resource_descriptor_default_urls_dict),
                is_obsolete=xref.is_obsolete, pages=xref.pages)
                for xref in reference.cross_references])
        for reference in references]
