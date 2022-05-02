from typing import Dict, List, Any

from elasticsearch import Elasticsearch
from literature.config import config
from literature.models import ReferenceModel
from sqlalchemy.orm import Session

from literature.models import ModCorpusAssociationModel, ModModel, ResourceDescriptorModel
from literature.schemas import ReferenceSchemaNeedReviewShow, CrossReferenceSchemaShow

from fastapi import HTTPException, status


def search_references(query: str = None, facets_values: Dict[str, List[str]] = None,
                      facets_limits: Dict[str, int] = None, return_facets_only: bool = False):
    if query is None and facets_values is None and not return_facets_only:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="requested a search but no query and no facets provided")
    if facets_limits is None:
        facets_limits = {}
    es_host = config.ELASTICSEARCH_HOST
    es = Elasticsearch(hosts=es_host + ":" + config.ELASTICSEARCH_PORT)
    es_body: Dict[str, Any] = {
        "query": {
            "bool": {
                "must": [],
                "filter": {
                    "bool": {}
                }
            }
        },
        "aggregations": {
            "pubmed_types.keyword": {
                "terms": {
                    "field": "pubmed_types.keyword",
                    "size": facets_limits["pubmed_types.keyword"] if "pubmed_types.keyword" in facets_limits else 10
                }
            }
        }
    }
    if return_facets_only:
        del es_body["query"]
        es_body["size"] = 0
        res = es.search(index="references_index", body=es_body)
        return {"hits": [], "aggregations": res["aggregations"]}
    if query:
        es_body["query"]["bool"]["must"].append({"match": {"title": query}})
    if facets_values:
        for facet_field, facet_list_values in facets_values.items():
            es_body["query"]["bool"]["filter"]["bool"]["must"] = []
            es_body["query"]["bool"]["filter"]["bool"]["must"].append({"bool": {"should": []}})
            for facet_value in facet_list_values:
                es_body["query"]["bool"]["filter"]["bool"]["must"][-1]["bool"]["should"].append({"term": {}})
                es_body["query"]["bool"]["filter"]["bool"]["must"][-1]["bool"]["should"][-1]["term"][facet_field] = facet_value
    else:
        del es_body["query"]["bool"]["filter"]
    res = es.search(index="references_index", body=es_body)
    return {
        "hits": [{"curie": ref["_source"]["curie"], "title": ref["_source"]["title"]} for ref in res["hits"]["hits"]],
        "aggregations": res["aggregations"]
    }


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
            mod_corpus_association_id=[mca.mod_corpus_association_id for mca in reference.mod_corpus_association if
                                       mca.mod.abbreviation == mod_abbreviation][0],
            resource_title=reference.resource.title if reference.resource else "",
            cross_references=[CrossReferenceSchemaShow(
                curie=xref.curie, url=convert_xref_curie_to_url(xref.curie, resource_descriptor_default_urls_dict),
                is_obsolete=xref.is_obsolete, pages=xref.pages)
                for xref in reference.cross_reference])
        for reference in references]
