from typing import Dict, List, Any, Optional

from elasticsearch import Elasticsearch
from agr_literature_service.api.config import config
from agr_literature_service.api.models import ReferenceModel
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ModCorpusAssociationModel, ModModel, ResourceDescriptorModel
from agr_literature_service.api.schemas import ReferenceSchemaNeedReviewShow, CrossReferenceSchemaShow

from fastapi import HTTPException, status


def search_references(query: str = None, facets_values: Dict[str, List[str]] = None,
                      size_result_count: Optional[int] = 10, page: Optional[int] = 0,
                      facets_limits: Dict[str, int] = None, return_facets_only: bool = False,
                      author_filter: Optional[str] = None, date_pubmed_modified: Optional[List[str]] = None,
                      date_pubmed_arrive: Optional[List[str]] = None, query_fields: str = None):
    if query is None and facets_values is None and not return_facets_only:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="requested a search but no query and no facets provided")
    if facets_limits is None:
        facets_limits = {}
    if size_result_count is None:
        size_result_count = 10
    if page is None:
        page = 0
    from_entry = page * size_result_count
    es_host = config.ELASTICSEARCH_HOST
    es = Elasticsearch(hosts=es_host + ":" + config.ELASTICSEARCH_PORT)
    es_body: Dict[str, Any] = {
        "query": {
            "bool": {
                "must": [],
                "should": [],
                "filter": {
                    "bool": {}
                }
            }
        },
        "highlight": {
            "fields": [
                {"title": {"type": "unified"}},
                {"abstract": {"type": "unified"}}
            ]
        },
        "aggregations": {
            "pubmed_types.keyword": {
                "terms": {
                    "field": "pubmed_types.keyword",
                    "size": facets_limits["pubmed_types.keyword"] if "pubmed_types.keyword" in facets_limits else 10
                }
            },
            "category.keyword": {
                "terms": {
                    "field": "category.keyword",
                    "size": facets_limits["category.keyword"] if "category.keyword" in facets_limits else 10
                }
            },
            "pubmed_publication_status.keyword": {
                "terms": {
                    "field": "pubmed_publication_status.keyword",
                    "size": facets_limits["pubmed_publication_status.keyword"] if "pubmed_publication_status.keyword" in facets_limits else 10
                }
            },
            "mods_in_corpus.keyword": {
                "terms": {
                    "field": "mods_in_corpus.keyword",
                    "min_doc_count": 0,
                    "size": facets_limits[
                        "mods_in_corpus.keyword"] if "mods_in_corpus.keyword" in facets_limits else 10
                }
            },
            "mods_needs_review.keyword": {
                "terms": {
                    "field": "mods_needs_review.keyword",
                    "min_doc_count": 0,
                    "size": facets_limits[
                        "mods_needs_review.keyword"] if "mods_needs_review.keyword" in facets_limits else 10
                }
            },
            "authors.name.keyword": {
                "terms": {
                    "field": "authors.name.keyword",
                    "size": facets_limits[
                        "authors.name.keyword"] if "authors.name.keyword" in facets_limits else 10
                }
            }
        },
        "from": from_entry,
        "size": size_result_count,
        "track_total_hits": True
    }
    if return_facets_only:
        del es_body["query"]
        es_body["size"] = 0
        res = es.search(index=config.ELASTICSEARCH_INDEX, body=es_body)
        return {"hits": [], "aggregations": res["aggregations"]}
    if query and (query_fields == "all" or query_fields is None):
        es_body["query"]["bool"]["should"] = [
            {
                "wildcard" if "*" in query or "?" in query else "match": {
                    "title": query
                }
            },
            {
                "wildcard" if "*" in query or "?" in query else "match": {
                    "abstract": query
                }
            }
        ]
    elif query and query_fields == "title":
        es_body["query"]["bool"]["must"].append(
            {
                "wildcard" if "*" in query or "?" in query else "match": {
                    "title": query
                }
            })
    elif query and query_fields == "abstract":
        es_body["query"]["bool"]["must"].append(
            {
                "wildcard" if "*" in query or "?" in query else "match": {
                    "abstract": query
                }
            })
    if facets_values:
        for facet_field, facet_list_values in facets_values.items():
            if "must" not in es_body["query"]["bool"]["filter"]["bool"]:
                es_body["query"]["bool"]["filter"]["bool"]["must"] = []
            es_body["query"]["bool"]["filter"]["bool"]["must"].append({"bool": {"must": []}})
            for facet_value in facet_list_values:
                es_body["query"]["bool"]["filter"]["bool"]["must"][-1]["bool"]["must"].append({"term": {}})
                es_body["query"]["bool"]["filter"]["bool"]["must"][-1]["bool"]["must"][-1]["term"][facet_field] = facet_value
    if date_pubmed_modified or date_pubmed_arrive:
        if "must" not in es_body["query"]["bool"]["filter"]["bool"]:
            es_body["query"]["bool"]["filter"]["bool"]["must"] = []
        if date_pubmed_modified:
            es_body["query"]["bool"]["filter"]["bool"]["must"].append(
                {
                    "range": {
                        "date_last_modified_in_pubmed": {
                            "gte": date_pubmed_modified[0],
                            "lt": date_pubmed_modified[1]
                        }
                    }
                })
        if date_pubmed_arrive:
            es_body["query"]["bool"]["filter"]["bool"]["must"].append(
                {
                    "range": {
                        "date_arrived_in_pubmed": {
                            "gte": date_pubmed_arrive[0],
                            "lt": date_pubmed_arrive[1]
                        }
                    }
                })
    if not facets_values and not date_pubmed_modified and not date_pubmed_arrive:
        del es_body["query"]["bool"]["filter"]
    if author_filter:
        es_body["aggregations"]["authors.name.keyword"]["terms"]["include"] = ".*" + author_filter + ".*"
    res = es.search(index=config.ELASTICSEARCH_INDEX, body=es_body)
    return {
        "hits": [{
            "curie": ref["_source"]["curie"],
            "title": ref["_source"]["title"],
            "date_published": ref["_source"]["date_published"],
            "abstract": ref["_source"]["abstract"],
            "cross_references": ref["_source"]["cross_references"],
            "authors": ref["_source"]["authors"],
            "highlight":
                ref["highlight"] if "highlight" in ref else ""
        } for ref in res["hits"]["hits"]],
        "aggregations": res["aggregations"],
        "return_count": res["hits"]["total"]["value"]
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

    mod_id_to_mod = dict([(x.mod_id, x.abbreviation) for x in db.query(ModModel).all()])

    return [
        ReferenceSchemaNeedReviewShow(
            curie=reference.curie,
            title=reference.title,
            abstract=reference.abstract,
            category=reference.category,
            mod_corpus_association_id=[mca.mod_corpus_association_id for mca in reference.mod_corpus_association if
                                       mca.mod.abbreviation == mod_abbreviation][0],
            resource_title=reference.resource.title if reference.resource else "",
            cross_references=[CrossReferenceSchemaShow(
                cross_reference_id=xref.cross_reference_id, curie=xref.curie, curie_prefix=xref.curie_prefix,
                url=convert_xref_curie_to_url(xref.curie, resource_descriptor_default_urls_dict),
                is_obsolete=xref.is_obsolete, pages=xref.pages) for xref in reference.cross_reference],
            workflow_tags=[{"reference_workflow_tag_id": wft.reference_workflow_tag_id, "workflow_tag_id": wft.workflow_tag_id, "mod_abbreviation": mod_id_to_mod.get(wft.mod_id, '')} for wft in reference.workflow_tag])
        for reference in references]
