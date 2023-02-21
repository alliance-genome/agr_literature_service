from typing import Dict, List, Any, Optional
import logging
from datetime import datetime, timezone

from elasticsearch import Elasticsearch
from agr_literature_service.api.config import config
from agr_literature_service.api.models import ReferenceModel
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ModCorpusAssociationModel, ModModel, ResourceDescriptorModel
from agr_literature_service.api.schemas import ReferenceSchemaNeedReviewShow, CrossReferenceSchemaShow

from fastapi import HTTPException, status

logger = logging.getLogger(__name__)


def date_str_to_micro_seconds(date_str: str, start_of_day: bool):
    # convert string to Datetime int that is stored in Elastic search
    # initial strings are in the format:- "2010-10-28T04:00:00.000"
    # So just grab chars before T and converts to seconds after epoch
    # then mulitply by 1000000 and convert to int.
    if start_of_day:
        date_str = f"{date_str.split('T')[0]}T00:00:00-05:00"
    else:
        date_str = f"{date_str.split('T')[0]}T23:59:00-05:00"

    date_time = datetime.fromisoformat(date_str)
    return int(date_time.timestamp() * 1000000)


def search_date_range(es_body,
                      date_pubmed_modified: Optional[List[str]] = None,
                      date_pubmed_arrive: Optional[List[str]] = None,
                      date_published: Optional[List[str]] = None):
    # date_pubmed_X is split to just get the date and remove the time element
    # as elastic search does a tr comparison and if the end date has the time
    # element stil in it fails even if the date bits match as the string is longer.
    if "must" not in es_body["query"]["bool"]["filter"]["bool"]:
        es_body["query"]["bool"]["filter"]["bool"]["must"] = []
    if date_pubmed_modified:
        es_body["query"]["bool"]["filter"]["bool"]["must"].append(
            {
                "range": {
                    "date_last_modified_in_pubmed": {
                        "gte": date_pubmed_modified[0].split('T')[0],
                        "lte": date_pubmed_modified[1].split('T')[0]
                    }
                }
            })
    if date_pubmed_arrive:
        es_body["query"]["bool"]["filter"]["bool"]["must"].append(
            {
                "range": {
                    "date_arrived_in_pubmed": {
                        "gte": date_pubmed_arrive[0].split('T')[0],
                        "lte": date_pubmed_arrive[1].split('T')[0]
                    }
                }
            })
    if date_published:
        try:
            start = date_str_to_micro_seconds(date_published[0], True)
            end = date_str_to_micro_seconds(date_published[1], False)
        except Exception as e:
            logger.error(f"Exception in conversion {e} for start={date_published[0]} and end={date_published[1]}")
        es_body["query"]["bool"]["filter"]["bool"]["must"].append(
            {
                "bool": {
                    "should": [
                        {
                            "range": {
                                "date_published_end": {
                                    "gte": start,
                                    "lte": end
                                }
                            }
                        },
                        {
                            "range": {
                                "date_published_start": {
                                    "lte": start,
                                    "gte": end
                                }
                            }
                        },
                        {
                            "bool": {
                                "must": [
                                    {
                                        "range": {
                                            "date_published_start": {
                                                "lte": start
                                            }
                                        }
                                    },
                                    {
                                        "range": {
                                            "date_published_end": {
                                                "gte": end
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            })


# flake8: noqa: C901
def search_references(query: str = None, facets_values: Dict[str, List[str]] = None,
                      size_result_count: Optional[int] = 10, sort_by_published_date_order: Optional[str] = "asc",
                      page: Optional[int] = 0,
                      facets_limits: Dict[str, int] = None, return_facets_only: bool = False,
                      author_filter: Optional[str] = None, date_pubmed_modified: Optional[List[str]] = None,
                      date_pubmed_arrive: Optional[List[str]] = None,
                      date_published: Optional[List[str]] = None,
                      query_fields: str = None, partial_match: bool = True):
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
                {"abstract": {"type": "unified"}},
                {"keywords": {"type": "unified"}}
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
        "track_total_hits": True,
        "sort": [
            {
                "date_published.keyword": {
                    "order": sort_by_published_date_order
                }
            }
        ]
    }
    if sort_by_published_date_order is None:
        del es_body["sort"]
    elif sort_by_published_date_order not in ["desc", "asc"]:
        del es_body["sort"]
    if return_facets_only:
        del es_body["query"]
        es_body["size"] = 0
        res = es.search(index=config.ELASTICSEARCH_INDEX, body=es_body)
        return {"hits": [], "aggregations": res["aggregations"]}
    if query and (query_fields == "All" or query_fields is None):
        es_body["query"]["bool"]["must"].append({
            "bool": {
                "should":
                    [
                        {
                        "simple_query_string":{
                            "fields":[
                                "title","keywords","abstract"
                            ],
                            "query" : query + "*" if partial_match else query,
                            "analyze_wildcard": "true",
                            "flags" : "PHRASE|PREFIX|WHITESPACE"
                            }
                        },
                        {
                            "wildcard" : {
                                "curie.keyword": "*" + query
                            }
                        },
                        {
                            "wildcard": {
                                "cross_references.curie.keyword": "*" + query
                            }
                        }
                    ]
            }
        })
    elif query and (query_fields == "Title" or query_fields=="Abstract" or query_fields == "Keyword"):
        if query_fields == "Title":
                es_field = "title"
        elif query_fields == "Abstract":
                es_field = "abstract"
        elif query_fields =="Keyword":
                es_field = "keywords"
        es_body["query"]["bool"]["must"].append(
            {
                "simple_query_string":{
                    "fields":[
                        es_field,
                    ],
                    "query" : query + "*" if partial_match else query,
                    "analyze_wildcard": "true",
                    "flags" : "PHRASE|PREFIX|WHITESPACE"
                }
            })
    elif query and query_fields == "Curie":
        es_body["query"]["bool"]["must"].append(
            {
                "wildcard" : {
                    "curie.keyword": "*" + query
                }
            })
    elif query and query_fields == "Xref":
        es_body["query"]["bool"]["must"].append(
            {
                "wildcard": {
                    "cross_references.curie.keyword": "*" + query
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

    date_range = False
    if date_pubmed_modified or date_pubmed_arrive or date_published:
        date_range = True
        search_date_range(es_body, date_pubmed_modified, date_pubmed_arrive, date_published)
    if not facets_values and not date_range:
        del es_body["query"]["bool"]["filter"]

    if author_filter:
        es_body["aggregations"]["authors.name.keyword"]["terms"]["include"] = ".*" + author_filter + ".*"
    res = es.search(index=config.ELASTICSEARCH_INDEX, body=es_body)
    return {
        "hits": [{
            "curie": ref["_source"]["curie"],
            "title": ref["_source"]["title"],
            "date_published": ref["_source"]["date_published"],
            "date_published_start": ref["_source"]["date_published_start"],
            "date_published_end": ref["_source"]["date_published_end"],
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
