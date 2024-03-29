from typing import Dict, List, Any, Optional
import logging
from datetime import datetime

from elasticsearch import Elasticsearch
from agr_literature_service.api.config import config

from fastapi import HTTPException, status

from agr_literature_service.api.crud.topic_entity_tag_utils import get_map_ateam_curies_to_names

logger = logging.getLogger(__name__)


def date_str_to_micro_seconds(date_str: str, start: bool):
    # convert string to Datetime int that is stored in Elastic search
    # initial strings are in the format:- "2010-10-28T04:00:00.000"
    # So just grab chars before T and converts to seconds after epoch
    # then mulitply by 1000000 and convert to int.
    date_time = datetime.fromisoformat(date_str)
    if start:
        return_date= date_time.replace(hour=0,minute=0)
    else:
        return_date = date_time.replace(hour=23, minute=59)

    return int(return_date.timestamp() * 1000000)


def search_date_range(es_body,
                      date_pubmed_modified: Optional[List[str]] = None,
                      date_pubmed_arrive: Optional[List[str]] = None,
                      date_published: Optional[List[str]] = None,
                      date_created: Optional[List[str]] = None):
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
                        "gte": date_pubmed_modified[0],
                        "lte": date_pubmed_modified[1]
                    }
                }
            })
    if date_pubmed_arrive:
        es_body["query"]["bool"]["filter"]["bool"]["must"].append(
            {
                "range": {
                    "date_arrived_in_pubmed": {
                        "gte": date_pubmed_arrive[0],
                        "lte": date_pubmed_arrive[1]
                    }
                }
            })
    if date_created:
        es_body["query"]["bool"]["filter"]["bool"]["must"].append(
            {
                "range": {
                    "date_created": {
                        "gte": date_str_to_micro_seconds(date_created[0],True),
                        "lte": date_str_to_micro_seconds(date_created[1], False)
                    }
                }
            })
    if date_published:
        start = date_published[0]
        end = date_published[1]
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
def search_references(query: str = None, facets_values: Dict[str, List[str]] = None, negated_facets_values: Dict[str, List[str]] = None,
                      size_result_count: Optional[int] = 10, sort_by_published_date_order: Optional[str] = "asc",
                      page: Optional[int] = 1,
                      facets_limits: Dict[str, int] = None, return_facets_only: bool = False,
                      author_filter: Optional[str] = None, date_pubmed_modified: Optional[List[str]] = None,
                      date_pubmed_arrive: Optional[List[str]] = None,
                      date_published: Optional[List[str]] = None,
                      date_created: Optional[List[str]] = None,
                      query_fields: str = None, partial_match: bool = True):
    if query is None and facets_values is None and not return_facets_only:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="requested a search but no query and no facets provided")
    if facets_limits is None:
        facets_limits = {}
    if size_result_count is None:
        size_result_count = 10
    if page is None:
        page = 1

    from_entry = (page-1) * size_result_count
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
                {"keywords": {"type": "unified"}},
                {"citation": {"type": "unified"}}
            ]
        },
        "aggregations": {
            "mod_reference_types.keyword": {
                "terms": {
                    "field": "mod_reference_types.keyword",
                    "size": facets_limits["mod_reference_types.keyword"] if "mod_reference_types.keyword" in facets_limits else 10
                }
            },
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
                    "min_doc_count": 0,
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
            "mods_in_corpus_or_needs_review.keyword": {
                "terms": {
                    "field": "mods_in_corpus_or_needs_review.keyword",
                    "min_doc_count": 0,
                    "size": facets_limits[
                        "mods_in_corpus_or_needs_review.keyword"] if "mods_in_corpus_or_needs_review.keyword" in facets_limits else 10
                }
            },
            "authors.name.keyword": {
                "terms": {
                    "field": "authors.name.keyword",
                    "size": facets_limits[
                        "authors.name.keyword"] if "authors.name.keyword" in facets_limits else 10
                }
            },
            "topic_entity_tags.topic.keyword": {
                "terms": {
                    "field": "topic_entity_tags.topic.keyword",
                    "size": facets_limits[
                        "topic_entity_tags.topic.keyword"] if "topic_entity_tags.topic.keyword" in facets_limits else 10
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
        add_curie_to_name_values(res)
        return {"hits": [], "aggregations": res["aggregations"]}
    if query and (query_fields == "All" or query_fields is None):
        es_body["query"]["bool"]["must"].append({
            "bool": {
                "should":
                    [
                        {
                        "simple_query_string":{
                            "fields":[
                                "title","keywords","abstract","citation"
                            ],
                            "query" : query + "*" if partial_match else query,
                            "analyze_wildcard": "true",
                            "flags" : "PHRASE|PREFIX|WHITESPACE|OR|AND|ESCAPE"
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
    elif query and (query_fields == "Title" or query_fields=="Abstract" or query_fields == "Keyword" or query_fields == "Citation"):
        if query_fields == "Title":
                es_field = "title"
        elif query_fields == "Abstract":
                es_field = "abstract"
        elif query_fields =="Keyword":
                es_field = "keywords"
        elif query_fields =="Citation":
                es_field = "citation"
        es_body["query"]["bool"]["must"].append(
            {
                "simple_query_string":{
                    "fields":[
                        es_field,
                    ],
                    "query" : query + "*" if partial_match else query,
                    "analyze_wildcard": "true",
                    "flags" : "PHRASE|PREFIX|WHITESPACE|OR|AND|ESCAPE"
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

    if negated_facets_values:
        for facet_field, facet_list_values in negated_facets_values.items():
            if "must_not" not in es_body["query"]["bool"]["filter"]["bool"]:
                es_body["query"]["bool"]["filter"]["bool"]["must_not"] = []
            for facet_value in facet_list_values:
                es_body["query"]["bool"]["filter"]["bool"]["must_not"].append({"term": {}})
                es_body["query"]["bool"]["filter"]["bool"]["must_not"][-1]["term"][facet_field] = facet_value

    date_range = False
    if date_pubmed_modified or date_pubmed_arrive or date_published or date_created:
        date_range = True
        search_date_range(es_body, date_pubmed_modified, date_pubmed_arrive, date_published, date_created)
    if not facets_values and not date_range and not negated_facets_values:
        del es_body["query"]["bool"]["filter"]

    if author_filter:
        es_body["aggregations"]["authors.name.keyword"]["terms"]["include"] = ".*" + author_filter + ".*"
    res = es.search(index=config.ELASTICSEARCH_INDEX, body=es_body)
    add_curie_to_name_values(res)
    return {
        "hits": [{
            "curie": ref["_source"]["curie"],
            "citation": ref["_source"]["citation"],
            "title": ref["_source"]["title"],
            "date_published": ref["_source"]["date_published"],
            "date_published_start": ref["_source"]["date_published_start"],
            "date_published_end": ref["_source"]["date_published_end"],
            "date_created": ref["_source"]["date_created"],
            "abstract": ref["_source"]["abstract"],
            "cross_references": ref["_source"]["cross_references"],
            "authors": ref["_source"]["authors"],
            "mod_reference_types": ref["_source"]["mod_reference_types"],
            "highlight":
                ref["highlight"] if "highlight" in ref else ""
        } for ref in res["hits"]["hits"]],
        "aggregations": res["aggregations"],
        "return_count": res["hits"]["total"]["value"]
    }


def add_curie_to_name_values(es_result_object):
    aggregations_atp = ["topic_entity_tags.topic.keyword"]
    curie_to_name_map = get_map_ateam_curies_to_names(curies_category="atpterm", curies=[
        bucket["key"] for aggreg_to_map in aggregations_atp for bucket in es_result_object["aggregations"][
            aggreg_to_map]["buckets"]])
    for aggreg_to_map in aggregations_atp:
        for idx, bucket in enumerate(es_result_object["aggregations"][aggreg_to_map]["buckets"]):
            es_result_object["aggregations"][aggreg_to_map]["buckets"][idx]["name"] = curie_to_name_map[
                bucket["key"].upper()]

