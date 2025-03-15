from collections import defaultdict
from typing import Dict, List, Any, Optional
import logging
from datetime import datetime
import re

from elasticsearch import Elasticsearch
from agr_literature_service.api.config import config

from fastapi import HTTPException, status

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.crud.topic_entity_tag_utils import get_map_ateam_curies_to_names
from agr_literature_service.api.crud.workflow_tag_crud import atp_get_all_descendents

logger = logging.getLogger(__name__)

file_workflow_root_ids = ["ATP:0000140", "ATP:0000161"]
reference_classification_root_ids = ["ATP:0000165"]
entity_extraction_root_ids = ["ATP:0000172"]
manual_indexing_root_ids = ["ATP:0000273"]

# ------------------- Helper Functions for TET Facets -------------------
def processCombinedTETFacets(data, tetNestedFacetsValues):
    """Combine TET facet values into a single dictionary using the first value of each facet."""
    TET_FACETS_LIST = ["topics", "confidence_levels", "source_methods", "source_evidence_assertions"]
    for facet in TET_FACETS_LIST:
        if facet in data and data[facet]:
            facet_key = f"topic_entity_tags.{facet[:-1]}.keyword"  # e.g., topics -> topic
            tetNestedFacetsValues.append({facet_key: data[facet][0]})

def processSingleFacet(facetArray, facetKey, tetNestedFacetsValues):
    """Append each value of a single TET facet to the nested facets list."""
    for value in facetArray:
        tetNestedFacetsValues.append({facetKey: value})
# ---------------- End Helper Functions for TET Facets -------------------

def date_str_to_micro_seconds(date_str: str, start: bool):
    """Convert an ISO date string to microseconds since epoch."""
    date_time = datetime.fromisoformat(date_str)
    if start:
        return_date = date_time.replace(hour=0, minute=0)
    else:
        return_date = date_time.replace(hour=23, minute=59)
    return int(return_date.timestamp() * 1000000)

def search_date_range(es_body,
                      date_pubmed_modified: Optional[List[str]] = None,
                      date_pubmed_arrive: Optional[List[str]] = None,
                      date_published: Optional[List[str]] = None,
                      date_created: Optional[List[str]] = None):
    """Add date range filters to the ES query."""
    if "must" not in es_body["query"]["bool"]["filter"]["bool"]:
        es_body["query"]["bool"]["filter"]["bool"]["must"] = []
    if date_pubmed_modified:
        es_body["query"]["bool"]["filter"]["bool"]["must"].append({
            "range": {"date_last_modified_in_pubmed": {"gte": date_pubmed_modified[0], "lte": date_pubmed_modified[1]}}
        })
    if date_pubmed_arrive:
        es_body["query"]["bool"]["filter"]["bool"]["must"].append({
            "range": {"date_arrived_in_pubmed": {"gte": date_pubmed_arrive[0], "lte": date_pubmed_arrive[1]}}
        })
    if date_created:
        es_body["query"]["bool"]["filter"]["bool"]["must"].append({
            "range": {"date_created": {"gte": date_str_to_micro_seconds(date_created[0], True),
                                        "lte": date_str_to_micro_seconds(date_created[1], False)}}
        })
    if date_published:
        start = date_published[0]
        end = date_published[1]
        es_body["query"]["bool"]["filter"]["bool"]["must"].append({
            "bool": {
                "should": [
                    {"range": {"date_published_end": {"gte": start, "lte": end}}},
                    {"range": {"date_published_start": {"lte": start, "gte": end}}},
                    {"bool": {"must": [
                        {"range": {"date_published_start": {"lte": start}}},
                        {"range": {"date_published_end": {"gte": end}}}
                    ]}}
                ]
            }
        })

# ------------------- getSearchParams Function -------------------
def getSearchParams(request_body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build query parameters from the request body.
    Expects keys such as 'searchQuery', 'searchFacetsValues', etc.
    """
    query_str = request_body.get("searchQuery", "")
    query_str = re.sub(r'\|', r'\\|', query_str)
    query_str = re.sub(r'\+', r'\\+', query_str)
    query_str = re.sub(r'OR', r'|', query_str)
    query_str = re.sub(r'AND', r'+', query_str)
    query_str = query_str.strip()

    params = {
        "query": query_str,
        "size_result_count": request_body.get("searchSizeResultsCount", 10),
        "page": request_body.get("searchResultsPage", 1),
        "negated_facets_values": request_body.get("searchExcludedFacetsValues", {}),
        "facets_limits": request_body.get("searchFacetsLimits", {}),
        "author_filter": request_body.get("authorFilter", ""),
        "query_fields": request_body.get("query_fields", "All"),
        "sort_by_published_date_order": request_body.get("sortByPublishedDate", "asc"),
        "partial_match": request_body.get("partialMatch", True),
        "mod_abbreviation": request_body.get("mod_abbreviation", "SGD")
    }

    data = request_body.get("searchFacetsValues", {})
    tetNestedFacetsValues = []
    facetsValues = {}
    TET_FACETS_LIST = ["topics", "confidence_levels", "source_methods", "source_evidence_assertions"]
    if request_body.get("applyToSingleTag", False):
        processCombinedTETFacets(data, tetNestedFacetsValues)
    else:
        for key in TET_FACETS_LIST:
            if key in data:
                facetType = key[:-1]
                keyword = f"topic_entity_tags.{facetType}.keyword"
                processSingleFacet(data[key], keyword, tetNestedFacetsValues)
    for key in data:
        if key not in TET_FACETS_LIST:
            facetsValues[key] = data[key]

    WORKFLOW_FACETS = ["file_workflow", "manual_indexing", "entity_extraction", "reference_classification"]
    for key in data:
        if key not in TET_FACETS_LIST or key in WORKFLOW_FACETS:
            facetsValues[key] = data[key]

    # Force mod filters to be lists.
    facetsValues["workflow_tags.mod_abbreviation"] = request_body.get("workflow_tags.mod_abbreviation", [params["mod_abbreviation"]])
    facetsValues["topic_entity_tags.data_provider.keyword"] = request_body.get("topic_entity_tags.data_provider.keyword", [params["mod_abbreviation"]])
    
    params["facets_values"] = facetsValues
    params["tet_nested_facets_values"] = {
        "apply_to_single_tag": request_body.get("applyToSingleTag", False),
        "tet_facets_values": tetNestedFacetsValues
    }
    
    if request_body.get("datePubmedModified"):
        params["date_pubmed_modified"] = request_body["datePubmedModified"]
    if request_body.get("datePubmedAdded"):
        params["date_pubmed_arrive"] = request_body["datePubmedAdded"]
    if request_body.get("datePublished"):
        params["date_published"] = request_body["datePublished"]
    if request_body.get("dateCreated"):
        params["date_created"] = request_body["dateCreated"]

    return params
# ---------------- End getSearchParams -------------------

def remap_highlights(highlights):
    """Replace 'authors.name' with 'authors' in highlight keys."""
    remapped = {}
    for key, value in highlights.items():
        remapped[key.replace("authors.name", "authors")] = value
    return remapped

def extract_tet_aggregation_data(res: Dict[str, Any], main_key: str, data_key: str) -> Dict[str, Any]:
    main_agg = res["aggregations"].get(main_key, {})
    if main_agg.get("filter_by_other_tet_values"):
        return main_agg.get("filter_by_other_tet_values", {}).get(data_key, {})
    return main_agg.get(data_key, {})

def add_tet_facets_values(es_body, tet_nested_facets_values, apply_to_single_tet) -> Dict[str, List[str]]:
    tet_facet_values = defaultdict(list)
    for facet_name_value_dict in tet_nested_facets_values.get("tet_facets_values", []):
        add_nested_query(es_body, facet_name_value_dict)
        if apply_to_single_tet:
            for facet_name, facet_value in facet_name_value_dict.items():
                tet_facet_values[facet_name.replace("topic_entity_tags.", "").replace(".keyword", "")] = facet_value
    return tet_facet_values

def add_nested_query(es_body, facet_name_values_dict):
    mod_value = facet_name_values_dict.get("topic_entity_tags.data_provider.keyword")
    if not mod_value or not isinstance(mod_value, str):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Missing data_provider filter for topic_entity_tags search")
    must_conditions = [{"term": {k: v}} for k, v in facet_name_values_dict.items()]
    nested_query = {
        "nested": {
            "path": "topic_entity_tags",
            "query": {"bool": {"must": must_conditions}}
        }
    }
    es_body["query"]["bool"]["filter"]["bool"]["must"].append(nested_query)

def create_filtered_aggregation(path, tet_facets, term_field, term_key, size=10, mod: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a nested aggregation for TETs.
    If a mod is provided, wrap the inner aggregation with a filter on "topic_entity_tags.data_provider.keyword".
    """
    tet_agg = {"nested": {"path": path}}
    inner_agg = {
        "terms": {"field": term_field, "size": size},
        "aggs": {"docs_count": {"reverse_nested": {}}}
    }
    if mod:
        tet_agg["aggs"] = {
            "filtered_by_mod": {
                "filter": {"term": {"topic_entity_tags.data_provider.keyword": mod}},
                "aggs": {term_key: inner_agg}
            }
        }
    else:
        tet_agg["aggs"] = {term_key: inner_agg}
    return tet_agg

def apply_all_tags_tet_aggregations(es_body, tet_facets, facets_limits, tet_mod: Optional[str] = None) -> None:
    es_body["aggregations"]["topic_aggregation"] = create_filtered_aggregation(
        path="topic_entity_tags",
        tet_facets=tet_facets,
        term_field="topic_entity_tags.topic.keyword",
        term_key="topics",
        size=facets_limits.get("topics", 10),
        mod=tet_mod
    )
    es_body["aggregations"]["confidence_aggregation"] = create_filtered_aggregation(
        path="topic_entity_tags",
        tet_facets=tet_facets,
        term_field="topic_entity_tags.confidence_level.keyword",
        term_key="confidence_levels",
        size=facets_limits.get("confidence_levels", 10),
        mod=tet_mod
    )
    es_body["aggregations"]["source_method_aggregation"] = create_filtered_aggregation(
        path="topic_entity_tags",
        tet_facets=tet_facets,
        term_field="topic_entity_tags.source_method.keyword",
        term_key="source_methods",
        size=facets_limits.get("source_methods", 10),
        mod=tet_mod
    )
    es_body["aggregations"]["source_evidence_assertion_aggregation"] = create_filtered_aggregation(
        path="topic_entity_tags",
        tet_facets=tet_facets,
        term_field="topic_entity_tags.source_evidence_assertion.keyword",
        term_key="source_evidence_assertions",
        size=facets_limits.get("source_evidence_assertions", 10),
        mod=tet_mod
    )

def ensure_structure(es_body):
    if "query" not in es_body:
        es_body["query"] = {}
    if "bool" not in es_body["query"]:
        es_body["query"]["bool"] = {}
    if "filter" not in es_body["query"]["bool"]:
        es_body["query"]["bool"]["filter"] = {"bool": {}}
    if "must" not in es_body["query"]["bool"]["filter"]["bool"]:
        es_body["query"]["bool"]["filter"]["bool"]["must"] = []

def add_curie_to_name_values(aggregations):
    curie_keys = [bucket["key"] for bucket in aggregations.get("buckets", [])]
    curie_to_name_map = get_map_ateam_curies_to_names(
        category="atpterm",
        curies=[ck.upper() for ck in curie_keys if ck.upper().startswith("ATP:")]
    )
    curie_to_name_map.update(get_map_ateam_curies_to_names(
        category="ecoterm",
        curies=[ck.upper() for ck in curie_keys if ck.upper().startswith("ECO:")]
    ))
    for bucket in aggregations.get("buckets", []):
        bucket["name"] = curie_to_name_map.get(bucket["key"].upper(), "Unknown")

def get_atp_ids(root_atp_ids):
    return [child for root_atp_id in root_atp_ids for child in atp_get_all_descendents(root_atp_id)]


def process_search_results(res: Dict[str, Any]) -> Dict[str, Any]:
    hits = [{
        "curie": ref["_source"]["curie"],
        "citation": ref["_source"]["citation"],
        "title": ref["_source"]["title"],
        "date_published": ref["_source"]["date_published"],
        "date_published_start": ref["_source"]["date_published_start"],
        "date_published_end": ref["_source"]["date_published_end"],
        "date_created": ref["_source"]["date_created"],
        "abstract": ref["_source"]["abstract"],
        "cross_references": ref["_source"]["cross_references"],
        "workflow_tags": ref["_source"]["workflow_tags"],
        "mod_reference_types": ref["_source"]["mod_reference_types"],
        "language": ref["fields"]["language.keyword"],
        "authors": ref["_source"]["authors"],
        "highlight": remap_highlights(ref.get("highlight", {}))
    } for ref in res["hits"]["hits"]]

    # Process TET aggregations.
    topics = extract_tet_aggregation_data(res, "topic_aggregation", "topics")
    confidence_levels = extract_tet_aggregation_data(res, "confidence_aggregation", "confidence_levels")
    source_methods = extract_tet_aggregation_data(res, "source_method_aggregation", "source_methods")
    source_evidence_assertions = extract_tet_aggregation_data(res, "source_evidence_assertion_aggregation", "source_evidence_assertions")

    # Remove TET aggregation keys.
    for key in ["topic_aggregation", "confidence_aggregation", "source_method_aggregation", "source_evidence_assertion_aggregation"]:
        res["aggregations"].pop(key, None)

    add_curie_to_name_values(topics)
    add_curie_to_name_values(source_evidence_assertions)

    # Process workflow tags using the nested aggregation filtered by mod.
    workflow_tags_agg = res["aggregations"].get("workflow_tags", {})
    workflow_tag_buckets = []
    if "filtered_by_mod" in workflow_tags_agg:
        workflow_tag_buckets = workflow_tags_agg["filtered_by_mod"].get("workflow_tag_ids", {}).get("buckets", [])
    # Reassign to the key the UI expects.
    res["aggregations"]["workflow_tags.workflow_tag_id.keyword"] = {
        "buckets": workflow_tag_buckets,
        "doc_count_error_upper_bound": 0,
        "sum_other_doc_count": 0
    }

    add_curie_to_name_values({"buckets": workflow_tag_buckets})
    atp_ids = {
        "file_workflow": get_atp_ids(file_workflow_root_ids),
        "reference_classification": get_atp_ids(reference_classification_root_ids),
        "entity_extraction": get_atp_ids(entity_extraction_root_ids),
        "manual_indexing": get_atp_ids(manual_indexing_root_ids)
    }
    bucket_lookup = {bucket["key"].upper(): bucket for bucket in workflow_tag_buckets}
    grouped_workflow_tags = {category: [] for category in atp_ids}
    for category, id_list in atp_ids.items():
        for expected_id in id_list:
            expected_upper = expected_id.upper()
            if expected_upper in bucket_lookup:
                grouped_workflow_tags[category].append(bucket_lookup[expected_upper])
    for category, buckets in grouped_workflow_tags.items():
        filtered_buckets = [b for b in buckets if b.get("reverse_docs", {}).get("doc_count", b["doc_count"]) > 0]
        sorted_buckets = sorted(filtered_buckets,
                                key=lambda x: x.get("reverse_docs", {}).get("doc_count", x["doc_count"]),
                                reverse=True)
        res["aggregations"][category] = {
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0,
            "buckets": [{
                "key": bucket["key"],
                "doc_count": bucket.get("reverse_docs", {}).get("doc_count", bucket["doc_count"]),
                "name": bucket.get("name", bucket["key"])
            } for bucket in sorted_buckets]
        }
    res["aggregations"].pop("workflow_tags", None)

    res["aggregations"]["topics"] = topics
    res["aggregations"]["confidence_levels"] = confidence_levels
    res["aggregations"]["source_methods"] = source_methods
    res["aggregations"]["source_evidence_assertions"] = source_evidence_assertions

    return {
        "hits": hits,
        "aggregations": res["aggregations"],
        "return_count": res["hits"]["total"]["value"]
    }

def search_references(query: str = None,
                      facets_values: Optional[Dict[str, List[str]]] = None,
                      negated_facets_values: Optional[Dict[str, List[str]]] = None,
                      size_result_count: Optional[int] = 10,
                      sort_by_published_date_order: Optional[str] = "asc",
                      page: Optional[int] = 1,
                      facets_limits: Optional[Dict[str, int]] = None,
                      return_facets_only: bool = False,
                      author_filter: Optional[str] = None,
                      date_pubmed_modified: Optional[List[str]] = None,
                      date_pubmed_arrive: Optional[List[str]] = None,
                      date_published: Optional[List[str]] = None,
                      date_created: Optional[List[str]] = None,
                      query_fields: Optional[str] = None,
                      partial_match: bool = True,
                      tet_nested_facets_values: Optional[Dict] = None,
                      request_body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if query is None and facets_values is None and not return_facets_only:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="requested a search but no query and no facets provided")
    if facets_limits is None:
        facets_limits = {}
    if size_result_count is None:
        size_result_count = 10
    if page is None:
        page = 1

    # Get parameters from request_body.
    params = getSearchParams(request_body) if request_body else {}
    mod = None
    if facets_values and "workflow_tags.mod_abbreviation" in facets_values:
        mod = facets_values["workflow_tags.mod_abbreviation"][0]
    if not mod:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Missing mod filter in facets_values")
    tet_mod = None
    if facets_values and "topic_entity_tags.data_provider.keyword" in facets_values:
        tet_mod = facets_values["topic_entity_tags.data_provider.keyword"][0]

    from_entry = (page - 1) * size_result_count
    es_host = config.ELASTICSEARCH_HOST
    es = Elasticsearch(hosts=es_host + ":" + config.ELASTICSEARCH_PORT)
    es_body: Dict[str, Any] = {
        "query": {
            "bool": {
                "must": [],
                "should": [],
                "filter": {"bool": {}}
            }
        },
        "fields": ["language.keyword"],
        "highlight": {
            "fields": [
                {"title": {"type": "unified"}},
                {"abstract": {"type": "unified"}},
                {"keywords": {"type": "unified"}},
                {"citation": {"type": "unified"}},
                {"authors.name": {"type": "unified"}}
            ]
        },
        "aggregations": {
            "language.keyword": {
                "terms": {
                    "field": "language.keyword",
                    "min_doc_count": 0,
                    "size": facets_limits.get("mod_reference_types.keyword", 10)
                }
            },
            "mod_reference_types.keyword": {
                "terms": {
                    "field": "mod_reference_types.keyword",
                    "size": facets_limits.get("mod_reference_types.keyword", 10)
                }
            },
            "pubmed_types.keyword": {
                "terms": {
                    "field": "pubmed_types.keyword",
                    "size": facets_limits.get("pubmed_types.keyword", 10)
                }
            },
            "category.keyword": {
                "terms": {
                    "field": "category.keyword",
                    "size": facets_limits.get("category.keyword", 10)
                }
            },
            "pubmed_publication_status.keyword": {
                "terms": {
                    "field": "pubmed_publication_status.keyword",
                    "min_doc_count": 0,
                    "size": facets_limits.get("pubmed_publication_status.keyword", 10)
                }
            },
            "mods_in_corpus.keyword": {
                "terms": {
                    "field": "mods_in_corpus.keyword",
                    "min_doc_count": 0,
                    "size": facets_limits.get("mods_in_corpus.keyword", 10)
                }
            },
            "mods_needs_review.keyword": {
                "terms": {
                    "field": "mods_needs_review.keyword",
                    "min_doc_count": 0,
                    "size": facets_limits.get("mods_needs_review.keyword", 10)
                }
            },
            "mods_in_corpus_or_needs_review.keyword": {
                "terms": {
                    "field": "mods_in_corpus_or_needs_review.keyword",
                    "min_doc_count": 0,
                    "size": facets_limits.get("mods_in_corpus_or_needs_review.keyword", 10)
                }
            },
            "authors.name.keyword": {
                "terms": {
                    "field": "authors.name.keyword",
                    "size": facets_limits.get("authors.name.keyword", 10)
                }
            },
            # Workflow Tags aggregation filtered by mod.
            "workflow_tags": {
                "nested": {"path": "workflow_tags"},
                "aggs": {
                    "filtered_by_mod": {
                        "filter": {"term": {"workflow_tags.mod_abbreviation.keyword": mod}},
                        "aggs": {
                            "workflow_tag_ids": {
                                "terms": {
                                    "field": "workflow_tags.workflow_tag_id.keyword",
                                    "min_doc_count": 0,
                                    "size": 100
                                },
                                "aggs": {
                                    "reverse_docs": {"reverse_nested": {}}
                                }
                            }
                        }
                    }
                }
            }
        },
        "from": from_entry,
        "size": size_result_count,
        "track_total_hits": True,
        "sort": [{"date_published_start": {"order": sort_by_published_date_order, "missing": "_last"}}]
    }
    if sort_by_published_date_order not in ["desc", "asc"]:
        del es_body["sort"]

    ensure_structure(es_body)

    tet_facets = {}
    if tet_nested_facets_values and "tet_facets_values" in tet_nested_facets_values:
        tet_facets = add_tet_facets_values(es_body, tet_nested_facets_values,
                                           tet_nested_facets_values.get("apply_to_single_tag", False))
    es_body["aggregations"]["topic_aggregation"] = create_filtered_aggregation(
        path="topic_entity_tags",
        tet_facets=tet_facets,
        term_field="topic_entity_tags.topic.keyword",
        term_key="topics",
        size=facets_limits.get("topics", 10),
        mod=tet_mod
    )
    es_body["aggregations"]["confidence_aggregation"] = create_filtered_aggregation(
        path="topic_entity_tags",
        tet_facets=tet_facets,
        term_field="topic_entity_tags.confidence_level.keyword",
        term_key="confidence_levels",
        size=facets_limits.get("confidence_levels", 10),
        mod=tet_mod
    )
    es_body["aggregations"]["source_method_aggregation"] = create_filtered_aggregation(
        path="topic_entity_tags",
        tet_facets=tet_facets,
        term_field="topic_entity_tags.source_method.keyword",
        term_key="source_methods",
        size=facets_limits.get("source_methods", 10),
        mod=tet_mod
    )
    es_body["aggregations"]["source_evidence_assertion_aggregation"] = create_filtered_aggregation(
        path="topic_entity_tags",
        tet_facets=tet_facets,
        term_field="topic_entity_tags.source_evidence_assertion.keyword",
        term_key="source_evidence_assertions",
        size=facets_limits.get("source_evidence_assertions", 10),
        mod=tet_mod
    )
    
    if return_facets_only:
        del es_body["query"]
        es_body["size"] = 0
        res = es.search(index=config.ELASTICSEARCH_INDEX, body=es_body)
        return process_search_results(res)
    if query and (query_fields == "All" or query_fields is None):
        es_body["query"]["bool"]["must"].append({
            "bool": {
                "should": [
                    {"simple_query_string": {
                        "fields": ["title", "keywords", "abstract", "citation"],
                        "query": query + "*" if partial_match else query,
                        "analyze_wildcard": "true",
                        "flags": "PHRASE|PREFIX|WHITESPACE|OR|AND|ESCAPE"
                    }},
                    {"match": {"authors.name": {"query": query.lower(), "analyzer": "authorNameAnalyzer"}}},
                    {"wildcard": {"curie.keyword": "*" + query}},
                    {"wildcard": {"cross_references.curie.keyword": "*" + query}}
                ]
            }
        })
    elif query and query_fields in ["Title", "Abstract", "Keyword", "Citation"]:
        if query_fields == "Title":
            es_field = "title"
        elif query_fields == "Abstract":
            es_field = "abstract"
        elif query_fields == "Keyword":
            es_field = "keywords"
        elif query_fields == "Citation":
            es_field = "citation"
        es_body["query"]["bool"]["must"].append({
            "simple_query_string": {
                "fields": [es_field],
                "query": query + "*" if partial_match else query,
                "analyze_wildcard": "true",
                "flags": "PHRASE|PREFIX|WHITESPACE|OR|AND|ESCAPE"
            }
        })
    elif query and query_fields == "Curie":
        es_body["query"]["bool"]["must"].append({"wildcard": {"curie.keyword": "*" + query}})
    elif query and query_fields == "Xref":
        es_body["query"]["bool"]["must"].append({"wildcard": {"cross_references.curie.keyword": "*" + query}})

    WORKFLOW_FACETS = ["file_workflow", "manual_indexing", "entity_extraction", "reference_classification"]
    if facets_values:
        for facet_field, facet_list_values in facets_values.items():
            if facet_field in ["workflow_tags.mod_abbreviation", "topic_entity_tags.data_provider.keyword"]:
                continue
            if "must" not in es_body["query"]["bool"]["filter"]["bool"]:
                es_body["query"]["bool"]["filter"]["bool"]["must"] = []
            if facet_field in WORKFLOW_FACETS:
                mod_value = facets_values.get("workflow_tags.mod_abbreviation", [])
                if not isinstance(mod_value, list):
                    mod_value = [mod_value] if mod_value else []
                if not mod_value:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Missing workflow_tags.mod_abbreviation filter for workflow tags search"
                    )
                nested_query = {
                    "nested": {
                        "path": "workflow_tags",
                        "query": {
                            "bool": {
                                "must": [
                                    {"term": {"workflow_tags.mod_abbreviation.keyword": mod_value[0]}},
                                    {"terms": {"workflow_tags.workflow_tag_id.keyword": facet_list_values}}
                                ]
                            }
                        }
                    }
                }
                es_body["query"]["bool"]["filter"]["bool"]["must"].append(nested_query)
            else:
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
        author_filter_query = {"match": {"authors.name": {"query": author_filter, "analyzer": "authorNameAnalyzer"}}}
        es_body["query"]["bool"]["must"].append(author_filter_query)
    res = es.search(index=config.ELASTICSEARCH_INDEX, body=es_body)
    formatted_results = process_search_results(res)
    return formatted_results
