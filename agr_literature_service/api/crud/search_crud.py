from collections import defaultdict
from typing import Dict, List, Any, Optional
import logging
import re
import time
from datetime import datetime
# from os import getcwd


from elasticsearch import Elasticsearch
from agr_literature_service.api.config import config

from fastapi import HTTPException, status

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.crud.topic_entity_tag_utils import get_map_ateam_curies_to_names
from agr_literature_service.api.crud.workflow_tag_crud import atp_get_all_descendents
from agr_literature_service.lit_processing.utils.db_read_utils import get_mod_abbreviations

logger = logging.getLogger(__name__)

file_workflow_root_ids = ["ATP:0000140", "ATP:0000161"]
reference_classification_root_ids = ["ATP:0000165"]
entity_extraction_root_ids = ["ATP:0000172"]
manual_indexing_root_ids = ["ATP:0000273"]
curation_classification_root_ids = ["ATP:0000311", "ATP:0000210"]
community_curation_classification_root_ids = ["ATP:0000235"]

WORKFLOW_FACETS = [
    "file_workflow",
    "manual_indexing",
    "entity_extraction",
    "reference_classification",
    "curation_classification",
    "community_curation"
]

# Accepts: ORCID:0000-... (any case), orcid:..., or bare 0000-....
_ORCID_INPUT = re.compile(r'(?i)^(?:\s*orcid:\s*)?([0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9Xx]{4})\s*$')


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
def search_references(query: str = None, facets_values: Dict[str, List[str]] = None,
                      negated_facets_values: Dict[str, List[str]] = None,
                      size_result_count: Optional[int] = 10,
                      sort_by_published_date_order: Optional[str] = "desc",
                      page: Optional[int] = 1,
                      facets_limits: Dict[str, int] = None,
                      return_facets_only: bool = False,
                      author_filter: Optional[str] = None,
                      date_pubmed_modified: Optional[List[str]] = None,
                      date_pubmed_arrive: Optional[List[str]] = None,
                      date_published: Optional[List[str]] = None,
                      date_created: Optional[List[str]] = None,
                      query_fields: str = None, partial_match: bool = True,
                      tet_nested_facets_values: Optional[Dict] = None,
                      sort: Optional[List[Dict[str, Any]]] = None):
    has_any_input = any([
        query,
        facets_values,
        author_filter,
        date_pubmed_modified,
        date_pubmed_arrive,
        date_published,
        date_created,
        tet_nested_facets_values,
        sort
    ])
    if not has_any_input and not return_facets_only:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="requested a search but no query/filters provided")

    if facets_limits is None:
        facets_limits = {}
    if size_result_count is None:
        size_result_count = 10
    if page is None:
        page = 1

    author_filter = (author_filter or "").strip() or None

    """
    Primary sort: date_published_start - orders by publication date first
    Secondary sort: date_created - deterministic fallback when date_published_start is missing
    Tertiary sort: _score - keeps ES relevance in the mix when scores are tied
    Final tie-breaker: curie.keyword - gives stable ordering for completely identical values
    """
    order = sort_by_published_date_order if sort_by_published_date_order in ("asc", "desc") else "desc"
    base_sort =	[
	{"date_published_start": {"order": order, "missing": "_last"}},
        {"date_created": {"order": order, "missing": "_last"}},
        {"_score": {"order": "desc"}},
        {"curie.keyword": {"order": "asc"}},
    ]
    
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
        "fields": ["language.keyword"],
        "highlight": {
            "fields": [
                {"title": {"type": "unified"}},
                {"abstract": {"type": "unified"}},
                {"keywords": {"type": "unified"}},
                {"citation": {"type": "unified"}},
                {"authors.name": {"type": "unified"}},
                {"authors.orcid": {"type": "unified"}}
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
                "nested": { "path": "authors" },
                "aggs": {
                    "terms": {
                        "terms": {
                            "field": "authors.name.keyword",
                            "size": facets_limits.get("authors.name.keyword", 10)
                        }
                    }
                }
            },
            "workflow_tags": {
                "nested": {
                    "path": "workflow_tags"
                },
                "aggs": {
                    "by_mod_abbreviation": {
                        "terms": {
                            "field": "workflow_tags.mod_abbreviation",
                            "min_doc_count": 0,
                            "size": 100
                        },
                        "aggs": {
                            "workflow_tag_ids": {
                                "terms": {
                                    "field": "workflow_tags.workflow_tag_id.keyword",
                                    "min_doc_count": 0,
                                    "size": 100
                                },
                                "aggs": {
                                    "reverse_docs": {
                                        "reverse_nested": {}
                                    }
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
        "sort": base_sort
    }

    # ----- sorting vs recency boosting -----
    if sort:
        es_body["sort"] = sort
    else:
     
        es_body["sort"] = base_sort
    
    ensure_structure(es_body)

    # set tet_data_providers & wft_mod_abbreviations
    if facets_values is None:
        facets_values = {}
    tet_data_providers = list(
        set(
            facets_values.get("mods_in_corpus.keyword", []) +
            facets_values.get("mods_in_needs_review.keyword", []) +
            facets_values.get("mods_in_corpus_or_needs_review.keyword", [])
        )
    )
    if len(tet_data_providers) == 0:
        tet_data_providers = get_mod_abbreviations()
    wft_mod_abbreviations = tet_data_providers

    # search papers by TET
    tet_facets = {}
    if tet_nested_facets_values and "tet_facets_values" in tet_nested_facets_values:
        tet_facets = add_tet_facets_values(es_body, tet_nested_facets_values,
                                           tet_nested_facets_values.get("apply_to_single_tag", False))
    apply_all_tags_tet_aggregations(es_body, tet_facets, facets_limits, tet_data_providers)

    if return_facets_only:
        del es_body["query"]
        es_body["size"] = 0
        res = es.search(index=config.ELASTICSEARCH_INDEX, body=es_body)
        return process_search_results(res, wft_mod_abbreviations)

    if query and (query_fields == "All" or query_fields is None):
        q_free = strip_orcid_prefix_for_free_text(query)
        shoulds = [
            {
                "simple_query_string": {
                    "fields": ["title", "keywords", "abstract", "citation"],
                    "query": (q_free + "*") if partial_match else q_free,
                    "analyze_wildcard": True,
                    "flags": "PHRASE|PREFIX|WHITESPACE|OR|AND|ESCAPE"
                }
            },
            {"wildcard": {"curie.keyword": f"*{q_free}"}},
            {"wildcard": {"cross_references.curie.keyword": f"*{q_free}"}},
            nested_author_name_query(query),
        ]

        core = extract_orcid_core(query)
        if core:
            shoulds.append(nested_orcid_exact(core))

        es_body["query"]["bool"]["must"].append({"bool": {"should": shoulds}})

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
    elif query and query_fields == "Author":
        es_body["query"]["bool"]["must"].append(nested_author_name_query(query))
    elif query and query_fields == "ORCID":
        core = extract_orcid_core(query)
        if core:
            es_body["query"]["bool"]["must"].append(nested_orcid_exact(core))
        else:
            es_body["query"]["bool"]["must"].append({"match_none": {}})
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
            if facet_field in WORKFLOW_FACETS:
                if len(facet_list_values) > 1:
                    for tag in facet_list_values:
                        nested_query = {
                            "nested": {
                                "path": "workflow_tags",
                                "query": {
                                    "bool": {
                                        "must": [
                                            {"terms": {"workflow_tags.mod_abbreviation": wft_mod_abbreviations}},
                                            {"term": {"workflow_tags.workflow_tag_id.keyword": tag}}
                                        ]
                                    }
                                }
                            }
                        }
                        es_body["query"]["bool"]["filter"]["bool"]["must"].append(nested_query)
                else:
                    # only one tag provided – use a single nested query.
                    nested_query = {
                        "nested": {
                            "path": "workflow_tags",
                            "query": {
                                "bool": {
                                    "must": [
                                        {"terms": {"workflow_tags.mod_abbreviation": wft_mod_abbreviations}},
                                        {"term": {"workflow_tags.workflow_tag_id.keyword": facet_list_values[0]}}
                                    ]
                                }
                            }
                        }
                    }
                    es_body["query"]["bool"]["filter"]["bool"]["must"].append(nested_query)
            else:
                if facet_field == 'authors.name.keyword':
                    for facet_value in facet_list_values:
                        es_body["query"]["bool"]["filter"]["bool"]["must"].append({
                            "nested": {
                                "path": "authors",
                                "query": { "term": { facet_field: facet_value } }
                            }
                        })
                else:
                    # Standard facet application
                    es_body["query"]["bool"]["filter"]["bool"]["must"].append({"bool": {"must": []}})
                    for facet_value in facet_list_values:
                        es_body["query"]["bool"]["filter"]["bool"]["must"][-1]["bool"]["must"].append({"term": {}})
                        es_body["query"]["bool"]["filter"]["bool"]["must"][-1]["bool"]["must"][-1]["term"][facet_field] = facet_value

    if negated_facets_values:
        for facet_field, facet_list_values in negated_facets_values.items():
            if "must_not" not in es_body["query"]["bool"]["filter"]["bool"]:
                es_body["query"]["bool"]["filter"]["bool"]["must_not"] = []
            if facet_field == 'authors.name.keyword':
                for facet_value in facet_list_values:
                    es_body["query"]["bool"]["filter"]["bool"]["must_not"].append({
                        "nested": {
                            "path": "authors",
                            "query": { "term": { facet_field: facet_value } }
                        }
                    })
            else:
                for facet_value in facet_list_values:
                    es_body["query"]["bool"]["filter"]["bool"]["must_not"].append({"term": {}})
                    es_body["query"]["bool"]["filter"]["bool"]["must_not"][-1]["term"][facet_field] = facet_value

    date_range = False
    if date_pubmed_modified or date_pubmed_arrive or date_published or date_created:
        date_range = True
        search_date_range(es_body, date_pubmed_modified, date_pubmed_arrive, date_published, date_created)
    if not facets_values and not date_range and not negated_facets_values and not tet_facets:
        del es_body["query"]["bool"]["filter"]

    if author_filter:
        name = (author_filter or "").strip()
        if name:
            shoulds = []
            shoulds.append(nested_author_name_query(name))
            shoulds.append({
                "nested": {
                    "path": "authors",
                    "query": {
                        "term": {"authors.name.keyword": name}
                    },
                    "score_mode": "max"
                }
            })
            core = extract_orcid_core(name)
            if core:
                shoulds.append(nested_orcid_exact(core))

            es_body["query"]["bool"]["must"].append({
                "bool": {"should": shoulds, "minimum_should_match": 1}
            })

    # Apply recency boost if we are not hard-sorting
    # Papers published within the last 1 year → boosted by 3.0×
    # Papers published between 1 and 10 years ago → boosted by 1.5×
    # Papers older than 10 years → no boost (weight defaults to 1.0)
    if "sort" not in es_body:
        apply_recency_boost(es_body, windows_days=(365, 1095), weights=(3.0, 1.5))

    res = es.search(index=config.ELASTICSEARCH_INDEX, body=es_body)
    formatted_results = process_search_results(res, wft_mod_abbreviations)
    return formatted_results


def process_search_results(res, wft_mod_abbreviations):  # pragma: no cover

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

    # extract topic entity tag aggregations.
    topic_aggs = process_topic_entity_tags_aggregations(res)

    # extract workflow tags aggregations.
    workflow_aggs = process_workflow_tags_aggregations(res, wft_mod_abbreviations)

    # merge the processed aggregations into the response.
    res['aggregations'].update(topic_aggs)
    res['aggregations'].update(workflow_aggs)

    # take care of authors
    agg = res['aggregations'].get("authors.name.keyword")
    if isinstance(agg, dict) and "aggs" in agg or "terms" in agg:
        # new nested shape: { "authors.name.keyword": { "doc_count":..., "terms": { "buckets":[...] } } }
        inner = agg.get("terms") if "terms" in agg else agg.get("aggs", {}).get("terms")
        if isinstance(inner, dict) and "buckets" in inner:
            res['aggregations']["authors.name.keyword"] = inner

    return {
        "hits": hits,
        "aggregations": res['aggregations'],
        "return_count": res["hits"]["total"]["value"]
    }


def process_topic_entity_tags_aggregations(res):  # pragma: no cover
    """
    process topic entity tag aggregations that were created using the wrapped
    aggregation (which uses an extra filter so that results come only from allowed
    data providers). This function processes aggregations for topics, confidence levels,
    source methods, & source evidence assertions—merging raw and group assertions.
    """

    def extract_filtered_agg(res, main_key, data_key):
        agg = res['aggregations'].get(main_key, {})
        if "filtered" in agg:
            agg = agg["filtered"]
        if "filter_by_other_tet_values" in agg:
            agg = agg["filter_by_other_tet_values"]
        return agg.get(data_key, {})

    topics             = extract_filtered_agg(res, 'topic_aggregation',             'topics')
    confidence_levels  = extract_filtered_agg(res, 'confidence_aggregation',      'confidence_levels')
    source_methods     = extract_filtered_agg(res, 'source_method_aggregation',   'source_methods')

    raw_sea = extract_filtered_agg(
        res,
        'source_evidence_assertion_aggregation',
        'source_evidence_assertions'
    )
    group_sea = extract_filtered_agg(
        res,
        'source_evidence_assertion_group_aggregation',
        'source_evidence_assertions'
    )

    merged_buckets = {}
    for b in raw_sea.get('buckets', []):
        merged_buckets[b['key']] = b
    for b in group_sea.get('buckets', []):
        # overwrite or add
        merged_buckets[b['key']] = b

    source_evidence_assertions = {
        "doc_count_error_upper_bound": 0,
        "sum_other_doc_count": 0,
        "buckets": list(merged_buckets.values())
    }

    # remove all the temporary aggs
    for k in [
        'topic_aggregation',
        'confidence_aggregation',
        'source_method_aggregation',
        'source_evidence_assertion_aggregation',
        'source_evidence_assertion_group_aggregation'
    ]:
        res['aggregations'].pop(k, None)

    # add human‐readable names for any ATP/ECO curies
    add_curie_to_name_values(topics)
    add_curie_to_name_values(source_evidence_assertions)

    # reorder the source_evidence_assertions buckets to the exact sequence desired
    desired_order = [
        "automated assertion",
        "machine learning method evidence used in automatic assertion",
        "string-matching method evidence used in automatic assertion",
        "manual assertion",
        "documented statement evidence used in manual assertion by author",
        "documented statement evidence used in manual assertion by professional biocurator",
    ]
    buckets = source_evidence_assertions.get("buckets", [])
    bucket_map = {b["name"]: b for b in buckets}
    source_evidence_assertions["buckets"] = [
        bucket_map[name] for name in desired_order if name in bucket_map
    ]

    """
    # ——— KEYWORD‑BASED SORTING ———
    key_phrases = [
        'automated assertion',
        'machine learning',
        'string-matching',
        'manual assertion',
        'author',
        'biocurator',
    ]
    def sort_key(bucket):
        name = bucket['name'].lower()
        for idx, phrase in enumerate(key_phrases):
            if phrase in name:
                return idx
        # any bucket without a match goes last
        return len(key_phrases)

    source_evidence_assertions['buckets'].sort(key=sort_key)
    """

    return {
        "topics":                    topics,
        "confidence_levels":         confidence_levels,
        "source_methods":            source_methods,
        "source_evidence_assertions": source_evidence_assertions
    }


def process_workflow_tags_aggregations(res, wft_mod_abbreviations):  # pragma: no cover

    """
    process workflow_tags aggregations by grouping workflow tags by mod_abbreviation,
    summing counts for allowed mods and then returning a dictionary keyed by workflow
    tag categories (such as file_workflow, reference_classification, etc.).
    """
    workflow_tags_nested = res['aggregations'].get("workflow_tags", {})
    mod_buckets = workflow_tags_nested.get("by_mod_abbreviation", {}).get("buckets", [])
    
    mod_bucket_lookup = {}
    for mod_bucket in mod_buckets:
        mod_key = mod_bucket["key"]
        tag_buckets = mod_bucket.get("workflow_tag_ids", {}).get("buckets", [])
        mod_bucket_lookup[mod_key] = {bucket["key"].upper(): bucket for bucket in tag_buckets}
    
    atp_ids = {
        "file_workflow": get_atp_ids(file_workflow_root_ids),
        "reference_classification": get_atp_ids(reference_classification_root_ids),
        "entity_extraction": get_atp_ids(entity_extraction_root_ids),
        "manual_indexing": get_atp_ids(manual_indexing_root_ids),
        "curation_classification": get_atp_ids(curation_classification_root_ids),
        "community_curation": get_atp_ids(community_curation_classification_root_ids)
    }
    
    grouped_workflow_tags = {category: {} for category in atp_ids}
    for category, id_list in atp_ids.items():
        for mod, bucket_lookup in mod_bucket_lookup.items():
            # only process allowed mods.
            if mod.upper() not in wft_mod_abbreviations:
                continue
            for expected_id in id_list:
                expected_upper = expected_id.upper()
                if expected_upper in bucket_lookup:
                    bucket = bucket_lookup[expected_upper]
                    # use reverse nested count if available.
                    count = bucket.get("reverse_docs", {}).get("doc_count", bucket["doc_count"])
                    if expected_upper not in grouped_workflow_tags[category]:
                        grouped_workflow_tags[category][expected_upper] = {
                            "key": expected_upper,
                            "doc_count": 0,
                            "name": bucket.get("name", expected_upper)
                        }
                    grouped_workflow_tags[category][expected_upper]["doc_count"] += count

    # build final aggregation results per workflow tag category.
    final_workflow_aggs = {}
    for category, buckets_dict in grouped_workflow_tags.items():
        buckets_list = list(buckets_dict.values())
        filtered_buckets = [
            b for b in buckets_list
            if b.get("reverse_docs", {}).get("doc_count", b["doc_count"]) > 0
        ]
        sorted_buckets = sorted(
            filtered_buckets,
            key=lambda x: x.get("reverse_docs", {}).get("doc_count", x["doc_count"]),
            reverse=True
        )
        aggregated_result = {
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0,
            "buckets": sorted_buckets
        }
        add_curie_to_name_values(aggregated_result)
        final_workflow_aggs[category] = aggregated_result

    res['aggregations'].pop("workflow_tags", None)
    
    return final_workflow_aggs


def remap_highlights(highlights):  # pragma: no cover

    remapped = {}
    for key, value in highlights.items():
        new_key = key.replace('authors.name', 'authors')
        remapped[new_key] = value
    return remapped


def add_tet_facets_values(es_body, tet_nested_facets_values, apply_to_single_tet):  # pragma: no cover
    tet_facet_values = defaultdict(list)
    ##Not Facets
    levels = []
    if tet_nested_facets_values["tet_facets_negative_values"]:
        if "must" not in es_body["query"]["bool"]["filter"]["bool"]:
            ##Not 100% sure this first line is necessary
            es_body["query"]["bool"]["filter"]["bool"]["must"] = []

        for level in tet_nested_facets_values['tet_facets_negative_values'][0][
            'topic_entity_tags.confidence_level.keyword']:
            levels.append({"term": {"topic_entity_tags.confidence_level.keyword": level}})

    for facet_name_value_dict in tet_nested_facets_values.get("tet_facets_values", []):
        add_nested_query(es_body, facet_name_value_dict, levels)
        if apply_to_single_tet:
            for facet_name, facet_value in facet_name_value_dict.items():
                tet_facet_values[facet_name.replace("topic_entity_tags.", "").replace(".keyword", "")] = facet_value
    return tet_facet_values


def add_nested_query(es_body, facet_name_values_dict,levels):  # pragma: no cover
    must_conditions = []
    for facet_name, facet_values in facet_name_values_dict.items():
        if facet_name == "topic_entity_tags.source_evidence_assertion.keyword":
            vals = facet_values if isinstance(facet_values, (list, tuple)) else [facet_values]
            if any(v.upper() in ("ECO:0007669", "ECO:0006155") for v in vals):
                facet_name = "topic_entity_tags.source_evidence_assertion_group.keyword"
        must_conditions.append({
            "term": {facet_name: facet_values}
        })

    nested_query = {
        "nested": {
            "path": "topic_entity_tags",
            "query": {
                "bool": {
                    "must": must_conditions,
                    "must_not" :levels
                }
            }
        }
    }

    es_body["query"]["bool"]["filter"]["bool"]["must"].append(nested_query)


def create_filtered_aggregation_with_dp(path, tet_facets, term_field, term_key, allowed_dp, size=10):  # pragma: no cover
    """
    build a filtered aggregation that:
      1. uses the nested path.
      2. applies any tet_facets filter.
      3. further restricts the results to papers whose
         topic_entity_tags.data_provider is in allowed_dp.
      4. aggregates on the given term_field.
    """

    # start with the base filtered aggregation.
    base_agg = create_filtered_aggregation(path, tet_facets, term_field, term_key, size)

    # wrap the base aggregation with an extra filter on data_provider.
    wrapped_agg = {
        "nested": {
            "path": path
        },
        "aggs": {
            "filtered": {
                "filter": {
                    "terms": {
                        f"{path}.data_provider": allowed_dp
                    }
                },
                "aggs": base_agg["aggs"]
            }
        }
    }
    return wrapped_agg


def create_filtered_aggregation(path, tet_facets, term_field, term_key, size=10):  # pragma: no cover

    tet_agg = {
        "nested": {
            "path": path
        }
    }
    if tet_facets:
        tet_agg["aggs"] = {
            "filter_by_other_tet_values": {
                "filter": {
                    "bool": {
                        "must": [
                            {"term": {f"topic_entity_tags.{filter_field}.keyword": filter_value}}
                            for filter_field, filter_value in tet_facets.items()
                        ]
                    }
                },
                "aggs": {
                    term_key: {
                        "terms": {
                            "field": term_field,
                            "size": size
                        },
                        "aggs": {
                            # reverse nesting to count documents
                            "docs_count": {
                                "reverse_nested": {}
                            }
                        }
                    }
                }
            }
        }
        ##We want to see all options here.
        if term_field == 'topic_entity_tags.confidence_level.keyword' :
            tet_agg['aggs']['filter_by_other_tet_values']['aggs']['confidence_levels']['terms']['min_doc_count']=0
    else:
        tet_agg["aggs"] = {
            term_key: {
                "terms": {
                    "field": term_field,
                    "size": size
                },
                "aggs": {
                    # reverse nesting to count documents
                    "docs_count": {
                        "reverse_nested": {}
                    }
                }
            }
        }
    return tet_agg

    
def apply_all_tags_tet_aggregations(es_body, tet_facets, facets_limits, tet_data_providers):  # pragma: no cover

    """
    build aggregations for topic entity tags.
    we now apply an extra filter so that the aggregations are computed only
    for topic entity tags whose data_provider is in tet_data_providers.
    """

    allowed_dp = [dp.upper() for dp in tet_data_providers]

    es_body["aggregations"]["topic_aggregation"] = create_filtered_aggregation_with_dp(
        path="topic_entity_tags",
        tet_facets=tet_facets,
        term_field="topic_entity_tags.topic.keyword",
        term_key="topics",
        allowed_dp=allowed_dp,
        size=facets_limits.get("topics", 10)
    )
    es_body["aggregations"]["confidence_aggregation"] = create_filtered_aggregation_with_dp(
        path="topic_entity_tags",
        tet_facets=tet_facets,
        term_field="topic_entity_tags.confidence_level.keyword",
        term_key="confidence_levels",
        allowed_dp=allowed_dp,
        size=facets_limits.get("confidence_levels", 10)
    )
    es_body["aggregations"]["source_method_aggregation"] = create_filtered_aggregation_with_dp(
        path="topic_entity_tags",
        tet_facets=tet_facets,
        term_field="topic_entity_tags.source_method.keyword",
        term_key="source_methods",
        allowed_dp=allowed_dp,
        size=facets_limits.get("source_methods", 10)
    )

    # adding this fix to restore the SEA facet list under group‐filtered searches
    #
    # Hits are still filtered by source_evidence_assertion=eco:0007669 or eco:0006155)
    # Facet counts for SEA will be computed over that hit set, but not further restricted
    # by the SEA filter itself. So you’ll see:
    # eco:0008004 & eco:0008021 buckets (the two raw codes),
    # and the eco:0007669 combined bucket

    sea_tet_facets = {
        k:v for k,v in tet_facets.items()
        if k != "source_evidence_assertion"
    }

    es_body["aggregations"]["source_evidence_assertion_aggregation"] = create_filtered_aggregation_with_dp(
        path="topic_entity_tags",
        tet_facets=sea_tet_facets,
        term_field="topic_entity_tags.source_evidence_assertion.keyword",
        term_key="source_evidence_assertions",
        allowed_dp=allowed_dp,
        size=facets_limits.get("source_evidence_assertions", 10) 
    )
    es_body["aggregations"]["source_evidence_assertion_group_aggregation"] = create_filtered_aggregation_with_dp(
        path="topic_entity_tags",
        tet_facets=sea_tet_facets,
        term_field="topic_entity_tags.source_evidence_assertion_group.keyword",
        term_key="source_evidence_assertions",
        allowed_dp=allowed_dp,
	size=facets_limits.get("source_evidence_assertions", 10)
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

    curie_keys = [
        bucket["key"] for bucket in aggregations.get("buckets", [])
    ]
    curie_to_name_map = get_map_ateam_curies_to_names(
        category="atpterm",
        curies=[curie_key.upper() for curie_key in curie_keys if curie_key.upper().startswith("ATP:")]
    )
    curie_to_name_map.update(get_map_ateam_curies_to_names(
        category="ecoterm",
        curies=[curie_key.upper() for curie_key in curie_keys if curie_key.upper().startswith("ECO:")]
    ))

    # iterate over the buckets and add names
    for bucket in aggregations.get("buckets", []):
        curie_name = curie_to_name_map.get(bucket["key"].upper(), "Unknown")
        if bucket["key"].upper() == 'ECO:0006155':
            curie_name = 'manual assertion'
        elif bucket["key"].upper() == 'ECO:0007669':
            curie_name = 'automated assertion'
        bucket["name"] = curie_name


def get_atp_ids(root_atp_ids):
    return [child for root_atp_id in root_atp_ids for child in atp_get_all_descendents(root_atp_id)]


def nested_author_name_query(q: str) -> dict:
    return {
        "nested": {
            "path": "authors",
            "query": {"match": {"authors.name": {"query": q, "analyzer": "authorNameAnalyzer"}}},
            "score_mode": "max"
        }
    }


def normalize_orcid(raw: str) -> str:
    """
    Strip optional 'ORCID:' prefix (any case), trim, lowercase.
    """
    m = _ORCID_INPUT.match(raw or "")
    # if the input doesn’t match the pattern, it returns the trimmed, lowercased original string 
    if not m:
        return (raw or "").strip().lower()
    # otherwise, it will return something like '0000-0001-0111-111x'
    return m.group(1).lower()


def orcid_variants(raw: str):
    s = (raw or "").strip()
    m = _ORCID_INPUT.match(s)
    hyph = m.group(1) if m else s
    hyph_l = hyph.lower(); hyph_u = hyph.upper()

    return [
        hyph_l, hyph_u,                         # bare hyphenated
        f"orcid:{hyph_l}", f"ORCID:{hyph_u}",   # prefixed
        f"https://orcid.org/{hyph_l}", f"http://orcid.org/{hyph_l}",
        f"https://orcid.org/{hyph_u}", f"http://orcid.org/{hyph_u}",
        hyph_l.replace("-", ""), hyph_u.replace("-", "")  # no hyphen
    ]


def orcid_prefix(hyph_lower: str) -> str:
    """First 3 blocks for prefix searches, e.g. '0000-0001-1111'."""
    if "-" in hyph_lower:
        parts = hyph_lower.split("-")
        if len(parts) >= 3:
            return "-".join(parts[:3])
    return hyph_lower[:13]


def extract_orcid_core(raw: str) -> Optional[str]:
    """
    Return the hyphenated ORCID core (lowercased), e.g., '0000-0001-1111-1111',
    or None if it doesn't look like an ORCID.
    Handles both 'ORCID:' (uppercase) and 'orcid:' (lowercase) prefixes.
    """
    if not raw:
        return None
    
    s = raw.strip()
    m = _ORCID_INPUT.match(s)
    if m:
        return m.group(1).lower()
    
    # Handle both uppercase and lowercase ORCID: prefixes
    if s.upper().startswith("ORCID:"):
        tail = s.split(":", 1)[1].strip()
        m = _ORCID_INPUT.match(tail)
        if m:
            return m.group(1).lower()
    
    return None


def strip_orcid_prefix_for_free_text(q: str) -> str:
    # only for simple_query_string / wildcard clauses
    return re.sub(r'(?i)^\s*orcid:\s*', '', q or '').strip()


def nested_orcid_exact(core_lower: str) -> dict:
    normalized_orcid = f"orcid:{core_lower}".lower()
    
    return {
        "nested": {
            "path": "authors",
            "query": {
                "term": {
                    "authors.orcid.keyword": normalized_orcid
                }
            },
            "score_mode": "max"
        }
    }


def apply_recency_boost(es_body,
                        windows_days=(365, 1095),
                        weights=(3.0, 1.5),
                        field="date_published_start"):
    """
    Add mapping-agnostic recency boosts using range 'should' clauses.
    - Adds both seconds and millis thresholds so it works for either mapping.
    - Skips fields that may be text (we only touch `field`).
    """
    if "query" not in es_body or not es_body["query"]:
        es_body["query"] = {"bool": {"must": [{"match_all": {}}]}}
    elif "bool" not in es_body["query"]:
        es_body["query"] = {"bool": {"must": [es_body["query"]]}}

    now_sec = int(time.time())
    now_ms  = now_sec * 1000

    shoulds = es_body["query"]["bool"].setdefault("should", [])

    for days, boost in zip(windows_days, weights):
        # seconds window (for fields indexed as epoch seconds / numeric)
        cutoff_sec = now_sec - days * 24 * 3600
        shoulds.append({
            "range": {
                field: {
                    "gte": cutoff_sec,
                    "boost": float(boost)
                }
            }
        })
        # millis window (for fields indexed as date (epoch_millis) )
        cutoff_ms = now_ms - days * 24 * 3600 * 1000
        shoulds.append({
            "range": {
                field: {
                    "gte": cutoff_ms,
                    "boost": float(boost)
                }
            }
        })

    # make sure 'should' contributes even if there are 'must' clauses
    es_body["query"]["bool"]["minimum_should_match"] = 0
