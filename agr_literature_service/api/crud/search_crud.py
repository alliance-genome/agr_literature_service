from collections import defaultdict
from typing import Dict, List, Any, Optional
import logging
import re
import unicodedata
from datetime import datetime, date, time, timezone

from elasticsearch import Elasticsearch
from agr_literature_service.api.config import config

from fastapi import HTTPException, status

from agr_literature_service.api.crud.topic_entity_tag_utils import get_map_ateam_curies_to_names
from agr_literature_service.api.crud.workflow_tag_crud import atp_get_all_descendents
from agr_literature_service.lit_processing.utils.db_read_utils import get_mod_abbreviations

from agr_literature_service.api.crud.search_ranking import (
    TEXT_FIELDS,
    strip_orcid_prefix_for_free_text,
    add_simple_text_field_query,
    build_all_text_query,
    nested_author_name_match_any,
    nested_author_name_exact_keyword,
    nested_author_name_prefix_keyword,
    nested_author_name_match_prefix,
    nested_author_name_exact_token,
    apply_scoring_and_sort,
    build_author_bucket_function_score,
    author_bucket_sort,
    build_content_gate_filter,
)
from agr_literature_service.api.crud.search_filters import (
    apply_all_date_filters,
)

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

_XREF_BARE_PATTERNS = [
    re.compile(r"^10\.\d{4,9}/\S+$", re.I),        # DOI
    re.compile(r"^pmcid:?pmc\d+$", re.I),          # PMCID or PMCID:PMC123456
    re.compile(r"^pmc\d+$", re.I),                 # bare PMC123456
    re.compile(r"^wbpaper\d+$", re.I),             # WBPaper00064897
    re.compile(r"^zdb-[A-Z]+-\d{6}-\d+$", re.I),   # ZDB-PUB-YYYYMM-#
    re.compile(r"^xb-art-\d+$", re.I),             # XB-ART-59278
    re.compile(r"^fbrf\d+$", re.I),                # FBrf0076951
    re.compile(r"^s\d{9}$", re.I),                 # SGD S000339612 / S100000615
    re.compile(r"^\d{1,}$"),                       # plain numeric (PMID / MGI / RGD, etc.)
]


# flake8: noqa: C901
def search_references(
    query: str = None,
    facets_values: Dict[str, List[str]] = None,
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
    query_fields: str = None,
    partial_match: bool = True,
    tet_nested_facets_values: Optional[Dict] = None,
    sort: Optional[List[Dict[str, Any]]] = None,
):
    has_any_input = any([
        query, facets_values, author_filter, date_pubmed_modified, date_pubmed_arrive,
        date_published, date_created, tet_nested_facets_values, sort
    ])
    if not has_any_input and not return_facets_only:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="requested a search but no query/filters provided",
        )

    if facets_limits is None:
        facets_limits = {}
    if size_result_count is None:
        size_result_count = 10
    if page is None:
        page = 1

    if query and (query_fields == "All" or query_fields is None):
        if query.upper().startswith("XENBASE:"):
            query = query.upper().replace("XENBASE:", "")
        q_norm = normalize_user_query(query)
        if q_norm.upper().startswith("AGRKB:"):
            query_fields = "Curie"
        elif ':' in q_norm:
            curie_prefix_list = get_mod_abbreviations()  # e.g. ["SGD", "WB", "XB", ...]
            # normalize to a set for easy lookup
            curie_prefix_list = set(curie_prefix_list)

            # also accept publication and DOI prefixes
            curie_prefix_list.update({"PMID", "PMCID", "DOI"})

            query_prefix = q_norm.split(':', 1)[0]
            if query_prefix.upper() in curie_prefix_list:
                query_fields = "Xref"
        else:
            if looks_like_xref_id_without_prefix(q_norm):
                query_fields = "Xref"

    author_filter = (author_filter or "").strip() or None
    author_exact_token_for_boost = None
    uses_rescore = False

    # Default recency order
    order = sort_by_published_date_order if sort_by_published_date_order in ("asc", "desc") else "desc"

    # Pagination
    from_entry = (page - 1) * size_result_count

    es_host = config.ELASTICSEARCH_HOST
    es = Elasticsearch(
        hosts=es_host + ":" + config.ELASTICSEARCH_PORT,
        timeout=30,  # Increased timeout from default 10s to 30s
        max_retries=3,  # Add retries for resilience
        retry_on_timeout=True
    )

    # Base request body
    es_body: Dict[str, Any] = {
        "query": {"bool": {"must": [], "should": [], "filter": {"bool": {}}}},
        "fields": ["language.keyword"],
        "highlight": {
            "require_field_match": False,
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
                    "size": facets_limits.get("language.keyword", 10)
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
                "nested": {"path": "authors"},
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
                "nested": {"path": "workflow_tags"},
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
                                "aggs": {"reverse_docs": {"reverse_nested": {}}}
                            }
                        }
                    }
                }
            }
        },
        "from": from_entry,
        "size": size_result_count,
        "track_total_hits": True
    }

    # Determine if this is a text-based search that should get recency boosting
    is_text_search = bool(query or author_filter or query_fields in {
        "All", "Title", "Abstract", "Keyword", "Citation"
    })

    ensure_structure(es_body)

    # set tet_data_providers & wft_mod_abbreviations
    if facets_values is None:
        facets_values = {}
    tet_data_providers = list(set(
        facets_values.get("mods_in_corpus.keyword", []) +
        facets_values.get("mods_needs_review.keyword", []) +
        facets_values.get("mods_in_corpus_or_needs_review.keyword", [])
    ))
    if len(tet_data_providers) == 0:
        tet_data_providers = get_mod_abbreviations()
    wft_mod_abbreviations = [dp.upper() for dp in tet_data_providers]

    # search papers by TET
    tet_facets = {}
    if tet_nested_facets_values and "tet_facets_values" in tet_nested_facets_values:
        tet_facets = add_tet_facets_values(
            es_body,
            tet_nested_facets_values,
            tet_nested_facets_values.get("apply_to_single_tag", False),
        )
    apply_all_tags_tet_aggregations(es_body, tet_facets, facets_limits, tet_data_providers)

    # If only facets are requested, short-circuit
    if return_facets_only:
        es_body.pop("query", None)
        es_body["size"] = 0
        res = es.search(index=config.ELASTICSEARCH_INDEX, body=es_body)
        return process_search_results(res, wft_mod_abbreviations)

    # --------------------------- Query building ---------------------------

    # 1) ALL fields (or None)
    if query and (query_fields == "All" or query_fields is None):
        q_raw = (query or "").strip()
        q_norm = normalize_user_query(q_raw)
        q_free = strip_orcid_prefix_for_free_text(q_norm)

        # Build the standard multi-field query bundle
        bundle = build_all_text_query(q_free, size_result_count, include_id_author_helpers=True)
        for m in bundle["must"]:
            es_body["query"]["bool"]["must"].append(m)
        if bundle.get("should"):
            es_body["query"]["bool"]["should"].extend(bundle["should"])
            es_body["query"]["bool"]["minimum_should_match"] = 0
        if bundle.get("rescore"):
            es_body["rescore"] = bundle["rescore"]
        uses_rescore = bool(bundle.get("uses_rescore"))

        # ORCID support
        core = extract_orcid_core(q_raw)
        if core:
            es_body["query"]["bool"]["should"].append(nested_orcid_exact(core))

        # ---------------------------
        # Content-gate filter: drop queries that are just stopwords
        # ---------------------------
        cg = build_content_gate_filter(q_free)   # from search_ranking.py
        if cg:
            ensure_structure(es_body)  # guarantees filter.bool.must exists
            es_body["query"]["bool"]["filter"]["bool"]["must"].append(cg)

    # 2) Single-field text: Title / Abstract / Keyword / Citation
    elif query and query_fields in TEXT_FIELDS:
        qn = normalize_user_query(query)
        add_simple_text_field_query(es_body, TEXT_FIELDS[query_fields], qn, partial_match)
        
    # 3) Author search
    elif query and query_fields == "Author":
        q = (query or "").strip()
        is_quoted = len(q) >= 2 and q[0] == '"' and q[-1] == '"'
        phrase = q[1:-1].strip() if is_quoted else q

        # ORCID typed into the author box -> considered a "near" signal
        core = extract_orcid_core(query)
        orcid_clause = nested_orcid_exact(core) if core else None

        # Build the author function_score clause and attach it as a MUST under the outer bool
        ensure_structure(es_body)
        es_body["query"]["bool"].setdefault("must", [])
        es_body["query"]["bool"]["must"].append(
            build_author_bucket_function_score(
                phrase,
                is_full_name=(is_quoted or " " in phrase),
                partial_match=partial_match,
                orcid_nested_clause=orcid_clause,
            )
        )

        # Sorting: exact bucket first, then base_sort inside each bucket
        es_body["sort"] = author_bucket_sort(order)

        # We’re not using rescore or generic recency boosting in this author mode
        uses_rescore = False
        author_bucket_mode = True

    # 4) ORCID
    elif query and query_fields == "ORCID":
        core = extract_orcid_core(query)
        if core:
            es_body["query"]["bool"]["must"].append(nested_orcid_exact(core))
        else:
            es_body["query"]["bool"]["must"].append({"match_none": {}})

    # 5) Alliance Curie
    elif query and query_fields == "Curie":
        es_body["query"]["bool"]["must"].append({"wildcard": {"curie.keyword": f"*{query}"}})

    # 6) Cross Reference
    elif query and query_fields == "Xref":
        es_body["query"]["bool"]["must"].append(
            {"wildcard": {"cross_references.curie.keyword": f"*{query}"}}
        )

    # Facets (positive)
    if facets_values:
        for facet_field, facet_list_values in facets_values.items():
            if "must" not in es_body["query"]["bool"]["filter"]["bool"]:
                es_body["query"]["bool"]["filter"]["bool"]["must"] = []

            if facet_field in WORKFLOW_FACETS:
                # workflow tags are nested
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
                if facet_field == "authors.name.keyword":
                    for facet_value in facet_list_values:
                        es_body["query"]["bool"]["filter"]["bool"]["must"].append({
                            "nested": {
                                "path": "authors",
                                "query": {"term": {facet_field: facet_value}}
                            }
                        })
                else:
                    # Bundle multiple values under a single bool.must for this field
                    group: Dict[str, Any] = {"bool": {"must": []}}
                    for facet_value in facet_list_values:
                        group["bool"]["must"].append({"term": {facet_field: facet_value}})
                    es_body["query"]["bool"]["filter"]["bool"]["must"].append(group)

    # Facets (negative)
    if negated_facets_values:
        for facet_field, facet_list_values in negated_facets_values.items():
            if "must_not" not in es_body["query"]["bool"]["filter"]["bool"]:
                es_body["query"]["bool"]["filter"]["bool"]["must_not"] = []

            if facet_field == "authors.name.keyword":
                for facet_value in facet_list_values:
                    es_body["query"]["bool"]["filter"]["bool"]["must_not"].append({
                        "nested": {
                            "path": "authors",
                            "query": {"term": {facet_field: facet_value}}
                        }
                    })
            else:
                for facet_value in facet_list_values:
                    es_body["query"]["bool"]["filter"]["bool"]["must_not"].append({"term": {facet_field: facet_value}})

    # Date ranges
    date_range = apply_all_date_filters(
        es_body,
        date_pubmed_modified=date_pubmed_modified,
        date_pubmed_arrive=date_pubmed_arrive,
        date_published=date_published,
        date_created=date_created,
    )

    # Remove empty filter if no facets/dates were added
    if not facets_values and not date_range and not negated_facets_values and not tet_facets:
        es_body["query"]["bool"].pop("filter", None)

    # Additional author_filter (outside the query_fields switch)
    if author_filter:
        name = (author_filter or "").strip()
        if name:
            shoulds = [nested_author_name_match_any(name)]  # no boost
            shoulds.append({
                "nested": {
                    "path": "authors",
                    "query": {"term": {"authors.name.keyword": name}},
                    "score_mode": "max",
                }
            })
            core = extract_orcid_core(name)
            if core:
                shoulds.append(nested_orcid_exact(core))

            es_body["query"]["bool"]["must"].append({"bool": {"should": shoulds, "minimum_should_match": 1}})

    # --------------------------- Scoring + Sorting policy ---------------------------

    if sort:
        es_body["sort"] = sort
    else:
        # If we’re in the special Author bucket mode, we already set the sort
        if locals().get("author_bucket_mode"):
            pass  # keep author-specific sort, no recency function_score
        else:
            apply_scoring_and_sort(
                es_body,
                is_text_search=is_text_search,
                uses_rescore=uses_rescore,
                order=order,
            )

    # Execute
    res = es.search(index=config.ELASTICSEARCH_INDEX, body=es_body)
    formatted_results = process_search_results(res, wft_mod_abbreviations)
    return formatted_results


# --------------------------- Results shaping ---------------------------

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

    # merge processed aggs
    res["aggregations"].update(topic_aggs)
    res["aggregations"].update(workflow_aggs)

    # unwrap nested authors agg to the expected shape
    agg = res["aggregations"].get("authors.name.keyword")
    if isinstance(agg, dict) and ("aggs" in agg or "terms" in agg):
        inner = agg.get("terms") if "terms" in agg else agg.get("aggs", {}).get("terms")
        if isinstance(inner, dict) and "buckets" in inner:
            res['aggregations']["authors.name.keyword"] = inner

    return {
        "hits": hits,
        "aggregations": res["aggregations"],
        "return_count": res["hits"]["total"]["value"]
    }


def process_topic_entity_tags_aggregations(res):  # pragma: no cover
    """
    Post-process TET aggregations (topics, confidence, source methods, SEA)
    """
    def extract_filtered_agg(res, main_key, data_key):
        agg = res['aggregations'].get(main_key, {})
        if "filtered" in agg:
            agg = agg["filtered"]
        if "filter_by_other_tet_values" in agg:
            agg = agg["filter_by_other_tet_values"]
        return agg.get(data_key, {})

    topics = extract_filtered_agg(res, "topic_aggregation", "topics")
    confidence_levels = extract_filtered_agg(res, "confidence_aggregation", "confidence_levels")
    source_methods = extract_filtered_agg(res, "source_method_aggregation", "source_methods")

    raw_sea = extract_filtered_agg(res, "source_evidence_assertion_aggregation", "source_evidence_assertions")
    group_sea = extract_filtered_agg(res, "source_evidence_assertion_group_aggregation", "source_evidence_assertions")

    merged_buckets = {}
    for b in raw_sea.get("buckets", []):
        merged_buckets[b["key"]] = b
    for b in group_sea.get("buckets", []):
        merged_buckets[b["key"]] = b  # overwrite or add

    source_evidence_assertions = {
        "doc_count_error_upper_bound": 0,
        "sum_other_doc_count": 0,
        "buckets": list(merged_buckets.values()),
    }

    # remove temp aggs
    for k in [
        'topic_aggregation',
        'confidence_aggregation',
        'source_method_aggregation',
        'source_evidence_assertion_aggregation',
        'source_evidence_assertion_group_aggregation',
    ]:
        res['aggregations'].pop(k, None)

    # add labels to ATP/ECO curies
    add_curie_to_name_values(topics)
    add_curie_to_name_values(source_evidence_assertions)

    # reorder SEA buckets to desired sequence
    desired_order = [
        "automated assertion",
        "machine learning method evidence used in automatic assertion",
        "string-matching method evidence used in automatic assertion",
        "manual assertion",
        "documented statement evidence used in manual assertion by author",
        "documented statement evidence used in manual assertion by professional biocurator",
    ]
    buckets = source_evidence_assertions.get("buckets", [])
    by_name = {b.get("name"): b for b in buckets}
    source_evidence_assertions["buckets"] = [by_name[name] for name in desired_order if name in by_name]

    return {
        "topics": topics,
        "confidence_levels": confidence_levels,
        "source_methods": source_methods,
        "source_evidence_assertions": source_evidence_assertions,
    }


def process_workflow_tags_aggregations(res, wft_mod_abbreviations):  # pragma: no cover
    """
    Group workflow tag buckets by category and MOD, sum counts, label names.
    """
    workflow_tags_nested = res["aggregations"].get("workflow_tags", {})
    mod_buckets = workflow_tags_nested.get("by_mod_abbreviation", {}).get("buckets", [])

    mod_bucket_lookup: Dict[str, Dict[str, Dict[str, Any]]] = {}
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

    grouped_workflow_tags: Dict[str, Dict[str, Any]] = {category: {} for category in atp_ids}
    for category, id_list in atp_ids.items():
        for mod, bucket_lookup in mod_bucket_lookup.items():
            if mod.upper() not in wft_mod_abbreviations:
                continue
            for expected_id in id_list:
                expected_upper = expected_id.upper()
                if expected_upper in bucket_lookup:
                    bucket = bucket_lookup[expected_upper]
                    count = bucket.get("reverse_docs", {}).get("doc_count", bucket["doc_count"])
                    if expected_upper not in grouped_workflow_tags[category]:
                        grouped_workflow_tags[category][expected_upper] = {
                            "key": expected_upper,
                            "doc_count": 0,
                            "name": bucket.get("name", expected_upper)
                        }
                    grouped_workflow_tags[category][expected_upper]["doc_count"] += count

    final_workflow_aggs: Dict[str, Any] = {}
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

    res["aggregations"].pop("workflow_tags", None)
    return final_workflow_aggs


def remap_highlights(highlights):  # pragma: no cover
    remapped = {}
    for key, value in highlights.items():
        new_key = key.replace("authors.name", "authors")
        remapped[new_key] = value
    return remapped


# --------------------------- TET (nested) aggs & filters ---------------------------

def add_tet_facets_values(es_body, tet_nested_facets_values, apply_to_single_tet):  # pragma: no cover
    tet_facet_values = defaultdict(list)
    levels = []

    if tet_nested_facets_values.get("tet_facets_negative_values"):
        if "must" not in es_body["query"]["bool"]["filter"]["bool"]:
            es_body["query"]["bool"]["filter"]["bool"]["must"] = []
        for level in tet_nested_facets_values["tet_facets_negative_values"][0].get(
            "topic_entity_tags.confidence_level.keyword", []
        ):
            levels.append({"term": {"topic_entity_tags.confidence_level.keyword": level}})

    for facet_name_value_dict in tet_nested_facets_values.get("tet_facets_values", []):
        add_nested_query(es_body, facet_name_value_dict, levels)
        if apply_to_single_tet:
            for facet_name, facet_value in facet_name_value_dict.items():
                key = facet_name.replace("topic_entity_tags.", "").replace(".keyword", "")
                tet_facet_values[key] = facet_value

    return tet_facet_values


def add_nested_query(es_body, facet_name_values_dict, levels):  # pragma: no cover
    must_conditions = []
    for facet_name, facet_values in facet_name_values_dict.items():
        # Remap SEA group if needed
        if facet_name == "topic_entity_tags.source_evidence_assertion.keyword":
            vals = facet_values if isinstance(facet_values, (list, tuple)) else [facet_values]
            if any(v.upper() in ("ECO:0007669", "ECO:0006155") for v in vals):
                facet_name = "topic_entity_tags.source_evidence_assertion_group.keyword"
        must_conditions.append({"term": {facet_name: facet_values}})

    nested_query = {
        "nested": {
            "path": "topic_entity_tags",
            "query": {"bool": {"must": must_conditions, "must_not": levels}},
        }
    }
    es_body["query"]["bool"]["filter"]["bool"]["must"].append(nested_query)


def create_filtered_aggregation_with_dp(path, tet_facets, term_field, term_key, allowed_dp, size=10):  # pragma: no cover
    base_agg = create_filtered_aggregation(path, tet_facets, term_field, term_key, size)
    return {
        "nested": {"path": path},
        "aggs": {
            "filtered": {
                "filter": {"terms": {f"{path}.data_provider": allowed_dp}},
                "aggs": base_agg["aggs"]
            }
        }
    }


def create_filtered_aggregation(path, tet_facets, term_field, term_key, size=10):  # pragma: no cover
    tet_agg: Dict[str, Any] = {"nested": {"path": path}}

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
                        "terms": {"field": term_field, "size": size},
                        "aggs": {"docs_count": {"reverse_nested": {}}}
                    }
                }
            }
        }
        # Ensure we keep empty buckets visible for confidence_levels
        if term_field == "topic_entity_tags.confidence_level.keyword":
            tet_agg["aggs"]["filter_by_other_tet_values"]["aggs"]["confidence_levels"]["terms"]["min_doc_count"] = 0
    else:
        tet_agg["aggs"] = {
            term_key: {
                "terms": {"field": term_field, "size": size},
                "aggs": {"docs_count": {"reverse_nested": {}}}
            }
        }
    return tet_agg


def apply_all_tags_tet_aggregations(es_body, tet_facets, facets_limits, tet_data_providers):  # pragma: no cover
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

    # SEA facets: count over filtered hits but not restricted by SEA value itself
    sea_tet_facets = {k: v for k, v in tet_facets.items() if k != "source_evidence_assertion"}

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


# --------------------------- Utility helpers ---------------------------

def ensure_structure(es_body: Dict[str, Any]) -> None:
    if "query" not in es_body:
        es_body["query"] = {}
    if "bool" not in es_body["query"]:
        es_body["query"]["bool"] = {}
    if "filter" not in es_body["query"]["bool"]:
        es_body["query"]["bool"]["filter"] = {"bool": {}}
    if "must" not in es_body["query"]["bool"]["filter"]["bool"]:
        es_body["query"]["bool"]["filter"]["bool"]["must"] = []


def add_curie_to_name_values(aggregations: Dict[str, Any]) -> None:
    curie_keys = [bucket["key"] for bucket in aggregations.get("buckets", [])]
    curie_to_name_map = get_map_ateam_curies_to_names(
        category="atpterm",
        curies=[c.upper() for c in curie_keys if c.upper().startswith("ATP:")]
    )
    curie_to_name_map.update(get_map_ateam_curies_to_names(
        category="ecoterm",
        curies=[c.upper() for c in curie_keys if c.upper().startswith("ECO:")]
    ))

    for bucket in aggregations.get("buckets", []):
        key_u = bucket["key"].upper()
        curie_name = curie_to_name_map.get(key_u, "Unknown")
        if key_u == "ECO:0006155":
            curie_name = "manual assertion"
        elif key_u == "ECO:0007669":
            curie_name = "automated assertion"
        bucket["name"] = curie_name


def get_atp_ids(root_atp_ids: List[str]) -> List[str]:
    return [child for root in root_atp_ids for child in atp_get_all_descendents(root)]


# --------------------------- ORCID helpers (kept local) ---------------------------

def normalize_orcid(raw: str) -> str:
    """
    Strip optional 'ORCID:' prefix (any case), trim, lowercase.
    If the input doesn't match, returns trimmed lowercase original.
    """
    m = _ORCID_INPUT.match(raw or "")
    if not m:
        return (raw or "").strip().lower()
    return m.group(1).lower()


def orcid_variants(raw: str) -> List[str]:
    s = (raw or "").strip()
    m = _ORCID_INPUT.match(s)
    hyph = m.group(1) if m else s
    hyph_l = hyph.lower()
    hyph_u = hyph.upper()

    return [
        hyph_l, hyph_u,
        f"orcid:{hyph_l}", f"ORCID:{hyph_u}",
        f"https://orcid.org/{hyph_l}", f"http://orcid.org/{hyph_l}",
        f"https://orcid.org/{hyph_u}", f"http://orcid.org/{hyph_u}",
        hyph_l.replace("-", ""), hyph_u.replace("-", "")
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
    """
    if not raw:
        return None
    s = raw.strip()
    m = _ORCID_INPUT.match(s)
    if m:
        return m.group(1).lower()
    if s.upper().startswith("ORCID:"):
        tail = s.split(":", 1)[1].strip()
        m2 = _ORCID_INPUT.match(tail)
        if m2:
            return m2.group(1).lower()
    return None


def nested_orcid_exact(core_lower: str) -> dict:
    """
    - Accepts bare, ORCID:-prefixed, URL, and no-hyphen forms.
    - Queries authors.orcid.keyword (exact string match via normalizer).
    """
    variants = orcid_variants(core_lower)  # uses your existing helper
    return {
        "nested": {
            "path": "authors",
            "query": {"terms": {"authors.orcid.keyword": variants}},
            "score_mode": "max",
        }
    }


# example input: "  Yeast：   cell   cycle　"
# example output: "Yeast: cell cycle"
# Changes made:
# Full-width colon (：) → standard ASCII colon (:)
# Full-width space (　) → normal space
# Multiple spaces collapsed into one
# Leading/trailing spaces trimmed
def normalize_user_query(s: str) -> str:
    if not s:
        return s
    s = unicodedata.normalize("NFKC", s)   # fixes full-width punctuation like '：'
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def looks_like_xref_id_without_prefix(s: str) -> bool:
    s = (s or "").strip()
    if ":" in s:
        return False
    return any(pat.match(s) for pat in _XREF_BARE_PATTERNS)
