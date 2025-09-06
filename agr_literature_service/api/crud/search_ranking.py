from typing import Dict, Any, List, Optional

# --------------------------- Public constants ---------------------------

TEXT_FIELDS = {
    "Title": "title",
    "Abstract": "abstract",
    "Keyword": "keywords",   # PubMed Keyword
    "Citation": "citation",
}

PHRASE_FIELDS = ["title^6", "keywords^5", "abstract^3", "citation^2"]
ALL_TEXT_FIELDS = ["title", "keywords", "abstract", "citation"]


# --------------------------- Rescore helpers ---------------------------

def _rescore_window(size_result_count: int) -> int:
    return max(200, int(size_result_count or 10) * 5)


def rescore_exact_phrase(phrase: str, fields: List[str], size: int, weight: float) -> Dict[str, Any]:
    return {
        "window_size": _rescore_window(size),
        "query": {
            "rescore_query": {
                "multi_match": {"query": phrase, "type": "phrase", "slop": 1, "fields": fields}
            },
            "query_weight": 1.0,
            "rescore_query_weight": weight,
            "score_mode": "total"
        }
    }


# --------------------------- Recency function_score ---------------------------

def apply_balanced_recency_boost(es_body: Dict[str, Any], field: str = "date_published_start") -> None:
    """
    Strong recency favoring with extra-hard penalties for old papers.
    """
    base_query = es_body.get("query") or {"match_all": {}}
    es_body["query"] = {
        "function_score": {
            "query": base_query,
            "functions": [
                # Future-dated papers
                {"filter": {"range": {field: {"gt": "now"}}}, "weight": 6.0},

                # Smooth decay (≈ halves each ~365 days; ignore first 2 weeks)
                {"gauss": {field: {"origin": "now", "scale": "365d", "offset": "14d", "decay": 0.5}}},

                # Recent tiers
                {"filter": {"range": {field: {"gte": "now-183d", "lte": "now"}}}, "weight": 5.5},
                {"filter": {"range": {field: {"gte": "now-365d", "lt": "now-183d"}}}, "weight": 4.0},
                {"filter": {"range": {field: {"gte": "now-1095d", "lt": "now-365d"}}}, "weight": 2.5},
                {"filter": {"range": {field: {"gte": "now-1825d", "lt": "now-1095d"}}}, "weight": 1.2},

                # Older clamps
                {"filter": {"range": {field: {"lt": "now-3650d", "gte": "now-7300d"}}}, "weight": 0.05},
                {"filter": {"range": {field: {"lt": "now-7300d"}}}, "weight": 0.005},
            ],
            "score_mode": "multiply",
            "boost_mode": "multiply",
            "max_boost": 10.0,
        }
    }


# --------------------------- Author / ORCID building blocks ---------------------------
# Each of these helpers builds a different style of match query on authors.name.
# They are meant to be combined with different boosts so Elasticsearch can rank:
#   exact match > close match > looser match.
# --------------------------------------------------------------------------------------


# --------------------------------------------------------------------------------------
# nested_author_name_match_any
# Use case: Flexible analyzed match on authors.name using authorNameAnalyzer.
#   - Use boost=10.0 for primary "match" signals.
#   - Leave boost=None for lightweight fallback scoring.
# --------------------------------------------------------------------------------------
def nested_author_name_match_any(name: str, boost: float | None = None) -> dict:
    """
    Analyzed match on authors.name using authorNameAnalyzer.
    - boost=None  -> lower-weighted fallback
    - boost=10.0  -> stronger signal
    """
    node = {
        "nested": {
            "path": "authors",
            "query": {"match": {"authors.name": {"query": name, "analyzer": "authorNameAnalyzer"}}},
            "score_mode": "max",
        }
    }
    if boost is not None:
        node["nested"]["boost"] = boost
    return node


# --------------------------------------------------------------------------------------
# nested_author_name_exact_keyword
# Use case: Exact match against the keyword field (no analysis, case-sensitive).
# Best for quoted full names or when you want a strict literal match.
# --------------------------------------------------------------------------------------
def nested_author_name_exact_keyword(name: str, boost: float = 8.0) -> dict:
    return {
        "nested": {
            "path": "authors",
            "query": {"term": {"authors.name.keyword": name}},
            "score_mode": "max",
            "boost": boost
        }
    }


# --------------------------------------------------------------------------------------
# nested_author_name_prefix_keyword
# Use case: Prefix match against the keyword field (wildcard query).
# Useful for "search-as-you-type" on the raw name (e.g., "Step*" -> "Stephens").
# Less precise and more expensive than exact or token matches, so lower boost.
# --------------------------------------------------------------------------------------
def nested_author_name_prefix_keyword(prefix: str, boost: float = 4.0) -> dict:
    return {
        "nested": {
            "path": "authors",
            "query": {"wildcard": {"authors.name.keyword": f"{prefix}*"}},
            "score_mode": "max",
            "boost": boost
        }
    }


# --------------------------------------------------------------------------------------
# nested_author_name_match_prefix
# Use case: Phrase-prefix match on the analyzed field.
# Good for multi-word partials, e.g., "Arthur C" matches "Arthur Clarke".
# More flexible than raw wildcard because it respects tokenization.
# --------------------------------------------------------------------------------------
def nested_author_name_match_prefix(name: str, boost: float = 3.0) -> dict:
    return {
        "nested": {
            "path": "authors",
            "query": {"match_phrase_prefix": {"authors.name": {"query": name}}},
            "score_mode": "max",
            "boost": boost
        }
    }


# --------------------------------------------------------------------------------------
# nested_author_name_exact_token
# Use case: Exact token match on the standard analyzer.
# Ensures a standalone token match (e.g., "West") does not match "Westerfield".
# This is the strongest signal for single-token author searches.
# --------------------------------------------------------------------------------------
def nested_author_name_exact_token(name: str, boost: float = 12.0) -> dict:
    """
    Exact token match (no prefix expansion): ensures 'West' != 'Westerfield'.
    """
    return {
        "nested": {
            "path": "authors",
            "query": {"match": {"authors.name": {"query": name, "analyzer": "standard", "operator": "and"}}},
            "score_mode": "max",
            "boost": boost
        }
    }


"""
Summary of relative strengths:
- Exact token (exact_token) → strongest for single-word names.
- Exact keyword (exact_keyword) → strong for quoted/full names.
- Analyzed match (match_any with boost) → strong fallback.
- Plain analyzed query (match_any without boost) → softer fallback.
- Prefix (prefix_keyword, match_prefix) → weakest, for partial typing.
"""


def nested_orcid_exact(core_lower: str) -> dict:
    normalized_orcid = f"orcid:{core_lower}".lower()
    return {
        "nested": {
            "path": "authors",
            "query": {"term": {"authors.orcid.keyword": normalized_orcid}},
            "score_mode": "max"
        }
    }


# --------------------------- Author bucketed ranking ---------------------------


def _author_exact_clause_full_name(phrase: str) -> Dict[str, Any]:
    return {
        "nested": {
            "path": "authors",
            "query": {"term": {"authors.name.keyword": phrase}},
            "score_mode": "max",
        }
    }


def _author_near_clauses_full_name(phrase: str) -> List[Dict[str, Any]]:
    return [{
        "nested": {
            "path": "authors",
            "query": {
                "match_phrase": {
                    "authors.name": {"query": phrase, "slop": 1, "analyzer": "authorNameAnalyzer"}
                }
            },
            "score_mode": "max",
        }
    }]


def _author_exact_clause_single_token(token: str) -> Dict[str, Any]:
    # exact token (no prefix): 'West' != 'Westerfield'
    return nested_author_name_exact_token(token, boost=1.0)  # boost ignored in function filter


def _author_near_clauses_single_token(token: str, *, partial: bool) -> List[Dict[str, Any]]:
    near = [
        nested_author_name_exact_keyword(token, boost=1.0),
        nested_author_name_match_any(token, boost=1.0),
    ]
    if partial:
        near += [
            nested_author_name_match_prefix(token, boost=1.0),
            nested_author_name_prefix_keyword(token, boost=1.0),
        ]
    return near


def build_author_bucket_function_score(
    phrase: str,
    *,
    is_full_name: bool,
    partial_match: bool,
    orcid_core: Optional[str] = None
) -> Dict[str, Any]:
    """
    Build a function_score that buckets author hits:
      - exact author match gets score 2.0
      - near author (and optional ORCID) gets score 1.0
    The caller should place this object inside query.bool.must.
    """
    if is_full_name:
        exact_clause = _author_exact_clause_full_name(phrase)
        near_clauses = _author_near_clauses_full_name(phrase)
    else:
        exact_clause = _author_exact_clause_single_token(phrase)
        near_clauses = _author_near_clauses_single_token(phrase, partial=partial_match)

    # Optionally treat ORCID match as a "near" signal within the author search
    if orcid_core:
        near_clauses.append(nested_orcid_exact(orcid_core))

    return {
        "function_score": {
            "query": {
                "bool": {
                    "should": [exact_clause] + near_clauses,
                    "minimum_should_match": 1
                }
            },
            "functions": [
                {"filter": exact_clause, "weight": 2.0},  # exact bucket
            ],
            "score_mode": "max",
            "boost_mode": "replace"
        }
    }


def author_bucket_sort(order: str = "desc") -> List[Dict[str, Any]]:
    """
    Sort exact bucket (score=2) above near bucket (score=1),
    then use base_sort inside each bucket.
    """
    return [
        {"_score": {"order": "desc"}},  # 2.0 then 1.0
        {"date_published_start": {"order": order, "missing": "_last"}},
        {"date_created": {"order": order, "missing": "_last"}},
        {"curie.keyword": {"order": "asc"}},
    ]


# --------------------------- Query builders ---------------------------


def strip_orcid_prefix_for_free_text(q: str) -> str:
    import re
    return re.sub(r'(?i)^\s*orcid:\s*', '', q or '').strip()


def add_simple_text_field_query(es_body: Dict[str, Any], field_name: str, query: str, partial: bool) -> None:
    q = f"{query}*" if partial else query
    es_body["query"]["bool"]["must"].append({
        "simple_query_string": {
            "fields": [field_name],
            "query": q,
            "analyze_wildcard": True,
            "flags": "PHRASE|PREFIX|WHITESPACE|OR|AND|ESCAPE",
            "default_operator": "and",
        }
    })


def build_id_xref_author_helpers(phrase: str, *, include_author: bool = True) -> List[Dict[str, Any]]:
    helpers: List[Dict[str, Any]] = [
        {"wildcard": {"curie.keyword": f"*{phrase}"}},
        {"wildcard": {"cross_references.curie.keyword": f"*{phrase}"}},
    ]
    if include_author:
        helpers += [
            nested_author_name_exact_token(phrase, boost=15.0),
            nested_author_name_match_any(phrase, boost=10.0),
            nested_author_name_exact_keyword(phrase, boost=9.0),
            nested_author_name_prefix_keyword(phrase, boost=6.0),
            nested_author_name_match_prefix(phrase, boost=3.0),
            nested_author_name_match_any(phrase),  # no boost
        ]
    return helpers


def build_all_text_query(
    q_free: str,
    size_result_count: int,
    *,
    include_id_author_helpers: bool = True
) -> Dict[str, Any]:
    is_quoted = len(q_free) >= 2 and q_free[0] == '"' and q_free[-1] == '"'
    phrase = q_free[1:-1].strip() if is_quoted else q_free

    id_shoulds = build_id_xref_author_helpers(phrase, include_author=include_id_author_helpers)

    res: Dict[str, Any] = {"must": [], "should": [], "rescore": None, "uses_rescore": False}

    if is_quoted and len(phrase.split()) >= 2:
        res["must"].append({
            "multi_match": {"type": "phrase", "query": phrase, "slop": 1, "fields": PHRASE_FIELDS}
        })
        res["should"].extend(id_shoulds)
        res["rescore"] = rescore_exact_phrase(phrase, PHRASE_FIELDS, size_result_count, weight=20.0)
        res["uses_rescore"] = True
    else:
        exact_phrase_should = {
            "multi_match": {"type": "phrase", "query": phrase, "slop": 1, "fields": PHRASE_FIELDS, "boost": 9.0}
        }
        prefix_should = {
            "simple_query_string": {
                "fields": ALL_TEXT_FIELDS,
                "query": phrase + "*",
                "analyze_wildcard": True,
                "flags": "PHRASE|PREFIX|WHITESPACE|OR|AND|ESCAPE",
                "default_operator": "and"
            }
        }
        res["must"].append({"bool": {"should": [exact_phrase_should, prefix_should] + id_shoulds,
                                     "minimum_should_match": 1}})
        res["rescore"] = rescore_exact_phrase(phrase, PHRASE_FIELDS, size_result_count, weight=18.0)
        res["uses_rescore"] = True
    return res


# --------------------------- Scoring + Sorting policy ---------------------------


def apply_scoring_and_sort(
    es_body: Dict[str, Any],
    *,
    is_text_search: bool,
    uses_rescore: bool,
    order: str = "desc"
) -> None:
    """
    Centralized policy:
      - Text searches: apply recency + score-first ordering (unless rescore -> score-only).
      - Non-text searches (facets-only/dates): pure recency-first ordering.
    """
    if is_text_search:
        apply_balanced_recency_boost(es_body)
        if uses_rescore:
            es_body.pop("sort", None)  # ES: score-only ordering when rescore exists
        else:
            es_body["sort"] = [
                {"_score": {"order": "desc"}},
                {"date_published_start": {"order": order, "missing": "_last"}},
                {"date_created": {"order": order, "missing": "_last"}},
                {"curie.keyword": {"order": "asc"}},
            ]
    else:
        es_body["sort"] = [
            {"date_published_start": {"order": order, "missing": "_last"}},
            {"date_created": {"order": order, "missing": "_last"}},
            {"_score": {"order": "desc"}},
            {"curie.keyword": {"order": "asc"}},
        ]
