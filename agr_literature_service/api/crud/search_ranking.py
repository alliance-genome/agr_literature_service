from typing import Dict, Any, List, Optional, cast

# =========================== Boost policy (central config) ===========================

# Boost Set: BOOST_PHRASE_FIELDS
# Query Type (ES): multi_match "phrase" match_phrase
# Match Style: Exact phrase, same order (tight match)
# Example Query → Match Example:
#     "oxidative stress response" → matches title “Oxidative stress response in yeast”
# Use Case: Reward exact or near-exact phrases (titles, keywords, known terms)
BOOST_PHRASE_FIELDS: Dict[str, float] = {
    "title": 6.0,
    "keywords": 5.0,
    "abstract": 3.0,
    "citation": 2.0,
}

# Boost Set: BOOST_BEST_FIELDS
# Query Type (ES): multi_match "best_fields"
# Match Style: Bag-of-words, order not required; uses MSM (minimum_should_match) %
# Example Query → Match Example:
#     "oxidative stress response" → matches title “Response to oxidative stress in yeast”
# Use Case: General search, supports flexible token order and partial overlap
BOOST_BEST_FIELDS: Dict[str, float] = {
    "title": 5.0,
    "keywords": 2.0,
    "abstract": 2.0,
    "citation": 1.0,
}

# Boost Set: BOOST_PREFIX_FIELDS
# Query Type (ES): multi_match "phrase_prefix" match_phrase_prefix
# Match Style: Prefix expansion of last token
# Example Query → Match Example:
#     "bioelectroca" → matches title “Bioelectrocatalysis for hydrogen production”
# Use Case: Type-ahead / partial typing before full query entered
BOOST_PREFIX_FIELDS: Dict[str, float] = {
    "title": 2.0,
    "keywords": 1.0,
    "abstract": 1.0,
    "citation": 1.0,
}

# Boost Constant: BOOST_EXACT_TITLE_KEYWORD
# Query Type (ES): term on title.keyword
# Match Style: Exact literal match (no analysis)
# Example Query → Match Example:
#     Paste full title "Aerobic mild bioelectrocatalysis: Disentangling dual redox pathways..."
#        → matches document whose title.keyword is exactly that string
# Use Case: User pastes full title literally; ensures exact match bubbles to the top
BOOST_EXACT_TITLE_KEYWORD: float = 6.0


# Boost Constant: BOOST_SINGLE_FIELD_MATCH
# Query Type (ES): match with OR + MSM (minimum_should_match)
# Match Style: Token-based, word order not required
# Example Query → Match Example:
#     Field=Title, query "Disentangling redox pathways"
#        → matches title containing all tokens, any order
# Use Case: Single-field search (Title, Abstract, Keywords) where recall matters;
#           fallback to token matches
BOOST_SINGLE_FIELD_MATCH: float = 2.0

# Boost Constant: BOOST_SINGLE_FIELD_PHRASE
# Query Type (ES): match_phrase
# Match Style: Exact phrase within one field
# Example Query → Match Example:
#     Field=Abstract, query "hydrogen evolution reaction"
#        → matches abstract sentence with that phrase
# Use Case: User wants phrase-level precision inside one field
#           (stronger than match, weaker than literal keyword)
BOOST_SINGLE_FIELD_PHRASE: float = 4.0

# Boost Constant: BOOST_SINGLE_FIELD_PREFIX
# Query Type (ES): match_phrase_prefix
# Match Style: Prefix expansion inside one field
# Example Query → Match Example:
#     Field=Keyword, query "transgen" → matches keyword "transgenic allele"
# Use Case: Type-ahead within a chosen field; ensures partial typing still returns
#           useful results, but with low weight
BOOST_SINGLE_FIELD_PREFIX: float = 1.2


def _fields_with_boost(mapping: Dict[str, float]) -> List[str]:
    """Convert {"title": 5.0, ...} -> ["title^5.0", ...] for ES field lists."""
    return [f"{f}^{w}" for f, w in mapping.items()]


# --------------------------- Public constants ---------------------------

TEXT_FIELDS = {
    "Title": "title",
    "Abstract": "abstract",
    "Keyword": "keywords",   # PubMed Keyword
    "Citation": "citation",
}

# Keep these exported for any callers that rely on them
PHRASE_FIELDS = _fields_with_boost(BOOST_PHRASE_FIELDS)
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
                {"filter": {"range": {field: {"gt": "now"}}}, "weight": 2.5},

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


# --------------------------- Author building blocks ---------------------------
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
    node: Dict[str, Any] = {
        "nested": {
            "path": "authors",
            "query": {"match": {"authors.name": {"query": name, "analyzer": "authorNameAnalyzer"}}},
            "score_mode": "max",
        }
    }
    if boost is not None:
        nested = cast(Dict[str, Any], node["nested"])
        nested["boost"] = float(boost)
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
    orcid_nested_clause: Optional[Dict[str, Any]] = None,
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

    # Optionally include an external ORCID nested clause (built by caller)
    if orcid_nested_clause:
        near_clauses.append(orcid_nested_clause)

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


def compute_minimum_should_match(query: str) -> str:
    """
    Compute an appropriate minimum_should_match threshold string
    based on the length (in tokens) of the query.
    """
    n = len((query or "").split())
    if n >= 16:
        return "70%"
    if n >= 10:
        return "75%"
    if n >= 6:
        return "80%"
    return "100%"  # short queries still require full coverage


def add_simple_text_field_query(es_body: dict, field: str, q: str, partial_match: bool = True):
    """
    Analyzer-based per-field query. No query_string.
    Works even when q contains ':' or other punctuation.
    """
    should = [
        {"match_phrase": {field: {"query": q, "slop": 1, "boost": BOOST_SINGLE_FIELD_PHRASE}}},
        {"match": {field: {
            "query": q,
            "operator": "or",
            "minimum_should_match": compute_minimum_should_match(q),
            "boost": BOOST_SINGLE_FIELD_MATCH,
        }}},
    ]
    if partial_match:
        should.append({"match_phrase_prefix": {field: {"query": q, "boost": BOOST_SINGLE_FIELD_PREFIX}}})

    # Exact field fallback only for Title (useful for full-title pastes)
    if field == "title":
        should.append({"term": {"title.keyword": {"value": q, "boost": BOOST_EXACT_TITLE_KEYWORD}}})

    es_body["query"]["bool"]["must"].append({"bool": {"should": should, "minimum_should_match": 1}})


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


def build_all_text_query(q: str, size_result_count: int = 10, include_id_author_helpers: bool = False):
    """
    Analyzer-based multi-field query (no query_string).
    Keeps boosts and is safe for punctuation like ':'.
    Adds an exact title keyword fallback to help full-title pastes.
    """
    q = (q or "").strip()
    must: List[Dict[str, Any]] = []
    should: List[Dict[str, Any]] = []
    rescore: Optional[Dict[str, Any]] = None

    # Primary: analyzer-based best_fields with MSM (no query_string)
    must.append({
        "multi_match": {
            "query": q,
            "fields": _fields_with_boost(BOOST_BEST_FIELDS),
            "type": "best_fields",
            "operator": "or",
            "minimum_should_match": compute_minimum_should_match(q),
            "tie_breaker": 0.1,
        }
    })

    # Phrase boost across text fields
    should.append({
        "multi_match": {
            "query": q,
            "fields": _fields_with_boost(BOOST_PHRASE_FIELDS),
            "type": "phrase",
            "slop": 1,
        }
    })

    # Gentle prefix help
    should.append({
        "multi_match": {
            "query": q,
            "fields": _fields_with_boost(BOOST_PREFIX_FIELDS),
            "type": "phrase_prefix",
        }
    })

    # Exact title fallback (helps "All" mode for full-title pastes)
    should.append({"term": {"title.keyword": {"value": q, "boost": BOOST_EXACT_TITLE_KEYWORD}}})

    return {"must": must, "should": should, "rescore": rescore, "uses_rescore": False}


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
