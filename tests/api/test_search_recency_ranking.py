"""SCRUM-2278: recency must not swamp text relevance.

Runs the real all-text query plus the recency function_score against a small
Elasticsearch index. Before the fix, a paper a few months newer that matched
the term only in its abstract outranked one matching in the title, abstract and
MeSH heading, and every paper older than about 12 years scored exactly 0 and
was ordered by date alone.
"""
from datetime import datetime, timedelta

import pytest
from elasticsearch import Elasticsearch

from agr_literature_service.api.config import config
from agr_literature_service.api.crud import search_ranking as sr

INDEX = "test_recency_ranking_index"

FILLER = ("Here we describe the behavioural and molecular responses of adult animals "
          "exposed to several compounds over a number of days in controlled conditions.")


def _days_ago(days: int) -> str:
    return (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")


def _rich(curie: str, days: int) -> dict:
    """Matches 'hallucinogen' in the title, abstract and MeSH heading."""
    return {
        "curie": curie,
        "title": "The biosynthetic pathway of the hallucinogen mescaline",
        "abstract": "Mescaline is among the earliest identified natural hallucinogen compounds. " + FILLER,
        "mesh_terms": [{"heading_term": "Hallucinogens"}],
        "date_published_start": _days_ago(days),
    }


def _thin(curie: str, days: int) -> dict:
    """Matches 'hallucinogen' once, in the abstract only."""
    return {
        "curie": curie,
        "title": "Metabolism study of two phenethylamine derived psychoactive substances",
        "abstract": FILLER + " They are derivatives of the classic hallucinogen mescaline.",
        "mesh_terms": [{"heading_term": "Psychotropic Drugs"}],
        "date_published_start": _days_ago(days),
    }


@pytest.fixture(scope="module")
def es():
    if "es.amazonaws.com" in config.ELASTICSEARCH_HOST:
        pytest.exit("**** Warning: not allow to run test on stage or prod elasticsearch index *****")
    client = Elasticsearch(hosts=config.ELASTICSEARCH_HOST + ":" + config.ELASTICSEARCH_PORT)
    client.indices.delete(index=INDEX, ignore=[400, 404])
    client.indices.create(index=INDEX, body={
        "mappings": {
            "properties": {
                "curie": {"type": "keyword"},
                "title": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "abstract": {"type": "text"},
                "keywords": {"type": "text"},
                "citation": {"type": "text"},
                "mesh_terms": {"properties": {"heading_term": {"type": "text"}}},
                "date_published_start": {"type": "date", "format": "yyyy-MM-dd"},
                "date_created": {"type": "long"},
            }
        }
    })
    docs = [
        # Recent pair, ~1 year apart in age (pair 2 in the curator report)
        _thin("RECENT:thin", 500), _rich("RECENT:rich", 850),
        # Old pair, both older than 12 years (pair 1 in the curator report)
        _thin("OLD:thin", 4900), _rich("OLD:rich", 5100),
        # Same paper, two ages: recency still breaks the tie
        _rich("TIE:new", 30), _rich("TIE:old", 3000),
    ]
    for i, doc in enumerate(docs):
        client.index(index=INDEX, id=i, body=doc)
    client.indices.refresh(index=INDEX)
    yield client
    client.indices.delete(index=INDEX, ignore=[400, 404])


def _search(client, query: str, curie_prefix: str) -> list:
    bundle = sr.build_all_text_query(query, include_id_author_helpers=False)
    es_body: dict = {
        "query": {"bool": {
            "must": bundle["must"],
            "should": bundle["should"],
            "filter": [{"prefix": {"curie": curie_prefix}}],
        }},
        "size": 10,
    }
    sr.apply_scoring_and_sort(es_body, is_text_search=True, uses_rescore=False)
    res = client.search(index=INDEX, body=es_body)
    return [(h["_source"]["curie"], h["_score"]) for h in res["hits"]["hits"]]


def test_stronger_match_beats_slightly_newer_paper(es):
    hits = _search(es, "hallucinogen", "RECENT:")
    assert [c for c, _ in hits] == ["RECENT:rich", "RECENT:thin"]


def test_old_papers_keep_text_relevance(es):
    hits = _search(es, "hallucinogen", "OLD:")
    assert [c for c, _ in hits] == ["OLD:rich", "OLD:thin"]
    assert all(score > 0 for _, score in hits)


def test_recency_breaks_ties_between_equal_matches(es):
    hits = _search(es, "hallucinogen", "TIE:")
    assert [c for c, _ in hits] == ["TIE:new", "TIE:old"]
    (_, new_score), (_, old_score) = hits
    assert new_score / old_score <= 1.0 + sr.RECENCY_MAX_BONUS
