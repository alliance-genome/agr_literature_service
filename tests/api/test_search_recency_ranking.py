"""SCRUM-2278: recency must not swamp text relevance.

Runs the real all-text query plus the recency function_score against a small
Elasticsearch index. Before the fix, a paper a few months newer that matched
the term only in its abstract outranked one matching in the title, abstract and
MeSH heading, and every paper older than about 12 years scored exactly 0 and
was ordered by date alone. A paper matching in its title only also outranked
one matching in its title, abstract, keywords and MeSH, because each query
clause counted only its best field and the shorter title scored higher.

SCRUM-6865: the all-fields search now ranks by the fields a paper matches, then
by recency, so the cases below follow the "Xenobiotic" report on prod.
"""
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import pytest
from elasticsearch import Elasticsearch

from agr_literature_service.api.config import config
from agr_literature_service.api.crud import search_ranking as sr

INDEX = "test_recency_ranking_index"

FILLER = ("Here we describe the behavioural and molecular responses of adult animals "
          "exposed to several compounds over a number of days in controlled conditions.")


def _days_ago(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")


def _is_aws_es_host(host: str) -> bool:
    """True for an AWS-hosted (stage/prod) Elasticsearch endpoint."""
    hostname = urlparse(host if "://" in host else "//" + host).hostname or ""
    return hostname == "es.amazonaws.com" or hostname.endswith(".es.amazonaws.com")


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


def _broad(curie: str, days: int) -> dict:
    """Matches 'xenobiotic' in a long title plus abstract, keyword and MeSH (AGRKB:101000001213786)."""
    return {
        "curie": curie,
        "title": ("Identification and Functional Validation of the Role of Solute Carriers in Shaping "
                  "Xenobiotic Tolerance in Polyphagous Pests of Spodoptera litura"),
        "abstract": FILLER + " These genes drive the development of xenobiotic tolerance; overexpression "
                             "conferred tolerance to xenobiotics.",
        "keywords": ["solute carrier", "tolerance", "xenobiotics"],
        "mesh_terms": [{"heading_term": "Xenobiotics"}, {"heading_term": "Insecticides"}],
        "date_published_start": _days_ago(days),
    }


def _title_only(curie: str, days: int) -> dict:
    """Matches 'xenobiotic' in a short title only (AGRKB:101000001190387)."""
    return {
        "curie": curie,
        "title": "A ginger compound alleviates gut inflammation via the xenobiotic receptor PXR",
        "abstract": FILLER,
        "mesh_terms": [{"heading_term": "Colitis"}],
        "date_published_start": _days_ago(days),
    }


def _short_title(curie: str, days: int) -> dict:
    """Matches 'xenobiotic' in a five-word title (and so its citation) only (AGRKB:101000000991665)."""
    title = "Transposable elements and xenobiotic resistance."
    return {
        "curie": curie,
        "title": title,
        "citation": "Ffrench-Constant RH, (2023) " + title,
        "abstract": FILLER,
        "mesh_terms": [{"heading_term": "Insecticide Resistance"}],
        "date_published_start": _days_ago(days),
    }


def _dense(curie: str, days: int) -> dict:
    """Matches 'xenobiotic' in a short title and repeatedly in the abstract, keywords and MeSH."""
    return {
        "curie": curie,
        "title": "Xenobiotic responses of Drosophila to insecticides",
        "abstract": "Xenobiotic response genes respond to xenobiotic exposure; the xenobiotic "
                    "response is general. " + FILLER,
        "keywords": ["xenobiotic response"],
        "mesh_terms": [{"heading_term": "Xenobiotics"}],
        "date_published_start": _days_ago(days),
    }


def _no_mesh(curie: str, days: int) -> dict:
    """Matches 'xenobiotic' in the title, abstract and keywords, with no MeSH match."""
    doc = _dense(curie, days)
    doc["mesh_terms"] = [{"heading_term": "Lung"}]
    return doc


def _no_title(curie: str, days: int) -> dict:
    """Matches 'xenobiotic' in the abstract, keywords and MeSH, but not the title."""
    doc = _dense(curie, days)
    doc["title"] = "Inflammation-driven downregulation of CYP2E1 in hepatocarcinogenesis"
    return doc


@pytest.fixture(scope="module")
def es():
    if _is_aws_es_host(config.ELASTICSEARCH_HOST):
        pytest.exit("**** Warning: not allow to run test on stage or prod elasticsearch index *****")
    client = Elasticsearch(hosts=config.ELASTICSEARCH_HOST + ":" + config.ELASTICSEARCH_PORT)
    client.indices.delete(index=INDEX, ignore=[400, 404])
    client.indices.create(index=INDEX, body={
        "mappings": {
            "properties": {
                "curie": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
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
        # Same age: matches in more fields beat a shorter title-only match
        _broad("BREADTH:broad", 400), _title_only("BREADTH:title", 400),
        # SCRUM-6865: a short title-only match three years older (AGRKB:101000000991665)
        _broad("SHORT:broad", 280), _short_title("SHORT:title", 1400),
        # SCRUM-6865: same fields, the older paper's shorter title and repeated
        # term favour it on BM25 (AGRKB:101000000947122 vs 101000001208210)
        _dense("SAME:old", 1450), _broad("SAME:new", 350),
        # SCRUM-6865: one extra MeSH match on a paper eleven years older
        # (AGRKB:101000000257013 vs 101000001211879)
        _dense("CLOSE:old", 4700), _no_mesh("CLOSE:new", 1000),
        # ... but two more matching fields still win over a newer paper
        _broad("FAR:old", 4700), _short_title("FAR:new", 30),
        # A title match beats a brand-new paper matching the other fields only
        # (like "Inflammation-Driven Downregulation of CYP2E1", 2026)
        _dense("TITLE:old", 3200), _no_title("TITLE:new", 30),
    ]
    for i, doc in enumerate(docs):
        # As in the real index, the citation repeats the title
        doc.setdefault("citation", "Doe J, (2020) " + doc["title"])
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
            "filter": [{"prefix": {"curie.keyword": curie_prefix}}],
        }},
        "size": 10,
    }
    sr.apply_scoring_and_sort(es_body, is_text_search=True, uses_rescore=False,
                              field_match_functions=bundle["field_match_functions"])
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
    assert new_score - old_score <= sr.FIELD_MATCH_RECENCY_BONUS


def test_matches_in_more_fields_beat_shorter_title_only_match(es):
    hits = _search(es, "xenobiotic", "BREADTH:")
    assert [c for c, _ in hits] == ["BREADTH:broad", "BREADTH:title"]


def test_more_fields_beat_older_short_title_match(es):
    hits = _search(es, "xenobiotic", "SHORT:")
    assert [c for c, _ in hits] == ["SHORT:broad", "SHORT:title"]


def test_newer_paper_wins_when_the_same_fields_match(es):
    hits = _search(es, "xenobiotic", "SAME:")
    assert [c for c, _ in hits] == ["SAME:new", "SAME:old"]


def test_much_newer_paper_beats_one_extra_mesh_match(es):
    hits = _search(es, "xenobiotic", "CLOSE:")
    assert [c for c, _ in hits] == ["CLOSE:new", "CLOSE:old"]


def test_two_more_fields_beat_a_newer_paper(es):
    hits = _search(es, "xenobiotic", "FAR:")
    assert [c for c, _ in hits] == ["FAR:old", "FAR:new"]


def test_title_match_beats_newer_paper_without_one(es):
    hits = _search(es, "xenobiotic", "TITLE:")
    assert [c for c, _ in hits] == ["TITLE:old", "TITLE:new"]
