import pytest
from datetime import datetime

from elasticsearch import Elasticsearch
from starlette.testclient import TestClient

from fastapi import status
from agr_literature_service.api.config import config
from agr_literature_service.api.main import app
from .test_mod_corpus_association import test_mca # noqa
from ..fixtures import db # noqa
from .test_reference import test_reference # noqa
from .test_mod import test_mod # noqa
from .fixtures import auth_headers # noqa


@pytest.fixture(scope='module')
def initialize_elasticsearch():
    print("***** Initializing Elasticsearch Data *****")
    es = Elasticsearch(hosts=config.ELASTICSEARCH_HOST + ":" + config.ELASTICSEARCH_PORT)
    doc1 = {
        "curie": "AGRKB:101000000000001",
        "title": "superlongword super super super super test test test",
        "pubmed_types": ["Journal Article", "Review"],
        "abstract": "Really quite a lot of great information in this article",
        "date_published": "1901",
        "date_published_start": datetime.strptime('10/10/2021', '%m/%d/%Y').timestamp(),
        "date_published_end": datetime.strptime('11/10/2021', '%m/%d/%Y').timestamp(),
        "authors": [{"name": "John Q Public", "orcid": "null"}, {"name": "Socrates", "orcid": "null"}],
        "cross_references": [{"curie": "FB:FBrf0000001", "is_obsolete": "false"}, {"curie": "FB:FBrf0000002", "is_obsolete": "true"}]
    }
    doc2 = {
        "curie": "AGRKB:101000000000002",
        "title": "cell title",
        "pubmed_types": ["Book"],
        "abstract": "Its really worth reading this article",
        "date_published": "2022",
        "date_published_start": datetime.strptime('10/10/2022', '%m/%d/%Y').timestamp(),
        "date_published_end": datetime.strptime('10/10/2022', '%m/%d/%Y').timestamp(),
        "authors": [{"name": "Jane Doe", "orcid": "null"}],
        "cross_references": [{"curie": "PMID:0000001", "is_obsolete": "false"}]
    }
    doc3 = {
        "curie": "AGRKB:101000000000003",
        "title": "Book 1",
        "pubmed_types": ["Book", "Abstract", "Category1", "Category2", "Category3"],
        "abstract": "A book written about science",
        "date_published": "1950-06-03",
        "date_published_start": datetime.strptime('01/01/2023', '%m/%d/%Y').timestamp(),
        "date_published_end": datetime.strptime('10/01/2023', '%m/%d/%Y').timestamp(),
        "authors": [{"name": "Sam", "orcid": "null"}, {"name": "Plato", "orcid": "null"}],
        "cross_references": [{"curie": "FB:FBrf0000001", "is_obsolete": "false"}, {"curie": "SGD:S000000123", "is_obsolete": "true"}]
    }
    doc4 = {
        "curie": "AGRKB:101000000000004",
        "title": "Book 2",
        "pubmed_types": ["Book", "Category4", "Test", "category5", "Category6", "Category7"],
        "abstract": "The other book written about science",
        "date_published_start": datetime.strptime('10/01/2021', '%m/%d/%Y').timestamp(),
        "date_published_end": datetime.strptime('10/30/2022', '%m/%d/%Y').timestamp(),
        "date_published": "2010",
        "authors": [{"name": "Euphrates", "orcid": "null"}, {"name": "Aristotle", "orcid": "null"}],
        "cross_references": [{"curie": "MGI:12345", "is_obsolete": "false"}]
    }
    es.index(index="references_index", id=1, body=doc1)
    es.index(index="references_index", id=2, body=doc2)
    es.index(index="references_index", id=3, body=doc3)
    es.index(index="references_index", id=4, body=doc4)
    es.indices.refresh(index="references_index")
    yield None
    print("***** Cleaning Up Elasticsearch Data *****")
    es.indices.delete(index="references_index")
    print("deleted references_index")


class TestSearch:

    def test_search_references_return_facets_only(self, initialize_elasticsearch, auth_headers): # noqa
        with TestClient(app) as client:
            facets_values = {
                "pubmed_types.keyword": (["Journal Article", "Review"])
            }
            search_data = {"query": None, "facets_values": facets_values, "return_facets_only": True}
            response = client.post(url="/search/references/", json=search_data, headers=auth_headers)
            res = response.json()
            assert "aggregations" in res and "pubmed_types.keyword" in res["aggregations"]
            assert len(res["aggregations"]["pubmed_types.keyword"]["buckets"]) > 0

    def test_search_references_no_facets(self, initialize_elasticsearch, auth_headers): # noqa
        with TestClient(app) as client:
            search_data = {"query": "cell", "facets_values": None, "return_facets_only": False}
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers).json()
            assert len(res) > 0

    def test_search_references_with_facets(self, initialize_elasticsearch, auth_headers): # noqa
        with TestClient(app) as client:
            search_data = {
                "query": "cell",
                "facets_values": {
                    "pubmed_types.keyword": ["Journal Article", "Review"]
                },
                "return_facets_only": False
            }
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers).json()
            assert "hits" in res
            assert "aggregations" in res

    def test_search_result_count(self, initialize_elasticsearch, auth_headers): # noqa
        with TestClient(app) as client:
            search_data = {
                "query": "superlongword super super super super test test test",
                "facets_values": {
                    "pubmed_types.keyword": ["Journal Article", "Review"]
                },
                "return_facets_only": False
            }
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers).json()
            assert "return_count" in res
            assert res["return_count"] == 1

    def test_search_max_results(self, initialize_elasticsearch, auth_headers): # noqa
        with TestClient(app) as client:
            test_size = 2
            search_data = {
                "query": None,
                "facets_values": {
                    "pubmed_types.keyword": ["Book"]
                },
                "return_facets_only": False,
                "size_result_count": test_size
            }
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers).json()
            assert "hits" in res
            assert len(res["hits"]) == test_size

    def test_search_references_facets_limits(self, initialize_elasticsearch, auth_headers): # noqa
        with TestClient(app) as client:
            search_data = {
                "return_facets_only": True,
                "facets_limits": {"pubmed_types.keyword": 15}
            }
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers).json()
            assert len(res["aggregations"]["pubmed_types.keyword"]["buckets"]) > 10
            res = client.post(url="/search/references/", json={"return_facets_only": True}, headers=auth_headers).json()
            assert len(res["aggregations"]["pubmed_types.keyword"]["buckets"]) == 10

    def test_search_references_empty(self, initialize_elasticsearch, auth_headers): # noqa
        with TestClient(app) as client:
            search_data = {
                "query": None,
                "return_facets_only": None,
                "facets_values": None
            }
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers)
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_search_references_wildcard(self, initialize_elasticsearch, auth_headers): # noqa
        with TestClient(app) as client:
            search_data = {
                "query": "boo*",
                "return_facets_only": False,
                "facets_values": None
            }
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers).json()
            assert len(res["hits"]) == 2

    def test_search_need_review(self, test_mca): # noqa
        with TestClient(app) as client:
            res = client.get(url="/search/need_review", params={"mod_abbreviation": "0015_AtDB", "count": 10})
            assert res.status_code == status.HTTP_200_OK
            assert len(res.json()) > 0
