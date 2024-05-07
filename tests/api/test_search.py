import pytest
from unittest.mock import patch
from starlette.testclient import TestClient

from fastapi import status
# from agr_literature_service.api.config import config
from agr_literature_service.api.main import app
from .test_mod_corpus_association import test_mca # noqa
from ..fixtures import db # noqa
from .test_reference import test_reference # noqa
from .test_mod import test_mod # noqa
from .fixtures import auth_headers # noqa


@pytest.fixture(scope='module')
def mock_elasticsearch():
    with patch('elasticsearch.Elasticsearch') as mock:
        # mocking index existence
        mock.return_value.indices.exists.return_value = True
        yield mock


class TestSearch:

    def test_search_references_return_facets_only(self, mock_elasticsearch, auth_headers): # noqa
        mock_es_instance = mock_elasticsearch.return_value
        mock_es_instance.search.return_value = {
            "aggregations": {
                "pubmed_types.keyword": {
                    "buckets": [{"key": "Journal Article", "doc_count": 10},
                                {"key": "Review", "doc_count": 5}]
                }
            }
        }

        with TestClient(app) as client:
            facets_values = {"pubmed_types.keyword": ["Journal Article", "Review"]}
            search_data = {"query": None, "facets_values": facets_values, "return_facets_only": True}
            response = client.post("/search/references/", json=search_data, headers=auth_headers)
            res = response.json()

            assert "aggregations" in res
            assert len(res["aggregations"]["pubmed_types.keyword"]["buckets"]) == 2


    def test_search_references_no_facets(self, mock_elasticsearch, auth_headers): # noqa
        mock_es_instance = mock_elasticsearch.return_value
        mock_es_instance.search.return_value = {
            "hits": {
                "total": {"value": 1, "relation": "eq"},
                "hits": [{"_source": {"title": "cell title"}}]
            }
        }

        with TestClient(app) as client:
            search_data = {"query": "cell", "facets_values": None, "return_facets_only": False}
            response = client.post("/search/references/", json=search_data, headers=auth_headers)
            res = response.json()
        assert "hits" in res
        assert len(res["hits"]) > 0

    def test_search_references_with_facets(self, mock_elasticsearch, auth_headers): # noqa
        mock_es_instance = mock_elasticsearch.return_value
        mock_es_instance.search.return_value = {
            "hits": {
                "total": {"value": 1, "relation": "eq"},
                "hits": [{"_source": {"title": "cell title", "pubmed_types": ["Journal Article"]}}]
            },
            "aggregations": {
                "pubmed_types.keyword": {
                    "buckets": [{"key": "Journal Article", "doc_count": 1}]
                }
            }
        }

        with TestClient(app) as client:
            search_data = {
                "query": "cell",
                "facets_values": {"pubmed_types.keyword": ["Journal Article", "Review"]},
                "return_facets_only": False
            }
            res = client.post("/search/references/", json=search_data, headers=auth_headers).json()

            assert "hits" in res
            assert "aggregations" in res
            assert len(res["hits"]) == 1

    def test_search_result_count(self, mock_elasticsearch, auth_headers): # noqa
        mock_es_instance = mock_elasticsearch.return_value
        mock_es_instance.search.return_value = {
            "hits": {
                "total": {"value": 1, "relation": "eq"},
                "hits": [{"_source": {"title": "superlongword super super super super test test test"}}]
            }
        }

        with TestClient(app) as client:
            search_data = {
                "query": "superlongword super super super super test test test",
                "facets_values": {"pubmed_types.keyword": ["Journal Article", "Review"]},
                "return_facets_only": False
            }
            res = client.post("/search/references/", json=search_data, headers=auth_headers).json()

            assert "hits" in res
            assert len(res["hits"]) == 1
            assert res["hits"][0]["_source"]["title"] == "superlongword super super super super test test test"


    def test_search_max_results(self, mock_elasticsearch, auth_headers): # noqa
        mock_es_instance = mock_elasticsearch.return_value
        mock_es_instance.search.return_value = {
            "hits": {
                "total": {"value": 2, "relation": "eq"},
                "hits": [
                    {"_source": {"title": "Book 1", "pubmed_types": ["Book"]}},
                    {"_source": {"title": "Book 2", "pubmed_types": ["Book"]}}
                ]
            }
        }

        with TestClient(app) as client:
            search_data = {
                "query": None,
                "facets_values": {"pubmed_types.keyword": ["Book"]},
                "return_facets_only": False,
                "size_result_count": 2
            }
            res = client.post("/search/references/", json=search_data, headers=auth_headers).json()

            assert "hits" in res
            assert len(res["hits"]) == 2

    def test_search_references_facets_limits(self, mock_elasticsearch, auth_headers): # noqa
        mock_es_instance = mock_elasticsearch.return_value
        mock_es_instance.search.side_effect = [
            {
                "aggregations": {
                    "pubmed_types.keyword": {
                        "buckets": [{"key": "Journal Article", "doc_count": 12}]
                    }
                }
            },
            {
                "aggregations": {
                    "pubmed_types.keyword": {
                        "buckets": [{"key": "Journal Article", "doc_count": 8}]
                    }
                }
            }
        ]

        with TestClient(app) as client:
            search_data = {
                "return_facets_only": True,
                "facets_limits": {"pubmed_types.keyword": 15}
            }
            res = client.post("/search/references/", json=search_data, headers=auth_headers).json()
            assert len(res["aggregations"]["pubmed_types.keyword"]["buckets"]) == 1
            assert res["aggregations"]["pubmed_types.keyword"]["buckets"][0]["doc_count"] == 12

            # Test default limits
            search_data = {"return_facets_only": True}
            res = client.post("/search/references/", json=search_data, headers=auth_headers).json()
            assert len(res["aggregations"]["pubmed_types.keyword"]["buckets"]) == 1
            assert res["aggregations"]["pubmed_types.keyword"]["buckets"][0]["doc_count"] == 8

    def test_search_references_empty(self, mock_elasticsearch, auth_headers): # noqa
        mock_es_instance = mock_elasticsearch.return_value
        mock_es_instance.search.return_value = {
            "hits": {
                "total": {"value": 0, "relation": "eq"},
                "hits": []
            }
        }

        with TestClient(app) as client:
            search_data = {
                "query": None,
                "return_facets_only": None,
                "facets_values": None
            }
            response = client.post("/search/references/", json=search_data, headers=auth_headers)

            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_search_references_wildcard(self, mock_elasticsearch, auth_headers): # noqa
        mock_es_instance = mock_elasticsearch.return_value
        mock_es_instance.search.return_value = {
            "hits": {
                "total": {"value": 2, "relation": "eq"},
                "hits": [
                    {"_source": {"title": "Book 1", "pubmed_types": ["Book"]}},
                    {"_source": {"title": "Book 2", "pubmed_types": ["Book"]}}
                ]
            }
        }

        with TestClient(app) as client:
            search_data = {
                "query": "boo*",
                "return_facets_only": False,
                "facets_values": None
            }
            res = client.post("/search/references/", json=search_data, headers=auth_headers).json()
            assert len(res["hits"]) == 2


    def test_search_on_abstract(self, mock_elasticsearch, auth_headers): # noqa
        mock_es_instance = mock_elasticsearch.return_value
        mock_es_instance.search.return_value = {
            "hits": {
                "total": {"value": 2, "relation": "eq"},
                "hits": [
                    {"_source": {"abstract": "Really quite a lot of great information in this article"}},
                    {"_source": {"abstract": "Its really worth reading this article"}}
                ]
            }
        }

        with TestClient(app) as client:
            search_data = {
                "query": "really",
                "return_facets_only": False,
                "facets_values": None,
                "query_field": "abstract"
            }
            res = client.post("/search/references/", json=search_data, headers=auth_headers).json()
            assert len(res["hits"]) == 2

    def test_search_sort(self, mock_elasticsearch, auth_headers): # noqa
        mock_es_instance = mock_elasticsearch.return_value
        mock_es_instance.search.return_value = {
            "hits": {
                "total": {"value": 1, "relation": "eq"},
                "hits": [{"_source": {"date_published": "1901"}}]
            }
        }

        with TestClient(app) as client:
            search_data = {
                "query": "",
                "sort": [
                    {
                        "date_published.keyword": {
                            "order": "asc"
                        }
                    }
                ]
            }
            res = client.post("/search/references/", json=search_data, headers=auth_headers).json()
            assert "hits" in res
            assert res["hits"][0]["_source"]["date_published"] == "1901"
