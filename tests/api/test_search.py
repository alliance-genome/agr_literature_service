import pytest
from datetime import datetime

from elasticsearch import Elasticsearch
from starlette.testclient import TestClient
from unittest.mock import patch

from fastapi import status
from agr_literature_service.api.config import config
from agr_literature_service.api.main import app
from .test_mod_corpus_association import test_mca  # noqa
from ..fixtures import db  # noqa
from .test_reference import test_reference  # noqa
from .test_mod import test_mod  # noqa
# from .fixtures import auth_headers  # noqa


@pytest.fixture(scope="module", autouse=True)
def patch_get_map_ateam_curies_to_names():
    with patch("agr_literature_service.api.crud.search_crud.get_map_ateam_curies_to_names") as mock_func:
        mock_func.return_value = {'ATP:0000196': 'antibody extraction complete'}
        yield


@pytest.fixture(scope='module')
def setup_elasticsearch():
    es = Elasticsearch()
    es.indices.create(index='test_ref_index', ignore=400)
    yield
    es.indices.delete(index='test_ref_index', ignore=[400, 404])


@pytest.fixture(scope='module')
def initialize_elasticsearch():
    print("***** Initializing Elasticsearch Data *****")
    if ("es.amazonaws.com" in config.ELASTICSEARCH_HOST):
        msg = "**** Warning: not allowed to run test on stage or prod elasticsearch index *****"
        pytest.exit(msg)
    es = Elasticsearch(hosts=config.ELASTICSEARCH_HOST + ":" + config.ELASTICSEARCH_PORT)

    # delete the index if it exists
    if es.indices.exists(index=config.ELASTICSEARCH_INDEX):
        es.indices.delete(index=config.ELASTICSEARCH_INDEX)

    # Create the index with analyzer settings
    index_settings = {
        "settings": {
            "analysis": {
                "analyzer": {
                    "authorNameAnalyzer": {
                        "type": "custom",
                        "tokenizer": "whitespace",
                        "filter": ["asciifolding", "lowercase"]
                    }
                }
            }
        }
    }
    es.indices.create(index=config.ELASTICSEARCH_INDEX, body=index_settings)

    doc1 = {
        "curie": "AGRKB:101000000000001",
        "citation": "citation1",
        "title": "superlongword super super super super test test test",
        "pubmed_types": ["Journal Article", "Review"],
        "abstract": "Really quite a lot of great information in this article",
        "date_published": "1901",
        "date_published_start": datetime.strptime('10/10/2021', '%m/%d/%Y').timestamp(),
        "date_published_end": datetime.strptime('11/10/2021', '%m/%d/%Y').timestamp(),
        "authors": [
            {"name": "John Q Public", "orcid": "0000-0000-0000-0000"},
            {"name": "Socrates", "orcid": "0000-0000-0000-0001"}
        ],
        "cross_references": [
            {"curie": "FB:FBrf0000001", "is_obsolete": "false"},
            {"curie": "FB:FBrf0000002", "is_obsolete": "true"}
        ],
        "workflow_tags": [
            {"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}
        ],
        "mod_reference_types": ["review"],
        "language": "English",
        "date_created": "1636139454923830"
    }
    doc2 = {
        "curie": "AGRKB:101000000000002",
        "citation": "citation2",
        "title": "cell title",
        "pubmed_types": ["Book"],
        "abstract": "Its really worth reading this article",
        "date_published_start": datetime.strptime('10/10/2021', '%m/%d/%Y').timestamp(),
        "date_published_end": datetime.strptime('11/10/2021', '%m/%d/%Y').timestamp(),
        "date_published": "2022",
        "authors": [{"name": "Jane Doe", "orcid": "0000-0000-0000-0002"}],
        "cross_references": [{"curie": "PMID:0000001", "is_obsolete": "false"}],
        "workflow_tags": [
            {"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}
        ],
        "mod_reference_types": ["note"],
        "language": "English",
        "date_created": "1636139454923830"
    }
    doc3 = {
        "curie": "AGRKB:101000000000003",
        "citation": "citation3",
        "title": "Book 1",
        "pubmed_types": ["Book", "Abstract", "Category1", "Category2", "Category3"],
        "abstract": "A book written about science",
        "date_published": "1950-06-03",
        "date_published_start": datetime.strptime('10/10/2021', '%m/%d/%Y').timestamp(),
        "date_published_end": datetime.strptime('11/10/2021', '%m/%d/%Y').timestamp(),
        "authors": [{"name": "Sam", "orcid": "null"}, {"name": "Plato", "orcid": "null"}],
        "cross_references": [
            {"curie": "FB:FBrf0000001", "is_obsolete": "false"},
            {"curie": "SGD:S000000123", "is_obsolete": "true"}
        ],
        "workflow_tags": [
            {"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}
        ],
        "mod_reference_types": ["Journal"],
        "language": "English",
        "date_created": "1636139454923830"
    }
    doc4 = {
        "curie": "AGRKB:101000000000004",
        "citation": "citation4",
        "title": "Book 2",
        "pubmed_types": ["Book", "Category4", "Test", "category5", "Category6", "Category7"],
        "abstract": "The other book written about science",
        "date_published": "2010",
        "date_published_start": datetime.strptime('10/10/2021', '%m/%d/%Y').timestamp(),
        "date_published_end": datetime.strptime('11/10/2021', '%m/%d/%Y').timestamp(),
        "authors": [{"name": "Euphrates", "orcid": "null"}, {"name": "Aristotle", "orcid": "null"}],
        "cross_references": [{"curie": "MGI:12345", "is_obsolete": "false"}],
        "workflow_tags": [
            {"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}
        ],
        "mod_reference_types": ["paper"],
        "language": "English",
        "date_created": "1636139454923830"
    }
    es.index(index=config.ELASTICSEARCH_INDEX, id=1, body=doc1)
    es.index(index=config.ELASTICSEARCH_INDEX, id=2, body=doc2)
    es.index(index=config.ELASTICSEARCH_INDEX, id=3, body=doc3)
    es.index(index=config.ELASTICSEARCH_INDEX, id=4, body=doc4)
    es.indices.refresh(index=config.ELASTICSEARCH_INDEX)
    yield None
    print("***** Cleaning Up Elasticsearch Data *****")
    es.indices.delete(index=config.ELASTICSEARCH_INDEX)
    print("deleted test index")


class TestSearch:

    def test_search_references_return_facets_only(self, initialize_elasticsearch, auth_headers):
        with TestClient(app) as client:
            facets_values = {
                "pubmed_types.keyword": (["Journal Article", "Review"])
            }
            search_data = {
                "query": None,
                "facets_values": facets_values,
                "return_facets_only": True
            }
            response = client.post(url="/search/references/", json=search_data, headers=auth_headers)
            res = response.json()
            assert "aggregations" in res
            assert "pubmed_types.keyword" in res["aggregations"]
            assert len(res["aggregations"]["pubmed_types.keyword"]["buckets"]) > 0

    def test_search_references_no_facets(self, initialize_elasticsearch, auth_headers):
        with TestClient(app) as client:
            search_data = {"query": "cell", "facets_values": None, "return_facets_only": False}
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers).json()
            assert len(res) > 0

    def test_search_references_with_facets(self, initialize_elasticsearch, auth_headers):
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

    def test_search_result_count(self, initialize_elasticsearch, auth_headers):
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

    def test_search_max_results(self, initialize_elasticsearch, auth_headers):
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

    def test_search_references_facets_limits(self, initialize_elasticsearch, auth_headers):
        with TestClient(app) as client:
            search_data = {
                "return_facets_only": True,
                "facets_limits": {"pubmed_types.keyword": 15}
            }
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers).json()
            assert len(res["aggregations"]["pubmed_types.keyword"]["buckets"]) > 10
            # now test default of 10 if not specified
            res = client.post(url="/search/references/", json={"return_facets_only": True}, headers=auth_headers).json()
            assert len(res["aggregations"]["pubmed_types.keyword"]["buckets"]) == 10

    def test_search_references_empty(self, initialize_elasticsearch, auth_headers):
        with TestClient(app) as client:
            # No 'query' or 'facets_values' or 'return_facets_only' => 422
            search_data = {
                "query": None,
                "return_facets_only": None,
                "facets_values": None
            }
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers)
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_search_references_wildcard(self, initialize_elasticsearch, auth_headers):
        with TestClient(app) as client:
            search_data = {
                "query": "boo*",
                "return_facets_only": False,
                "facets_values": None
            }
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers).json()
            # Book 1 and Book 2 match "boo*" on title
            assert len(res["hits"]) == 2

    def test_search_on_abstract(self, initialize_elasticsearch, auth_headers):
        with TestClient(app) as client:
            search_data = {
                "query": "really",
                "return_facets_only": False,
                "facets_values": None,
                "query_field": "abstract"
            }
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers).json()
            # Should match doc1 and doc2, both have "really"
            assert len(res["hits"]) == 2

    def test_search_sort(self, initialize_elasticsearch, auth_headers):
        with TestClient(app) as client:
            # Sort ascending by date_published
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
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers).json()
            assert "hits" in res
            # The earliest date_published in the sample docs is "1901"
            assert res["hits"][0]['date_published'] == "1901"
