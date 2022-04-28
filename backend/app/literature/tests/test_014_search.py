import pytest

from elasticsearch import Elasticsearch
from literature.crud.search_crud import search_references
from fastapi import HTTPException
from literature.config import config


@pytest.fixture(scope='module')
def initialize_elasticsearch():
    print("***** Initializing Elasticsearch Data *****")
    es = Elasticsearch(hosts=config.ELASTICSEARCH_HOST + ":" + config.ELASTICSEARCH_PORT)
    doc1 = {
        "curie": "AGR:AGR-Reference-0000000001",
        "title": "test title",
        "pubmed_types": ["Journal Article", "Review"]
    }
    doc2 = {
        "curie": "AGR:AGR-Reference-0000000002",
        "title": "cell title",
        "pubmed_types": ["Book"]
    }
    doc3 = {
        "curie": "AGR:AGR-Reference-0000000003",
        "title": "Book 1",
        "pubmed_types": ["Book", "Abstract", "Category1", "Category2", "Category3"]
    }
    doc4 = {
        "curie": "AGR:AGR-Reference-0000000004",
        "title": "Book 2",
        "pubmed_types": ["Book", "Category4", "Test", "category5", "Category6", "Category7"]
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

    def test_search_references_return_facets_only(self, initialize_elasticsearch):
        facets_values = {
            "pubmed_types.keyword": (["Journal Article", "Review"])
        }
        res = search_references(query=None, facets_values=facets_values, return_facets_only=True)
        assert "aggregations" in res and "pubmed_types.keyword" in res["aggregations"]
        assert len(res["aggregations"]["pubmed_types.keyword"]["buckets"]) > 0

    def test_search_references_no_facets(self, initialize_elasticsearch):
        res = search_references(query="cell", facets_values=None, return_facets_only=False)
        assert len(res) > 0

    def test_search_references_with_facets(self, initialize_elasticsearch):
        facets_values = {
            "pubmed_types.keyword": ["Journal Article", "Review"]
        }
        res = search_references(query="cell", facets_values=facets_values, return_facets_only=False)
        assert "hits" in res
        assert "aggregations" in res

    def test_search_references_facets_limits(self, initialize_elasticsearch):
        res = search_references(return_facets_only=True, facets_limits={"pubmed_types.keyword": 15})
        assert len(res["aggregations"]["pubmed_types.keyword"]["buckets"]) > 10
        res = search_references(return_facets_only=True)
        assert len(res["aggregations"]["pubmed_types.keyword"]["buckets"]) == 10

    def test_search_references_empty(self, initialize_elasticsearch):
        with pytest.raises(HTTPException):
            search_references(query=None, facets_values=None, return_facets_only=False)
