import pytest

from elasticsearch import Elasticsearch
from agr_literature_service.api.crud.search_crud import search_references
from fastapi import HTTPException
from agr_literature_service.api.config import config


@pytest.fixture(scope='module')
def initialize_elasticsearch():
    print("***** Initializing Elasticsearch Data *****")
    es = Elasticsearch(hosts=config.ELASTICSEARCH_HOST + ":" + config.ELASTICSEARCH_PORT)
    doc1 = {
        "curie": "AGR:AGR-Reference-0000000001",
        "title": "test title",
        "pubmed_types": ["Journal Article", "Review"],
        "abstract": "Really quite a lot of great information in this article",
        "date_published": "1901",
        "authors" : [{"name": "John Q Public", "orcid": "null"}, {"name": "Socrates", "orcid": "null"}],
        "cross_references": [{"curie": "FB:FBrf0000001", "is_obsolete": "false"},{"curie": "FB:FBrf0000002", "is_obsolete": "true"}]
    }
    doc2 = {
        "curie": "AGR:AGR-Reference-0000000002",
        "title": "cell title",
        "pubmed_types": ["Book"],
        "abstract": "Its really worth reading this article",
        "date_published": "2022",
        "authors" : [{"name": "Jane Doe", "orcid": "null"}],
        "cross_references": [{"curie": "PMID:0000001", "is_obsolete": "false"}]
    }
    doc3 = {
        "curie": "AGR:AGR-Reference-0000000003",
        "title": "Book 1",
        "pubmed_types": ["Book", "Abstract", "Category1", "Category2", "Category3"],
        "abstract": "A book written about science",
        "date_published": "1950-06-03",
        "authors" : [{"name": "Sam", "orcid": "null"}, {"name": "Plato", "orcid": "null"}],
        "cross_references": [{"curie": "FB:FBrf0000001", "is_obsolete": "false"},{"curie": "SGD:S000000123", "is_obsolete": "true"}]
    }
    doc4 = {
        "curie": "AGR:AGR-Reference-0000000004",
        "title": "Book 2",
        "pubmed_types": ["Book", "Category4", "Test", "category5", "Category6", "Category7"],
        "abstract": "The other book written about science",
        "date_published": "2010",
        "authors" : [{"name": "Euphrates", "orcid": "null"}, {"name": "Aristotle", "orcid": "null"}],
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

    def test_search_result_count(self, initialize_elasticsearch):
        facets_values = {
            "pubmed_types.keyword": ["Journal Article", "Review"]
        }
        res = search_references(query="test", facets_values=facets_values, return_facets_only=False)
        assert "return_count" in res
        assert res["return_count"] == 1

    def test_search_max_results(self, initialize_elasticsearch):
        facets_values = {
            "pubmed_types.keyword": ["Book"]
        }
        test_size = 2
        res = search_references(query=None, facets_values=facets_values, return_facets_only=False, size_result_count=test_size)
        assert "hits" in res
        assert len(res["hits"]) == test_size

    def test_search_references_facets_limits(self, initialize_elasticsearch):
        res = search_references(return_facets_only=True, facets_limits={"pubmed_types.keyword": 15})
        assert len(res["aggregations"]["pubmed_types.keyword"]["buckets"]) > 10
        res = search_references(return_facets_only=True)
        assert len(res["aggregations"]["pubmed_types.keyword"]["buckets"]) == 10

    def test_search_references_empty(self, initialize_elasticsearch):
        with pytest.raises(HTTPException):
            search_references(query=None, facets_values=None, return_facets_only=False)
