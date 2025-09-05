import pytest
from datetime import datetime

from elasticsearch import Elasticsearch
from starlette.testclient import TestClient
from unittest.mock import patch

from fastapi import status
from agr_literature_service.api.config import config
from agr_literature_service.api.main import app
from .test_mod_corpus_association import test_mca # noqa
from ..fixtures import db # noqa
from .test_reference import test_reference # noqa
from .test_mod import test_mod # noqa
from .fixtures import auth_headers # noqa


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
        msg = "**** Warning: not allow to run test on stage or prod elasticsearch index *****"
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
                    },
                    "autocompleteAnalyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase"]
                    },
                    "autocompleteSearchAnalyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase"]
                    }
                },
                "normalizer": {
                    "languageNormalizer": {
                        "type": "custom",
                        "filter": ["lowercase"]
                    },
                    "sortNormalizer": {
                        "type": "custom",
                        "filter": ["lowercase"]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "authors": {
                    "type": "nested",
                    "properties": {
                        "name": {
                            "type": "text",
                            "analyzer": "authorNameAnalyzer",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "normalizer": "sortNormalizer",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "orcid": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "normalizer": "sortNormalizer",
                                    "ignore_above": 256
                                }
                            }
                        }
                    }
                },
                "workflow_tags": {
                    "type": "nested",
                    "properties": {
                        "workflow_tag_id": {
                            "type": "text",
                            "analyzer": "autocompleteAnalyzer",
                            "search_analyzer": "autocompleteSearchAnalyzer",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "normalizer": "languageNormalizer",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "mod_abbreviation": {
                            "type": "keyword",
                            "normalizer": "sortNormalizer"
                        }
                    }
                },
                "language": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "normalizer": "languageNormalizer"
                        }
                    }
                },
                # --- add these explicit mappings ---
                # You store epoch microseconds as numbers; map to long for sort/range
                "date_created": {"type": "long"},
                # Allow either ISO dates (yyyy-MM-dd) or epoch seconds/millis
                "date_published_start": {
                    "type": "date",
                    "format": "strict_date_optional_time||yyyy-MM-dd||epoch_millis||epoch_second"
                },
                "date_published_end": {
                    "type": "date",
                    "format": "strict_date_optional_time||yyyy-MM-dd||epoch_millis||epoch_second"
                },
                "date_arrived_in_pubmed": {
                    "type": "date",
                    "format": "strict_date_optional_time||yyyy-MM-dd"
                },
                "date_last_modified_in_pubmed": {
                    "type": "date",
                    "format": "strict_date_optional_time||yyyy-MM-dd"
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
        "cross_references": [{"curie": "FB:FBrf0000001", "is_obsolete": "false"}, {"curie": "FB:FBrf0000002", "is_obsolete": "true"}],
        "workflow_tags": [{"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}],
        "mod_reference_types": ["review"],
        "language" : "English",
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
        "workflow_tags": [{"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}],
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
        "cross_references": [{"curie": "FB:FBrf0000001", "is_obsolete": "false"}, {"curie": "SGD:S000000123", "is_obsolete": "true"}],
        "workflow_tags": [{"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}],
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
        "workflow_tags": [{"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}],
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

    def test_search_on_abstract(self, initialize_elasticsearch, auth_headers): # noqa
        with TestClient(app) as client:
            search_data = {
                "query": "really",
                "return_facets_only": False,
                "facets_values": None,
                "query_field": "abstract"
            }
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers).json()
            assert len(res["hits"]) == 2

    def test_search_sort(self, initialize_elasticsearch, auth_headers): # noqa
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
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers).json()
            assert "hits" in res
            assert res["hits"][0]['date_published'] == "1901"

    def test_search_query_field_author(self, initialize_elasticsearch, auth_headers):  # noqa
        with TestClient(app) as client:
            payload = {
                "query": "Jane Doe",
                "query_field": "Author",
                "return_facets_only": False
            }
            res = client.post("/search/references/", json=payload, headers=auth_headers).json()
            assert res["return_count"] >= 1
            curies = {h["curie"] for h in res["hits"]}
            assert "AGRKB:101000000000002" in curies  # Jane Doe

    def test_search_query_field_orcid_valid_and_invalid(self, initialize_elasticsearch, auth_headers):  # noqa
        with TestClient(app) as client:
            # valid ORCID → one of our docs (Socrates)
            ok = {
                "query": "ORCID:0000-0000-0000-0001",
                "query_field": "ORCID",
                "return_facets_only": False
            }
            res_ok = client.post("/search/references/", json=ok, headers=auth_headers).json()
            assert res_ok["return_count"] == 1
            assert res_ok["hits"][0]["curie"] == "AGRKB:101000000000001"

            # invalid/non-existent ORCID → zero
            bad = {
                "query": "ORCID:0000-0000-0000-9999",
                "query_field": "ORCID",
                "return_facets_only": False
            }
            res_bad = client.post("/search/references/", json=bad, headers=auth_headers).json()
            assert res_bad["return_count"] == 0

    def test_search_query_field_curie(self, initialize_elasticsearch, auth_headers):  # noqa
        with TestClient(app) as client:
            payload = {
                "query": "AGRKB:101000000000003",
                "query_field": "Curie",
                "return_facets_only": False
            }
            res = client.post("/search/references/", json=payload, headers=auth_headers).json()
            assert res["return_count"] >= 1
            curies = {h["curie"] for h in res["hits"]}
            assert "AGRKB:101000000000003" in curies

    def test_search_query_field_xref(self, initialize_elasticsearch, auth_headers):  # noqa
        with TestClient(app) as client:
            payload = {
                "query": "PMID:0000001",
                "query_field": "Xref",
                "return_facets_only": False
            }
            res = client.post("/search/references/", json=payload, headers=auth_headers).json()
            assert res["return_count"] == 1
            assert res["hits"][0]["curie"] == "AGRKB:101000000000002"

    def test_search_with_author_filter_only(self, initialize_elasticsearch, auth_headers):  # noqa
        with TestClient(app) as client:
            payload = {
                "query": None,
                "author_filter": "Socrates",  # nested author filter path
                "return_facets_only": False
            }
            res = client.post("/search/references/", json=payload, headers=auth_headers).json()
            assert res["return_count"] >= 1
            curies = {h["curie"] for h in res["hits"]}
            assert "AGRKB:101000000000001" in curies

    def test_search_date_filters_published_and_created(self, initialize_elasticsearch, auth_headers):  # noqa
        with TestClient(app) as client:
            payload = {
                "query": None,
                "return_facets_only": False,
                "date_published": ["2021-10-01", "2021-12-01"],
                "date_created": ["2021-11-05T00:00:00.000", "2021-11-05T23:59:00.000"],
            }
            res = client.post("/search/references/", json=payload, headers=auth_headers).json()
            # all 4 seeded docs fall in these ranges
            assert res["return_count"] == 4

    def test_search_negated_facets_exclude_paper(self, initialize_elasticsearch, auth_headers):  # noqa
        with TestClient(app) as client:
            payload = {
                "query": None,
                "return_facets_only": False,
                "negated_facets_values": {"mod_reference_types.keyword": ["paper"]}
            }
            res = client.post("/search/references/", json=payload, headers=auth_headers).json()
            curies = {h["curie"] for h in res["hits"]}
            assert "AGRKB:101000000000004" not in curies  # doc4 is 'paper'
            # still returns others
            assert "AGRKB:101000000000001" in curies
            assert "AGRKB:101000000000002" in curies
            assert "AGRKB:101000000000003" in curies

    def test_search_workflow_facet_filter(self, initialize_elasticsearch, auth_headers):  # noqa
        with TestClient(app) as client:
            payload = {
                "query": None,
                "return_facets_only": False,
                "facets_values": {"file_workflow": ["ATP:0000196"]},
            }
            res = client.post("/search/references/", json=payload, headers=auth_headers).json()
            # all 4 seeded docs carry ATP:0000196 for mod FB
            assert res["return_count"] == 4

    def test_search_free_text_orcid_in_all_fields(self, initialize_elasticsearch, auth_headers):  # noqa
        with TestClient(app) as client:
            payload = {
                "query": "ORCID:0000-0000-0000-0002",  # Jane Doe
                "return_facets_only": False
            }
            res = client.post("/search/references/", json=payload, headers=auth_headers).json()
            curies = {h["curie"] for h in res["hits"]}
            assert "AGRKB:101000000000002" in curies
