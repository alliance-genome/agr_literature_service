import pytest
import datetime

from elasticsearch import Elasticsearch
from starlette.testclient import TestClient
from unittest.mock import patch

from agr_literature_service.api.config import config
from agr_literature_service.api.main import app


from .test_mod_corpus_association import test_mca # noqa
from ..fixtures import db # noqa
from .test_reference import test_reference # noqa
from .test_mod import test_mod # noqa
from .fixtures import auth_headers # noqa

#########################################################################
# PS, PE published start and end dates.
# SS, SE selected query start and end dates.
#
#             PS------------------PE
#
# 1)       SS----SE
# 2)                           SS---------SE
# 3)                SS------SE
# 4)       SS--------------------------SE (But is covered by 1 and 2 already)
# These are the 4 cases of overlaps.
#
# Need to check date SE before PS and SS after PE for NO results.
# lets call these case 5 (before) and 6 (after).
#########################################################################


@pytest.fixture(scope="module", autouse=True)
def patch_get_map_ateam_curies_to_names():
    with patch("agr_literature_service.api.crud.search_crud.get_map_ateam_curies_to_names") as mock_func:
        mock_func.return_value = {'ATP:0000196': 'antibody extraction complete'}
        yield


@pytest.fixture(scope='module')
def initialize_elasticsearch():
    print("***** Initializing Elasticsearch Data *****")
    if ("es.amazonaws.com" in config.ELASTICSEARCH_HOST):
        msg = "**** Warning: not allow to run test on stage or prod elasticsearch index *****"
        pytest.exit(msg)
    es = Elasticsearch(hosts=config.ELASTICSEARCH_HOST + ":" + config.ELASTICSEARCH_PORT)
    
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
                }
            }
        }
    }
    es.indices.create(index=config.ELASTICSEARCH_INDEX, body=index_settings)
        
    date1 = '2022-01-01'
    date2 = '2022-03-28'
    date3 = '2022-09-27'
    date4 = '2022-09-28'
    date5 = '2022-12-31'
    # consecutive days
    date6 = '2019-08-31'
    date7 = '2019-09-01'
    date8 = '2019-09-02'

    doc1 = {
        "curie": "AGRKB:101000000000100",
        "citation": "citation1",
        "title": "superlongword super super super super test test test",
        "pubmed_types": ["Journal Article", "Review"],
        "abstract": "Really quite a lot of great information in this article",
        "date_published": date1,
        "date_arrived_in_pubmed": date1,
        "date_published_start": date1,  # full year of 2022
        "date_published_end": date5,
        "authors": [{"name": "John Q Public", "orcid": "null"}, {"name": "Socrates", "orcid": "null"}],
        "cross_references": [{"curie": "FB:FBrf0000001", "is_obsolete": "false"}, {"curie": "FB:FBrf0000002", "is_obsolete": "true"}],
        "workflow_tags": [{"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}],
        "mod_reference_types": ["paper"],
        "language": "English",
        "date_created": "1636139454923830"
    }
    doc2 = {
        "curie": "AGRKB:101000000000200",
        "citation": "citation2",
        "title": "cell title",
        "pubmed_types": ["Book"],
        "abstract": "Its really worth reading this article",
        "date_published": date2,
        "date_arrived_in_pubmed": date2,
        "date_published_start": date1,  # One day 1st Jan 2022
        "date_published_end": date1,
        "authors": [{"name": "Jane Doe", "orcid": "null"}],
        "cross_references": [{"curie": "PMID:0000001", "is_obsolete": "false"}],
        "workflow_tags": [{"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}],
        "mod_reference_types": ["paper"],
        "language": "English",
        "date_created": "1636139454923830"
    }
    doc3 = {
        "curie": "AGRKB:101000000000300",
        "citation": "citation3",
        "title": "Book 3",
        "pubmed_types": ["Book", "Abstract", "Category1", "Category2", "Category3"],
        "abstract": "A book written about science",
        "date_published": date3,
        "date_arrived_in_pubmed": date3,
        "date_published_start": date1,  # jan 1st -> Mar 28th
        "date_published_end": date2,
        "authors": [{"name": "Sam", "orcid": "null"}, {"name": "Plato", "orcid": "null"}],
        "cross_references": [{"curie": "FB:FBrf0000001", "is_obsolete": "false"}, {"curie": "SGD:S000000123", "is_obsolete": "true"}],
        "workflow_tags": [{"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}],
        "mod_reference_types": ["note"],
        "language": "English",
        "date_created": "1636139454923830"
    }
    doc4 = {
        "curie": "AGRKB:101000000000400",
        "citation": "citation4",
        "title": "Book 4",
        "pubmed_types": ["Book", "Category4", "Test", "category5", "Category6", "Category7"],
        "abstract": "The other book written about science",
        "date_published_start": date1,  # full year
        "date_published_end": date5,
        "date_published": date4,
        "date_arrived_in_pubmed": date4,
        "authors": [{"name": "Euphrates", "orcid": "null"}, {"name": "Aristotle", "orcid": "null"}],
        "cross_references": [{"curie": "MGI:12345", "is_obsolete": "false"}],
        "workflow_tags": [{"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}],
        "mod_reference_types": ["paper"],
        "language": "English",
        "date_created": "1636139454923830"
    }
    doc5 = {
        "curie": "AGRKB:101000000000500",
        "citation": "citation5",
        "title": "Book 5",
        "pubmed_types": ["Book", "Category4", "Test", "category5", "Category6", "Category7"],
        "abstract": "The other book written about science",
        "date_published_start": date4,  # 28th Sep -> dec 31st
        "date_published_end": date5,
        "date_published": date5,
        "date_arrived_in_pubmed": date5,
        "authors": [{"name": "Euphrates", "orcid": "null"}, {"name": "Aristotle", "orcid": "null"}],
        "cross_references": [{"curie": "MGI:12345", "is_obsolete": "false"}],
        "workflow_tags": [{"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}],
        "mod_reference_types": ["Journal"],
        "language": "English",
        "date_created": "1636139454923830"
    }
    doc6 = {
        "curie": "AGRKB:101000000000600",
        "citation": "citation6",
        "title": "Book 6",
        "pubmed_types": ["Book", "Category4", "Test", "category5", "Category6", "Category7"],
        "abstract": "The other book written about science",
        "date_published_start": date3,  # 27th -> 28th Sept.
        "date_published_end": date4,
        "date_published": date2,
        "date_arrived_in_pubmed": date5,
        "authors": [{"name": "Euphrates", "orcid": "null"}, {"name": "Aristotle", "orcid": "null"}],
        "cross_references": [{"curie": "MGI:12345", "is_obsolete": "false"}],
        "workflow_tags": [{"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}],
        "mod_reference_types": ["paper"],
        "language": "English",
        "date_created": "1636139454923830"
    }
    doc7 = {
        "curie": "AGRKB:101000000000700",
        "citation": "citation7",
        "title": "Book 7",
        "pubmed_types": ["Book", "Category4", "Test", "category5", "Category6", "Category7"],
        "abstract": "The other book written about science",
        "date_published_start": date6,  # 31st aug
        "date_published_end": date6,
        "date_published": date6,
        "date_arrived_in_pubmed": date6,
        "authors": [{"name": "Euphrates", "orcid": "null"}, {"name": "Aristotle", "orcid": "null"}],
        "cross_references": [{"curie": "MGI:12345", "is_obsolete": "false"}],
        "workflow_tags": [{"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}],
        "mod_reference_types": ["paper"],
        "language": "English",
        "date_created": "1636139454923830"
    }
    doc8 = {
        "curie": "AGRKB:101000000000800",
        "citation": "citation8",
        "title": "Book 8",
        "pubmed_types": ["Book", "Category4", "Test", "category5", "Category6", "Category7"],
        "abstract": "The other book written about science",
        "date_published_start": date7,  # 1st sept
        "date_published_end": date7,
        "date_published": date7,
        "date_arrived_in_pubmed": date7,
        "authors": [{"name": "Euphrates", "orcid": "null"}, {"name": "Aristotle", "orcid": "null"}],
        "cross_references": [{"curie": "MGI:12345", "is_obsolete": "false"}],
        "workflow_tags": [{"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}],
        "mod_reference_types": ["paper"],
        "date_created": "1636139454923830"
    }
    doc9 = {
        "curie": "AGRKB:101000000000900",
        "citation": "citation9",
        "title": "Book 9",
        "pubmed_types": ["Book", "Category4", "Test", "category5", "Category6", "Category7"],
        "abstract": "The other book written about science",
        "date_published_start": date8,  # 2nd Aug
        "date_published_end": date8,
        "date_published": date8,
        "date_arrived_in_pubmed": date8,
        "authors": [{"name": "Euphrates", "orcid": "null"}, {"name": "Aristotle", "orcid": "null"}],
        "cross_references": [{"curie": "MGI:12345", "is_obsolete": "false"}],
        "workflow_tags": [{"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}],
        "mod_reference_types": ["paper"],
        "language": "English",
        "date_created": "1636139454923830"
    }

    es.index(index="references_index", id=1, body=doc1)
    es.index(index="references_index", id=2, body=doc2)
    es.index(index="references_index", id=3, body=doc3)
    es.index(index="references_index", id=4, body=doc4)
    es.index(index="references_index", id=5, body=doc5)
    es.index(index="references_index", id=6, body=doc6)
    es.index(index="references_index", id=7, body=doc7)
    es.index(index="references_index", id=8, body=doc8)
    es.index(index="references_index", id=9, body=doc9)
    es.indices.refresh(index="references_index")
    yield None
    print("***** Cleaning Up Elasticsearch Data *****")
    es.indices.delete(index="references_index")
    print("deleted references_index")


class TestSearch:

    def test_search_references_with_date_pubmed_arrive(self, initialize_elasticsearch, auth_headers): # noqa
        # search for pub date between 1st jan and march 28th
        # This should get 2 records date1 and date2 defined by "date_arrived_in_pubmed".
        #
        # date_pubmed_arrive[1] set to 2022-01-01T05:00:00.000Z and
        # date_pubmed_arrive[1] set to 2022-03-28T03:59:59.999Z as this is what the ui
        # would return.
        # So this checks the whole dates are used and ignores the time bit.
        with TestClient(app) as client:
            search_data = {
                "query": "",
                "facets_limits": {"pubmed_types.keyword": 10,
                                  "category.keyword": 10,
                                  "pubmed_publication_status.keyword": 10,
                                  "authors.name.keyword": 10},
                "author_filter": "",
                "query_fields": "All",
                "date_pubmed_arrive": ["2022-01-01",
                                       "2022-03-28"]
            }
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers).json()
            assert "hits" in res
            assert "aggregations" in res
            assert res["return_count"] == 2

    def test_search_references_case1(self, initialize_elasticsearch, auth_headers): # noqa
        # search for pub date between 1st jan and march 28th
        # This should get 4 records.
        #
        # date_published[0] set to 2022-01-01T05:00:00.000Z and
        # date_published[1] set to 2022-03-28T03:59:59.999Z as this is what the ui
        # would return.
        # So this checks the whole dates are used and ignores the time bit.
        with TestClient(app) as client:
            date1_str = '2022-01-01'
            date2_str = '2022-03-28'

            date1 = int(datetime.datetime.strptime(date1_str, '%Y-%m-%d').timestamp()) * 1000000
            date2 = int(datetime.datetime.strptime(date2_str, '%Y-%m-%d').timestamp()) * 1000000
            print(f"date1: {date1}")
            print(f"date2: {date2}")
            # print(f"date1 conv {date_str_to_micro_seconds('2022-01-01T', True)}")
            search_data = {
                "query": "",
                "facets_limits": {"pubmed_types.keyword": 10,
                                  "category.keyword": 10,
                                  "pubmed_publication_status.keyword": 10,
                                  "authors.name.keyword": 10},
                "author_filter": "",
                "query_fields": "All",
                "date_published": ["2022-01-01",
                                   "2022-03-28"]
            }
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers).json()
            print(datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo)
            print(res)
            assert "hits" in res
            assert "aggregations" in res
            assert res["return_count"] == 4

    def test_search_references_case2(self, initialize_elasticsearch, auth_headers): # noqa
        # Should just find 3 record where the end is overlapped.
        with TestClient(app) as client:
            search_data = {
                "query": "",
                "facets_limits": {"pubmed_types.keyword": 10,
                                  "category.keyword": 10,
                                  "pubmed_publication_status.keyword": 10,
                                  "authors.name.keyword": 10},
                "author_filter": "",
                "query_fields": "All",
                "date_published": ["2022-12-30T04:00:00.000Z",
                                   "2023-01-01T03:59:59.999Z"]
            }
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers).json()
            assert "hits" in res
            assert "aggregations" in res
            assert res["return_count"] == 3

    def test_search_references_case3(self, initialize_elasticsearch, auth_headers): # noqa
        # Should just find 3 record where the end is matched.
        with TestClient(app) as client:
            search_data = {
                "query": "",
                "facets_limits": {"pubmed_types.keyword": 10,
                                  "category.keyword": 10,
                                  "pubmed_publication_status.keyword": 10,
                                  "authors.name.keyword": 10},
                "author_filter": "",
                "query_fields": "All",
                "date_published": ["2022-09-25T04:00:00.000Z",
                                   "2022-09-29T03:59:59.999Z"]
            }
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers).json()
            assert "hits" in res
            assert "aggregations" in res
            assert res["return_count"] == 3

    def test_search_references_case4(self, initialize_elasticsearch, auth_headers): # noqa
        # Should just find all first 6 records.
        with TestClient(app) as client:
            search_data = {
                "query": "",
                "facets_limits": {"pubmed_types.keyword": 10,
                                  "category.keyword": 10,
                                  "pubmed_publication_status.keyword": 10,
                                  "authors.name.keyword": 10},
                "author_filter": "",
                "query_fields": "All",
                "date_published": ["2020-01-01T04:00:00.000Z",
                                   "2023-01-29T03:59:59.999Z"]
            }
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers).json()
            assert "hits" in res
            assert "aggregations" in res
            assert res["return_count"] == 6

    def test_search_references_case5(self, initialize_elasticsearch, auth_headers): # noqa
        # Should just find 0 records as before and records.
        with TestClient(app) as client:
            search_data = {
                "query": "",
                "facets_limits": {"pubmed_types.keyword": 10,
                                  "category.keyword": 10,
                                  "pubmed_publication_status.keyword": 10,
                                  "authors.name.keyword": 10},
                "author_filter": "",
                "query_fields": "All",
                "date_published": ["2018-09-25T04:00:00.000Z",
                                   "2018-09-29T03:59:59.999Z"]
            }
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers).json()
            assert "hits" in res
            assert "aggregations" in res
            assert res["return_count"] == 0

    def test_search_references_case6(self, initialize_elasticsearch, auth_headers): # noqa
        # Should just 0 records as after all records.
        with TestClient(app) as client:
            search_data = {
                "query": "",
                "facets_limits": {"pubmed_types.keyword": 10,
                                  "category.keyword": 10,
                                  "pubmed_publication_status.keyword": 10,
                                  "authors.name.keyword": 10},
                "author_filter": "",
                "query_fields": "All",
                "date_published": ["2023-01-27T04:00:00.000Z",
                                   "2023-01-29T03:59:59.999Z"]
            }
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers).json()
            assert "hits" in res
            assert "aggregations" in res
            assert res["return_count"] == 0

    def test_search_references_day(self, initialize_elasticsearch, auth_headers): # noqa
        # Should just 0 records as after all records.
        with TestClient(app) as client:
            search_data = {
                "query": "",
                "facets_limits": {"pubmed_types.keyword": 10,
                                  "category.keyword": 10,
                                  "pubmed_publication_status.keyword": 10,
                                  "authors.name.keyword": 10},
                "author_filter": "",
                "query_fields": "All",
                "date_published": ["2019-08-31",
                                   "2019-08-31"]
            }
            res = client.post(url="/search/references/", json=search_data, headers=auth_headers).json()
            assert "hits" in res
            assert "aggregations" in res
            assert res["return_count"] == 1
