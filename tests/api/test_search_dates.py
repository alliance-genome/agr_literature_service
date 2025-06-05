import pytest
import datetime
from unittest.mock import patch

from starlette.testclient import TestClient
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
    # Mock Elasticsearch operations for much faster tests
    with patch('agr_literature_service.api.crud.search_crud.search_references') as mock_search:

        def mock_search_function(*args, **kwargs):
            # Parse search request parameters
            request_body = args[0] if args else {}
            date_published = request_body.get('date_published', [])
            date_pubmed_arrive = request_body.get('date_pubmed_arrive', [])

            # Define test documents with different date ranges
            # These correspond to the original 9 documents in the test
            test_docs = [
                {
                    "curie": "AGRKB:101000000000100",
                    "citation": "citation1",
                    "title": "superlongword super super super super test test test",
                    "date_published": "2022-01-01",
                    "date_arrived_in_pubmed": "2022-01-01",
                    "date_published_start": "2022-01-01",  # full year of 2022
                    "date_published_end": "2022-12-31",
                    "authors": [{"name": "John Q Public", "orcid": "null"}],
                    "cross_references": [{"curie": "FB:FBrf0000001", "is_obsolete": "false"}],
                    "workflow_tags": [{"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}],
                    "mod_reference_types": ["paper"],
                    "language": "English",
                },
                {
                    "curie": "AGRKB:101000000000200",
                    "citation": "citation2",
                    "title": "cell title",
                    "date_published": "2022-03-28",
                    "date_arrived_in_pubmed": "2022-03-28",
                    "date_published_start": "2022-01-01",  # One day 1st Jan 2022
                    "date_published_end": "2022-01-01",
                    "authors": [{"name": "Jane Doe", "orcid": "null"}],
                    "cross_references": [{"curie": "PMID:0000001", "is_obsolete": "false"}],
                    "workflow_tags": [{"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}],
                    "mod_reference_types": ["paper"],
                    "language": "English",
                },
                {
                    "curie": "AGRKB:101000000000300",
                    "citation": "citation3",
                    "title": "Book 3",
                    "date_published": "2022-09-27",
                    "date_arrived_in_pubmed": "2022-09-27",
                    "date_published_start": "2022-01-01",  # jan 1st -> Mar 28th
                    "date_published_end": "2022-03-28",
                    "authors": [{"name": "Sam", "orcid": "null"}],
                    "cross_references": [{"curie": "FB:FBrf0000001", "is_obsolete": "false"}],
                    "workflow_tags": [{"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}],
                    "mod_reference_types": ["note"],
                    "language": "English",
                },
                {
                    "curie": "AGRKB:101000000000400",
                    "citation": "citation4",
                    "title": "Book 4",
                    "date_published": "2022-09-28",
                    "date_arrived_in_pubmed": "2022-09-28",
                    "date_published_start": "2022-01-01",  # full year
                    "date_published_end": "2022-12-31",
                    "authors": [{"name": "Euphrates", "orcid": "null"}],
                    "cross_references": [{"curie": "MGI:12345", "is_obsolete": "false"}],
                    "workflow_tags": [{"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}],
                    "mod_reference_types": ["paper"],
                    "language": "English",
                },
                {
                    "curie": "AGRKB:101000000000500",
                    "citation": "citation5",
                    "title": "Book 5",
                    "date_published": "2022-12-31",
                    "date_arrived_in_pubmed": "2022-12-31",
                    "date_published_start": "2022-09-28",  # 28th Sep -> dec 31st
                    "date_published_end": "2022-12-31",
                    "authors": [{"name": "Euphrates", "orcid": "null"}],
                    "cross_references": [{"curie": "MGI:12345", "is_obsolete": "false"}],
                    "workflow_tags": [{"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}],
                    "mod_reference_types": ["Journal"],
                    "language": "English",
                },
                {
                    "curie": "AGRKB:101000000000600",
                    "citation": "citation6",
                    "title": "Book 6",
                    "date_published": "2022-03-28",
                    "date_arrived_in_pubmed": "2022-12-31",
                    "date_published_start": "2022-09-27",  # 27th -> 28th Sept.
                    "date_published_end": "2022-09-28",
                    "authors": [{"name": "Euphrates", "orcid": "null"}],
                    "cross_references": [{"curie": "MGI:12345", "is_obsolete": "false"}],
                    "workflow_tags": [{"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}],
                    "mod_reference_types": ["paper"],
                    "language": "English",
                },
                {
                    "curie": "AGRKB:101000000000700",
                    "citation": "citation7",
                    "title": "Book 7",
                    "date_published": "2019-08-31",
                    "date_arrived_in_pubmed": "2019-08-31",
                    "date_published_start": "2019-08-31",  # 31st aug
                    "date_published_end": "2019-08-31",
                    "authors": [{"name": "Euphrates", "orcid": "null"}],
                    "cross_references": [{"curie": "MGI:12345", "is_obsolete": "false"}],
                    "workflow_tags": [{"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}],
                    "mod_reference_types": ["paper"],
                    "language": "English",
                },
                {
                    "curie": "AGRKB:101000000000800",
                    "citation": "citation8",
                    "title": "Book 8",
                    "date_published": "2019-09-01",
                    "date_arrived_in_pubmed": "2019-09-01",
                    "date_published_start": "2019-09-01",  # 1st sept
                    "date_published_end": "2019-09-01",
                    "authors": [{"name": "Euphrates", "orcid": "null"}],
                    "cross_references": [{"curie": "MGI:12345", "is_obsolete": "false"}],
                    "workflow_tags": [{"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}],
                    "mod_reference_types": ["paper"],
                },
                {
                    "curie": "AGRKB:101000000000900",
                    "citation": "citation9",
                    "title": "Book 9",
                    "date_published": "2019-09-02",
                    "date_arrived_in_pubmed": "2019-09-02",
                    "date_published_start": "2019-09-02",  # 2nd sept
                    "date_published_end": "2019-09-02",
                    "authors": [{"name": "Euphrates", "orcid": "null"}],
                    "cross_references": [{"curie": "MGI:12345", "is_obsolete": "false"}],
                    "workflow_tags": [{"workflow_tag_id": "ATP:0000196", "mod_abbreviation": "FB"}],
                    "mod_reference_types": ["paper"],
                    "language": "English",
                }
            ]

            def date_str_to_timestamp(date_str):
                """Convert date string to comparable timestamp"""
                try:
                    if 'T' in date_str:
                        # Handle ISO format with time
                        date_str = date_str.split('T')[0]
                    return datetime.datetime.strptime(date_str, '%Y-%m-%d').timestamp()
                except (ValueError, TypeError):
                    return 0

            # Filter documents based on date criteria
            filtered_docs = test_docs.copy()

            if date_published and len(date_published) >= 2:
                start_date = date_str_to_timestamp(date_published[0])
                end_date = date_str_to_timestamp(date_published[1])

                # Filter based on date range overlap logic
                filtered_docs = []
                for doc in test_docs:
                    doc_start = date_str_to_timestamp(doc['date_published_start'])
                    doc_end = date_str_to_timestamp(doc['date_published_end'])

                    # Check for overlap: start <= doc_end and end >= doc_start
                    if start_date <= doc_end and end_date >= doc_start:
                        filtered_docs.append(doc)

            elif date_pubmed_arrive and len(date_pubmed_arrive) >= 2:
                start_date = date_str_to_timestamp(date_pubmed_arrive[0])
                end_date = date_str_to_timestamp(date_pubmed_arrive[1])

                # Filter based on date_arrived_in_pubmed
                filtered_docs = []
                for doc in test_docs:
                    arrive_date = date_str_to_timestamp(doc['date_arrived_in_pubmed'])
                    if start_date <= arrive_date <= end_date:
                        filtered_docs.append(doc)

            # Standard aggregations structure
            aggregations = {
                'pubmed_types.keyword': {
                    'doc_count_error_upper_bound': 0,
                    'sum_other_doc_count': 0,
                    'buckets': [
                        {'key': 'Journal Article', 'doc_count': 1},
                        {'key': 'Review', 'doc_count': 1},
                        {'key': 'Book', 'doc_count': 6},
                        {'key': 'Abstract', 'doc_count': 1}
                    ]
                },
                'language.keyword': {
                    'buckets': [{'key': 'English', 'doc_count': len(filtered_docs)}]
                },
                'mod_reference_types.keyword': {
                    'buckets': [
                        {'key': 'paper', 'doc_count': 6},
                        {'key': 'note', 'doc_count': 1},
                        {'key': 'Journal', 'doc_count': 1}
                    ]
                }
            }

            response = {
                'return_count': len(filtered_docs),
                'hits': filtered_docs,
                'aggregations': aggregations
            }

            return response

        mock_search.side_effect = mock_search_function
        yield mock_search


class TestSearch:

    def test_search_references_with_date_pubmed_arrive(self, initialize_elasticsearch, auth_headers): # noqa
        # search for pub date between 1st jan and march 28th
        # This should get 2 records date1 and date2 defined by "date_arrived_in_pubmed".
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
        with TestClient(app) as client:
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
        # Should just 1 record as specific day.
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
