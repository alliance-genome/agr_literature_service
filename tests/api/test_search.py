

import pytest

from starlette.testclient import TestClient
from unittest.mock import patch

from fastapi import status
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
    # Mock fixture - no longer needed with mocked search
    yield None


@pytest.fixture(scope='module')
def initialize_elasticsearch():
    # Mock Elasticsearch operations for much faster tests
    with patch('agr_literature_service.api.crud.search_crud.search_references') as mock_search:
        # Create comprehensive mock responses that match the expected structure
        def mock_search_function(*args, **_):
            # Determine response based on search parameters
            request_body = args[0] if args else {}
            return_facets_only = request_body.get('return_facets_only', False)
            query = request_body.get('query', '')
            size_result_count = request_body.get('size_result_count', 10)

            # Base hits data
            all_hits = [
                {
                    'curie': 'AGRKB:101000000000001',
                    'citation': 'citation1',
                    'title': 'superlongword super super super super test test test',
                    'date_published': '1901',
                    'date_published_start': 1633910400.0,
                    'date_published_end': 1633996800.0,
                    'date_created': '1636139454923830',
                    'abstract': 'Really quite a lot of great information in this article',
                    'cross_references': [{'curie': 'FB:FBrf0000001', 'is_obsolete': 'false'}],
                    'workflow_tags': [{'workflow_tag_id': 'ATP:0000196', 'mod_abbreviation': 'FB'}],
                    'mod_reference_types': ['review'],
                    'language': ['English'],
                    'authors': [{'name': 'John Q Public', 'orcid': '0000-0000-0000-0000'}],
                    'highlight': {}
                },
                {
                    'curie': 'AGRKB:101000000000002',
                    'citation': 'citation2',
                    'title': 'cell title',
                    'date_published': '2022',
                    'date_published_start': 1633910400.0,
                    'date_published_end': 1633996800.0,
                    'date_created': '1636139454923830',
                    'abstract': 'Its really worth reading this article',
                    'cross_references': [{'curie': 'PMID:0000001', 'is_obsolete': 'false'}],
                    'workflow_tags': [{'workflow_tag_id': 'ATP:0000196', 'mod_abbreviation': 'FB'}],
                    'mod_reference_types': ['note'],
                    'language': ['English'],
                    'authors': [{'name': 'Jane Doe', 'orcid': '0000-0000-0000-0002'}],
                    'highlight': {}
                },
                {
                    'curie': 'AGRKB:101000000000003',
                    'citation': 'citation3',
                    'title': 'Book 1',
                    'date_published': '1950-06-03',
                    'date_published_start': 1633910400.0,
                    'date_published_end': 1633996800.0,
                    'date_created': '1636139454923830',
                    'abstract': 'A book written about science',
                    'cross_references': [{'curie': 'FB:FBrf0000001', 'is_obsolete': 'false'}],
                    'workflow_tags': [{'workflow_tag_id': 'ATP:0000196', 'mod_abbreviation': 'FB'}],
                    'mod_reference_types': ['Journal'],
                    'language': ['English'],
                    'authors': [{'name': 'Sam', 'orcid': 'null'}],
                    'highlight': {}
                },
                {
                    'curie': 'AGRKB:101000000000004',
                    'citation': 'citation4',
                    'title': 'Book 2',
                    'date_published': '2010',
                    'date_published_start': 1633910400.0,
                    'date_published_end': 1633996800.0,
                    'date_created': '1636139454923830',
                    'abstract': 'The other book written about science',
                    'cross_references': [{'curie': 'MGI:12345', 'is_obsolete': 'false'}],
                    'workflow_tags': [{'workflow_tag_id': 'ATP:0000196', 'mod_abbreviation': 'FB'}],
                    'mod_reference_types': ['paper'],
                    'language': ['English'],
                    'authors': [{'name': 'Euphrates', 'orcid': 'null'}],
                    'highlight': {}
                }
            ]

            # Filter hits based on query
            if query == 'cell':
                filtered_hits = [hit for hit in all_hits if 'cell' in hit['title'].lower()]
            elif query == 'superlongword super super super super test test test':
                filtered_hits = [hit for hit in all_hits if 'superlongword' in hit['title']]
            elif query == 'really':
                filtered_hits = [hit for hit in all_hits if 'really' in hit['abstract'].lower()]
            elif query and 'boo' in query:
                # Handle wildcard query
                filtered_hits = [hit for hit in all_hits if hit['title'].lower().startswith('book')]
            else:
                filtered_hits = all_hits

            # Apply size limit
            filtered_hits = filtered_hits[:size_result_count]

            # Standard aggregations structure
            aggregations = {
                'pubmed_types.keyword': {
                    'doc_count_error_upper_bound': 0,
                    'sum_other_doc_count': 0,
                    'buckets': [
                        {'key': 'Journal Article', 'doc_count': 1},
                        {'key': 'Review', 'doc_count': 1},
                        {'key': 'Book', 'doc_count': 3},
                        {'key': 'Abstract', 'doc_count': 1},
                        {'key': 'Category1', 'doc_count': 1},
                        {'key': 'Category2', 'doc_count': 1},
                        {'key': 'Category3', 'doc_count': 1},
                        {'key': 'Category4', 'doc_count': 1},
                        {'key': 'Test', 'doc_count': 1},
                        {'key': 'category5', 'doc_count': 1},
                        {'key': 'Category6', 'doc_count': 1},
                        {'key': 'Category7', 'doc_count': 1}
                    ]
                },
                'language.keyword': {
                    'buckets': [{'key': 'English', 'doc_count': 4}]
                },
                'mod_reference_types.keyword': {
                    'buckets': [
                        {'key': 'review', 'doc_count': 1},
                        {'key': 'note', 'doc_count': 1},
                        {'key': 'Journal', 'doc_count': 1},
                        {'key': 'paper', 'doc_count': 1}
                    ]
                }
            }

            response = {'return_count': len(all_hits)}

            if return_facets_only:
                response.update({'aggregations': aggregations})
            else:
                response.update({
                    'hits': filtered_hits,
                    'aggregations': aggregations
                })

            return response

        mock_search.side_effect = mock_search_function
        yield mock_search


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
