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
        "citation": "citation1",
        "title": "superlongword super super super super test test test",
        "pubmed_types": ["Journal Article", "Review"],
        "abstract": "Really quite a lot of great information in this article",
        "date_published": "1901",
        "date_published_start": datetime.strptime('10/10/2021', '%m/%d/%Y').timestamp(),
        "date_published_end": datetime.strptime('11/10/2021', '%m/%d/%Y').timestamp(),
        "authors": [{"name": "John Q Public", "orcid": "null"}, {"name": "Socrates", "orcid": "null"}],
        "cross_references": [{"curie": "FB:FBrf0000001", "is_obsolete": "false"}, {"curie": "FB:FBrf0000002", "is_obsolete": "true"}],
        "mod_reference_types": ["review"],
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
        "authors": [{"name": "Jane Doe", "orcid": "null"}],
        "cross_references": [{"curie": "PMID:0000001", "is_obsolete": "false"}],
        "mod_reference_types": ["note"],
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
        "mod_reference_types": ["Journal"],
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
        "mod_reference_types": ["paper"],
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


class TestSort:

    def test_sort_need_review(self, test_mca): # noqa
        with TestClient(app) as client:
            res = client.get(url="/sort/need_review", params={"mod_abbreviation": "0015_AtDB", "count": 10})
            assert res.status_code == status.HTTP_200_OK
            assert len(res.json()) > 0

    def test_sort_prepublication_pipeline(self, auth_headers): # noqa
        with TestClient(app) as client:
            new_mod = {
                "abbreviation": "WB",
                "short_name": "WB",
                "full_name": "WormBase"
            }
            response = client.post(url="/mod/", json=new_mod, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            reference_create_json = {
                "cross_references": [
                    {
                        "curie": "PMID:1113",
                        "is_obsolete": "false"
                    }
                ],
                "mod_corpus_associations": [
                    {
                        "mod_abbreviation": "WB",
                        "mod_corpus_sort_source": "prepublication_pipeline",
                        "corpus": "true"
                    }
                ],
                "title": "pmid_fake",
                "prepublication_pipeline": "true"
            }
            response = client.post(url="/reference/", json=reference_create_json, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            res = client.get(url="/sort/prepublication_pipeline", params={"mod_abbreviation": "WB", "count": 10})
            assert res.status_code == status.HTTP_200_OK
            assert len(res.json()) > 0
