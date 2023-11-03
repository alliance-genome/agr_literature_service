from starlette.testclient import TestClient

from fastapi import status
from agr_literature_service.api.main import app
from .test_mod_corpus_association import test_mca # noqa
from ..fixtures import db # noqa
from .test_reference import test_reference # noqa
from .test_mod import test_mod # noqa
from .fixtures import auth_headers # noqa


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
