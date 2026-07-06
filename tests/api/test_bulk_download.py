import json

from fastapi import status
from sqlalchemy import text
from starlette.testclient import TestClient

from agr_literature_service.api.main import app
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


def _insert_xref_descriptor(db, prefix):  # noqa
    """Register a cross-reference db_prefix so /cross_reference/ POSTs are accepted."""
    with db.begin():
        db.execute(text("INSERT INTO resource_descriptors (db_prefix, name, default_url) "
                        f"VALUES ('{prefix}', 'Madeup {prefix}', 'http://www.example.com/[%s]')"))


def _by_curie(payload):
    """Turn the streamed JSON array into a {curie: entry} dict for easy assertions."""
    return {entry["curie"]: entry for entry in payload}


class TestBulkDownloadReferencesExternalIds:

    def test_references_external_ids(self, db, auth_headers):  # noqa
        _insert_xref_descriptor(db, "XREF")
        with TestClient(app) as client:
            # Reference A: two cross references, one active + one obsolete
            ref_a = client.post(url="/reference/",
                                json={"title": "Ref A", "category": "research_article"},
                                headers=auth_headers).json()["curie"]
            client.post(url="/cross_reference/",
                        json={"curie": "XREF:refA-active", "reference_curie": ref_a,
                              "pages": ["reference"], "is_obsolete": False},
                        headers=auth_headers)
            client.post(url="/cross_reference/",
                        json={"curie": "XREF:refA-obsolete", "reference_curie": ref_a,
                              "pages": ["reference"], "is_obsolete": True},
                        headers=auth_headers)

            # Reference B: no cross references (exercises the outer-join / empty-list path)
            ref_b = client.post(url="/reference/",
                                json={"title": "Ref B", "category": "research_article"},
                                headers=auth_headers).json()["curie"]

            response = client.get(url="/bulk_download/references/external_ids/", headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK

            # Body must be a single valid JSON array (streamed, but buffered by TestClient)
            payload = json.loads(response.content)
            assert isinstance(payload, list)
            by_curie = _by_curie(payload)

            assert ref_a in by_curie
            assert ref_b in by_curie

            xrefs_a = {x["curie"]: x["is_obsolete"] for x in by_curie[ref_a]["cross_references"]}
            assert xrefs_a == {"XREF:refA-active": False, "XREF:refA-obsolete": True}

            assert by_curie[ref_b]["cross_references"] == []

    def test_invalid_token_returns_401_not_500(self):
        """A malformed/wrong-issuer bearer token must yield a clean 401, not a 500.

        Regression test for the auth fix: PyJWKClient raises a PyJWTError (not a
        jose JWTError) that previously bubbled up as a 500 Internal Server Error.
        """
        with TestClient(app) as client:
            response = client.get(url="/bulk_download/references/external_ids/",
                                  headers={"Authorization": "Bearer not-a-valid-jwt"})
            assert response.status_code == status.HTTP_401_UNAUTHORIZED


class TestBulkDownloadResourcesExternalIds:

    def test_resources_external_ids(self, db, auth_headers):  # noqa
        _insert_xref_descriptor(db, "XREF")
        with TestClient(app) as client:
            resource_curie = client.post(url="/resource/", json={"title": "Res A"},
                                         headers=auth_headers).json()["curie"]
            client.post(url="/cross_reference/",
                        json={"curie": "XREF:resA-active", "resource_curie": resource_curie,
                              "is_obsolete": False},
                        headers=auth_headers)

            response = client.get(url="/bulk_download/resources/external_ids/", headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK

            payload = json.loads(response.content)
            assert isinstance(payload, list)
            by_curie = _by_curie(payload)

            assert resource_curie in by_curie
            xrefs = {x["curie"]: x["is_obsolete"] for x in by_curie[resource_curie]["cross_references"]}
            assert xrefs == {"XREF:resA-active": False}
