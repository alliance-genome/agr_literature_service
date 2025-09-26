from collections import namedtuple
from unittest.mock import patch
import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa
from .test_mod import test_mod  # noqa
from .test_reference import test_reference  # noqa

TestManualIndexingTagData = namedtuple(
    "TestManualIndexingTagData",
    [
        "response",
        "new_manual_indexing_tag_id",
        "new_reference_curie",
        "new_mod_abbreviation",
        "init_payload",
    ],
)


# --- Patch helpers used by CRUD.get_manual_indexing_tag() ---
#  get_workflow_tags_from_process(process_atp_id) -> List[str]
def _patch_get_workflow_tags_from_process(process_atp_id: str):
    # pretend there are two allowed child tags for ATP:0000208 (process id used in code)
    return ["ATP:curation_tag1", "ATP:curation_tag2"]


#  get_name_to_atp_for_all_children(process_atp_id) -> (name_to_atp, atp_to_name)
def _patch_get_name_to_atp_for_all_children(process_atp_id: str):
    name_to_atp = {"Tag One": "ATP:curation_tag1", "Tag Two": "ATP:curation_tag2"}
    atp_to_name = {"ATP:curation_tag1": "Tag One", "ATP:curation_tag2": "Tag Two"}
    return name_to_atp, atp_to_name


@pytest.fixture
def test_manual_indexing_tag(db, auth_headers, test_reference, test_mod):  # noqa
    """
    Create a starter manual_indexing_tag row we can use across tests.
    """
    print("***** Adding a test manual_indexing_tag *****")
    with TestClient(app) as client:
        init_payload = {
            "mod_abbreviation": test_mod.new_mod_abbreviation,
            "reference_curie": test_reference.new_ref_curie,
            "curation_tag": "ATP:curation_tag1",
            "confidence_score": 0.9,
        }
        resp = client.post("/manual_indexing_tag/", json=init_payload, headers=auth_headers)
        assert resp.status_code == status.HTTP_201_CREATED, resp.text
        new_id = resp.json()
        yield TestManualIndexingTagData(resp, new_id, test_reference.new_ref_curie,
                                        test_mod.new_mod_abbreviation, init_payload)


class TestManualIndexingTag:
    def test_create(self, test_manual_indexing_tag):  # noqa
        # Creation already asserted in fixture; just ensure return type is int
        assert isinstance(test_manual_indexing_tag.new_manual_indexing_tag_id, int)

    def test_show(self, test_manual_indexing_tag, auth_headers):  # noqa
        with TestClient(app) as client:
            url = f"/manual_indexing_tag/{test_manual_indexing_tag.new_manual_indexing_tag_id}"
            r = client.get(url, headers=auth_headers)
            assert r.status_code == status.HTTP_200_OK, r.text
            data = r.json()
            # basic shape checks
            assert data["curation_tag"] == "ATP:curation_tag1"
            assert data["reference_curie"] == test_manual_indexing_tag.new_reference_curie
            assert data["mod_abbreviation"] == test_manual_indexing_tag.new_mod_abbreviation
            # confidence_score should round-trip
            assert data["confidence_score"] == pytest.approx(0.9)

    def test_patch(self, test_manual_indexing_tag, auth_headers):  # noqa
        with TestClient(app) as client:
            updates = {
                "confidence_score": 0.42,
                "validation_by_biocurator": "right",
                "note": "patched note",
            }
            url = f"/manual_indexing_tag/{test_manual_indexing_tag.new_manual_indexing_tag_id}"
            r = client.patch(url, headers=auth_headers, json=updates)
            assert r.status_code == status.HTTP_202_ACCEPTED, r.text
            # re-read
            r2 = client.get(url, headers=auth_headers)
            assert r2.status_code == status.HTTP_200_OK, r2.text
            data = r2.json()
            for k, v in updates.items():
                assert data[k] == v
            # ensure immutable fields were not touched by this patch
            assert data["curation_tag"] == "ATP:curation_tag1"
            assert data["reference_curie"] == test_manual_indexing_tag.new_reference_curie
            assert data["mod_abbreviation"] == test_manual_indexing_tag.new_mod_abbreviation

    def test_delete(self, test_manual_indexing_tag, auth_headers):  # noqa
        with TestClient(app) as client:
            url = f"/manual_indexing_tag/{test_manual_indexing_tag.new_manual_indexing_tag_id}"
            r = client.delete(url, headers=auth_headers)
            assert r.status_code == status.HTTP_204_NO_CONTENT, r.text
            # subsequent GET should 404
            r2 = client.get(url, headers=auth_headers)
            assert r2.status_code == status.HTTP_404_NOT_FOUND

    @patch(
        "agr_literature_service.api.crud.manual_indexing_tag_crud.get_name_to_atp_for_all_children",
        _patch_get_name_to_atp_for_all_children,
    )
    @patch(
        "agr_literature_service.api.crud.manual_indexing_tag_crud.get_workflow_tags_from_process",
        _patch_get_workflow_tags_from_process,
    )
    def test_get_manual_indexing_tag_without_mod_filter(self, test_manual_indexing_tag, auth_headers):  # noqa
        """
        Exercise GET /get_manual_indexing_tag/{reference_curie}
        Ensures we get both 'current_curation_tag' (the DB row) and 'all_curation_tags' (patched).
        """
        with TestClient(app) as client:
            url = f"/manual_indexing_tag/get_manual_indexing_tag/{test_manual_indexing_tag.new_reference_curie}"
            r = client.get(url, headers=auth_headers)
            assert r.status_code == status.HTTP_200_OK, r.text
            data = r.json()
            assert "current_curation_tag" in data
            assert "all_curation_tags" in data
            # the patched dictionary should be present
            assert data["all_curation_tags"] == {
                "ATP:0000208": "ATP:0000208",
                "ATP:0000227": "ATP:0000227",
                "ATP:curation_tag1": "Tag One",
                "ATP:curation_tag2": "Tag Two",
            }
            # and we should see our current/created row reflected
            current = data["current_curation_tag"]
            assert isinstance(current, list) and len(current) >= 1
            assert any(row["curation_tag"] == "ATP:curation_tag1" for row in current)

    def test_get_manual_indexing_tag_with_non_zfin_mod_returns_empty(self, test_manual_indexing_tag, auth_headers):  # noqa
        """
        Router short-circuits to [] when a non-ZFIN mod_abbreviation path param is provided.
        """
        with TestClient(app) as client:
            url = (
                f"/manual_indexing_tag/get_manual_indexing_tag/"
                f"{test_manual_indexing_tag.new_reference_curie}/WB"
            )
            r = client.get(url, headers=auth_headers)
            assert r.status_code == status.HTTP_200_OK, r.text
            assert r.json() == []

    @pytest.mark.xfail(reason="Router uses body.manual_indexing_tag instead of body.curation_tag")
    def test_set_manual_indexing_tag_endpoint(self, test_reference, test_mod, auth_headers):  # noqa
        """
        Surfacing the current router bug: SetManualIndexingTagBody defines 'curation_tag',
        but the router passes 'body.manual_indexing_tag' to CRUD.
        """
        with TestClient(app) as client:
            body = {
                "reference_curie": test_reference.new_ref_curie,
                "mod_abbreviation": test_mod.new_mod_abbreviation,
                "curation_tag": "ATP:curation_tag2",
                "confidence_score": 0.75,
            }
            r = client.post("/manual_indexing_tag/set_manual_indexing_tag", json=body, headers=auth_headers)
            # Expect failure until the router bug is fixed; once fixed, flip to assert 200 and validate payload.
            assert r.status_code == status.HTTP_200_OK
