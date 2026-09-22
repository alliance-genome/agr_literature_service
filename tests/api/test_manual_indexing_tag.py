from collections import namedtuple
from unittest.mock import patch
import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import CrossReferenceModel, ReferenceModel
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


#  get_name_to_atp_for_descendants(process_atp_id) -> (name_to_atp, atp_to_name)
def _patch_get_name_to_atp_for_descendants(process_atp_id: str):
    name_to_atp = {"Tag One": "ATP:curation_tag1", "Tag Two": "ATP:curation_tag2"}
    atp_to_name = {"ATP:curation_tag1": "Tag One", "ATP:curation_tag2": "Tag Two"}
    return name_to_atp, atp_to_name


TEST_PMID_XREF = "PMID:88888888"
TEST_MOD_XREF = "WB:WBPaper88888888"


@pytest.fixture
def test_reference_xrefs(db, test_reference):  # noqa
    """
    Attach a PMID and a MOD cross_reference to the test reference so the
    create/patch paths can be exercised with non-AGRKB identifiers.
    """
    ref = db.query(ReferenceModel).filter_by(curie=test_reference.new_ref_curie).one()
    for xref_curie, prefix in ((TEST_PMID_XREF, "PMID"), (TEST_MOD_XREF, "WB")):
        db.add(CrossReferenceModel(curie=xref_curie, curie_prefix=prefix,
                                   reference_id=ref.reference_id))
    db.commit()
    yield test_reference.new_ref_curie


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
            assert r.status_code == status.HTTP_200_OK, r.text
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
        "agr_literature_service.api.crud.manual_indexing_tag_crud.get_name_to_atp_for_descendants",
        _patch_get_name_to_atp_for_descendants,
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
            url = (
                f"/manual_indexing_tag/get_manual_indexing_tag/"
                f"{test_manual_indexing_tag.new_reference_curie}/"
                f"{test_manual_indexing_tag.new_mod_abbreviation}"
            )
            r = client.get(url, headers=auth_headers)
            assert r.status_code == status.HTTP_200_OK, r.text
            data = r.json()

            assert "current_curation_tag" in data
            assert isinstance(data["current_curation_tag"], dict)
            assert data["current_curation_tag"]["curation_tag"] == "ATP:curation_tag1"
            assert data["current_curation_tag"]["reference_curie"] == test_manual_indexing_tag.new_reference_curie
            assert data["current_curation_tag"]["mod_abbreviation"] == test_manual_indexing_tag.new_mod_abbreviation

            assert data["all_curation_tags"] == {
                "ATP:0000208": "ATP:0000208",
                "ATP:0000227": "ATP:0000227",
                "ATP:curation_tag1": "Tag One",
                "ATP:curation_tag2": "Tag Two",
            }

    def test_set_manual_indexing_tag_endpoint(self, test_reference, test_mod, auth_headers):  # noqa
        """
        The deprecated POST /set_manual_indexing_tag still works: it writes the
        same row as POST / but returns the created record instead of its id.
        """
        with TestClient(app) as client:
            body = {
                "reference_curie": test_reference.new_ref_curie,
                "mod_abbreviation": test_mod.new_mod_abbreviation,
                "curation_tag": "ATP:curation_tag2",
                "confidence_score": 0.75,
            }
            r = client.post("/manual_indexing_tag/set_manual_indexing_tag", json=body, headers=auth_headers)
            assert r.status_code == status.HTTP_200_OK, r.text
            data = r.json()
            assert data["curation_tag"] == "ATP:curation_tag2"
            assert data["reference_curie"] == test_reference.new_ref_curie
            assert data["mod_abbreviation"] == test_mod.new_mod_abbreviation
            assert data["confidence_score"] == pytest.approx(0.75)


class TestManualIndexingTagReferenceIdentifiers:
    """
    create/patch accept an AGRKB curie, a PMID curie or a MOD curie; anything
    that is not an AGRKB curie is normalized via the cross_reference table.
    """

    def _create(self, client, auth_headers, reference_curie, mod_abbreviation, curation_tag):  # noqa
        return client.post(
            "/manual_indexing_tag/",
            json={
                "reference_curie": reference_curie,
                "mod_abbreviation": mod_abbreviation,
                "curation_tag": curation_tag,
                "confidence_score": 0.5,
            },
            headers=auth_headers,
        )

    @pytest.mark.parametrize("lookup", [TEST_PMID_XREF, TEST_MOD_XREF])
    def test_create_with_xref(self, lookup, test_reference_xrefs, test_mod, auth_headers):  # noqa
        """A PMID or MOD curie resolves to the reference's AGRKB curie on create."""
        with TestClient(app) as client:
            r = self._create(client, auth_headers, lookup,
                             test_mod.new_mod_abbreviation, "ATP:curation_tag1")
            assert r.status_code == status.HTTP_201_CREATED, r.text
            new_id = r.json()
            assert isinstance(new_id, int)

            shown = client.get(f"/manual_indexing_tag/{new_id}", headers=auth_headers)
            assert shown.status_code == status.HTTP_200_OK, shown.text
            assert shown.json()["reference_curie"] == test_reference_xrefs

    def test_create_with_unknown_xref(self, test_reference_xrefs, test_mod, auth_headers):  # noqa
        """An xref that is not in cross_reference is a 422, not a 404."""
        with TestClient(app) as client:
            r = self._create(client, auth_headers, "PMID:99999999",
                             test_mod.new_mod_abbreviation, "ATP:curation_tag1")
            assert r.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, r.text
            assert "PMID:99999999" in r.json()["detail"]

    def test_create_duplicate_across_identifier_forms(
        self, test_reference_xrefs, test_mod, auth_headers  # noqa
    ):
        """
        The duplicate-record guard still fires when the second create names the
        same reference by a different identifier form.
        """
        with TestClient(app) as client:
            first = self._create(client, auth_headers, test_reference_xrefs,
                                 test_mod.new_mod_abbreviation, "ATP:curation_tag1")
            assert first.status_code == status.HTTP_201_CREATED, first.text

            dup = self._create(client, auth_headers, TEST_PMID_XREF,
                               test_mod.new_mod_abbreviation, "ATP:curation_tag1")
            assert dup.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, dup.text
            # abc_utils.send_manual_indexing_to_abc matches on this wording
            assert "already exists" in dup.json()["detail"]

    def test_patch_reference_curie_with_xref(
        self, test_manual_indexing_tag, test_reference_xrefs, auth_headers  # noqa
    ):
        """patch() normalizes a MOD curie the same way create() does."""
        with TestClient(app) as client:
            url = f"/manual_indexing_tag/{test_manual_indexing_tag.new_manual_indexing_tag_id}"
            r = client.patch(url, headers=auth_headers,
                             json={"reference_curie": TEST_MOD_XREF})
            assert r.status_code == status.HTTP_200_OK, r.text
            assert r.json()["reference_curie"] == test_reference_xrefs

    def test_patch_reference_curie_with_unknown_xref(
        self, test_manual_indexing_tag, auth_headers  # noqa
    ):
        with TestClient(app) as client:
            url = f"/manual_indexing_tag/{test_manual_indexing_tag.new_manual_indexing_tag_id}"
            r = client.patch(url, headers=auth_headers,
                             json={"reference_curie": "WB:WBPaper00000000"})
            assert r.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, r.text
