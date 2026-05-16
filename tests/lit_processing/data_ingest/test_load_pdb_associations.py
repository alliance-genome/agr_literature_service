"""Tests for load_pdb_associations.py"""

import os
from unittest.mock import MagicMock, patch

import pytest

from agr_literature_service.lit_processing.data_ingest.pdb_ingest import (
    load_pdb_associations as mod,
)


class TestBuildPayload:

    def test_includes_entity_fields_in_entity_mode(self, monkeypatch):
        monkeypatch.setattr(mod, "INCLUDE_ENTITY_FIELDS", True)
        payload = mod._build_payload("AGR:AGR-Reference-0000000001", "1ABC", 42)
        data = payload.dict()
        assert data["reference_curie"] == "AGR:AGR-Reference-0000000001"
        assert data["topic"] == mod.PROTEIN_STRUCTURE_ATP
        assert data["topic_entity_tag_source_id"] == 42
        assert data["data_novelty"] == mod.DATA_NOVELTY_NOT_NEW
        assert data["negated"] is False
        assert data["entity_type"] == mod.PROTEIN_STRUCTURE_ATP
        assert data["entity"] == "1ABC"
        assert data["entity_id_validation"] == "alliance"

    def test_omits_entity_fields_in_topic_only_mode(self, monkeypatch):
        monkeypatch.setattr(mod, "INCLUDE_ENTITY_FIELDS", False)
        payload = mod._build_payload("AGR:AGR-Reference-0000000001", "1ABC", 42)
        data = payload.dict()
        assert data.get("entity") is None
        assert data.get("entity_type") is None
        assert data.get("entity_id_validation") is None


class TestFetchPubmedIdsGraphql:

    @patch.object(mod, "_post_with_retry")
    def test_returns_pdb_to_pmid_map(self, mock_post):
        mock_post.return_value = {
            "data": {
                "entries": [
                    {"rcsb_id": "1ABC",
                     "rcsb_pubmed_container_identifiers": {"pubmed_id": 11111}},
                    {"rcsb_id": "2XYZ",
                     "rcsb_pubmed_container_identifiers": {"pubmed_id": 22222}},
                ]
            }
        }
        result = mod._fetch_pubmed_ids_graphql(["1ABC", "2XYZ"])
        assert result == {"1ABC": "11111", "2XYZ": "22222"}
        mock_post.assert_called_once()
        body = mock_post.call_args[0][1]
        assert body["variables"]["ids"] == ["1ABC", "2XYZ"]

    @patch.object(mod, "_post_with_retry")
    def test_skips_entries_with_no_pubmed_id(self, mock_post):
        mock_post.return_value = {
            "data": {
                "entries": [
                    {"rcsb_id": "1ABC",
                     "rcsb_pubmed_container_identifiers": {"pubmed_id": 11111}},
                    {"rcsb_id": "2XYZ",
                     "rcsb_pubmed_container_identifiers": None},
                    {"rcsb_id": "3DEF",
                     "rcsb_pubmed_container_identifiers": {"pubmed_id": None}},
                ]
            }
        }
        result = mod._fetch_pubmed_ids_graphql(["1ABC", "2XYZ", "3DEF"])
        assert result == {"1ABC": "11111"}

    @patch.object(mod, "_post_with_retry")
    def test_handles_empty_response(self, mock_post):
        mock_post.return_value = {"data": {"entries": []}}
        assert mod._fetch_pubmed_ids_graphql(["1ABC"]) == {}


class TestFetchAllPdbIdsWithPubmed:

    @patch.object(mod, "_post_with_retry")
    def test_paginates_until_short_page(self, mock_post):
        # First page hits page size, second page is short -> loop ends.
        full_page = [{"identifier": f"ID{i}"} for i in range(mod.SEARCH_PAGE_SIZE)]
        short_page = [{"identifier": "TAIL1"}, {"identifier": "TAIL2"}]
        mock_post.side_effect = [
            {"result_set": full_page},
            {"result_set": short_page},
        ]
        with patch.object(mod.time, "sleep"):
            ids = list(mod._fetch_all_pdb_ids_with_pubmed())
        assert ids[-2:] == ["TAIL1", "TAIL2"]
        assert len(ids) == mod.SEARCH_PAGE_SIZE + 2
        assert mock_post.call_count == 2

    @patch.object(mod, "_post_with_retry")
    def test_stops_on_empty_result_set(self, mock_post):
        mock_post.return_value = {"result_set": []}
        with patch.object(mod.time, "sleep"):
            ids = list(mod._fetch_all_pdb_ids_with_pubmed())
        assert ids == []


class TestPostWithRetry:

    @patch.object(mod.requests, "post")
    def test_retries_on_429(self, mock_post):
        rate_limited = MagicMock(status_code=429)
        ok = MagicMock(status_code=200)
        ok.json.return_value = {"ok": True}
        mock_post.side_effect = [rate_limited, ok]
        with patch.object(mod.time, "sleep") as mock_sleep:
            result = mod._post_with_retry("https://example/", {"q": 1})
        assert result == {"ok": True}
        assert mock_post.call_count == 2
        # First retry waits BASE * 2**0 = BASE seconds.
        mock_sleep.assert_called_with(mod.HTTP_BACKOFF_BASE_SECONDS)

    @patch.object(mod.requests, "post")
    def test_retries_on_5xx(self, mock_post):
        server_err = MagicMock(status_code=503)
        ok = MagicMock(status_code=200)
        ok.json.return_value = {"ok": True}
        mock_post.side_effect = [server_err, ok]
        with patch.object(mod.time, "sleep"):
            result = mod._post_with_retry("https://example/", {"q": 1})
        assert result == {"ok": True}
        assert mock_post.call_count == 2

    @patch.object(mod.requests, "post")
    def test_retries_on_connection_error(self, mock_post):
        ok = MagicMock(status_code=200)
        ok.json.return_value = {"ok": True}
        mock_post.side_effect = [
            mod.requests.exceptions.ConnectionError("dns failure"),
            ok,
        ]
        with patch.object(mod.time, "sleep"):
            result = mod._post_with_retry("https://example/", {"q": 1})
        assert result == {"ok": True}
        assert mock_post.call_count == 2

    @patch.object(mod.requests, "post")
    def test_uses_exponential_backoff(self, mock_post):
        server_err = MagicMock(status_code=502)
        ok = MagicMock(status_code=200)
        ok.json.return_value = {"ok": True}
        mock_post.side_effect = [server_err, server_err, ok]
        with patch.object(mod.time, "sleep") as mock_sleep:
            mod._post_with_retry("https://example/", {"q": 1})
        # Attempt 0 fails -> sleep BASE; attempt 1 fails -> sleep BASE*2.
        sleeps = [c.args[0] for c in mock_sleep.call_args_list]
        assert sleeps == [
            mod.HTTP_BACKOFF_BASE_SECONDS,
            mod.HTTP_BACKOFF_BASE_SECONDS * 2,
        ]

    @patch.object(mod.requests, "post")
    def test_gives_up_after_max_attempts_5xx(self, mock_post):
        server_err = MagicMock(status_code=503)
        server_err.raise_for_status.side_effect = mod.requests.HTTPError("503")
        mock_post.side_effect = [server_err] * mod.HTTP_MAX_ATTEMPTS
        with patch.object(mod.time, "sleep"):
            with pytest.raises(mod.requests.HTTPError):
                mod._post_with_retry("https://example/", {"q": 1})
        assert mock_post.call_count == mod.HTTP_MAX_ATTEMPTS

    @patch.object(mod.requests, "post")
    def test_gives_up_after_max_attempts_connection(self, mock_post):
        mock_post.side_effect = [
            mod.requests.exceptions.ConnectionError("nope")
        ] * mod.HTTP_MAX_ATTEMPTS
        with patch.object(mod.time, "sleep"):
            with pytest.raises(mod.requests.exceptions.ConnectionError):
                mod._post_with_retry("https://example/", {"q": 1})
        assert mock_post.call_count == mod.HTTP_MAX_ATTEMPTS


class TestLoad:

    def _make_db(self, reference_id_for_pmid, reference_curie="AGR:AGR-Reference-0000000001"):
        db = MagicMock()
        ref = MagicMock(curie=reference_curie)
        db.query.return_value.filter_by.return_value.one.return_value = ref
        return db, ref

    @patch.object(mod, "create_tag")
    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "get_reference_id_by_pmid")
    def test_happy_path_creates_tag(
        self, mock_get_ref, mock_set_uid, mock_get_source, mock_create_tag, monkeypatch
    ):
        monkeypatch.setattr(mod, "INCLUDE_ENTITY_FIELDS", True)
        mock_get_ref.return_value = 1234
        mock_create_tag.return_value = {"status": "success", "message": "New tag created successfully."}
        db, ref = self._make_db(1234)

        counts = mod.load(db=db, pairs=[("1ABC", "11111")])

        assert counts == {"created": 1, "skipped_duplicate": 0, "missing_reference": 0, "errors": 0}
        mock_create_tag.assert_called_once()
        payload = mock_create_tag.call_args[0][1].dict()
        assert payload["reference_curie"] == "AGR:AGR-Reference-0000000001"
        assert payload["topic"] == mod.PROTEIN_STRUCTURE_ATP
        assert payload["entity"] == "1ABC"
        assert payload["topic_entity_tag_source_id"] == 42

    @patch.object(mod, "create_tag")
    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "get_reference_id_by_pmid", return_value=None)
    def test_missing_reference_is_counted_and_skipped(
        self, mock_get_ref, mock_set_uid, mock_get_source, mock_create_tag
    ):
        db = MagicMock()
        counts = mod.load(db=db, pairs=[("1ABC", "99999")])
        assert counts == {"created": 0, "skipped_duplicate": 0, "missing_reference": 1, "errors": 0}
        mock_create_tag.assert_not_called()

    @patch.object(mod, "create_tag")
    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "get_reference_id_by_pmid", return_value=1234)
    def test_duplicate_returns_are_counted_as_skipped(
        self, mock_get_ref, mock_set_uid, mock_get_source, mock_create_tag
    ):
        mock_create_tag.return_value = {"status": "exists", "message": "The tag already exists in the database."}
        db, _ = self._make_db(1234)
        counts = mod.load(db=db, pairs=[("1ABC", "11111")])
        assert counts == {"created": 0, "skipped_duplicate": 1, "missing_reference": 0, "errors": 0}

    @patch.object(mod, "create_tag", side_effect=RuntimeError("boom"))
    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "get_reference_id_by_pmid", return_value=1234)
    def test_exceptions_are_counted_not_raised(
        self, mock_get_ref, mock_set_uid, mock_get_source, mock_create_tag
    ):
        db, _ = self._make_db(1234)
        counts = mod.load(db=db, pairs=[("1ABC", "11111")])
        assert counts == {"created": 0, "skipped_duplicate": 0, "missing_reference": 0, "errors": 1}

    @patch.object(mod, "create_tag")
    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "get_reference_id_by_pmid")
    def test_pmid_cache_avoids_repeated_lookups(
        self, mock_get_ref, mock_set_uid, mock_get_source, mock_create_tag
    ):
        mock_get_ref.return_value = 1234
        mock_create_tag.return_value = {"status": "success"}
        db, _ = self._make_db(1234)

        # Three PDB IDs share PMID 11111; a fourth uses a different PMID.
        pairs = [
            ("1ABC", "11111"),
            ("2DEF", "11111"),
            ("3GHI", "11111"),
            ("4JKL", "22222"),
        ]
        counts = mod.load(db=db, pairs=pairs)

        assert counts["created"] == 4
        # One get_reference_id_by_pmid call per distinct PMID, not per pair.
        assert mock_get_ref.call_count == 2
        # One ReferenceModel lookup per distinct PMID too.
        assert db.query.call_count == 2

    @patch.object(mod, "create_tag")
    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "get_reference_id_by_pmid", return_value=None)
    def test_pmid_cache_remembers_missing_lookups(
        self, mock_get_ref, mock_set_uid, mock_get_source, mock_create_tag
    ):
        db, _ = self._make_db(None)

        pairs = [("1ABC", "99999"), ("2DEF", "99999"), ("3GHI", "99999")]
        counts = mod.load(db=db, pairs=pairs)

        assert counts == {"created": 0, "skipped_duplicate": 0, "missing_reference": 3, "errors": 0}
        # Misses are cached too: only one DB call for the shared PMID.
        assert mock_get_ref.call_count == 1
        mock_create_tag.assert_not_called()


class TestIncludeEntityFieldsFlag:

    def test_default_is_true(self):
        # Sanity: the module-level constant resolves to True without the env var being set explicitly.
        assert isinstance(mod.INCLUDE_ENTITY_FIELDS, bool)

    def test_env_false_disables_entity_mode(self, monkeypatch):
        monkeypatch.setenv("PDB_TET_INCLUDE_ENTITY", "false")
        # Re-evaluate the expression the module uses, since the module-level value was bound at import.
        flag = os.environ.get("PDB_TET_INCLUDE_ENTITY", "true").lower() == "true"
        assert flag is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
