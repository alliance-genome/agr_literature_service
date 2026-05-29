"""Tests for load_pdb_associations.py"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from agr_literature_service.lit_processing.data_ingest.pdb_ingest import (
    load_pdb_associations as mod,
)


EMPTY_COUNTS = {
    "created": 0, "skipped_duplicate": 0, "missing_reference": 0,
    "errors": 0, "deleted_stale": 0,
    "topic_tet_created": 0, "topic_tet_skipped_duplicate": 0,
    "topic_tet_deleted_stale": 0,
}


class TestBuildPayloads:

    def test_build_xref_payload_uppercases_pdb_id(self):
        payload = mod._build_xref_payload("AGR:R1", "1abc")
        data = payload.dict()
        assert data["curie"] == "PDB:1ABC"
        assert data["reference_curie"] == "AGR:R1"
        assert data["pages"] == ["reference"]

    def test_build_topic_tet_payload_has_no_entity_fields(self):
        payload = mod._build_topic_tet_payload("AGR:R1", source_id=42)
        data = payload.dict()
        assert data["reference_curie"] == "AGR:R1"
        assert data["topic"] == mod.PROTEIN_STRUCTURE_ATP
        assert data["topic_entity_tag_source_id"] == 42
        assert data["data_novelty"] == mod.DATA_NOVELTY_NOT_NEW
        assert data["negated"] is False
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
                     "pubmed": {"rcsb_pubmed_container_identifiers": {"pubmed_id": 11111}}},
                    {"rcsb_id": "2XYZ",
                     "pubmed": {"rcsb_pubmed_container_identifiers": {"pubmed_id": 22222}}},
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
                     "pubmed": {"rcsb_pubmed_container_identifiers": {"pubmed_id": 11111}}},
                    {"rcsb_id": "2XYZ",
                     "pubmed": None},
                    {"rcsb_id": "3DEF",
                     "pubmed": {"rcsb_pubmed_container_identifiers": None}},
                    {"rcsb_id": "4GHI",
                     "pubmed": {"rcsb_pubmed_container_identifiers": {"pubmed_id": None}}},
                ]
            }
        }
        result = mod._fetch_pubmed_ids_graphql(["1ABC", "2XYZ", "3DEF", "4GHI"])
        assert result == {"1ABC": "11111"}

    @patch.object(mod, "_post_with_retry")
    def test_handles_empty_response(self, mock_post):
        mock_post.return_value = {"data": {"entries": []}}
        assert mod._fetch_pubmed_ids_graphql(["1ABC"]) == {}

    @patch.object(mod, "_post_with_retry")
    def test_raises_on_graphql_errors(self, mock_post):
        mock_post.return_value = {
            "errors": [
                {"message": "Validation error (FieldUndefined@[entries/foo]): "
                            "Field 'foo' in type 'CoreEntry' is undefined"}
            ]
        }
        with pytest.raises(RuntimeError, match="RCSB GraphQL error"):
            mod._fetch_pubmed_ids_graphql(["1ABC"])


class TestFetchAllPdbIdsWithPubmed:

    @patch.object(mod, "_post_with_retry")
    def test_paginates_until_short_page(self, mock_post):
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

    def _make_db(self, reference_curie="AGR:AGR-Reference-0000000001"):
        db = MagicMock()
        ref = MagicMock(curie=reference_curie)
        db.query.return_value.filter_by.return_value.one.return_value = ref
        return db, ref

    @patch.object(mod, "create_tag", return_value=(123, False))
    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod.cross_reference_crud, "create")
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "get_reference_id_by_pmid")
    def test_happy_path_creates_xref_and_topic_tet(
        self, mock_get_ref, mock_set_uid, mock_create, mock_source, mock_create_tag,
    ):
        mock_get_ref.return_value = 1234
        mock_create.return_value = 999
        db, _ = self._make_db()

        counts = mod.load(db=db, pairs=[("1abc", "11111")])

        assert counts == {
            **EMPTY_COUNTS, "created": 1, "topic_tet_created": 1,
        }
        mock_create.assert_called_once()
        _db_arg, xref_payload = mock_create.call_args[0]
        assert xref_payload.dict()["curie"] == "PDB:1ABC"
        # One topic-only TET created for the single touched reference.
        mock_create_tag.assert_called_once()
        tet_payload = mock_create_tag.call_args[0][1].dict()
        assert tet_payload["topic"] == mod.PROTEIN_STRUCTURE_ATP
        assert tet_payload["reference_curie"] == "AGR:AGR-Reference-0000000001"
        assert tet_payload["topic_entity_tag_source_id"] == 42
        assert tet_payload.get("entity") is None

    @patch.object(mod, "create_tag", return_value=(123, False))
    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod.cross_reference_crud, "create")
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "get_reference_id_by_pmid", return_value=None)
    def test_missing_reference_skips_xref_and_topic_tet(
        self, mock_get_ref, mock_set_uid, mock_create, mock_source, mock_create_tag,
    ):
        db = MagicMock()
        counts = mod.load(db=db, pairs=[("1ABC", "99999")])
        assert counts == {**EMPTY_COUNTS, "missing_reference": 1}
        mock_create.assert_not_called()
        mock_create_tag.assert_not_called()

    @patch.object(mod, "create_tag", return_value=(123, False))
    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod.cross_reference_crud, "create")
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "get_reference_id_by_pmid", return_value=1234)
    def test_409_xref_returns_are_counted_as_skipped(
        self, mock_get_ref, mock_set_uid, mock_create, mock_source, mock_create_tag,
    ):
        mock_create.side_effect = HTTPException(status_code=409, detail="duplicate")
        db, _ = self._make_db()
        counts = mod.load(db=db, pairs=[("1ABC", "11111")])
        assert counts == {
            **EMPTY_COUNTS, "skipped_duplicate": 1, "topic_tet_created": 1,
        }

    @patch.object(mod, "create_tag",
                  side_effect=HTTPException(status_code=409, detail={"reason": "duplicate"}))
    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod.cross_reference_crud, "create", return_value=1)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "get_reference_id_by_pmid", return_value=1234)
    def test_existing_topic_tet_counted_as_skipped_duplicate(
        self, mock_get_ref, mock_set_uid, mock_create, mock_source, mock_create_tag,
    ):
        db, _ = self._make_db()
        counts = mod.load(db=db, pairs=[("1ABC", "11111")])
        assert counts == {
            **EMPTY_COUNTS, "created": 1, "topic_tet_skipped_duplicate": 1,
        }

    @patch.object(mod, "create_tag", return_value=(123, False))
    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod.cross_reference_crud, "create", return_value=1)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "get_reference_id_by_pmid")
    def test_one_topic_tet_per_reference_not_per_pdb_id(
        self, mock_get_ref, mock_set_uid, mock_create, mock_source, mock_create_tag,
    ):
        mock_get_ref.return_value = 1234
        db, _ = self._make_db()

        pairs = [("1ABC", "11111"), ("2DEF", "11111"), ("3GHI", "11111")]
        counts = mod.load(db=db, pairs=pairs)

        assert counts["created"] == 3
        # All three PDB IDs share one reference -> exactly one topic TET call.
        assert mock_create_tag.call_count == 1
        assert counts["topic_tet_created"] == 1

    @patch.object(mod, "create_tag", side_effect=RuntimeError("boom"))
    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod.cross_reference_crud, "create", return_value=1)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "get_reference_id_by_pmid", return_value=1234)
    def test_topic_tet_exception_counted_as_error(
        self, mock_get_ref, mock_set_uid, mock_create, mock_source, mock_create_tag,
    ):
        db, _ = self._make_db()
        counts = mod.load(db=db, pairs=[("1ABC", "11111")])
        # xref created OK; topic-only TET creation raised.
        assert counts["created"] == 1
        assert counts["errors"] == 1
        assert counts["topic_tet_created"] == 0


class TestStaleCleanup:

    @patch.object(mod, "_delete_stale_topic_tets", return_value=4)
    @patch.object(mod, "_delete_stale_xrefs", return_value=7)
    @patch.object(mod, "create_tag", return_value=(123, False))
    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod.cross_reference_crud, "create", return_value=1)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "get_reference_id_by_pmid", return_value=1234)
    def test_cleanup_runs_for_xrefs_and_topic_tets(
        self, mock_get_ref, mock_set_uid, mock_create, mock_source, mock_create_tag,
        mock_del_xrefs, mock_del_tets, monkeypatch,
    ):
        monkeypatch.setattr(mod, "PDB_CLEANUP_MIN_PAIRS", 2)
        db = MagicMock()
        db.query.return_value.filter_by.return_value.one.return_value = MagicMock(curie="AGR:R1")

        counts = mod.load(db=db, pairs=[("1abc", "11111"), ("2def", "11111")])

        assert counts["deleted_stale"] == 7
        assert counts["topic_tet_deleted_stale"] == 4
        mock_del_xrefs.assert_called_once()
        _db_arg, current_curies_arg = mock_del_xrefs.call_args[0]
        assert current_curies_arg == {"PDB:1ABC", "PDB:2DEF"}
        mock_del_tets.assert_called_once()
        _db_arg2, source_arg, current_ref_ids_arg = mock_del_tets.call_args[0]
        assert source_arg == 42
        assert current_ref_ids_arg == {1234}

    @patch.object(mod, "_delete_stale_topic_tets")
    @patch.object(mod, "_delete_stale_xrefs")
    @patch.object(mod, "create_tag", return_value=(123, False))
    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod.cross_reference_crud, "create", return_value=1)
    @patch.object(mod, "set_global_user_id")
    @patch.object(mod, "get_reference_id_by_pmid", return_value=1234)
    def test_cleanup_skipped_when_below_threshold(
        self, mock_get_ref, mock_set_uid, mock_create, mock_source, mock_create_tag,
        mock_del_xrefs, mock_del_tets, monkeypatch,
    ):
        monkeypatch.setattr(mod, "PDB_CLEANUP_MIN_PAIRS", 1000)
        db = MagicMock()
        db.query.return_value.filter_by.return_value.one.return_value = MagicMock(curie="AGR:R1")

        counts = mod.load(db=db, pairs=[("1ABC", "11111")])

        assert counts["deleted_stale"] == 0
        assert counts["topic_tet_deleted_stale"] == 0
        mock_del_xrefs.assert_not_called()
        mock_del_tets.assert_not_called()


class TestDeleteStaleXrefs:

    def test_deletes_only_rows_not_in_current_curies(self):
        db = MagicMock()
        row1 = MagicMock(cross_reference_id=101, curie="PDB:1ABC")
        row2 = MagicMock(cross_reference_id=102, curie="PDB:2DEF")
        row3 = MagicMock(cross_reference_id=103, curie="PDB:3GHI")
        row4 = MagicMock(cross_reference_id=104, curie="PDB:4JKL")
        db.query.return_value.filter.return_value.all.return_value = [row1, row2, row3, row4]
        delete_result = MagicMock(rowcount=2)
        db.execute.return_value = delete_result

        current_curies = {"PDB:1ABC", "PDB:3GHI"}
        deleted = mod._delete_stale_xrefs(db, current_curies=current_curies)

        assert deleted == 2
        executed_stmt = db.execute.call_args[0][0]
        compiled = executed_stmt.compile(compile_kwargs={"literal_binds": True})
        sql = str(compiled).lower()
        assert "102" in sql and "104" in sql
        assert "101" not in sql and "103" not in sql
        db.commit.assert_called_once()

    def test_returns_zero_and_skips_delete_when_nothing_stale(self):
        db = MagicMock()
        row = MagicMock(cross_reference_id=101, curie="PDB:1ABC")
        db.query.return_value.filter.return_value.all.return_value = [row]
        current_curies = {"PDB:1ABC"}

        deleted = mod._delete_stale_xrefs(db, current_curies=current_curies)

        assert deleted == 0
        db.execute.assert_not_called()
        db.commit.assert_not_called()


class TestDeleteStaleTopicTets:

    def test_deletes_only_rows_not_in_current_reference_ids(self):
        db = MagicMock()
        row1 = MagicMock(topic_entity_tag_id=10, reference_id=1)
        row2 = MagicMock(topic_entity_tag_id=11, reference_id=2)
        row3 = MagicMock(topic_entity_tag_id=12, reference_id=3)
        db.query.return_value.filter.return_value.all.return_value = [row1, row2, row3]
        db.execute.return_value = MagicMock(rowcount=2)

        deleted = mod._delete_stale_topic_tets(
            db, source_id=42, current_reference_ids={2},
        )

        assert deleted == 2
        executed_stmt = db.execute.call_args[0][0]
        compiled = executed_stmt.compile(compile_kwargs={"literal_binds": True})
        sql = str(compiled).lower()
        assert "10" in sql and "12" in sql
        db.commit.assert_called_once()

    def test_returns_zero_and_skips_delete_when_nothing_stale(self):
        db = MagicMock()
        row = MagicMock(topic_entity_tag_id=10, reference_id=1)
        db.query.return_value.filter.return_value.all.return_value = [row]

        deleted = mod._delete_stale_topic_tets(
            db, source_id=42, current_reference_ids={1},
        )

        assert deleted == 0
        db.execute.assert_not_called()
        db.commit.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
