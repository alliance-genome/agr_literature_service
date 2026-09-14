"""Unit coverage for load_geo_topic_tags (SCRUM-6338).

The database query that selects FB references carrying a GEO cross-reference
needs a live Postgres and is covered by the make run-test-bash integration
target; here we assert the behaviour of the helpers and of the load loop, which
takes its references as an argument so it can run without a session.
"""
from unittest.mock import MagicMock, patch

from fastapi import HTTPException

from agr_literature_service.lit_processing.data_ingest.pubmed_ingest import (
    load_geo_topic_tags as mod,
)


EMPTY_COUNTS = {
    "refs_scanned": 0, "tet_created": 0, "tet_skipped_duplicate": 0, "errors": 0,
    "aborted": 0,
}

REFERENCES = [(1, "AGRKB:101000000000001"), (2, "AGRKB:101000000000002")]


class TestBuildTopicTetPayload:

    def test_payload_carries_the_high_throughput_topic_and_no_entity(self):
        payload = mod._build_topic_tet_payload("AGRKB:101000000000001", source_id=42)
        data = payload.dict()
        assert data["reference_curie"] == "AGRKB:101000000000001"
        assert data["topic"] == mod.HIGH_THROUGHPUT_ASSAY_ATP
        assert data["topic_entity_tag_source_id"] == 42
        assert data["negated"] is False
        assert data.get("entity") is None
        assert data.get("entity_type") is None
        assert data.get("species") is None

    def test_payload_sets_data_novelty_and_data_context_explicitly(self):
        """Both are sent rather than left to the server: data_novelty is a
        non-null column, and check_for_duplicate_tags keys on data_context, so
        an omitted value would make re-runs write duplicate rows."""
        data = mod._build_topic_tet_payload("AGRKB:101000000000001", source_id=42).dict()
        assert data["data_novelty"] == mod.DATA_NOVELTY_NOT_NEW
        assert data["data_context"] == mod.DATA_CONTEXT_EXPERIMENTALLY_STUDIED


class TestGetOrCreateSource:

    def _db_returning(self, existing):
        db = MagicMock()
        db.query.return_value.filter_by.return_value.one.return_value = MagicMock(mod_id=7)
        db.query.return_value.filter_by.return_value.one_or_none.return_value = existing
        return db

    def test_reuses_an_existing_source(self):
        db = self._db_returning(MagicMock(topic_entity_tag_source_id=99))
        assert mod.get_or_create_source(db, "FB") == 99
        db.add.assert_not_called()

    def test_reports_a_missing_source_without_creating_it_when_asked_not_to(self):
        """A dry run must leave the database untouched, and creating the source
        row is a write like any other."""
        db = self._db_returning(None)

        assert mod.get_or_create_source(db, "FB", create=False) is None
        db.add.assert_not_called()
        db.commit.assert_not_called()

    def test_creates_the_source_with_the_values_the_ticket_specifies(self):
        db = self._db_returning(None)

        mod.get_or_create_source(db, "FB")

        db.add.assert_called_once()
        source = db.add.call_args[0][0]
        assert source.source_evidence_assertion == mod.ECO_AUTOMATIC_ASSERTION
        assert source.source_method == mod.SOURCE_METHOD
        assert source.data_provider == mod.SOURCE_DATA_PROVIDER
        assert source.validation_type is None
        # secondary_data_provider is what scopes the tag to a MOD's grid; the
        # mod row looked up was FB's (mod_id=7).
        assert source.secondary_data_provider_id == 7


class TestLoad:

    @patch.object(mod, "create_tag", return_value=(123, False))
    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod, "set_global_user_id")
    def test_creates_one_tag_per_reference(self, _uid, _source, mock_create_tag):
        counts = mod.load(db=MagicMock(), references=REFERENCES)

        assert counts == {**EMPTY_COUNTS, "refs_scanned": 2, "tet_created": 2}
        assert mock_create_tag.call_count == 2
        tagged = [c.args[1].dict()["reference_curie"] for c in mock_create_tag.call_args_list]
        assert tagged == [curie for _id, curie in REFERENCES]

    @patch.object(mod, "create_tag",
                  side_effect=HTTPException(status_code=409, detail="duplicate"))
    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod, "set_global_user_id")
    def test_existing_tag_counted_as_skipped(self, _uid, _source, _create_tag):
        counts = mod.load(db=MagicMock(), references=REFERENCES[:1])
        assert counts == {**EMPTY_COUNTS, "refs_scanned": 1, "tet_skipped_duplicate": 1}

    @patch.object(mod, "create_tag", return_value=(123, True))
    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod, "set_global_user_id")
    def test_upsert_onto_an_existing_tag_counted_as_skipped(self, _uid, _source, _create_tag):
        """create_tag returns was_upsert=True when an existing tag absorbed the
        request -- nothing new was written, so it is not a creation."""
        counts = mod.load(db=MagicMock(), references=REFERENCES[:1])
        assert counts == {**EMPTY_COUNTS, "refs_scanned": 1, "tet_skipped_duplicate": 1}

    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod, "set_global_user_id")
    def test_unexpected_failure_rolls_back_and_processing_continues(self, _uid, _source):
        db = MagicMock()
        with patch.object(mod, "create_tag",
                          side_effect=[RuntimeError("server closed"), (123, False)]):
            counts = mod.load(db=db, references=REFERENCES)

        assert counts == {**EMPTY_COUNTS, "refs_scanned": 2, "tet_created": 1, "errors": 1}
        db.rollback.assert_called_once()

    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod, "set_global_user_id")
    def test_http_error_other_than_409_counted_as_error(self, _uid, _source):
        db = MagicMock()
        with patch.object(mod, "create_tag",
                          side_effect=HTTPException(status_code=500, detail="boom")):
            counts = mod.load(db=db, references=REFERENCES[:1])

        assert counts == {**EMPTY_COUNTS, "refs_scanned": 1, "errors": 1}

    @patch.object(mod, "create_tag")
    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod, "set_global_user_id")
    def test_dry_run_reports_the_work_without_writing(self, _uid, _source, mock_create_tag):
        counts = mod.load(db=MagicMock(), references=REFERENCES, dry_run=True)

        assert counts == {**EMPTY_COUNTS, "refs_scanned": 2, "tet_created": 2}
        mock_create_tag.assert_not_called()

    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod, "set_global_user_id")
    def test_gives_up_after_a_run_of_consecutive_failures(self, _uid, _source):
        """A misconfigured A-team connection makes every ATP id look invalid, so
        every tag fails for the same reason. Stop and say so rather than logging
        the same failure thousands of times."""
        references = [(i, f"AGRKB:10100000000{i:04d}") for i in range(20)]
        invalid = HTTPException(status_code=422, detail="ATP:0000150 is not valid.")

        with patch.object(mod, "create_tag", side_effect=invalid) as mock_create_tag:
            counts = mod.load(db=MagicMock(), references=references)

        assert mock_create_tag.call_count == mod.MAX_CONSECUTIVE_ERRORS
        assert counts["errors"] == mod.MAX_CONSECUTIVE_ERRORS
        assert counts["aborted"] == 1
        assert counts["tet_created"] == 0

    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod, "set_global_user_id")
    def test_failures_broken_up_by_successes_do_not_abort(self, _uid, _source):
        """Only an unbroken run means something systemic; scattered failures are
        ordinary and must not stop the run."""
        references = [(i, f"AGRKB:10100000000{i:04d}") for i in range(20)]
        boom = HTTPException(status_code=422, detail="nope")
        # Four failures, one success, repeating: never MAX_CONSECUTIVE_ERRORS in a row.
        side_effects = []
        for i in range(20):
            side_effects.append((123, False) if i % 5 == 4 else boom)

        with patch.object(mod, "create_tag", side_effect=side_effects) as mock_create_tag:
            counts = mod.load(db=MagicMock(), references=references)

        assert mock_create_tag.call_count == 20
        assert counts["aborted"] == 0
        assert counts["tet_created"] == 4
        assert counts["errors"] == 16

    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod, "set_global_user_id")
    def test_duplicate_skips_are_not_failures(self, _uid, _source):
        """A long run of 409s is the expected shape of a re-run, not a reason to
        abort."""
        references = [(i, f"AGRKB:10100000000{i:04d}") for i in range(20)]
        dup = HTTPException(status_code=409, detail="duplicate")

        with patch.object(mod, "create_tag", side_effect=dup) as mock_create_tag:
            counts = mod.load(db=MagicMock(), references=references)

        assert mock_create_tag.call_count == 20
        assert counts["aborted"] == 0
        assert counts["tet_skipped_duplicate"] == 20

    @patch.object(mod, "create_tag")
    def test_dry_run_writes_nothing_at_all(self, mock_create_tag):
        """Not just no tags: no automation user row and no source row either.
        set_global_user_id and get_or_create_source both INSERT."""
        db = MagicMock()
        db.query.return_value.filter_by.return_value.one_or_none.return_value = None

        with patch.object(mod, "set_global_user_id") as mock_set_uid:
            counts = mod.load(db=db, references=REFERENCES, dry_run=True)

        assert counts == {**EMPTY_COUNTS, "refs_scanned": 2, "tet_created": 2}
        mock_create_tag.assert_not_called()
        mock_set_uid.assert_not_called()
        db.add.assert_not_called()
        db.commit.assert_not_called()

    @patch.object(mod, "create_tag", return_value=(123, False))
    @patch.object(mod, "get_or_create_source", return_value=42)
    @patch.object(mod, "set_global_user_id")
    def test_queries_for_references_when_none_are_supplied(self, _uid, _source, _create_tag):
        db = MagicMock()
        with patch.object(mod, "_references_with_geo_xref",
                          return_value=REFERENCES) as mock_query:
            counts = mod.load(db=db, mod_abbreviation="FB", limit=5, since_days=7)

        assert counts["tet_created"] == 2
        mock_query.assert_called_once_with(db, mod_abbreviation="FB", limit=5, since_days=7)
