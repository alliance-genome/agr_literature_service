from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from agr_literature_service.lit_processing.data_ingest.interaction import mark_curation_status as mcs

MODULE = "agr_literature_service.lit_processing.data_ingest.interaction.mark_curation_status"


class TestGetTopSource:
    def test_empty_returns_none(self):
        assert mcs.get_top_source({}) is None

    def test_picks_source_with_most_rows(self):
        assert mcs.get_top_source({"IntAct": 2, "biogrid": 5}) == "biogrid"

    def test_tie_broken_alphabetically(self):
        assert mcs.get_top_source({"IntAct": 3, "biogrid": 3}) == "IntAct"


class TestFormatAllSources:
    def test_empty_returns_none(self):
        assert mcs.format_all_sources({}) is None

    def test_orders_by_count_desc_then_name(self):
        result = mcs.format_all_sources({"IntAct": 2, "biogrid": 5, "MINT": 1})
        assert result == "biogrid (5), IntAct (2), MINT (1)"

    def test_count_tie_orders_alphabetically(self):
        assert mcs.format_all_sources({"b": 1, "a": 1}) == "a (1), b (1)"


class TestModAbbreviation:
    def test_xenbase_maps_to_xb(self):
        assert mcs._mod_abbreviation("XBXL") == "XB"
        assert mcs._mod_abbreviation("XBXT") == "XB"

    def test_other_datasets_unchanged(self):
        assert mcs._mod_abbreviation("WB") == "WB"


class TestGetReferenceIdsForPmids:
    def test_maps_pmids_to_reference_ids(self):
        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = [
            ("PMID:111", 10), ("PMID:222", 20)
        ]
        result = mcs._get_reference_ids_for_pmids(db, {"111", "222"})
        assert result == {"111": 10, "222": 20}

    def test_empty_input_returns_empty(self):
        db = MagicMock()
        assert mcs._get_reference_ids_for_pmids(db, set()) == {}


class TestGetExistingStatusByReference:
    def test_returns_rows_keyed_by_reference_id(self):
        db = MagicMock()
        row = SimpleNamespace(reference_id=20, curation_status=None)
        db.query.return_value.filter.return_value.all.return_value = [row]
        result = mcs._get_existing_status_by_reference(db, mod_id=1, topic="ATP:0000069",
                                                       reference_ids=[10, 20])
        assert result == {20: row}

    def test_empty_reference_ids_returns_empty(self):
        db = MagicMock()
        assert mcs._get_existing_status_by_reference(db, 1, "ATP:0000069", []) == {}


class TestMarkInteractionCurationComplete:
    def _mod_session(self, mod_id=1):
        db = MagicMock()
        db.query.return_value.filter_by.return_value.one_or_none.return_value = \
            MagicMock(mod_id=mod_id)
        return db

    def test_non_curation_mod_is_noop(self):
        db = MagicMock()
        assert mcs.mark_interaction_curation_complete(db, "FB", "MOL", {"111"}, {}) is None
        db.add.assert_not_called()

    def test_unmapped_data_type_is_noop(self):
        db = self._mod_session()
        assert mcs.mark_interaction_curation_complete(db, "WB", "XXX", {"111"}, {}) is None
        db.add.assert_not_called()

    def test_missing_mod_is_noop(self):
        db = MagicMock()
        db.query.return_value.filter_by.return_value.one_or_none.return_value = None
        assert mcs.mark_interaction_curation_complete(db, "WB", "MOL", {"111"}, {}) is None
        db.add.assert_not_called()

    @patch(f"{MODULE}._get_existing_status_by_reference")
    @patch(f"{MODULE}._get_reference_ids_for_pmids")
    def test_insert_update_and_skip(self, mock_refs, mock_existing):
        db = self._mod_session()
        # 111->10 new(biogrid), 222->20 existing-set(skip),
        # 333->30 new(no source), 444->40 existing-NULL,no-note(fill+provenance)
        mock_refs.return_value = {"111": 10, "222": 20, "333": 30, "444": 40}
        existing_set = SimpleNamespace(curation_status="ATP:0000237", updated_by="curator")
        existing_null = SimpleNamespace(curation_status=None, updated_by="curator", note=None)
        mock_existing.return_value = {20: existing_set, 40: existing_null}
        pmid_to_src_counts = {"111": {"biogrid": 5, "IntAct": 2}, "444": {"MINT": 1}}

        result = mcs.mark_interaction_curation_complete(
            db, "WB", "MOL", {"111", "222", "333", "444"}, pmid_to_src_counts,
            in_corpus_set={"111", "222", "333", "444"})

        assert result == {"topic": "ATP:0000069", "added": 2, "updated": 1, "skipped": 1}

        added = [c.args[0] for c in db.add.call_args_list]
        by_ref = {obj.reference_id: obj for obj in added}
        assert set(by_ref) == {10, 30}
        assert by_ref[10].created_by == "biogrid"
        assert by_ref[10].updated_by == "biogrid"
        assert by_ref[10].note == "biogrid (5), IntAct (2)"
        assert by_ref[10].curation_status == mcs.CURATION_COMPLETE_STATUS
        assert by_ref[30].created_by == "load_interactions"
        assert by_ref[30].note is None
        # NULL-status row filled in place; provenance note added (was empty)
        assert existing_null.curation_status == mcs.CURATION_COMPLETE_STATUS
        assert existing_null.updated_by == "MINT"
        assert existing_null.note == "MINT (1)"
        # curator-set row untouched
        assert existing_set.curation_status == "ATP:0000237"
        db.commit.assert_called()

    @patch(f"{MODULE}._get_existing_status_by_reference")
    @patch(f"{MODULE}._get_reference_ids_for_pmids")
    def test_update_preserves_existing_curator_note(self, mock_refs, mock_existing):
        db = self._mod_session()
        mock_refs.return_value = {"111": 10}
        existing = SimpleNamespace(curation_status=None, updated_by="curator", note="curator note")
        mock_existing.return_value = {10: existing}
        result = mcs.mark_interaction_curation_complete(
            db, "WB", "MOL", {"111"}, {"111": {"biogrid": 2}}, in_corpus_set={"111"})
        assert result["updated"] == 1
        assert existing.curation_status == mcs.CURATION_COMPLETE_STATUS
        assert existing.note == "curator note"  # not overwritten
        db.add.assert_not_called()

    @patch(f"{MODULE}.get_mod_papers")
    @patch(f"{MODULE}._get_existing_status_by_reference", return_value={})
    @patch(f"{MODULE}._get_reference_ids_for_pmids")
    def test_fetches_corpus_when_not_supplied(self, mock_refs, _mock_existing, mock_mod_papers):
        db = self._mod_session()
        mock_mod_papers.return_value = ({"111"}, set())
        mock_refs.return_value = {"111": 10}
        mcs.mark_interaction_curation_complete(db, "WB", "GEN", {"111", "999"}, {})
        mock_mod_papers.assert_called_once()
        # GEN completes the genetic interaction topic, scoped to in-corpus papers
        assert db.add.call_args_list[0].args[0].topic == "ATP:0000068"
        assert mock_refs.call_args.args[1] == {"111"}

    @patch(f"{MODULE}._get_existing_status_by_reference", return_value={})
    @patch(f"{MODULE}._get_reference_ids_for_pmids")
    def test_commit_failure_is_rolled_back_not_raised(self, mock_refs, _mock_existing):
        db = self._mod_session()
        db.commit.side_effect = Exception("deadlock")
        mock_refs.return_value = {"111": 10}
        result = mcs.mark_interaction_curation_complete(
            db, "WB", "MOL", {"111"}, {}, in_corpus_set={"111"})
        assert result is None
        db.rollback.assert_called_once()
