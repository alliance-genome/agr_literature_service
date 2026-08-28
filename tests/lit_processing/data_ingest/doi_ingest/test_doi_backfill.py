"""Unit tests for the monthly DOI backfill scripts (SCRUM-4525). Pure-function
and MagicMock-based, no database or network required."""

from unittest.mock import MagicMock

from sqlalchemy.exc import IntegrityError

from agr_literature_service.lit_processing.data_ingest.doi_ingest.doi_backfill_utils import (
    BackfillStats,
    Candidate,
    add_doi_cross_references,
    normalize_doi,
)
from agr_literature_service.lit_processing.data_ingest.doi_ingest.add_missing_dois_from_europepmc import (
    collect_additions as europepmc_collect_additions,
    fetch_dois_for_pmids,
)


class TestNormalizeDoi:

    def test_plain_doi_passes_through(self):
        assert normalize_doi("10.1371/journal.pone.0123456") == "10.1371/journal.pone.0123456"

    def test_wrappers_are_stripped(self):
        for raw in ("doi:10.1234/abc", "DOI:10.1234/abc", "https://doi.org/10.1234/abc",
                    "http://dx.doi.org/10.1234/abc", "  10.1234/abc  "):
            assert normalize_doi(raw) == "10.1234/abc"

    def test_case_of_suffix_preserved(self):
        assert normalize_doi("10.1234/AbC.DeF") == "10.1234/AbC.DeF"

    def test_invalid_dois_rejected(self):
        for raw in (None, "", "not-a-doi", "10.1234", "11.1234/abc", "10.12/abc", "10.1234/with space"):
            assert normalize_doi(raw) is None


class TestEuropePmcFetch:

    def _session_returning(self, results):
        session = MagicMock()
        response = MagicMock()
        response.json.return_value = {"resultList": {"result": results}}
        response.raise_for_status.return_value = None
        session.get.return_value = response
        return session

    def test_fetch_maps_pmid_to_doi(self):
        session = self._session_returning([
            {"id": "111", "doi": "10.1234/aaa"},
            {"id": "222"},  # no DOI in Europe PMC
        ])
        assert fetch_dois_for_pmids(session, ["111", "222"]) == {"111": "10.1234/aaa"}
        query = session.get.call_args.kwargs["params"]["query"]
        assert "SRC:MED" in query and "EXT_ID:111" in query and "EXT_ID:222" in query

    def test_collect_additions_pairs_candidates(self):
        session = self._session_returning([{"id": "111", "doi": "10.1234/aaa"}])
        cand = Candidate(reference_id=1, curie="AGRKB:101", pmid="111")
        no_pmid = Candidate(reference_id=2, curie="AGRKB:102", pmid=None)
        stats = BackfillStats()
        additions = europepmc_collect_additions([cand, no_pmid], 100, stats, session=session)
        assert additions == [(cand, "10.1234/aaa")]
        assert stats.dois_found == 1


class TestAddDoiCrossReferences:

    def _db_with_existing(self, rows):
        db = MagicMock()
        db.execute.return_value.fetchall.return_value = rows
        return db

    def test_adds_new_doi(self):
        db = self._db_with_existing([])
        stats = BackfillStats()
        cand = Candidate(reference_id=1, curie="AGRKB:101")
        add_doi_cross_references(db, [(cand, "10.1234/aaa")], stats)
        assert stats.added == 1
        assert db.add.call_count == 1
        xref = db.add.call_args.args[0]
        assert xref.curie == "DOI:10.1234/aaa"
        assert xref.curie_prefix == "DOI"
        assert xref.reference_id == 1
        assert db.commit.called

    def test_dry_run_writes_nothing(self):
        db = self._db_with_existing([])
        stats = BackfillStats()
        add_doi_cross_references(db, [(Candidate(reference_id=1, curie="AGRKB:101"), "10.1234/aaa")],
                                 stats, dry_run=True)
        assert stats.added == 1
        assert not db.add.called
        assert not db.commit.called

    def test_conflict_with_other_reference_is_skipped_and_reported(self):
        db = self._db_with_existing([("DOI:10.1234/aaa", 99, False, "AGRKB:999")])
        stats = BackfillStats()
        add_doi_cross_references(db, [(Candidate(reference_id=1, curie="AGRKB:101"), "10.1234/aaa")], stats)
        assert stats.added == 0
        assert stats.conflict_other_reference == 1
        assert stats.conflicts == [("AGRKB:101", "DOI:10.1234/aaa", "AGRKB:999")]
        assert not db.add.called

    def test_curator_removed_doi_is_not_readded(self):
        db = self._db_with_existing([("DOI:10.1234/aaa", 1, True, "AGRKB:101")])
        stats = BackfillStats()
        add_doi_cross_references(db, [(Candidate(reference_id=1, curie="AGRKB:101"), "10.1234/aaa")], stats)
        assert stats.added == 0
        assert stats.removed_by_curator == 1
        assert not db.add.called

    def test_invalid_doi_is_counted_not_added(self):
        db = self._db_with_existing([])
        stats = BackfillStats()
        add_doi_cross_references(db, [(Candidate(reference_id=1, curie="AGRKB:101"), "garbage")], stats)
        assert stats.invalid_doi == 1
        assert stats.added == 0
        assert not db.add.called

    def test_duplicate_doi_within_run_second_reference_conflicts(self):
        db = self._db_with_existing([])
        stats = BackfillStats()
        additions = [
            (Candidate(reference_id=1, curie="AGRKB:101"), "10.1234/aaa"),
            (Candidate(reference_id=2, curie="AGRKB:102"), "10.1234/aaa"),
        ]
        add_doi_cross_references(db, additions, stats)
        assert stats.added == 1
        assert stats.conflict_other_reference == 1
        assert stats.conflicts == [("AGRKB:102", "DOI:10.1234/aaa", "AGRKB:101")]

    def test_dry_run_reports_intra_run_duplicate_like_a_real_run(self):
        """The dry run must preview the same add/conflict split as a real run
        when two candidates resolve to the same DOI."""
        db = self._db_with_existing([])
        stats = BackfillStats()
        additions = [
            (Candidate(reference_id=1, curie="AGRKB:101"), "10.1234/aaa"),
            (Candidate(reference_id=2, curie="AGRKB:102"), "10.1234/aaa"),
        ]
        add_doi_cross_references(db, additions, stats, dry_run=True)
        assert stats.added == 1
        assert stats.conflict_other_reference == 1
        assert stats.conflicts == [("AGRKB:102", "DOI:10.1234/aaa", "AGRKB:101")]
        assert not db.add.called

    def test_curator_removed_doi_blocks_case_variants(self):
        db = self._db_with_existing([("DOI:10.1234/ABC", 1, True, "AGRKB:101")])
        stats = BackfillStats()
        add_doi_cross_references(db, [(Candidate(reference_id=1, curie="AGRKB:101"), "10.1234/abc")], stats)
        assert stats.removed_by_curator == 1
        assert stats.added == 0
        assert not db.add.called

    def test_conflict_detected_across_case_variants(self):
        db = self._db_with_existing([("DOI:10.1234/AbC", 99, False, "AGRKB:999")])
        stats = BackfillStats()
        add_doi_cross_references(db, [(Candidate(reference_id=1, curie="AGRKB:101"), "10.1234/abc")], stats)
        assert stats.conflict_other_reference == 1
        assert stats.added == 0
        assert not db.add.called

    def test_concurrent_writer_race_skips_row_instead_of_aborting(self):
        """A unique-index race at commit time downgrades to a per-row retry;
        the losing row is reported, not raised, and stats reflect reality."""
        db = self._db_with_existing([])
        db.commit.side_effect = [
            IntegrityError("stmt", {}, Exception("duplicate key")),  # batch commit
            IntegrityError("stmt", {}, Exception("duplicate key")),  # per-row retry
        ]
        stats = BackfillStats()
        add_doi_cross_references(db, [(Candidate(reference_id=1, curie="AGRKB:101"), "10.1234/aaa")], stats)
        assert stats.added == 0
        assert stats.lost_race == 1
        assert stats.conflict_other_reference == 0
        assert stats.conflicts == [("AGRKB:101", "DOI:10.1234/aaa", "concurrent writer")]
        assert db.rollback.call_count == 2
