"""Unit tests for the monthly DOI backfill scripts (SCRUM-4525). Pure-function
and MagicMock-based, no database or network required."""

from unittest.mock import MagicMock

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
from agr_literature_service.lit_processing.data_ingest.doi_ingest.add_missing_dois_from_crossref import (
    first_page,
    normalize_title,
    work_matches_candidate,
    work_year,
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


class TestCrossrefMatching:

    def _work(self, title, year=2020, volume="12", page="345-356", doi="10.1234/x"):
        return {"title": [title], "DOI": doi, "volume": volume, "page": page,
                "issued": {"date-parts": [[year]]}}

    def _candidate(self, **kw):
        defaults = dict(reference_id=1, curie="AGRKB:101", title="A tale of two cells",
                        volume="12", page_range="345-356", year="2020")
        defaults.update(kw)
        return Candidate(**defaults)

    def test_normalize_title(self):
        # punctuation/spacing insensitive, and markup (tags, escaped or
        # double-escaped entities) matches its plain-text equivalent
        assert normalize_title("A Tale, of Two <i>Cells</i>!") == normalize_title("a tale of two cells")
        assert normalize_title("&lt;i&gt;C. elegans&lt;/i&gt; aging") == normalize_title("C. elegans aging")
        assert normalize_title("&amp;lt;i&amp;gt;dpp&amp;lt;/i&amp;gt; signaling") == normalize_title("dpp signaling")
        assert normalize_title(None) is None
        assert normalize_title("!!!") is None

    def test_first_page(self):
        assert first_page("345-356") == "345"
        assert first_page("e0123-e0130") == "e0123"
        assert first_page(None) is None

    def test_work_year_prefers_print(self):
        work = {"published-print": {"date-parts": [[2019]]}, "issued": {"date-parts": [[2020]]}}
        assert work_year(work) == 2019

    def test_match_title_and_year(self):
        assert work_matches_candidate(self._work("A tale of two cells."), self._candidate())

    def test_match_year_within_one(self):
        assert work_matches_candidate(self._work("A tale of two cells", year=2021), self._candidate())

    def test_reject_title_mismatch(self):
        assert not work_matches_candidate(self._work("A tale of three cells"), self._candidate())

    def test_reject_year_too_far_without_volume_page(self):
        work = self._work("A tale of two cells", year=2015, volume="99", page="1-2")
        assert not work_matches_candidate(work, self._candidate())

    def test_match_volume_and_page_when_no_year(self):
        work = self._work("A tale of two cells", year=2015)
        assert work_matches_candidate(work, self._candidate(year=None))

    def test_reject_missing_title(self):
        assert not work_matches_candidate({"DOI": "10.1234/x"}, self._candidate())


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
        db = self._db_with_existing([("DOI:10.1234/aaa", 99, False)])
        stats = BackfillStats()
        add_doi_cross_references(db, [(Candidate(reference_id=1, curie="AGRKB:101"), "10.1234/aaa")], stats)
        assert stats.added == 0
        assert stats.conflict_other_reference == 1
        assert stats.conflicts == [("AGRKB:101", "DOI:10.1234/aaa", "99")]
        assert not db.add.called

    def test_curator_removed_doi_is_not_readded(self):
        db = self._db_with_existing([("DOI:10.1234/aaa", 1, True)])
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
