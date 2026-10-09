from unittest.mock import MagicMock, patch

from agr_literature_service.api.models import CrossReferenceModel, ReferenceModel
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_references_by_doi \
    import build_doi_search_term, classify_doi_matches, add_pmid_to_existing_papers, get_dois_for_pmids, \
    send_report_for_merging_paper
from ....fixtures import db  # noqa

MODULE = 'agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_references_by_doi'


def _response(status_code, json_body=None):
    response = MagicMock(status_code=status_code)
    response.json.return_value = json_body
    return response


class TestBuildDoiSearchTerm:

    def test_strips_curie_prefix_and_quotes_doi(self):
        # Unquoted, PubMed falls back to automatic term mapping when the DOI is
        # not indexed and splits it into "10.1007"[All Fields] AND "11"[Publisher ID],
        # which matched an unrelated paper (SCRUM-6603).
        assert build_doi_search_term("DOI:10.1007/978-3-0348-8853-0_11") == \
            '"10.1007/978-3-0348-8853-0_11"[DOI]'

    def test_accepts_doi_without_prefix(self):
        assert build_doi_search_term("10.1186/s12874-017-0290-z") == '"10.1186/s12874-017-0290-z"[DOI]'


class TestClassifyDoiMatches:

    def test_accepts_pmid_whose_doi_matches(self):
        candidates = [(983878, "DOI:10.17912/micropub.biology.001013", "38152060")]
        pmid_to_dois = {"38152060": {"10.17912/micropub.biology.001013"}}

        to_add, to_merge, duplicates, rejected, _ = classify_doi_matches(candidates, pmid_to_dois, {})

        assert to_add == [(983878, "PMID:38152060")]
        assert to_merge == [] and duplicates == [] and rejected == []

    def test_doi_comparison_ignores_case(self):
        candidates = [(1, "DOI:10.1093/G3JOURNAL/JKAG266", "40000001")]
        pmid_to_dois = {"40000001": {"10.1093/g3journal/jkag266"}}

        to_add, _, _, rejected, _ = classify_doi_matches(candidates, pmid_to_dois, {})

        assert to_add == [(1, "PMID:40000001")]
        assert rejected == []

    def test_rejects_pmid_whose_doi_does_not_match(self):
        # Real false match seen on stage: this DOI came back as the MDPI Sensors paper.
        candidates = [(1036507, "DOI:10.7488/era/1802", "33807664")]
        pmid_to_dois = {"33807664": {"10.3390/s21051802"}}

        to_add, to_merge, duplicates, rejected, _ = classify_doi_matches(candidates, pmid_to_dois, {})

        assert to_add == [] and to_merge == [] and duplicates == []
        assert rejected == [(1036507, "DOI:10.7488/era/1802", "PMID:33807664")]

    def test_pmid_without_summary_is_unverified_not_rejected(self):
        # The esummary fetch failed, so nothing is known about the DOI: it must
        # not be added, and not be logged as a false match either.
        candidates = [(1, "DOI:10.1/x", "123")]

        to_add, _, _, rejected, unverified = classify_doi_matches(candidates, {}, {})

        assert to_add == [] and rejected == []
        assert unverified == [(1, "DOI:10.1/x", "PMID:123")]

    def test_pmid_already_in_database_goes_to_merge_report(self):
        candidates = [(5, "DOI:10.1/x", "123")]
        pmid_to_dois = {"123": {"10.1/x"}}

        to_add, to_merge, _, _, _ = classify_doi_matches(candidates, pmid_to_dois, {"PMID:123": 9})

        assert to_add == []
        assert to_merge == [("DOI:10.1/x", "PMID:123")]

    def test_two_references_resolving_to_same_pmid_are_not_added(self):
        # A PubMed record can carry more than one DOI; if two DOI-only references
        # both verify against it, they are duplicates of each other, and adding
        # the PMID to both violates idx_curie and rolls back the whole run.
        candidates = [(10, "DOI:10.1/a", "123"), (11, "DOI:10.1/b", "123"), (12, "DOI:10.1/c", "456")]
        pmid_to_dois = {"123": {"10.1/a", "10.1/b"}, "456": {"10.1/c"}}

        to_add, to_merge, duplicates, rejected, _ = classify_doi_matches(candidates, pmid_to_dois, {})

        assert to_add == [(12, "PMID:456")]
        assert duplicates == [("DOI:10.1/a", "PMID:123"), ("DOI:10.1/b", "PMID:123")]
        assert to_merge == [] and rejected == []


class TestAddPmidToExistingPapers:

    def test_conflicting_pmid_is_skipped_without_losing_the_others(self, db):  # noqa
        refs = [ReferenceModel(curie=f"AGR:AGR-Reference-000090000{i}", title=f"doi ref {i}")
                for i in range(3)]
        db.add_all(refs)
        db.flush()
        db.add(CrossReferenceModel(reference_id=refs[0].reference_id, curie="PMID:99990001",
                                   curie_prefix="PMID", is_obsolete=False))
        db.commit()

        added = add_pmid_to_existing_papers(db, [(refs[1].reference_id, "PMID:99990001"),
                                                 (refs[2].reference_id, "PMID:99990002")])
        db.commit()

        assert added == ["PMID:99990002"]
        rows = db.query(CrossReferenceModel.reference_id, CrossReferenceModel.curie).filter(
            CrossReferenceModel.curie.in_(["PMID:99990001", "PMID:99990002"])).all()
        assert sorted(rows) == sorted([(refs[0].reference_id, "PMID:99990001"),
                                       (refs[2].reference_id, "PMID:99990002")])


class TestGetDoisForPmids:

    @patch(f'{MODULE}.time.sleep')
    @patch(f'{MODULE}.requests.post')
    def test_retries_once_when_rate_limited(self, mock_post, _sleep, monkeypatch):
        monkeypatch.setenv('NCBI_API_KEY', 'test')
        summary = {"result": {"123": {"articleids": [{"idtype": "doi", "value": "10.1/X"}]}}}
        mock_post.side_effect = [_response(429), _response(200, summary)]

        assert get_dois_for_pmids(["123"]) == {"123": {"10.1/x"}}
        assert mock_post.call_count == 2

    @patch(f'{MODULE}.time.sleep')
    @patch(f'{MODULE}.requests.post')
    def test_failed_fetch_leaves_pmids_out(self, mock_post, _sleep, monkeypatch):
        monkeypatch.setenv('NCBI_API_KEY', 'test')
        mock_post.return_value = _response(500, {"error": "internal"})

        assert get_dois_for_pmids(["123"]) == {}


class TestSendReportForMergingPaper:

    @patch(f'{MODULE}.send_report')
    def test_duplicates_only_report_does_not_describe_doi_pmid_pairs(self, mock_send_report):
        send_report_for_merging_paper([], [("DOI:10.1/a", "PMID:123"), ("DOI:10.1/b", "PMID:123")])

        subject, message = mock_send_report.call_args.args
        assert "the other has a PMID" not in message
        assert "Paper with PMID" not in message
        assert "DOI:10.1/a" in message and "DOI:10.1/b" in message
        assert subject != "Duplicate Paper Pairs Detected: Merge Required"

    @patch(f'{MODULE}.send_report')
    def test_merge_pairs_report_is_unchanged(self, mock_send_report):
        send_report_for_merging_paper([("DOI:10.1/a", "PMID:123")])

        subject, message = mock_send_report.call_args.args
        assert subject == "Duplicate Paper Pairs Detected: Merge Required"
        assert "the other has a PMID" in message and "DOI:10.1/a" in message
