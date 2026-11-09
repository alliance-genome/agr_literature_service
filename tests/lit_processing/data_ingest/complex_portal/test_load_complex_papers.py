from unittest.mock import MagicMock, patch

import pytest
import requests

from agr_literature_service.lit_processing.data_ingest.complex_portal import load_complex_papers
from agr_literature_service.lit_processing.data_ingest.complex_portal.load_complex_papers import (
    build_report_message,
    classify_pmids,
    extract_pmids_from_complex,
    fetch_complex_pmids,
    get_json,
)


def _response(status_code, json_data=None):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = json_data
    if status_code >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(response=response)
    return response


class TestExtractPmidsFromComplex:

    def test_only_numeric_pubmed_ids(self):
        detail = {"crossReferences": [
            {"database": "pubmed", "identifier": "10871621"},
            {"database": "pubmed", "identifier": "not-a-pmid"},
            {"database": "wwpdb", "identifier": "1ABC"},
            {"database": "evidence ontology", "identifier": "ECO:0000353"},
            {"database": "pubmed", "identifier": "10871621"},
            {"database": "pubmed", "identifier": "9999"},
        ]}
        assert extract_pmids_from_complex(detail) == {"10871621", "9999"}

    def test_no_cross_references(self):
        assert extract_pmids_from_complex({}) == set()
        assert extract_pmids_from_complex({"crossReferences": None}) == set()


class TestGetJson:

    @patch.object(load_complex_papers.time, "sleep")
    @patch.object(load_complex_papers.requests, "get")
    def test_retries_then_succeeds(self, mock_get, mock_sleep):
        mock_get.side_effect = [_response(503), _response(200, {"ok": True})]
        assert get_json("http://x", base_delay=1) == {"ok": True}
        assert mock_get.call_count == 2
        mock_sleep.assert_called_once_with(1)

    @patch.object(load_complex_papers.requests, "get")
    def test_404_is_missing(self, mock_get):
        mock_get.return_value = _response(404)
        assert get_json("http://x") is None

    @patch.object(load_complex_papers.time, "sleep")
    @patch.object(load_complex_papers.requests, "get")
    def test_404_retried_for_search(self, mock_get, mock_sleep):
        mock_get.side_effect = [_response(404), _response(200, {"elements": []})]
        assert get_json("http://x", treat_404_as_missing=False) == {"elements": []}

    @patch.object(load_complex_papers.time, "sleep")
    @patch.object(load_complex_papers.requests, "get")
    def test_persistent_failure_raises(self, mock_get, mock_sleep):
        mock_get.return_value = _response(503)
        with pytest.raises(RuntimeError):
            get_json("http://x", max_retries=3)
        assert mock_get.call_count == 3


class TestFetchComplexPmids:

    @patch.object(load_complex_papers, "get_json")
    def test_collects_pmids_and_failures(self, mock_get_json):
        details = {
            "CPX-1": {"crossReferences": [{"database": "pubmed", "identifier": "111"},
                                          {"database": "pubmed", "identifier": "222"}]},
            "CPX-2": {"crossReferences": [{"database": "pubmed", "identifier": "222"}]},
            "CPX-3": None,
        }

        def fake_get_json(url, treat_404_as_missing=True):
            if url == load_complex_papers.SEARCH_URL:
                return {"elements": [{"complexAC": ac} for ac in ["CPX-1", "CPX-2", "CPX-3", "CPX-4"]]}
            ac = url.rsplit("/", 1)[1]
            if ac == "CPX-4":
                raise RuntimeError("get_json failed")
            return details[ac]

        mock_get_json.side_effect = fake_get_json
        pmid_to_complexes, failed = fetch_complex_pmids()
        assert pmid_to_complexes == {"111": {"CPX-1"}, "222": {"CPX-1", "CPX-2"}}
        assert failed == {"CPX-3", "CPX-4"}


class TestClassifyPmids:

    def test_groups(self):
        groups = classify_pmids(
            all_pmids={"1", "2", "3", "4", "5", "6", "7", "8"},
            all_pmids_db={"1", "2", "3", "4", "6", "7", "8"},
            sgd_status={"1": True, "2": False, "3": None, "7": None, "9": True},
            obsolete_mod_curie={"6", "7"},
        )
        assert groups["in_corpus"] == {"1"}
        assert groups["rejected"] == {"2"}
        assert groups["needs_review"] == {"3"}
        assert groups["not_associated"] == {"4", "8"}
        assert groups["not_in_db"] == {"5"}
        assert groups["obsolete_mod_curie"] == {"6", "7"}

    def test_in_corpus_paper_with_obsolete_curie_stays_in_corpus(self):
        groups = classify_pmids({"1"}, {"1"}, {"1": True}, {"1"})
        assert groups["in_corpus"] == {"1"}
        assert groups["obsolete_mod_curie"] == set()


class TestBuildReportMessage:

    def test_report(self):
        groups = classify_pmids({"1", "2", "3", "4", "5", "6"}, {"1", "2", "3", "4"},
                                {"1": True, "2": False, "3": None}, set())
        message = build_report_message(
            pmid_to_complexes={"1": {"CPX-1"}, "2": {"CPX-2"}, "3": {"CPX-3"}, "4": {"CPX-4"},
                               "5": {"CPX-5"}, "6": {"CPX-5", "CPX-6"}},
            failed_complexes={"CPX-9"},
            groups=groups,
            pmids_added_existing={"4"},
            pmids_added_new={"5"},
            pmids_not_loaded={"6"},
            obsolete_pmids={"6"},
        )
        assert "PMIDs cited by yeast complexes: 6" in message
        assert "In SGD corpus: 1" in message
        assert "Rejected by SGD (left alone): 1" in message
        assert "Already on SGD's needs-review list: 1" in message
        assert "PMID:4 [CPX-4] (existing)" in message
        assert "PMID:5 [CPX-5] (new)" in message
        assert "PMID:6 [CPX-5, CPX-6] (obsolete)" in message
        assert "CPX-9" in message

    def test_dry_run_report(self):
        groups = classify_pmids({"1", "2"}, {"1"}, {}, set())
        message = build_report_message({"1": set(), "2": set()}, set(), groups,
                                       set(), set(), set(), set(), dry_run=True)
        assert "(dry run)" in message
        assert "Would add to SGD needs review (in ABC, not associated): 1" in message
        assert "Would load from PubMed and add to SGD needs review: 1" in message
