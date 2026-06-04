import json
from unittest.mock import patch, MagicMock

import pytest
import requests

from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_geo_links import (
    get_geo_accessions_for_pmid,
    get_geo_accessions_for_pmids,
    get_geo_accessions_for_pmids_with_split,
)


def _mock_response(payload, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = json.dumps(payload)
    resp.json.return_value = payload
    resp.raise_for_status = MagicMock()
    return resp


ELINK_TWO_LINKS = {
    "linksets": [
        {
            "dbfrom": "pubmed",
            "ids": ["32606685"],
            "linksetdbs": [
                {"dbto": "gds", "linkname": "pubmed_gds", "links": ["200073427", "200050899"]}
            ],
        }
    ]
}

ESUMMARY_MIXED = {
    "result": {
        "uids": ["200073427", "200050899", "100012345"],
        "200073427": {"uid": "200073427", "accession": "GSE73427", "entrytype": "GSE",
                      "pubmedids": ["32606685"]},
        "200050899": {"uid": "200050899", "accession": "GSE50899", "entrytype": "GSE",
                      "pubmedids": ["32606685"]},
        "100012345": {"uid": "100012345", "accession": "GPL12345", "entrytype": "GPL",
                      "pubmedids": ["32606685"]},
    }
}


class TestGetGeoAccessionsForPmid:

    def test_returns_sorted_gse_accessions_for_pmid_with_geo_links(self):
        elink = _mock_response(ELINK_TWO_LINKS)
        esummary = _mock_response({
            "result": {
                "uids": ["200073427", "200050899"],
                "200073427": {"uid": "200073427", "accession": "GSE73427", "entrytype": "GSE",
                              "pubmedids": ["32606685"]},
                "200050899": {"uid": "200050899", "accession": "GSE50899", "entrytype": "GSE",
                              "pubmedids": ["32606685"]},
            }
        })
        with patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest."
                   "get_geo_links.requests.get",
                   side_effect=[elink, esummary]):
            assert get_geo_accessions_for_pmid("32606685") == ["GSE50899", "GSE73427"]

    def test_filters_out_non_gse_entrytypes(self):
        """elink may return GPL (platforms) and GSM (samples). Keep only GSE Series."""
        elink_three = {
            "linksets": [
                {
                    "dbfrom": "pubmed",
                    "ids": ["32606685"],
                    "linksetdbs": [{"dbto": "gds", "linkname": "pubmed_gds",
                                    "links": ["200073427", "200050899", "100012345"]}],
                }
            ]
        }
        with patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest."
                   "get_geo_links.requests.get",
                   side_effect=[_mock_response(elink_three), _mock_response(ESUMMARY_MIXED)]):
            assert get_geo_accessions_for_pmid("32606685") == ["GSE50899", "GSE73427"]

    def test_returns_empty_list_when_pmid_has_no_geo_links(self):
        elink_empty = {"linksets": [{"dbfrom": "pubmed", "ids": ["1"], "linksetdbs": []}]}
        with patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest."
                   "get_geo_links.requests.get",
                   side_effect=[_mock_response(elink_empty)]) as mock_get:
            assert get_geo_accessions_for_pmid("1") == []
            assert mock_get.call_count == 1

    def test_returns_empty_list_when_linkset_omits_linksetdbs_field(self):
        elink_omitted = {"linksets": [{"dbfrom": "pubmed", "ids": ["1"]}]}
        with patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest."
                   "get_geo_links.requests.get",
                   side_effect=[_mock_response(elink_omitted)]):
            assert get_geo_accessions_for_pmid("1") == []

    def test_deduplicates_accessions(self):
        elink_dup = {
            "linksets": [{"dbfrom": "pubmed", "ids": ["1"],
                          "linksetdbs": [{"dbto": "gds", "linkname": "pubmed_gds",
                                          "links": ["1", "2"]}]}]
        }
        esummary_dup = {
            "result": {
                "uids": ["1", "2"],
                "1": {"uid": "1", "accession": "GSE111", "entrytype": "GSE", "pubmedids": ["1"]},
                "2": {"uid": "2", "accession": "GSE111", "entrytype": "GSE", "pubmedids": ["1"]},
            }
        }
        with patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest."
                   "get_geo_links.requests.get",
                   side_effect=[_mock_response(elink_dup), _mock_response(esummary_dup)]):
            assert get_geo_accessions_for_pmid("1") == ["GSE111"]

    def test_retries_on_transient_request_exception_then_succeeds(self):
        elink = _mock_response(ELINK_TWO_LINKS)
        esummary = _mock_response({
            "result": {"uids": ["200073427", "200050899"],
                       "200073427": {"uid": "200073427", "accession": "GSE73427", "entrytype": "GSE",
                                     "pubmedids": ["32606685"]},
                       "200050899": {"uid": "200050899", "accession": "GSE50899", "entrytype": "GSE",
                                     "pubmedids": ["32606685"]}}
        })
        with patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest."
                   "get_geo_links.time.sleep"), \
             patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest."
                   "get_geo_links.requests.get",
                   side_effect=[requests.exceptions.ConnectionError("boom"), elink, esummary]):
            assert get_geo_accessions_for_pmid("32606685") == ["GSE50899", "GSE73427"]

    def test_passes_api_key_when_env_var_set(self, monkeypatch):
        monkeypatch.setenv("NCBI_API_KEY", "secret-key-123")
        elink_empty = {"linksets": [{"dbfrom": "pubmed", "ids": ["1"], "linksetdbs": []}]}
        with patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest."
                   "get_geo_links.requests.get",
                   return_value=_mock_response(elink_empty)) as mock_get:
            get_geo_accessions_for_pmid("1")
            params = mock_get.call_args.kwargs["params"]
            assert params.get("api_key") == "secret-key-123"


class TestGetGeoAccessionsForPmids:

    def test_groups_links_per_source_pmid(self):
        # elink merges the batch into one linkset (no per-PMID attribution); we
        # only use it to gather candidate GDS UIDs. Attribution comes from each
        # esummary record's own `pubmedids`.
        elink_merged = {
            "linksets": [
                {"dbfrom": "pubmed", "ids": ["111", "222", "333"],
                 "linksetdbs": [{"dbto": "gds", "linkname": "pubmed_gds",
                                 "links": ["1", "2", "3"]}]},
            ]
        }
        esummary_multi = {
            "result": {
                "uids": ["1", "2", "3"],
                "1": {"uid": "1", "accession": "GSE1", "entrytype": "GSE", "pubmedids": ["111"]},
                "2": {"uid": "2", "accession": "GSE2", "entrytype": "GSE", "pubmedids": ["111"]},
                "3": {"uid": "3", "accession": "GSE3", "entrytype": "GSE", "pubmedids": ["222"]},
            }
        }
        with patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest."
                   "get_geo_links.requests.get",
                   side_effect=[_mock_response(elink_merged), _mock_response(esummary_multi)]):
            result = get_geo_accessions_for_pmids(["111", "222", "333"])
        assert result == {"111": ["GSE1", "GSE2"], "222": ["GSE3"], "333": []}

    def test_elink_sends_comma_joined_ids(self):
        """elink is queried for the whole batch with comma-joined ids (the cheap,
        reliable form). Repeated `&id=` params force per-PMID linksets but make
        NCBI drop the response at scale, so we keep comma-joined and attribute via
        esummary `pubmedids` instead."""
        elink_empty = {
            "linksets": [{"dbfrom": "pubmed", "ids": ["111", "222", "333"], "linksetdbs": []}]
        }
        with patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest."
                   "get_geo_links.requests.get",
                   side_effect=[_mock_response(elink_empty)]) as mock_get:
            get_geo_accessions_for_pmids(["111", "222", "333"])
            params = mock_get.call_args.kwargs["params"]
            assert params["id"] == "111,222,333"

    def test_attributes_via_pubmedids_not_merged_elink_linkset(self):
        """Regression for the merged-linkset bug: elink pools all batch links into
        one linkset with no per-PMID attribution. A GSE must be attributed only to
        the PMID(s) named in its esummary `pubmedids`, and only to PMIDs actually
        in the batch (33526011's record also cites an out-of-batch PMID)."""
        elink_merged = {
            "linksets": [{"dbfrom": "pubmed", "ids": ["31000000", "33526011"],
                          "linksetdbs": [{"dbto": "gds", "linkname": "pubmed_gds",
                                          "links": ["200147729"]}]}]
        }
        esummary = {
            "result": {
                "uids": ["200147729"],
                "200147729": {"uid": "200147729", "accession": "GSE147729",
                              "entrytype": "GSE", "pubmedids": ["33526011", "36514338"]},
            }
        }
        with patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest."
                   "get_geo_links.requests.get",
                   side_effect=[_mock_response(elink_merged), _mock_response(esummary)]):
            result = get_geo_accessions_for_pmids(["31000000", "33526011"])
        assert result == {"31000000": [], "33526011": ["GSE147729"]}

    def test_returns_empty_dict_for_empty_input(self):
        with patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest."
                   "get_geo_links.requests.get") as mock_get:
            assert get_geo_accessions_for_pmids([]) == {}
            assert mock_get.call_count == 0

    def test_skips_esummary_when_no_links_anywhere(self):
        elink_all_empty = {
            "linksets": [
                {"dbfrom": "pubmed", "ids": ["111"], "linksetdbs": []},
                {"dbfrom": "pubmed", "ids": ["222"], "linksetdbs": []},
            ]
        }
        with patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest."
                   "get_geo_links.requests.get",
                   side_effect=[_mock_response(elink_all_empty)]) as mock_get:
            result = get_geo_accessions_for_pmids(["111", "222"])
            assert mock_get.call_count == 1
        assert result == {"111": [], "222": []}


class TestGetWithRetryJsonDecode:

    def test_get_with_retry_logs_body_preview_on_json_decode_error(self, caplog):
        bad_text = '{"linksets":[{"control":"\\x00\\x01"}]}'
        bad_resp = MagicMock()
        bad_resp.status_code = 200
        bad_resp.text = bad_text
        bad_resp.json.side_effect = requests.exceptions.JSONDecodeError(
            "Invalid control character at", bad_text, 85)
        bad_resp.raise_for_status = MagicMock()

        good_elink = _mock_response({"linksets": [
            {"dbfrom": "pubmed", "ids": ["1"], "linksetdbs": []}
        ]})

        caplog.set_level("WARNING",
                         logger="agr_literature_service.lit_processing."
                                "data_ingest.pubmed_ingest.get_geo_links")
        with patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest."
                   "get_geo_links.time.sleep"), \
             patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest."
                   "get_geo_links.requests.get",
                   side_effect=[bad_resp, good_elink]):
            get_geo_accessions_for_pmids(["1"])

        body_logs = [r.message for r in caplog.records if "body[:500]=" in r.message]
        assert body_logs, f"expected body[:500]= log line, got: {[r.message for r in caplog.records]}"
        assert "control" in body_logs[0]  # snippet of the bad body was captured


class TestGetGeoAccessionsForPmidsWithSplit:

    def test_returns_empty_dict_for_empty_input(self):
        assert get_geo_accessions_for_pmids_with_split([]) == {}

    def test_returns_primitive_result_on_clean_run(self):
        with patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest."
                   "get_geo_links.get_geo_accessions_for_pmids",
                   return_value={"1": ["GSE1"], "2": []}) as inner:
            result = get_geo_accessions_for_pmids_with_split(["1", "2"])
        assert result == {"1": ["GSE1"], "2": []}
        assert inner.call_count == 1

    def test_splits_on_batch_failure_and_merges_halves(self, caplog):
        caplog.set_level("WARNING",
                         logger="agr_literature_service.lit_processing."
                                "data_ingest.pubmed_ingest.get_geo_links")
        with patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest."
                   "get_geo_links.get_geo_accessions_for_pmids",
                   side_effect=[
                       RuntimeError("boom"),
                       {"1": ["GSEA"], "2": []},
                       {"3": [], "4": ["GSEB"]},
                   ]) as inner:
            result = get_geo_accessions_for_pmids_with_split(["1", "2", "3", "4"])
        assert result == {"1": ["GSEA"], "2": [], "3": [], "4": ["GSEB"]}
        assert inner.call_count == 3
        assert any("splitting into 2 + 2" in r.message for r in caplog.records)

    def test_logs_and_drops_single_pmid_dead_end(self, caplog):
        caplog.set_level("ERROR",
                         logger="agr_literature_service.lit_processing."
                                "data_ingest.pubmed_ingest.get_geo_links")
        with patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest."
                   "get_geo_links.get_geo_accessions_for_pmids",
                   side_effect=RuntimeError("permanently bad")):
            result = get_geo_accessions_for_pmids_with_split(["111"])
        assert result == {}
        assert any("PMID 111 gave up" in r.message for r in caplog.records)


@pytest.mark.webtest
class TestGetGeoLinksIntegration:
    """Live-network sanity checks. Skip in normal test runs via the 'webtest' marker."""

    def test_real_pmid_with_known_geo_link(self):
        result = get_geo_accessions_for_pmid("32606685")
        assert isinstance(result, list)
        assert all(a.startswith("GSE") for a in result)
