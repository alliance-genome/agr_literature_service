import json
from unittest.mock import patch, MagicMock

import pytest
import requests

from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_geo_links import (
    get_geo_accessions_for_pmid,
    get_geo_accessions_for_pmids,
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
        "200073427": {"uid": "200073427", "accession": "GSE73427", "entrytype": "GSE"},
        "200050899": {"uid": "200050899", "accession": "GSE50899", "entrytype": "GSE"},
        "100012345": {"uid": "100012345", "accession": "GPL12345", "entrytype": "GPL"},
    }
}


class TestGetGeoAccessionsForPmid:

    def test_returns_sorted_gse_accessions_for_pmid_with_geo_links(self):
        elink = _mock_response(ELINK_TWO_LINKS)
        esummary = _mock_response({
            "result": {
                "uids": ["200073427", "200050899"],
                "200073427": {"uid": "200073427", "accession": "GSE73427", "entrytype": "GSE"},
                "200050899": {"uid": "200050899", "accession": "GSE50899", "entrytype": "GSE"},
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
                "1": {"uid": "1", "accession": "GSE111", "entrytype": "GSE"},
                "2": {"uid": "2", "accession": "GSE111", "entrytype": "GSE"},
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
                       "200073427": {"uid": "200073427", "accession": "GSE73427", "entrytype": "GSE"},
                       "200050899": {"uid": "200050899", "accession": "GSE50899", "entrytype": "GSE"}}
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
        elink_multi = {
            "linksets": [
                {"dbfrom": "pubmed", "ids": ["111"],
                 "linksetdbs": [{"dbto": "gds", "linkname": "pubmed_gds", "links": ["1", "2"]}]},
                {"dbfrom": "pubmed", "ids": ["222"],
                 "linksetdbs": [{"dbto": "gds", "linkname": "pubmed_gds", "links": ["3"]}]},
                {"dbfrom": "pubmed", "ids": ["333"],
                 "linksetdbs": []},
            ]
        }
        esummary_multi = {
            "result": {
                "uids": ["1", "2", "3"],
                "1": {"uid": "1", "accession": "GSE1", "entrytype": "GSE"},
                "2": {"uid": "2", "accession": "GSE2", "entrytype": "GSE"},
                "3": {"uid": "3", "accession": "GSE3", "entrytype": "GSE"},
            }
        }
        with patch("agr_literature_service.lit_processing.data_ingest.pubmed_ingest."
                   "get_geo_links.requests.get",
                   side_effect=[_mock_response(elink_multi), _mock_response(esummary_multi)]):
            result = get_geo_accessions_for_pmids(["111", "222", "333"])
        assert result == {"111": ["GSE1", "GSE2"], "222": ["GSE3"], "333": []}

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


@pytest.mark.webtest
class TestGetGeoLinksIntegration:
    """Live-network sanity checks. Skip in normal test runs via the 'webtest' marker."""

    def test_real_pmid_with_known_geo_link(self):
        result = get_geo_accessions_for_pmid("32606685")
        assert isinstance(result, list)
        assert all(a.startswith("GSE") for a in result)
