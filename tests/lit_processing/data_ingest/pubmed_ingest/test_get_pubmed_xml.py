import os
from os import environ

import pytest

from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.get_pubmed_xml import download_pubmed_xml
from ....fixtures import cleanup_tmp_files_when_done # noqa


class TestGetPubmedXML:

    @pytest.mark.webtest
    def test_download_pubmed_xml(self, cleanup_tmp_files_when_done): # noqa
        base_path = environ.get('XML_PATH')
        download_pubmed_xml(["88888"])
        assert os.path.exists(os.path.join(base_path, "pubmed_xml", "88888.xml"))


# --- efetch failing part-way through a response (2026-10-08) -----------------
from unittest.mock import patch  # noqa: E402
import xml.etree.ElementTree as ET  # noqa: E402

from agr_literature_service.lit_processing.data_ingest.pubmed_ingest import get_pubmed_xml  # noqa: E402

_HEADER = '<?xml version="1.0" ?> <PubmedArticleSet> '


def _article(pmid):
    return (f'<PubmedArticle><MedlineCitation><PMID Version="1">{pmid}</PMID></MedlineCitation>'
            f'</PubmedArticle>')


_ERROR = ('<eFetchResult> <ERROR> Error: proxy_stream(): &lt;errorResponse&gt; &lt;message&gt;Failed to '
          'process PubOne response.&lt;/message&gt; &lt;originalURL&gt;/api?api_key=SECRET&lt;/originalURL&gt; '
          '&lt;statusCode&gt;404&lt;/statusCode&gt; &lt;/errorResponse</ERROR> </eFetchResult>')


def test_strip_efetch_error_keeps_complete_articles_and_hides_the_url():
    response = _HEADER + _article(1) + _article(2) + _ERROR
    with patch.object(get_pubmed_xml.logger, "warning") as warning:
        stripped = get_pubmed_xml.strip_efetch_error(response)
    assert stripped == _HEADER + _article(1) + _article(2) + "</PubmedArticleSet>"
    ET.fromstring(stripped)
    logged = warning.call_args.args[0] % warning.call_args.args[1:]
    assert "Failed to process PubOne response" in logged
    assert "SECRET" not in logged
    clean = _HEADER + _article(1) + "</PubmedArticleSet>"
    assert get_pubmed_xml.strip_efetch_error(clean) == clean


def _slice(tmp_path, response):
    found, md5 = set(), {}
    with patch.object(get_pubmed_xml, "fetch_pubmed_xml", return_value=response), \
            patch.object(get_pubmed_xml.time, "sleep"):
        get_pubmed_xml.download_pubmed_xml_slice(found, str(tmp_path) + "/", md5, "1,2,3")
    return found


def test_slice_with_embedded_error_saves_only_complete_articles(tmp_path):
    found = _slice(tmp_path, _HEADER + _article(1) + _article(2) + _ERROR)
    assert found == {"1", "2"}
    for pmid in ("1", "2"):
        ET.parse(tmp_path / f"{pmid}.xml")
    assert not (tmp_path / "3.xml").exists()


def test_slice_never_saves_a_broken_article(tmp_path):
    truncated = _HEADER + _article(1) + '<PubmedArticle><MedlineCitation><PMID Version="1">2</PMID>'
    found = _slice(tmp_path, truncated)
    assert found == {"1"}
    assert not (tmp_path / "2.xml").exists()


def test_missing_pmids_are_retried_once_in_small_slices(tmp_path, monkeypatch):
    monkeypatch.setenv("XML_PATH", str(tmp_path) + "/")
    monkeypatch.setattr(get_pubmed_xml, "RETRY_SLICE_SIZE", 2)
    calls = []

    def fake_slice(pmids_found, storage_path, md5dict, pmids_joined):
        pmids = pmids_joined.split(",")
        calls.append(pmids)
        # first pass: the request "fails" after the first PMID; retries succeed
        # except for 5, which is not in PubMed
        returned = pmids[:1] if len(calls) == 1 else [p for p in pmids if p != "5"]
        pmids_found.update(returned)
        for pmid in returned:
            md5dict[pmid] = "md5"

    with patch.object(get_pubmed_xml, "download_pubmed_xml_slice", side_effect=fake_slice):
        get_pubmed_xml.download_pubmed_xml(["1", "2", "3", "4", "5"])
    assert calls == [["1", "2", "3", "4", "5"], ["2", "3"], ["4", "5"]]
    assert (tmp_path / "pmids_not_found").read_text() == "5\n"


def test_no_retry_when_the_request_was_already_small(tmp_path, monkeypatch):
    monkeypatch.setenv("XML_PATH", str(tmp_path) + "/")
    calls = []

    def fake_slice(pmids_found, storage_path, md5dict, pmids_joined):
        calls.append(pmids_joined)

    with patch.object(get_pubmed_xml, "download_pubmed_xml_slice", side_effect=fake_slice):
        get_pubmed_xml.download_pubmed_xml(["1", "2"])
    assert calls == ["1,2"]
