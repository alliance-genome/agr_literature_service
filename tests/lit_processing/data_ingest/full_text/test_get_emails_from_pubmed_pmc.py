"""Unit tests for the PubMed/PMC email download step (SCRUM-6430).

Pure-function tests only: XML parsing, the two-tier cascade (with the network
layer monkeypatched) and the workflow-tag planning. No database required.
"""
from typing import Dict, List, Optional

import pytest

from agr_literature_service.lit_processing.data_ingest.full_text.get_emails_from_pubmed_pmc import (
    EMAIL_COMPLETE,
    EMAIL_NEEDED,
    SOURCE_PMC,
    SOURCE_PUBMED,
    download_emails,
    emails_from_text,
    parse_pmc_emails,
    parse_pubmed_emails,
    plan_tag_actions,
)


PUBMED_XML = b"""<?xml version="1.0" ?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID Version="1">30267671</PMID>
      <Article>
        <AuthorList>
          <Author>
            <AffiliationInfo>
              <Affiliation>Universidad Pablo de Olavide, Seville, Spain. Electronic address: csanoca@upo.es.</Affiliation>
            </AffiliationInfo>
          </Author>
        </AuthorList>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
  <PubmedArticle>
    <MedlineCitation>
      <PMID Version="1">11111111</PMID>
      <Article>
        <AuthorList>
          <Author>
            <AffiliationInfo>
              <Affiliation>Some University, Some City.</Affiliation>
            </AffiliationInfo>
          </Author>
        </AuthorList>
      </Article>
    </MedlineCitation>
    <PubmedData>
      <ReferenceList>
        <Reference>
          <ArticleIdList>
            <ArticleId IdType="pubmed">99999999</ArticleId>
          </ArticleIdList>
        </Reference>
      </ReferenceList>
    </PubmedData>
  </PubmedArticle>
</PubmedArticleSet>
"""

PMC_XML = b"""<?xml version="1.0" ?>
<pmc-articleset>
  <article>
    <front>
      <article-meta>
        <article-id pub-id-type="pmc">3710826</article-id>
        <article-id pub-id-type="pmid">23798402</article-id>
        <author-notes>
          <corresp id="cor1">Correspondence: tatiana.kutateladze@ucdenver.edu</corresp>
        </author-notes>
      </article-meta>
    </front>
  </article>
  <article>
    <front>
      <article-meta>
        <article-id pub-id-type="pmc">7777777</article-id>
        <contrib-group>
          <contrib contrib-type="author">
            <email>author.two@example.edu</email>
          </contrib>
        </contrib-group>
      </article-meta>
    </front>
  </article>
</pmc-articleset>
"""

ELINK_XML = b"""<?xml version="1.0" ?>
<eLinkResult>
  <LinkSet>
    <IdList><Id>23798402</Id></IdList>
    <LinkSetDb>
      <DbTo>pmc</DbTo>
      <Link><Id>3710826</Id></Link>
    </LinkSetDb>
  </LinkSet>
  <LinkSet>
    <IdList><Id>11111111</Id></IdList>
  </LinkSet>
</eLinkResult>
"""


def test_parse_pubmed_emails_extracts_from_affiliation():
    emails_by_pmid = parse_pubmed_emails(PUBMED_XML)
    assert emails_by_pmid == {"30267671": ["csanoca@upo.es"]}


def test_parse_pubmed_emails_skips_papers_without_email():
    emails_by_pmid = parse_pubmed_emails(PUBMED_XML)
    # affiliation without an address yields no entry; the PMID of a cited
    # reference (99999999) must not leak in via a descendant lookup
    assert "11111111" not in emails_by_pmid
    assert "99999999" not in emails_by_pmid


def test_parse_pmc_emails_corresp_and_contrib_fallback():
    emails_by_pmid = parse_pmc_emails(PMC_XML, {"7777777": "22222222"})
    # first article: pmid taken from its own article-id, email from <corresp>
    assert emails_by_pmid["23798402"] == ["tatiana.kutateladze@ucdenver.edu"]
    # second article: no pmid article-id -> resolved via the pmcid map,
    # no <corresp> -> falls back to <contrib-group>//<email>
    assert emails_by_pmid["22222222"] == ["author.two@example.edu"]


def test_emails_from_text_suppresses_role_accounts():
    assert emails_from_text("contact reprints@oup.com or jane.doe@stanford.edu") == [
        "jane.doe@stanford.edu"
    ]
    assert emails_from_text("no address here") == []


def test_download_emails_cascade(monkeypatch):
    """Tier 2 is only consulted for the PMIDs tier 1 missed, and tier 1 wins
    when both tiers have an address for the same paper."""
    calls: List[str] = []

    def fake_eutils_post(endpoint: str, params: Dict[str, str],
                         ids: List[str]) -> Optional[bytes]:
        calls.append(endpoint + ":" + params.get("db", params.get("dbfrom", "")))
        if endpoint == "efetch" and params["db"] == "pubmed":
            return PUBMED_XML
        if endpoint == "elink":
            # only the tier-1 misses may be linked
            assert "30267671" not in ids
            return ELINK_XML
        if endpoint == "efetch" and params["db"] == "pmc":
            return PMC_XML
        raise AssertionError("unexpected eutils call " + endpoint)

    import agr_literature_service.lit_processing.data_ingest.full_text.get_emails_from_pubmed_pmc as mod
    monkeypatch.setattr(mod, "eutils_post", fake_eutils_post)

    results = download_emails(["30267671", "23798402", "11111111"])
    assert results["30267671"] == (["csanoca@upo.es"], SOURCE_PUBMED)
    assert results["23798402"] == (["tatiana.kutateladze@ucdenver.edu"], SOURCE_PMC)
    assert "11111111" not in results
    assert calls == ["efetch:pubmed", "elink:pmc", "efetch:pmc"]


@pytest.mark.parametrize(
    "has_needed, has_failed, emails_found, expected",
    [
        # emails found: untagged pair walks seed(needed) -> transition(complete)
        (False, False, True, [("seed", EMAIL_NEEDED), ("transition", EMAIL_COMPLETE)]),
        # emails found: pending pair just completes
        (True, False, True, [("transition", EMAIL_COMPLETE)]),
        # emails found: failed pair retries (failed->needed) then completes
        (False, True, True, [("transition", EMAIL_NEEDED), ("transition", EMAIL_COMPLETE)]),
        # no emails: untagged pair is handed to the full-text pipeline
        (False, False, False, [("seed", EMAIL_NEEDED)]),
        # no emails: pending/failed pairs are left alone
        (True, False, False, []),
        (False, True, False, []),
    ],
)
def test_plan_tag_actions(has_needed, has_failed, emails_found, expected):
    assert plan_tag_actions(has_needed, has_failed, emails_found) == expected
