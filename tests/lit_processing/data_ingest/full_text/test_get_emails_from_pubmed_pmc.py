"""Unit tests for the PubMed/PMC email download step (SCRUM-6430).

No network and no database: the HTTP layer is exercised against a fake
requests.post, and the DB-facing functions against a fake session object.
"""
from typing import Dict, List, Optional

import pytest
import requests

import agr_literature_service.lit_processing.data_ingest.full_text.get_emails_from_pubmed_pmc as mod
from agr_literature_service.lit_processing.data_ingest.full_text.get_emails_from_pubmed_pmc import (
    EMAIL_COMPLETE,
    EMAIL_NEEDED,
    SOURCE_PMC,
    SOURCE_PUBMED,
    apply_results,
    download_emails,
    emails_from_text,
    eutils_post,
    fetch_pmc_emails,
    fetch_pubmed_emails,
    get_candidates,
    get_email_extraction_mods,
    map_pmids_to_pmcids,
    parse_args,
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
    assert emails_from_text("") == []


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


# ----------------------------------------------------------------------
# eutils_post retry/throttle layer
# ----------------------------------------------------------------------
class FakeResponse:
    def __init__(self, content=b"ok", error=False):
        self.content = content
        self._error = error

    def raise_for_status(self):
        if self._error:
            raise requests.HTTPError("boom")


def _no_sleep(monkeypatch):
    monkeypatch.setattr(mod.time, "sleep", lambda seconds: None)


def test_eutils_post_success(monkeypatch):
    _no_sleep(monkeypatch)
    posted = {}

    def fake_post(url, data=None, timeout=None):
        posted["url"] = url
        posted["data"] = data
        return FakeResponse(b"<xml/>")

    monkeypatch.setattr(mod.requests, "post", fake_post)
    content = eutils_post("efetch", {"db": "pubmed"}, ["1", "2"])
    assert content == b"<xml/>"
    assert posted["url"].endswith("/efetch.fcgi")
    assert ("id", "1") in posted["data"] and ("id", "2") in posted["data"]


def test_eutils_post_sends_api_key_when_set(monkeypatch):
    _no_sleep(monkeypatch)
    posted = {}

    def fake_post(url, data=None, timeout=None):
        posted["data"] = data
        return FakeResponse(b"<xml/>")

    monkeypatch.setattr(mod.requests, "post", fake_post)
    monkeypatch.setattr(mod, "NCBI_API_KEY", "test-key")
    assert eutils_post("efetch", {"db": "pubmed"}, ["1"]) == b"<xml/>"
    assert ("api_key", "test-key") in posted["data"]


def test_eutils_post_retries_then_succeeds(monkeypatch):
    _no_sleep(monkeypatch)
    attempts = []

    def fake_post(url, data=None, timeout=None):
        attempts.append(1)
        if len(attempts) < 3:
            raise requests.ConnectionError("down")
        return FakeResponse(b"late")

    monkeypatch.setattr(mod.requests, "post", fake_post)
    assert eutils_post("efetch", {"db": "pubmed"}, ["1"]) == b"late"
    assert len(attempts) == 3


def test_eutils_post_gives_up_after_retries(monkeypatch):
    _no_sleep(monkeypatch)

    def fake_post(url, data=None, timeout=None):
        return FakeResponse(error=True)

    monkeypatch.setattr(mod.requests, "post", fake_post)
    assert eutils_post("efetch", {"db": "pubmed"}, ["1"]) is None


def test_fetchers_skip_failed_chunks(monkeypatch):
    monkeypatch.setattr(mod, "eutils_post", lambda *args: None)
    assert fetch_pubmed_emails(["1", "2"]) == {}
    assert map_pmids_to_pmcids(["1", "2"]) == {}
    assert fetch_pmc_emails({"1": "10"}) == {}


def test_fetchers_skip_unparseable_chunks(monkeypatch):
    """A truncated / non-XML 200 body from NCBI must skip the chunk, not
    abort the whole run."""
    monkeypatch.setattr(mod, "eutils_post", lambda *args: b"<truncated")
    assert fetch_pubmed_emails(["1", "2"]) == {}
    assert map_pmids_to_pmcids(["1", "2"]) == {}
    assert fetch_pmc_emails({"1": "10"}) == {}


def test_download_emails_pmc_all_unions_both_tiers(monkeypatch):
    """With pmc_all, a paper both tiers hit gets the union of the addresses
    (PubMed's first) and a combined source."""
    pmc_overlap_xml = b"""<?xml version="1.0" ?>
    <pmc-articleset>
      <article>
        <front>
          <article-meta>
            <article-id pub-id-type="pmid">30267671</article-id>
            <author-notes>
              <corresp>Correspondence: second.author@upo.es</corresp>
            </author-notes>
          </article-meta>
        </front>
      </article>
    </pmc-articleset>
    """
    elink_xml = b"""<?xml version="1.0" ?>
    <eLinkResult>
      <LinkSet>
        <IdList><Id>30267671</Id></IdList>
        <LinkSetDb><DbTo>pmc</DbTo><Link><Id>555</Id></Link></LinkSetDb>
      </LinkSet>
    </eLinkResult>
    """

    def fake_eutils_post(endpoint, params, ids):
        if endpoint == "efetch" and params["db"] == "pubmed":
            return PUBMED_XML
        if endpoint == "elink":
            assert "30267671" in ids   # pmc_all: tier-1 hits are linked too
            return elink_xml
        return pmc_overlap_xml

    monkeypatch.setattr(mod, "eutils_post", fake_eutils_post)
    results = download_emails(["30267671"], pmc_all=True)
    assert results["30267671"] == (
        ["csanoca@upo.es", "second.author@upo.es"],
        SOURCE_PUBMED + "|" + SOURCE_PMC,
    )


# ----------------------------------------------------------------------
# DB-facing selection functions (fake session)
# ----------------------------------------------------------------------
class FakeDB:
    """Stands in for a SQLAlchemy session for functions that only run
    db.execute(...).fetchall(); records the bind params for assertions."""

    def __init__(self, rows):
        self.rows = rows
        self.params = None

    def execute(self, sql, params=None):
        self.params = params
        return self

    def fetchall(self):
        return self.rows


def test_get_email_extraction_mods():
    db = FakeDB([(1, "FB"), (4, "SGD")])
    assert get_email_extraction_mods(db) == {1: "FB", 4: "SGD"}


def test_get_candidates_strips_pmid_prefix_and_binds_window():
    db = FakeDB([(9, "AGRKB:1", "PMID:123", 4, "SGD", False, True)])
    candidates = get_candidates(db, [4], "2024-01-01", "2027-01-01", limit=5)
    assert candidates == [(9, "AGRKB:1", "123", 4, "SGD", False, True)]
    assert db.params["since"] == "2024-01-01"
    assert db.params["until"] == "2027-01-01"
    assert db.params["limit"] == 5
    assert db.params["curator_like"].endswith("%")


# ----------------------------------------------------------------------
# apply_results (crud layer monkeypatched)
# ----------------------------------------------------------------------
class FakeSession:
    def __init__(self):
        self.added = []
        self.commits = 0
        self.rollbacks = 0

    def add(self, obj):
        self.added.append(obj)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        pass


def test_apply_results_loads_emails_and_tags(monkeypatch):
    loaded = []
    transitions = []
    monkeypatch.setattr(mod, "set_reference_emails",
                        lambda db, curie, emails: loaded.append((curie, emails)))
    monkeypatch.setattr(mod, "transition_to_workflow_status",
                        lambda db, ref_id, mod_abbr, tag, transition_type: transitions.append(
                            (ref_id, mod_abbr, tag, transition_type)))
    db = FakeSession()
    candidates = [
        # one reference in two MOD corpora: emails loaded once, tagged per mod
        (9, "AGRKB:1", "123", 4, "SGD", False, False),
        (9, "AGRKB:1", "123", 2, "WB", True, False),
        # no emails, untagged: seeded needed
        (10, "AGRKB:2", "456", 4, "SGD", False, False),
        # no emails, already needed: left alone
        (11, "AGRKB:3", "789", 4, "SGD", True, False),
    ]
    results = {"123": (["a@b.edu"], SOURCE_PUBMED)}

    counters = apply_results(db, candidates, results, dry_run=False)

    assert loaded == [("AGRKB:1", ["a@b.edu"])]
    assert transitions == [("9", "SGD", EMAIL_COMPLETE, "automated"),
                           ("9", "WB", EMAIL_COMPLETE, "automated")]
    # seeded: needed for (9, SGD) before its transition, and for the
    # no-email untagged pair (10, SGD)
    assert [(w.reference_id, w.mod_id, w.workflow_tag_id) for w in db.added] == [
        (9, 4, EMAIL_NEEDED), (10, 4, EMAIL_NEEDED)]
    assert counters == {"emails_loaded": 1, "tagged_complete": 2,
                        "tagged_needed": 1, "errors": 0}


def test_apply_results_dry_run_writes_nothing(monkeypatch):
    monkeypatch.setattr(mod, "set_reference_emails",
                        lambda *args: pytest.fail("must not load in dry run"))
    monkeypatch.setattr(mod, "transition_to_workflow_status",
                        lambda *args, **kwargs: pytest.fail("must not transition in dry run"))
    db = FakeSession()
    candidates = [(9, "AGRKB:1", "123", 4, "SGD", False, False)]
    counters = apply_results(db, candidates, {"123": (["a@b.edu"], SOURCE_PUBMED)},
                             dry_run=True)
    assert db.added == [] and db.commits == 0
    assert counters["emails_loaded"] == 1 and counters["tagged_complete"] == 1


def test_apply_results_isolates_errors(monkeypatch):
    monkeypatch.setattr(mod, "set_reference_emails", lambda db, curie, emails: None)

    def explode(db, ref_id, mod_abbr, tag, transition_type):
        if ref_id == "9":
            raise RuntimeError("transition rejected")

    monkeypatch.setattr(mod, "transition_to_workflow_status", explode)
    db = FakeSession()
    candidates = [(9, "AGRKB:1", "123", 4, "SGD", True, False),
                  (10, "AGRKB:2", "456", 4, "SGD", True, False)]
    results = {"123": (["a@b.edu"], SOURCE_PUBMED),
               "456": (["c@d.edu"], SOURCE_PMC)}
    counters = apply_results(db, candidates, results, dry_run=False)
    # the first reference errored and rolled back; the second still completed
    assert counters["errors"] == 1
    assert counters["tagged_complete"] == 1
    assert db.rollbacks == 1


# ----------------------------------------------------------------------
# run() driver (all collaborators monkeypatched)
# ----------------------------------------------------------------------
def _patch_run_collaborators(monkeypatch, mods, candidates, results):
    monkeypatch.setattr(mod, "create_postgres_session", lambda verbose: FakeSession())
    monkeypatch.setattr(mod, "set_global_user_id", lambda db, name: None)
    monkeypatch.setattr(mod, "get_email_extraction_mods", lambda db: mods)
    monkeypatch.setattr(mod, "get_candidates",
                        lambda db, mod_ids, since, until, limit=None: candidates)
    monkeypatch.setattr(mod, "download_emails",
                        lambda pmids, pmc_all=False: results)


def test_run_end_to_end(monkeypatch):
    _patch_run_collaborators(
        monkeypatch,
        mods={4: "SGD"},
        candidates=[(9, "AGRKB:1", "123", 4, "SGD", False, False)],
        results={"123": (["a@b.edu"], SOURCE_PUBMED)},
    )
    counters = mod.run(since="2024-01-01", until="2027-01-01", dry_run=True)
    assert counters == {"emails_loaded": 1, "tagged_complete": 1,
                        "tagged_needed": 0, "errors": 0}


def test_run_mod_filter_and_empty_cases(monkeypatch):
    _patch_run_collaborators(monkeypatch, mods={4: "SGD"}, candidates=[], results={})
    # only_mods filters SGD away -> nothing to do
    assert mod.run(since="2024-01-01", until="2027-01-01",
                   only_mods=["WB"], dry_run=True) == {}
    # MOD selected but no candidates in the window
    assert mod.run(since="2024-01-01", until="2027-01-01", dry_run=True) == {}


def test_parse_args_defaults(monkeypatch):
    monkeypatch.setattr("sys.argv", ["get_emails_from_pubmed_pmc.py"])
    args = parse_args()
    assert args.since == mod.DEFAULT_SINCE
    assert not args.commit and args.mods is None and args.limit is None


def test_main_invokes_run(monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        ["get_emails_from_pubmed_pmc.py", "--mods", "SGD,WB", "--limit", "3"])
    calls = {}
    monkeypatch.setattr(mod, "run", lambda **kwargs: calls.update(kwargs))
    mod.main()
    assert calls["only_mods"] == ["SGD", "WB"]
    assert calls["limit"] == 3
    assert calls["dry_run"] is True   # no --commit
