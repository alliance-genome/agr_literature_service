"""Unit tests for the PubMed/PMC email download step (SCRUM-6430).

No network and no database: the HTTP layer is exercised against a fake
requests.post, and the DB-facing functions against a fake session object.
"""
from datetime import datetime
from typing import Dict, List, Optional

import pytest
import requests

import agr_literature_service.lit_processing.data_ingest.full_text.get_emails_from_pubmed_pmc as mod
from agr_literature_service.lit_processing.data_ingest.full_text.get_emails_from_pubmed_pmc import (
    EMAIL_EXTRACTION_JOB,
    EMAIL_NEEDED,
    SOURCE_PMC,
    SOURCE_PUBMED,
    apply_results,
    download_emails,
    emails_from_text,
    eutils_post,
    fetch_pmc_emails,
    fetch_pubmed_emails,
    get_jobs,
    job_is_done,
    map_pmids_to_pmcids,
    parse_args,
    parse_pmc_emails,
    parse_pubmed_emails,
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


RECENT = datetime(2026, 6, 1)
OLD = datetime(2023, 1, 1)
GIVE_UP_BEFORE = datetime(2024, 11, 1)


@pytest.mark.parametrize(
    "emails_found, entered_corpus, done",
    [
        (True, RECENT, True),    # emails found: the job succeeds
        (True, OLD, True),
        (False, RECENT, False),  # nothing yet: left for the full-text extraction
        (False, OLD, True),      # nothing for 2 years: given up, done without an email
    ],
)
def test_job_is_done(emails_found, entered_corpus, done):
    assert job_is_done(emails_found, entered_corpus, GIVE_UP_BEFORE) is done


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


def test_fetchers_warn_on_ncbi_error_documents(monkeypatch):
    """A well-formed NCBI error document parses cleanly with zero articles;
    it must be logged (not silent) and contribute nothing."""
    error_xml = b'<?xml version="1.0" ?><eFetchResult><ERROR>backend failed</ERROR></eFetchResult>'
    monkeypatch.setattr(mod, "eutils_post", lambda *args: error_xml)
    assert fetch_pubmed_emails(["1"]) == {}
    assert map_pmids_to_pmcids(["1"]) == {}
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


def test_get_jobs_reads_the_job_list():
    db = FakeDB([(77, 9, "AGRKB:1", "PMID:123", "SGD", RECENT),
                 (78, 10, "AGRKB:2", None, "WB", OLD)])
    jobs = get_jobs(db, only_mods=["sgd", "WB"], limit=5)
    assert jobs == [(77, 9, "AGRKB:1", "123", "SGD", RECENT),
                    (78, 10, "AGRKB:2", None, "WB", OLD)]
    assert db.params == {"needed": EMAIL_NEEDED, "job_like": f"%{EMAIL_EXTRACTION_JOB}%",
                         "mods": ["SGD", "WB"], "limit": 5}


def test_get_jobs_without_filters_binds_only_the_job():
    db = FakeDB([])
    assert get_jobs(db) == []
    assert set(db.params) == {"needed", "job_like"}


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


def _record_jobs(monkeypatch):
    finished = []
    monkeypatch.setattr(mod, "job_change_atp_code",
                        lambda db, reference_workflow_tag_id, condition: finished.append(
                            (reference_workflow_tag_id, condition)))
    return finished


def test_apply_results_loads_emails_and_finishes_jobs(monkeypatch):
    loaded = []
    monkeypatch.setattr(mod, "set_reference_emails",
                        lambda db, curie, emails: loaded.append((curie, emails)))
    finished = _record_jobs(monkeypatch)
    db = FakeSession()
    jobs = [
        # one reference in two MOD corpora: emails loaded once, each job finished
        (77, 9, "AGRKB:1", "123", "SGD", RECENT),
        (78, 9, "AGRKB:1", "123", "WB", RECENT),
        # no emails, recent: stays needed for the full-text extraction
        (79, 10, "AGRKB:2", "456", "SGD", RECENT),
        # no emails for 2 years: done without an email
        (80, 11, "AGRKB:3", "789", "SGD", OLD),
        # no PMID: nothing to download, only the give-up rule applies
        (81, 12, "AGRKB:4", None, "WB", OLD),
        (82, 13, "AGRKB:5", None, "WB", RECENT),
    ]
    results = {"123": (["a@b.edu"], SOURCE_PUBMED)}

    counters = apply_results(db, jobs, results, GIVE_UP_BEFORE, dry_run=False)

    assert loaded == [("AGRKB:1", ["a@b.edu"])]
    assert finished == [(77, "on_success"), (78, "on_success"), (80, "on_success"), (81, "on_success")]
    assert db.added == []   # no workflow tag is written directly
    assert counters == {"emails_loaded": 1, "complete_with_emails": 2, "complete_without_emails": 2,
                        "still_needed": 2, "errors": 0}


def test_apply_results_dry_run_writes_nothing(monkeypatch):
    monkeypatch.setattr(mod, "set_reference_emails",
                        lambda *args: pytest.fail("must not load in dry run"))
    monkeypatch.setattr(mod, "job_change_atp_code",
                        lambda *args, **kwargs: pytest.fail("must not finish jobs in dry run"))
    db = FakeSession()
    jobs = [(77, 9, "AGRKB:1", "123", "SGD", RECENT)]
    counters = apply_results(db, jobs, {"123": (["a@b.edu"], SOURCE_PUBMED)}, GIVE_UP_BEFORE, dry_run=True)
    assert db.added == [] and db.commits == 0
    assert counters["emails_loaded"] == 1 and counters["complete_with_emails"] == 1


def test_apply_results_isolates_errors(monkeypatch):
    monkeypatch.setattr(mod, "set_reference_emails", lambda db, curie, emails: None)

    def explode(db, reference_workflow_tag_id, condition):
        if reference_workflow_tag_id == 77:
            raise RuntimeError("transition rejected")

    monkeypatch.setattr(mod, "job_change_atp_code", explode)
    db = FakeSession()
    jobs = [(77, 9, "AGRKB:1", "123", "SGD", RECENT),
            (78, 10, "AGRKB:2", "456", "SGD", RECENT)]
    results = {"123": (["a@b.edu"], SOURCE_PUBMED),
               "456": (["c@d.edu"], SOURCE_PMC)}
    counters = apply_results(db, jobs, results, GIVE_UP_BEFORE, dry_run=False)
    # the first reference errored and rolled back; the second still completed
    assert counters["errors"] == 1
    assert db.rollbacks == 1 and db.commits == 1


# ----------------------------------------------------------------------
def _patch_run_collaborators(monkeypatch, jobs, results):
    monkeypatch.setattr(mod, "create_postgres_session", lambda verbose: FakeSession())
    monkeypatch.setattr(mod, "set_global_user_id", lambda db, name: None)
    monkeypatch.setattr(mod, "get_jobs", lambda db, only_mods=None, limit=None: jobs)
    monkeypatch.setattr(mod, "download_emails",
                        lambda pmids, pmc_all=False: results)


def test_run_end_to_end(monkeypatch):
    _patch_run_collaborators(
        monkeypatch,
        jobs=[(77, 9, "AGRKB:1", "123", "SGD", RECENT)],
        results={"123": (["a@b.edu"], SOURCE_PUBMED)},
    )
    counters = mod.run(dry_run=True)
    assert counters == {"emails_loaded": 1, "complete_with_emails": 1, "complete_without_emails": 0,
                        "still_needed": 0, "errors": 0}


def test_run_without_pmids_downloads_nothing(monkeypatch):
    _patch_run_collaborators(monkeypatch, jobs=[(81, 12, "AGRKB:4", None, "WB", OLD)], results={})
    monkeypatch.setattr(mod, "download_emails", lambda *args, **kwargs: pytest.fail("nothing to download"))
    counters = mod.run(dry_run=True)
    assert counters["complete_without_emails"] == 1


def test_run_with_no_jobs(monkeypatch):
    _patch_run_collaborators(monkeypatch, jobs=[], results={})
    assert mod.run(only_mods=["WB"], dry_run=True) == {}


def test_parse_args_defaults(monkeypatch):
    monkeypatch.setattr("sys.argv", ["get_emails_from_pubmed_pmc.py"])
    args = parse_args()
    assert args.give_up_after_days == mod.DEFAULT_GIVE_UP_AFTER_DAYS == 730
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
