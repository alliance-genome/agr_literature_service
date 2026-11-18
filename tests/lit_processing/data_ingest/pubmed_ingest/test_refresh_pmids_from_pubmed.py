"""SCRUM-6864: a PMID attached to an existing reference is refreshed from PubMed right away, without
changing the user the calling job writes as."""
import logging

from agr_literature_service.api.user import _set_current_user, get_current_agent_id, get_global_user_id
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest import pubmed_update_references_single_mod
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_references_single_mod import \
    refresh_pmids_from_pubmed

logger = logging.getLogger(__name__)


def test_refreshes_pmids_and_keeps_the_callers_user(monkeypatch):
    calls = []

    def fake_update_data(mod, pmids):
        calls.append((mod, pmids))
        # update_data sets its own script as the current user
        _set_current_user("pubmed_update_references_single_mod", 999)

    monkeypatch.setattr(pubmed_update_references_single_mod, "update_data", fake_update_data)
    saved = (get_global_user_id(), get_current_agent_id())
    try:
        _set_current_user("pubmed_search_new_references", 42)
        refresh_pmids_from_pubmed(["42502754", "42438718"], logger)
        assert calls == [(None, "42438718|42502754")]
        assert (get_global_user_id(), get_current_agent_id()) == ("pubmed_search_new_references", 42)
    finally:
        _set_current_user(*saved)


def test_nothing_to_refresh(monkeypatch):
    monkeypatch.setattr(pubmed_update_references_single_mod, "update_data",
                        lambda mod, pmids: (_ for _ in ()).throw(AssertionError("should not be called")))
    refresh_pmids_from_pubmed([], logger)


def test_a_failed_refresh_is_logged_not_raised(monkeypatch, caplog):
    def failing_update_data(mod, pmids):
        raise RuntimeError("PubMed down")

    monkeypatch.setattr(pubmed_update_references_single_mod, "update_data", failing_update_data)
    with caplog.at_level(logging.INFO):
        refresh_pmids_from_pubmed(["42438718"], logger)
    assert "PubMed down" in caplog.text
