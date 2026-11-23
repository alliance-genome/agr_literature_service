"""Unit tests for extract_emails' user attribution (SCRUM-6845 follow-up).

No database: the session factory and the processing steps are replaced with fakes.
"""
import sys

import agr_literature_service.lit_processing.data_ingest.full_text.extract_emails as mod


class FakeSession:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def test_script_name_is_the_module_name():
    assert mod.SCRIPT_NAME == "extract_emails"


def test_main_sets_the_script_agent_before_reading_or_writing(monkeypatch):
    calls = []
    session = FakeSession()

    monkeypatch.setattr(sys, "argv", ["extract_emails.py"])
    monkeypatch.setattr(mod, "create_postgres_session", lambda _verbose: session)
    monkeypatch.setattr(mod, "set_global_user_id",
                        lambda db, label: calls.append(("user", db, label)))
    monkeypatch.setattr(mod, "get_agrkb_md_reffile_mapping",
                        lambda db: calls.append(("mapping", db)) or {})
    monkeypatch.setattr(mod, "process_and_load_streaming",
                        lambda **kwargs: calls.append(("process", kwargs["mapping"])))

    mod.main()

    assert calls == [("user", session, "extract_emails"), ("mapping", session), ("process", {})]
    assert session.closed
