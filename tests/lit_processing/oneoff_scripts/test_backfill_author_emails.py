"""The SCRUM-6513 author-email backfill: the per-reference decision with every
database collaborator stubbed."""
import importlib.util
import json
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock, patch

# Ticket-prefixed script name is not an importable module name: load by path.
_SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "agr_literature_service" / "lit_processing" / "oneoff_scripts"
    / "SCRUM-6513_backfill_author_emails.py"
)
_spec = importlib.util.spec_from_file_location("backfill_author_emails", _SCRIPT)
assert _spec is not None and _spec.loader is not None
script = importlib.util.module_from_spec(_spec)
cast(Any, _spec.loader).exec_module(script)
mod = cast(Any, script)


def _pubmed(rank, name, email=None):
    author = {"name": name, "firstname": name.split()[0], "lastname": name.split()[-1], "authorRank": rank}
    if email:
        author["email"] = email
    return author


def _db(rank, name, email=None):
    return {"name": name, "first_name": name.split()[0], "last_name": name.split()[-1],
            "first_initial": "", "author_order": rank, "orcid": None, "affiliations": [],
            "email_address": email}


def _run(pubmed_authors, db_authors, curator=False, changed=1):
    with patch.object(mod, "_reference_touched_by_curator", return_value=curator), \
            patch.object(mod, "_db_author_dicts", return_value=db_authors), \
            patch.object(mod, "sync_author_emails", return_value=changed) as sync:
        result = mod.backfill_reference(MagicMock(), 42, pubmed_authors)
    return result, sync


def test_matching_authors_get_their_emails():
    (outcome, changed), sync = _run([_pubmed(1, "Ann Lee", "ann@x.org"), _pubmed(2, "Bo Kim")],
                                    [_db(1, "Ann Lee"), _db(2, "Bo Kim")])
    assert (outcome, changed) == ("updated", 1)
    authors = sync.call_args.args[2]
    assert [(a.order, a.email) for a in authors] == [(1, "ann@x.org"), (2, None)]


def test_already_set_when_nothing_changes():
    (outcome, changed), _ = _run([_pubmed(1, "Ann Lee", "ann@x.org")],
                                 [_db(1, "Ann Lee", "ann@x.org")], changed=0)
    assert (outcome, changed) == ("already_set", 0)


def test_different_author_lists_are_left_alone():
    (outcome, _), sync = _run([_pubmed(1, "Ann Lee", "ann@x.org"), _pubmed(2, "Bo Kim")],
                              [_db(1, "Bo Kim"), _db(2, "Ann Lee")])
    assert outcome == "author_mismatch"
    sync.assert_not_called()


def test_curator_managed_references_are_left_alone():
    (outcome, _), sync = _run([_pubmed(1, "Ann Lee", "ann@x.org")], [_db(1, "Ann Lee")], curator=True)
    assert outcome == "curator_managed"
    sync.assert_not_called()


def test_no_email_or_no_authors_skips_the_database():
    with patch.object(mod, "_reference_touched_by_curator") as curator:
        assert mod.backfill_reference(MagicMock(), 42, [_pubmed(1, "Ann Lee")]) == ("no_email_in_pubmed", 0)
        assert mod.backfill_reference(MagicMock(), 42, []) == ("no_authors", 0)
    curator.assert_not_called()


def test_pubmed_authors_reads_generated_json(tmp_path):
    (tmp_path / "123.json").write_text(json.dumps({"authors": [_pubmed(1, "Ann Lee", "ann@x.org")]}))
    (tmp_path / "456.json").write_text(json.dumps({"title": "no authors"}))
    assert mod._pubmed_authors(str(tmp_path), "123")[0]["email"] == "ann@x.org"
    assert mod._pubmed_authors(str(tmp_path), "456") == []
    assert mod._pubmed_authors(str(tmp_path), "789") is None


def test_parse_args_defaults_to_2024_dry_run():
    args = mod.parse_args([])
    assert (args.since, args.commit, args.limit) == ("2024-01-01", False, None)
    args = mod.parse_args(["--since", "2025-06-01", "--commit", "--limit", "10"])
    assert (args.since, args.commit, args.limit) == ("2025-06-01", True, 10)


def test_candidates_are_in_corpus_papers_by_corpus_entry_date():
    db = MagicMock()
    db.execute.return_value.fetchall.return_value = [(7, "PMID:123"), (9, "PMID:456")]
    assert mod.candidate_references(db, "2024-01-01") == [(7, "123"), (9, "456")]
    sql = " ".join(str(db.execute.call_args.args[0]).split())
    assert "mca.corpus IS TRUE AND mca.date_created >= :since" in sql
    assert "r.date_created" not in sql
    # any MOD: no MOD filter
    assert "abbreviation" not in sql
    assert db.execute.call_args.args[1] == {"since": "2024-01-01"}
