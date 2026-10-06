"""The SCRUM-6589 backfill: pure helpers and the conversion loop with every
collaborator stubbed."""
import importlib.util
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

# Ticket-prefixed script name is not an importable module name: load by path.
_SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "agr_literature_service" / "lit_processing" / "oneoff_scripts"
    / "SCRUM-6589_convert_office_supplements.py"
)
_spec = importlib.util.spec_from_file_location("convert_office_supplements", _SCRIPT)
assert _spec is not None and _spec.loader is not None
script = importlib.util.module_from_spec(_spec)
cast(Any, _spec.loader).exec_module(script)

mod = cast(Any, script)
parse_years = mod.parse_years
source_mod_abbreviation = cast(Any, script).source_mod_abbreviation


def test_parse_years_defaults_to_2025_and_2026():
    assert parse_years(None) == [2025, 2026]
    assert parse_years([]) == [2025, 2026]


def test_parse_years_accepts_repeats_and_commas():
    assert parse_years(["2026", "2024,2025", " 2025 "]) == [2024, 2025, 2026]


def test_parse_years_rejects_non_years():
    with pytest.raises(ValueError):
        parse_years(["25"])


def test_source_mod_abbreviation_first_owning_mod_or_none():
    shared = MagicMock()
    shared.mod = None
    sgd = MagicMock()
    sgd.mod = MagicMock(abbreviation="SGD")
    rf = MagicMock()
    rf.referencefile_mods = [shared, sgd]
    assert source_mod_abbreviation(rf) == "SGD"
    rf.referencefile_mods = [shared]
    assert source_mod_abbreviation(rf) is None


def _row(rf_id, reference_id, name, ext):
    rf = MagicMock()
    rf.referencefile_id = rf_id
    rf.reference_id = reference_id
    rf.display_name = name
    rf.file_extension = ext
    rf.reference = MagicMock(curie=f"AGRKB:{reference_id}")
    rf.referencefile_mods = []
    return rf


def _run(rows, commit, limit=None, converted=lambda rf: False,
         outcome=lambda rf: (True, None), in_progress=lambda rid: False, **kwargs):
    """Drive convert_office_supplements with every collaborator stubbed."""
    with patch.object(mod, "create_postgres_session", return_value=MagicMock()), \
            patch.object(mod, "set_global_user_id"), \
            patch.object(mod, "candidate_office_supplements", return_value=rows), \
            patch.object(mod, "upload_in_progress", side_effect=lambda db, rid: in_progress(rid)), \
            patch.object(mod, "is_office_supplement_converted", side_effect=lambda db, rid, rf: converted(rf)), \
            patch.object(mod, "process_office_to_markdown",
                         side_effect=lambda **kw: outcome(kw["office_ref_file"])) as convert, \
            patch.object(mod, "embed_reference") as embed, \
            patch.object(mod, "set_suppress_upload_guardrails") as guard:
        counts = mod.convert_office_supplements([2025], commit=commit, limit=limit, **kwargs)
    _run.last_guard = guard
    return counts, convert, embed


def test_commit_embeds_each_reference_once_after_its_conversions():
    rows = [_row(1, 10, "t1", "xlsx"), _row(2, 10, "m1", "docx"), _row(3, 11, "t2", "xlsx")]
    counts, convert, embed = _run(rows, commit=True)
    assert convert.call_count == 3
    assert counts["converted"] == 3 and counts["embedded_references"] == 2
    assert [c.args[1:] for c in embed.call_args_list] == [(10, "AGRKB:10"), (11, "AGRKB:11")]


def test_reference_with_only_failed_conversions_is_not_embedded():
    rows = [_row(1, 10, "t1", "xlsx"), _row(2, 11, "t2", "xlsx")]
    counts, convert, embed = _run(rows, commit=True,
                                  outcome=lambda rf: (False, "bad zip") if rf.reference_id == 10 else (True, None))
    assert counts["errors"] == 1 and counts["converted"] == 1
    assert [c.args[1] for c in embed.call_args_list] == [11]


def test_dry_run_converts_and_embeds_nothing():
    counts, convert, embed = _run([_row(1, 10, "t1", "xlsx")], commit=False)
    assert counts["converted"] == 1
    convert.assert_not_called()
    embed.assert_not_called()


def test_limit_counts_conversion_attempts_not_rows():
    # Rows 1-2 belong to a reference with a file upload in progress and row 3
    # is already converted: none of them count towards --limit, so a limit of
    # 1 still performs one real conversion (row 4) and stops before row 5.
    rows = [_row(1, 10, "a", "xlsx"), _row(2, 10, "b", "xlsx"), _row(3, 11, "c", "xlsx"),
            _row(4, 12, "d", "xlsx"), _row(5, 13, "e", "xlsx")]
    with patch.object(mod, "create_postgres_session", return_value=MagicMock()), \
            patch.object(mod, "set_global_user_id"), \
            patch.object(mod, "candidate_office_supplements", return_value=rows), \
            patch.object(mod, "is_office_supplement_converted",
                         side_effect=lambda db, rid, rf: rf.referencefile_id == 3), \
            patch.object(mod, "upload_in_progress", side_effect=lambda db, rid: rid == 10), \
            patch.object(mod, "process_office_to_markdown", return_value=(True, None)) as convert, \
            patch.object(mod, "embed_reference") as embed:
        counts = mod.convert_office_supplements([2025], commit=True, limit=1)
    assert convert.call_count == 1
    assert convert.call_args.kwargs["office_ref_file"].referencefile_id == 4
    assert counts == {"converted": 1, "already_converted": 1, "upload_in_progress": 2,
                      "errors": 0, "embedded_references": 1}
    assert [c.args[1] for c in embed.call_args_list] == [12]


def test_commit_suppresses_upload_guardrails_for_the_whole_run_and_restores_them():
    _run([_row(1, 10, "t1", "xlsx")], commit=True)
    assert [c.args for c in _run.last_guard.call_args_list] == [(True,), (False,)]


def test_guardrails_restored_even_when_the_loop_raises():
    with patch.object(mod, "create_postgres_session", return_value=MagicMock()), \
            patch.object(mod, "set_global_user_id"), \
            patch.object(mod, "candidate_office_supplements", side_effect=RuntimeError("db gone")), \
            patch.object(mod, "set_suppress_upload_guardrails") as guard:
        with pytest.raises(RuntimeError):
            mod.convert_office_supplements([2025], commit=True)
    # candidate query raised before the guard was set: nothing to restore
    guard.assert_not_called()
    with patch.object(mod, "create_postgres_session", return_value=MagicMock()), \
            patch.object(mod, "set_global_user_id"), \
            patch.object(mod, "candidate_office_supplements", return_value=[_row(1, 10, "t1", "xlsx")]), \
            patch.object(mod, "_convert_rows", side_effect=RuntimeError("boom")), \
            patch.object(mod, "set_suppress_upload_guardrails") as guard:
        with pytest.raises(RuntimeError):
            mod.convert_office_supplements([2025], commit=True)
    assert [c.args for c in guard.call_args_list] == [(True,), (False,)]


def test_dry_run_leaves_guardrails_alone():
    _run([_row(1, 10, "t1", "xlsx")], commit=False)
    _run.last_guard.assert_not_called()


def test_references_with_a_file_upload_in_progress_are_skipped_by_default():
    rows = [_row(1, 10, "a", "xlsx"), _row(2, 10, "b", "docx"), _row(3, 11, "c", "xlsx")]
    counts, convert, embed = _run(rows, commit=True, in_progress=lambda rid: rid == 10)
    assert convert.call_count == 1
    assert convert.call_args.kwargs["office_ref_file"].reference_id == 11
    assert counts["upload_in_progress"] == 2 and counts["converted"] == 1
    assert [c.args[1] for c in embed.call_args_list] == [11]


def test_include_upload_in_progress_converts_them():
    rows = [_row(1, 10, "a", "xlsx"), _row(3, 11, "c", "xlsx")]
    counts, convert, embed = _run(rows, commit=True, in_progress=lambda rid: rid == 10,
                                  include_upload_in_progress=True)
    assert convert.call_count == 2
    assert counts["upload_in_progress"] == 0 and counts["converted"] == 2


def test_default_mod_scope_excludes_rgd_and_agr_and_all_mods_lifts_it():
    with patch.object(mod, "create_postgres_session", return_value=MagicMock()), \
            patch.object(mod, "set_global_user_id"), \
            patch.object(mod, "candidate_office_supplements", return_value=[]) as candidates:
        mod.convert_office_supplements([2025], commit=False)
        mod.convert_office_supplements([2025], commit=False, all_mods=True)
        mod.convert_office_supplements([2025], commit=False, excluded_mods=("RGD", "MGI"), mod_abbreviation="WB")
    assert [c.args[1:] for c in candidates.call_args_list] == [
        ([2025], None, ("RGD", "AGR")), ([2025], None, ()), ([2025], "WB", ("RGD", "MGI"))]


def _corpus_entry_sql(**kwargs):
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.orm import Session
    query = mod.entered_corpus_reference_ids(Session(), [2025, 2026], **kwargs)
    return str(query.statement.compile(dialect=postgresql.dialect(),
                                       compile_kwargs={"literal_binds": True}))


def test_corpus_entry_year_is_last_change_to_true_else_date_created():
    sql = _corpus_entry_sql(excluded_mods=("rgd", "AGR"))
    assert "mod_corpus_association.corpus IS true" in sql
    # latest version row where corpus changed to True, for the same association
    assert "max(mod_corpus_association_version.date_updated)" in sql
    assert "mod_corpus_association_version.corpus_mod IS true" in sql
    assert ("mod_corpus_association_version.mod_corpus_association_id = "
            "mod_corpus_association.mod_corpus_association_id") in sql
    assert "mod_corpus_association.date_created)) IN (2025, 2026)" in sql
    assert "coalesce(" in sql
    assert "NOT IN ('RGD', 'AGR')" in sql


def test_corpus_entry_without_exclusion_or_with_one_mod():
    assert "NOT IN" not in _corpus_entry_sql()
    sql = _corpus_entry_sql(mod_abbreviation="wb")
    assert "mod.abbreviation = 'WB'" in sql
