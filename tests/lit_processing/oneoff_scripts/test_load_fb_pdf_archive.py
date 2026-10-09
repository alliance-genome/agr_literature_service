"""The SCRUM-6633 FlyBase PDF archive load: filename parsing, the workflow
status update and the load loop with every database collaborator stubbed."""
import csv
import importlib.util
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

# Ticket-prefixed script name is not an importable module name: load by path.
_SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "agr_literature_service" / "lit_processing" / "oneoff_scripts"
    / "SCRUM-6633_load_fb_pdf_archive.py"
)
_spec = importlib.util.spec_from_file_location("load_fb_pdf_archive", _SCRIPT)
assert _spec is not None and _spec.loader is not None
script = importlib.util.module_from_spec(_spec)
cast(Any, _spec.loader).exec_module(script)

mod = cast(Any, script)
FILES_UPLOADED = "ATP:0000134"


@pytest.mark.parametrize("filename, expected", [
    ("FBrf0123456.pdf", "FBrf0123456"),
    ("fbrf0123456.pdf", None),
    ("FBrf012345.pdf", None),
    ("FBrf01234567.pdf", None),
    ("FBrf0123456.PDF", None),
    ("FBrf0123456.txt", None),
    ("FBrf0123456_temp.pdf", None),
    ("12345678.pdf", None),
])
def test_parse_fbrf(filename, expected):
    assert mod.parse_fbrf(filename) == expected


def test_archive_files_lists_regular_files_by_name(tmp_path):
    (tmp_path / "FBrf0000002.pdf").write_bytes(b"b")
    (tmp_path / "FBrf0000001.pdf").write_bytes(b"a")
    (tmp_path / "subdir").mkdir()
    assert [name for name, _ in mod.archive_files(str(tmp_path))] == ["FBrf0000001.pdf", "FBrf0000002.pdf"]


def test_file_md5(tmp_path):
    pdf = tmp_path / "FBrf0000001.pdf"
    pdf.write_bytes(b"hello")
    assert mod.file_md5(str(pdf)) == "5d41402abc4b2a76b9719d911017c592"


class TestSetFileUploadedStatus:
    """The status is set without transition actions: create() or a plain
    in-place update, never transition_to_workflow_status."""

    def _run(self, current):
        db = MagicMock()
        with patch.object(mod, "_get_current_workflow_tag_db_obj", return_value=current) as lookup, \
                patch.object(mod, "create_wft") as create:
            change = mod.set_file_uploaded_status(db, "AGRKB:101000000000001")
        lookup.assert_called_once_with(db, "AGRKB:101000000000001", "ATP:0000140", "FB")
        return change, create, db

    def test_creates_tag_when_paper_has_none(self):
        change, create, db = self._run(None)
        assert change == "created"
        data = create.call_args.args[1]
        assert data.workflow_tag_id == FILES_UPLOADED
        assert data.mod_abbreviation == "FB"
        assert data.reference_curie == "AGRKB:101000000000001"

    def test_overwrites_other_tag_in_place(self):
        current = MagicMock(workflow_tag_id="ATP:0000141")  # file needed
        change, create, db = self._run(current)
        assert change == "ATP:0000141 -> ATP:0000134"
        assert current.workflow_tag_id == FILES_UPLOADED
        db.commit.assert_called_once()
        create.assert_not_called()

    def test_leaves_files_uploaded_alone(self):
        current = MagicMock(workflow_tag_id=FILES_UPLOADED)
        change, create, db = self._run(current)
        assert change is None
        db.commit.assert_not_called()
        create.assert_not_called()

    def test_script_never_imports_the_action_running_transition(self):
        assert not hasattr(mod, "transition_to_workflow_status")
        assert not hasattr(mod, "transition_WFT_for_uploaded_file")


def test_upload_archive_pdf_metadata(tmp_path):
    pdf = tmp_path / "FBrf0000001.pdf"
    pdf.write_bytes(b"%PDF")
    with patch.object(mod, "file_upload") as upload:
        mod.upload_archive_pdf(MagicMock(), "AGRKB:1", "FBrf0000001", str(pdf))
    kwargs = upload.call_args.kwargs
    assert kwargs["metadata"] == {
        "reference_curie": "AGRKB:1", "display_name": "FBrf0000001", "file_class": "main",
        "file_publication_status": "final", "file_extension": "pdf", "pdf_type": "pdf",
        "is_annotation": False, "mod_abbreviation": "FB"}
    assert kwargs["file"].filename == "FBrf0000001.pdf"
    assert kwargs["upload_if_already_converted"] is True


def _archive(tmp_path, *names):
    for name in names:
        (tmp_path / name).write_bytes(name.encode())
    return str(tmp_path)


def _ref(reference_id):
    return MagicMock(reference_id=reference_id, curie=f"AGRKB:{reference_id}")


def _run(folder, commit, refs=None, in_corpus=lambda curie: True, existing=lambda rid: set(),
         upload_error=None, **kwargs):
    """Drive load_fb_pdf_archive with every database collaborator stubbed.
    ``refs`` maps FBrf -> reference; ``existing`` gives a reference's FB md5s."""
    refs = refs or {}
    db = MagicMock()
    with patch.object(mod, "create_postgres_session", return_value=db), \
            patch.object(mod, "set_global_user_id"), \
            patch.object(mod, "find_reference", side_effect=lambda _db, fbrf: refs.get(fbrf)), \
            patch.object(mod, "check_if_paper_in_corpus",
                         side_effect=lambda _db, curie, mod_abbr: in_corpus(curie)), \
            patch.object(mod, "fb_main_pdf_md5s", side_effect=lambda _db, rid: existing(rid)), \
            patch.object(mod, "upload_archive_pdf", side_effect=upload_error) as upload, \
            patch.object(mod, "set_file_uploaded_status", return_value="created") as status, \
            patch.object(mod, "set_suppress_upload_guardrails") as guard:
        counts = mod.load_fb_pdf_archive(folder, commit=commit, **kwargs)
    return counts, upload, status, guard, db


def test_dry_run_writes_nothing(tmp_path):
    folder = _archive(tmp_path, "FBrf0000001.pdf")
    counts, upload, status, guard, _ = _run(folder, commit=False, refs={"FBrf0000001": _ref(1)})
    assert counts["uploaded"] == 1
    upload.assert_not_called()
    status.assert_not_called()
    guard.assert_not_called()


def test_commit_uploads_and_sets_status_with_guardrails_suppressed(tmp_path):
    folder = _archive(tmp_path, "FBrf0000001.pdf")
    counts, upload, status, guard, db = _run(folder, commit=True, refs={"FBrf0000001": _ref(1)})
    assert counts["uploaded"] == 1 and counts["wft_set"] == 1
    upload.assert_called_once_with(db, "AGRKB:1", "FBrf0000001", str(tmp_path / "FBrf0000001.pdf"))
    status.assert_called_once_with(db, "AGRKB:1")
    assert [c.args[0] for c in guard.call_args_list] == [True, False]


def test_skips_are_counted_and_the_run_continues(tmp_path):
    folder = _archive(tmp_path, "FBrf0000001.pdf", "FBrf0000002.pdf", "FBrf0000003.pdf",
                      "FBrf0000004.pdf", "notes.txt", "FBrf0000005.pdf")
    md5_of_4 = mod.file_md5(str(tmp_path / "FBrf0000004.pdf"))
    refs = {"FBrf0000002": _ref(2), "FBrf0000003": _ref(3), "FBrf0000004": _ref(4), "FBrf0000005": _ref(5)}
    existing = {3: {"some-other-md5"}, 4: {md5_of_4}}
    counts, upload, status, _, _ = _run(
        folder, commit=True, refs=refs,
        in_corpus=lambda curie: curie != "AGRKB:2",
        existing=lambda rid: existing.get(rid, set()))
    assert counts["files"] == 6
    assert counts["no_xref"] == 1            # FBrf0000001
    assert counts["not_in_corpus"] == 1      # FBrf0000002
    assert counts["has_other_fb_pdf"] == 1   # FBrf0000003
    assert counts["already_loaded"] == 1     # FBrf0000004
    assert counts["bad_name"] == 1           # notes.txt
    assert counts["uploaded"] == 1           # FBrf0000005
    assert [c.args[1] for c in upload.call_args_list] == ["AGRKB:5"]
    # already loaded still gets its status set, so a rerun finishes an interrupted file
    assert [c.args[1] for c in status.call_args_list] == ["AGRKB:4", "AGRKB:5"]


def test_include_existing_uploads_alongside_other_fb_pdf(tmp_path):
    folder = _archive(tmp_path, "FBrf0000003.pdf")
    counts, upload, _, _, _ = _run(folder, commit=True, refs={"FBrf0000003": _ref(3)},
                                   existing=lambda rid: {"some-other-md5"}, include_existing=True)
    assert counts["uploaded"] == 1 and counts["has_other_fb_pdf"] == 0
    upload.assert_called_once()


def test_upload_error_rolls_back_and_continues(tmp_path):
    folder = _archive(tmp_path, "FBrf0000001.pdf", "FBrf0000002.pdf")
    refs = {"FBrf0000001": _ref(1), "FBrf0000002": _ref(2)}
    outcomes = iter([RuntimeError("S3 down"), None])

    def fail_first(*args):
        outcome = next(outcomes)
        if outcome:
            raise outcome

    counts, upload, status, guard, db = _run(folder, commit=True, refs=refs, upload_error=fail_first)
    assert counts["errors"] == 1 and counts["uploaded"] == 1
    db.rollback.assert_called_once()
    assert [c.args[1] for c in status.call_args_list] == ["AGRKB:2"]


def test_guardrails_reset_when_the_run_raises(tmp_path):
    folder = _archive(tmp_path, "FBrf0000001.pdf")
    with patch.object(mod, "create_postgres_session", return_value=MagicMock()), \
            patch.object(mod, "set_global_user_id"), \
            patch.object(mod, "_load_files", side_effect=KeyboardInterrupt), \
            patch.object(mod, "set_suppress_upload_guardrails") as guard:
        with pytest.raises(KeyboardInterrupt):
            mod.load_fb_pdf_archive(folder, commit=True)
    assert [c.args[0] for c in guard.call_args_list] == [True, False]


def test_limit_counts_upload_attempts_only(tmp_path):
    folder = _archive(tmp_path, "FBrf0000001.pdf", "FBrf0000002.pdf", "FBrf0000003.pdf")
    refs = {"FBrf0000002": _ref(2), "FBrf0000003": _ref(3)}
    counts, upload, _, _, _ = _run(folder, commit=True, refs=refs, limit=1)
    assert counts["no_xref"] == 1    # a skip does not use up the limit
    assert counts["uploaded"] == 1
    assert [c.args[1] for c in upload.call_args_list] == ["AGRKB:2"]


def test_report_lists_files_not_uploaded(tmp_path):
    archive = tmp_path / "archive"
    archive.mkdir()
    folder = _archive(archive, "FBrf0000001.pdf", "bad.pdf")
    report = tmp_path / "report.tsv"
    _run(folder, commit=False, report_path=str(report))
    with open(report) as f:
        rows = list(csv.reader(f, delimiter="\t"))
    assert rows == [["file", "reason", "detail"],
                    ["FBrf0000001.pdf", "no_xref", "FB:FBrf0000001"],
                    ["bad.pdf", "bad_name", ""]]
