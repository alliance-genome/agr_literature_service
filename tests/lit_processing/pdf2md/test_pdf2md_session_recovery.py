"""pdf2md keeps going after a database error.

On 2026-10-01 the stage run hit a Postgres deadlock while saving one supplement's
GROBID Markdown. The error was caught and logged, but the session was never rolled
back, so every later statement failed with InFailedSqlTransaction: the other
methods, the images, the other supplements, and finally the job-status update that
crashed the whole run (56 references unprocessed, no error report sent).

Each test below poisons a real Postgres session the same way (a statement fails
inside the transaction) and then needs the next database call to succeed, which it
only can if the failure was rolled back.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from agr_literature_service.api.models import ModModel
from agr_literature_service.lit_processing.pdf2md import pdf2md, pdf2md_utils
from agr_literature_service.lit_processing.pdf2md.pdf2md_utils import recover_session
from ...fixtures import db  # noqa

UTILS = "agr_literature_service.lit_processing.pdf2md.pdf2md_utils"
MAIN = "agr_literature_service.lit_processing.pdf2md.pdf2md"


def _fail_inside_transaction(db):  # noqa
    """Raise the way a deadlock does: a DBAPI error that aborts the transaction."""
    db.execute(text("SELECT 1 / 0"))


def _session_is_usable(db):  # noqa
    return db.execute(text("SELECT 1")).scalar() == 1


def _error_record(db, reference_curie, display_name, file_extension, mod_abbreviation, error):  # noqa
    return {"mod_abbreviation": mod_abbreviation, "mod_cross_ref": "N/A", "reference_curie": reference_curie,
            "display_name": display_name, "file_extension": file_extension, "error": error}


class TestRecoverSession:

    def test_rolls_back_after_a_database_error(self, db):  # noqa
        try:
            _fail_inside_transaction(db)
        except DBAPIError as e:
            recover_session(db, e)
        assert _session_is_usable(db)

    def test_leaves_the_session_alone_after_other_errors(self):
        # e.g. a PDFX download timeout: nothing in the database failed, and a
        # rollback would throw away uncommitted work for no reason.
        session = MagicMock()
        recover_session(session, ValueError("PDFX timed out"))
        session.rollback.assert_not_called()


class TestConversionMethods:

    def test_one_failed_method_upload_does_not_fail_the_others(self, db):  # noqa
        calls = []

        def upload(db, metadata, file, upload_if_already_converted):  # noqa
            calls.append(metadata["display_name"])
            if len(calls) == 1:
                _fail_inside_transaction(db)
            assert _session_is_usable(db)

        pdf_file = SimpleNamespace(display_name="supp", file_class="supplement", referencefile_mods=[],
                                   referencefile_id=1)
        with patch(f"{UTILS}.download_file", return_value=b"%PDF-1.4"), \
                patch(f"{UTILS}.validate_supplement_pdf_for_pdfx", return_value=None), \
                patch(f"{UTILS}.submit_pdf_to_pdfx", return_value="pid"), \
                patch(f"{UTILS}.poll_pdfx_status"), \
                patch(f"{UTILS}.download_pdfx_result", return_value=b"# converted markdown"), \
                patch(f"{UTILS}.process_extracted_images", return_value=(0, 0, [])), \
                patch(f"{UTILS}.file_upload", side_effect=upload):
            success, methods, _ = pdf2md_utils._process_single_pdf_file(
                db, pdf_file, "AGRKB:101", "token", ["grobid", "docling", "marker"])

        assert success
        assert methods == ["docling", "marker"]


class TestExtractedImages:

    def test_one_failed_image_upload_does_not_fail_the_others(self, db):  # noqa
        calls = []

        def upload(db, metadata, file, upload_if_already_converted):  # noqa
            calls.append(metadata["display_name"])
            if len(calls) == 1:
                _fail_inside_transaction(db)
            assert _session_is_usable(db)
            return []

        manifest = {"images": [{"url": "u1"}, {"url": "u2"}, {"url": "u3"}]}
        with patch(f"{UTILS}.download_pdfx_image_manifest", return_value=manifest), \
                patch(f"{UTILS}.download_pdfx_image", return_value=b"\x89PNG"), \
                patch(f"{UTILS}._upload_figure_metadata_sidecar"), \
                patch(f"{UTILS}.file_upload", side_effect=upload):
            succeeded, failed, _ = pdf2md_utils.process_extracted_images(
                db, "pid", "token", "supp", "supplement", "AGRKB:101", "FB")

        assert (succeeded, failed) == (2, 1)


class TestSupplements:

    def test_one_failed_supplement_does_not_fail_the_others(self, db):  # noqa
        supplements = [SimpleNamespace(display_name="supp1"), SimpleNamespace(display_name="supp2")]

        def convert(db, pdf_file, **kwargs):  # noqa
            if pdf_file.display_name == "supp1":
                _fail_inside_transaction(db)
            assert _session_is_usable(db)
            return True, ["grobid"], None

        with patch(f"{UTILS}.is_eligible_for_supplement_conversion", return_value=True), \
                patch(f"{UTILS}.pending_supplement_sources", return_value=supplements), \
                patch(f"{UTILS}._process_single_pdf_file", side_effect=convert):
            succeeded, failed, _ = pdf2md_utils.process_supplemental_pdfs(db, 1, "AGRKB:101", "token")

        assert (succeeded, failed) == (1, 1)


def _run_main(db, jobs, convert, change_status):  # noqa
    """Run pdf2md.main() over ``jobs`` on the test session; return the report text."""
    ref_file_info = {"display_name": "main", "file_extension": "pdf"}
    mod_query = MagicMock()
    mod_query.filter.return_value.one.return_value.abbreviation = "FB"
    with patch(f"{MAIN}.sessionmaker", return_value=lambda: db), \
            patch.object(db, "query", side_effect=lambda *a, **k: mod_query), \
            patch(f"{MAIN}.get_jobs", side_effect=[jobs, []]), \
            patch(f"{MAIN}._resolve_workflow_ref_file_info", return_value=(ref_file_info, None)), \
            patch(f"{MAIN}.get_admin_token", return_value="token"), \
            patch(f"{MAIN}.process_single_reference", side_effect=convert), \
            patch(f"{MAIN}.job_change_atp_code", side_effect=change_status), \
            patch(f"{MAIN}._build_workflow_error_record", side_effect=_error_record), \
            patch(f"{MAIN}.send_report") as report:
        pdf2md.main()
    report.assert_called_once()
    return report.call_args.args[1]


def _jobs(*numbers):
    return [{"reference_id": n, "reference_workflow_tag_id": 1000 + n, "mod_id": 1,
             "reference_curie": f"AGRKB:10{n}"} for n in numbers]


class TestMainLoop:

    def test_a_database_error_on_one_job_does_not_stop_the_run(self, db):  # noqa
        # The stage crash: the error escaped into main's loop, the next statement
        # (the job-status update) failed, and the run ended. Each job must now
        # fail on its own, the run must reach the end, and the report must go out.
        status_updates = []

        def convert(db, info, token, **kwargs):  # noqa
            if not status_updates:
                _fail_inside_transaction(db)
            return True, None

        def change_status(db, workflow_tag_id, condition):  # noqa
            assert _session_is_usable(db)
            status_updates.append((workflow_tag_id, condition))

        report = _run_main(db, _jobs(1, 2), convert, change_status)

        assert status_updates == [(1001, "on_failed"), (1002, "on_success")]
        assert "AGRKB:101" in report

    def test_a_failed_success_transition_is_not_committed(self, db):  # noqa
        # job_change_atp_code sets the new tag in memory, then runs the
        # transition's actions, which raise HTTPException (not a database error).
        # The half-applied success state must be discarded before the job is
        # marked failed; otherwise the on_failed call's commit saves it.
        row = ModModel(abbreviation="0099_PdfDb", short_name="PdfDb", full_name="before")
        db.add(row)
        db.commit()
        row_id = row.mod_id
        status_updates = []

        def change_status(db, workflow_tag_id, condition):  # noqa
            status_updates.append((workflow_tag_id, condition))
            if condition == "on_success":
                db.get(ModModel, row_id).full_name = "half-applied success"
                raise HTTPException(status_code=422, detail="transition action failed")
            db.commit()

        report = _run_main(db, _jobs(1), lambda db, info, token, **kwargs: (True, None), change_status)

        committed = db.execute(text("SELECT full_name FROM mod WHERE mod_id = :i"), {"i": row_id}).scalar()
        assert committed == "before"
        assert status_updates == [(1001, "on_success"), (1001, "on_failed")]
        # counted once, as a failure: not also as a success
        assert "Total: 1, Success: 0, Failed: 1, Skipped: 0" in report
