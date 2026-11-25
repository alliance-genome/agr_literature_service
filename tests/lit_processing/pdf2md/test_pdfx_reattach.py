"""An interrupted on-demand conversion re-attaches to its PDFX job (SCRUM-6998).

The conversion worker records each PDF's PDFX process_id on the job when it
submits it. A run that resumes the job passes it back, and the PDF is polled
under that id instead of being submitted to PDFX again.
"""
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import requests

from agr_literature_service.api.models import ReferenceModel
from agr_literature_service.lit_processing.pdf2md import pdf2md_utils
from ...fixtures import db  # noqa

UTILS = "agr_literature_service.lit_processing.pdf2md.pdf2md_utils"
CRUD = "agr_literature_service.api.crud.referencefile_crud"
CURIE = "AGRKB:101000699801"
# above pdf2md's 10-byte minimum, so the merged Markdown is stored
MERGED_MARKDOWN = b"# Title\n\nThe merged text of the paper."


def _http_error(status_code):
    response = requests.Response()
    response.status_code = status_code
    return requests.HTTPError(f"{status_code} error", response=response)


class TestReattachPdfxJob:
    def test_a_completed_job_is_reused(self):
        with patch(f"{UTILS}.poll_pdfx_status", return_value={"status": "completed"}) as poll:
            assert pdf2md_utils._reattach_pdfx_job("p1", "paper", CURIE) == "p1"
        poll.assert_called_once_with("p1")

    @pytest.mark.parametrize("error", [
        _http_error(404),
        RuntimeError("PDFX job failed: out of memory"),
    ])
    def test_an_unknown_or_failed_job_is_given_up(self, error):
        with patch(f"{UTILS}.poll_pdfx_status", side_effect=error):
            assert pdf2md_utils._reattach_pdfx_job("p1", "paper", CURIE) is None

    @pytest.mark.parametrize("error", [
        TimeoutError("still running"),
        _http_error(503),
        requests.ConnectionError("pdfx unreachable"),
    ])
    def test_a_temporary_error_is_raised_not_resubmitted(self, error):
        """Review: PDFX may still be running the job, so only a 404 or a failed job
        leads to a new submission."""
        with patch(f"{UTILS}.poll_pdfx_status", side_effect=error):
            with pytest.raises(type(error)):
                pdf2md_utils._reattach_pdfx_job("p1", "paper", CURIE)


@pytest.fixture
def paper(db):  # noqa
    reference = ReferenceModel(curie=CURIE, category="research_article")
    db.add(reference)
    db.commit()
    db.refresh(reference)
    return reference


def _convert(db, poll, pdfx_process_id=None, on_submitted=None):  # noqa
    """Run one main PDF through _process_single_pdf_file, PDFX mocked."""
    pdf_file = SimpleNamespace(display_name="paper", file_class="main", referencefile_mods=[],
                               referencefile_id=1)
    with patch(f"{UTILS}.download_file", return_value=b"%PDF-1.4"), \
            patch(f"{UTILS}.submit_pdf_to_pdfx", return_value="new-pid") as submit, \
            patch(f"{UTILS}.poll_pdfx_status", side_effect=poll) as polled, \
            patch(f"{UTILS}.download_pdfx_result", return_value=MERGED_MARKDOWN), \
            patch(f"{UTILS}.process_extracted_images", return_value=(0, 0, [])), \
            patch(f"{CRUD}.boto3"), \
            patch(f"{CRUD}.upload_file_to_bucket"), \
            patch(f"{CRUD}.cleanup_old_pdf_file"), \
            patch(f"{CRUD}.transition_WFT_for_uploaded_file"):
        result = pdf2md_utils._process_single_pdf_file(
            db, pdf_file, CURIE, ["merged"], pdfx_process_id=pdfx_process_id,
            on_submitted=on_submitted)
    return result, submit, polled


def test_a_new_submission_is_reported_before_polling(db, paper):  # noqa
    reported = []
    (success, _methods, _error), submit, polled = _convert(
        db, poll=lambda pid: {"status": "completed"}, on_submitted=reported.append)
    assert success
    submit.assert_called_once()
    assert reported == ["new-pid"]
    polled.assert_called_once_with("new-pid")


def test_a_known_process_id_is_polled_not_resubmitted(db, paper):  # noqa
    reported = []
    (success, _methods, _error), submit, polled = _convert(
        db, poll=lambda pid: {"status": "completed"}, pdfx_process_id="old-pid",
        on_submitted=reported.append)
    assert success
    submit.assert_not_called()
    assert reported == []
    polled.assert_called_once_with("old-pid")


def test_an_unknown_process_id_falls_back_to_a_new_submission(db, paper):  # noqa
    def poll(pid):
        if pid == "old-pid":
            raise _http_error(404)
        return {"status": "completed"}

    reported = []
    (success, _methods, _error), submit, _polled = _convert(
        db, poll=poll, pdfx_process_id="old-pid", on_submitted=reported.append)
    assert success
    submit.assert_called_once()
    assert reported == ["new-pid"]
