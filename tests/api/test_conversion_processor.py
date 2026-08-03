"""Unit tests for the on-demand file conversion background task.

Everything the processor touches (DB session, conversion_manager, the pdf2md
primitives and crud helpers) is imported function-locally, so each collaborator
is patched at its source module and no real DB / S3 / network access happens.
"""
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

# The pdf2md conversion primitives depend on the external
# ``agr_abc_document_parsers`` package (installed in CI/Docker via
# requirements.txt). Stub it only when it is genuinely absent so this test can
# be run in a bare local checkout; CI keeps using the real package.
if "agr_abc_document_parsers" not in sys.modules:
    try:  # pragma: no cover - exercised only in local dev without the dep
        import agr_abc_document_parsers  # noqa: F401
    except ModuleNotFoundError:  # pragma: no cover
        _stub = types.ModuleType("agr_abc_document_parsers")
        _stub.convert_xml_to_markdown = lambda *a, **k: ("", "")  # type: ignore[attr-defined]
        sys.modules["agr_abc_document_parsers"] = _stub

from agr_literature_service.api.utils import conversion_processor as cp

CRUD = "agr_literature_service.api.crud.file_conversion_crud"
PDF2MD = "agr_literature_service.lit_processing.pdf2md.pdf2md_utils"


def _pdf_file(display_name="main.pdf", file_class="main", rf_id=7):
    f = MagicMock()
    f.display_name = display_name
    f.file_class = file_class
    f.referencefile_id = rf_id
    return f


@pytest.fixture
def manager():
    with patch.object(cp, "conversion_manager") as mgr:
        yield mgr


class TestAllExtractionMethods:
    def test_returns_method_keys(self):
        with patch(f"{PDF2MD}.EXTRACTION_METHODS", {"grobid": 1, "pdfx": 2}):
            assert set(cp._all_extraction_methods()) == {"grobid", "pdfx"}


class TestRecordSinglePdf:
    def test_merged_success_links_converted_row(self, manager):
        with patch(f"{CRUD}.find_converted_referencefile_id", return_value=99) as find:
            cp._record_single_pdf(
                db=MagicMock(), job_id="j1", reference_id=3,
                pdf_file=_pdf_file(), success=True,
                methods_uploaded=["merged", "grobid"], error=None,
                output_file_class="converted_merged_main",
            )
        find.assert_called_once()
        kwargs = manager.record_file_progress.call_args.kwargs
        assert kwargs["converted_display_name"] == "main.pdf_merged"
        assert kwargs["converted_referencefile_id"] == 99
        assert kwargs["success"] is True

    def test_no_merged_leaves_converted_null(self, manager):
        with patch(f"{CRUD}.find_converted_referencefile_id") as find:
            cp._record_single_pdf(
                db=MagicMock(), job_id="j1", reference_id=3,
                pdf_file=_pdf_file(), success=False,
                methods_uploaded=[], error="bad",
                output_file_class="converted_merged_main",
            )
        find.assert_not_called()
        kwargs = manager.record_file_progress.call_args.kwargs
        assert kwargs["converted_display_name"] is None
        assert kwargs["converted_file_class"] is None
        assert kwargs["error"] == "bad"


class TestConvertPendingNxml:
    def test_success(self, manager):
        nxml = _pdf_file(display_name="paper.nxml", file_class="nXML", rf_id=11)
        with patch(f"{PDF2MD}.process_nxml_to_markdown", return_value=(True, None)), \
                patch(f"{CRUD}.find_converted_referencefile_id", return_value=42):
            success, error = cp._convert_pending_nxml(
                MagicMock(), "j1", 3, "AGRKB:1", nxml, "WB",
            )
        assert success is True and error is None
        kwargs = manager.record_file_progress.call_args.kwargs
        assert kwargs["converted_display_name"] == "paper.nxml_nxml"
        assert kwargs["converted_referencefile_id"] == 42

    def test_failure(self, manager):
        nxml = _pdf_file(display_name="paper.nxml", file_class="nXML", rf_id=11)
        with patch(f"{PDF2MD}.process_nxml_to_markdown", return_value=(False, "grobid empty")), \
                patch(f"{CRUD}.find_converted_referencefile_id") as find:
            success, error = cp._convert_pending_nxml(
                MagicMock(), "j1", 3, "AGRKB:1", nxml, "WB",
            )
        assert success is False and error == "grobid empty"
        find.assert_not_called()


class TestConvertPendingPdf:
    def test_success(self, manager):
        with patch(f"{PDF2MD}._process_single_pdf_file",
                   return_value=(True, ["merged"], None)), \
                patch(f"{CRUD}.find_converted_referencefile_id", return_value=5), \
                patch(f"{PDF2MD}.EXTRACTION_METHODS", {"grobid": 1}):
            success, error = cp._convert_pending_pdf(
                MagicMock(), "j1", 3, "AGRKB:1", _pdf_file(),
                "converted_merged_main", "tok",
            )
        assert success is True and error is None

    def test_exception_is_captured(self, manager):
        with patch(f"{PDF2MD}._process_single_pdf_file",
                   side_effect=RuntimeError("pdfx down")), \
                patch(f"{PDF2MD}.EXTRACTION_METHODS", {"grobid": 1}):
            success, error = cp._convert_pending_pdf(
                MagicMock(), "j1", 3, "AGRKB:1", _pdf_file(),
                "converted_merged_main", "tok",
            )
        assert success is False and error == "pdfx down"


class TestConvertPendingMain:
    def test_mixed_nxml_and_pdf_with_failure(self, manager):
        assessment = {
            "mod_abbreviation": "WB",
            "pending_main": [
                {"kind": "nxml", "ref_file": _pdf_file("a.nxml", "nXML", 1)},
                {"kind": "pdf", "ref_file": _pdf_file("b.pdf", "main", 2)},
            ],
        }
        with patch.object(cp, "_convert_pending_nxml", return_value=(True, None)), \
                patch.object(cp, "_convert_pending_pdf", return_value=(False, "pdf boom")):
            any_failure, messages = cp._convert_pending_main(
                MagicMock(), "j1", 3, "AGRKB:1", assessment, lambda: "tok",
            )
        assert any_failure is True
        assert any("pdf boom" in m for m in messages)

    def test_empty_pending_main(self, manager):
        any_failure, messages = cp._convert_pending_main(
            MagicMock(), "j1", 3, "AGRKB:1", {"pending_main": None}, lambda: "tok",
        )
        assert any_failure is False and messages == []


class TestConvertPendingSupplements:
    def test_failure_message_collected(self, manager):
        assessment = {"pending_supplements": [_pdf_file("s.pdf", "supplement", 9)]}
        with patch.object(cp, "_convert_pending_pdf", return_value=(False, "too big")):
            any_failure, messages = cp._convert_pending_supplements(
                MagicMock(), "j1", 3, "AGRKB:1", assessment, lambda: "tok",
            )
        assert any_failure is True
        assert messages == ["PDF supplement 's.pdf': too big"]

    def test_success(self, manager):
        assessment = {"pending_supplements": [_pdf_file("s.pdf", "supplement", 9)]}
        with patch.object(cp, "_convert_pending_pdf", return_value=(True, None)):
            any_failure, messages = cp._convert_pending_supplements(
                MagicMock(), "j1", 3, "AGRKB:1", assessment, lambda: "tok",
            )
        assert any_failure is False and messages == []


class TestRunConversionJob:
    def test_happy_path_completes_job(self, manager):
        session = MagicMock()
        assessment = {"mod_abbreviation": "WB", "pending_main": [], "pending_supplements": []}
        with patch.object(cp, "SessionLocal", return_value=session), \
                patch(f"{CRUD}._assess_reference", return_value=assessment), \
                patch(f"{CRUD}.delete_tei_derived_md_rows"), \
                patch(f"{CRUD}.transition_completed_text_convert_tags"), \
                patch("agr_literature_service.api.crud.reference_utils.get_reference"), \
                patch(f"{PDF2MD}.sync_converted_file_mods_to_sources"), \
                patch("agr_literature_service.lit_processing.embedding."
                      "embedding_generation.maybe_generate_classifier_embeddings") as embed:
            cp.run_conversion_job("j1", 3, "AGRKB:1")

        embed.assert_called_once()
        manager.complete_job.assert_called_once()
        assert manager.complete_job.call_args.kwargs["success"] is True
        session.close.assert_called_once()

    def test_unexpected_error_marks_job_failed(self, manager):
        session = MagicMock()
        with patch.object(cp, "SessionLocal", return_value=session), \
                patch("agr_literature_service.api.crud.reference_utils.get_reference",
                      side_effect=RuntimeError("db exploded")):
            cp.run_conversion_job("j1", 3, "AGRKB:1")

        manager.complete_job.assert_called_once()
        kwargs = manager.complete_job.call_args.kwargs
        assert kwargs["success"] is False
        assert "db exploded" in kwargs["error"]
        session.close.assert_called_once()
