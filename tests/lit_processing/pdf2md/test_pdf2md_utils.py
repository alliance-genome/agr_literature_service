"""
Unit tests for pdf2md_utils.py

Tests cover:
- PDFX token management
- PDF submission to PDFX
- PDFX status polling
- Result downloading
- Curie resolution
- PDF file retrieval
- Processing result structures
"""
from unittest.mock import MagicMock, patch

import pytest
import requests
from agr_cognito_py import get_admin_token

from agr_literature_service.lit_processing.pdf2md.pdf2md_utils import (
    EXTRACTION_METHODS,
    PdfDetail,
    ProcessingResult,
    submit_pdf_to_pdfx,
    poll_pdfx_status,
    download_pdfx_result,
    resolve_curie_to_reference,
    get_pdf_files_for_reference,
    get_nxml_referencefile,
    process_nxml_to_markdown,
    process_supplemental_pdfs,
)


class TestExtractionMethods:
    """Test EXTRACTION_METHODS constant."""

    def test_extraction_methods_contains_expected_keys(self):
        assert "grobid" in EXTRACTION_METHODS
        assert "docling" in EXTRACTION_METHODS
        assert "marker" in EXTRACTION_METHODS
        assert "merged" in EXTRACTION_METHODS

    def test_extraction_methods_values_are_strings(self):
        for method, file_class in EXTRACTION_METHODS.items():
            assert isinstance(method, str)
            assert isinstance(file_class, str)
            assert file_class.startswith("converted_")


class TestTypedDicts:
    """Test TypedDict structures."""

    def test_pdf_detail_structure(self):
        detail: PdfDetail = {
            "referencefile_id": 123,
            "display_name": "test.pdf",
            "file_class": "main",
            "success": True,
            "methods_uploaded": ["grobid", "docling"],
            "error": None
        }
        assert detail["referencefile_id"] == 123
        assert detail["success"] is True
        assert detail["methods_uploaded"] == ["grobid", "docling"]

    def test_processing_result_structure(self):
        result: ProcessingResult = {
            "success": True,
            "reference_curie": "AGRKB:101000000000001",
            "input_curie": "PMID:12345",
            "pdfs_processed": 1,
            "pdfs_succeeded": 1,
            "pdfs_failed": 0,
            "details": [],
            "error": None
        }
        assert result["success"] is True
        assert result["pdfs_processed"] == 1


class TestGetPdfxToken:
    """Test get_pdfx_token function."""

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.get_authentication_token")
    def test_returns_token_from_get_authentication_token(self, mock_get_auth):
        """Test that get_pdfx_token delegates to get_authentication_token."""
        mock_get_auth.return_value = "test_token"

        token = get_admin_token()

        assert token == "test_token"
        mock_get_auth.assert_called_once()


class TestSubmitPdfToPdfx:
    """Test submit_pdf_to_pdfx function."""

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.requests.post")
    def test_successful_submission(self, mock_post):
        """Test successful PDF submission."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"process_id": "abc123"}
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        process_id = submit_pdf_to_pdfx(
            file_content=b"pdf_content",
            token="test_token",
            methods="grobid,docling",
            merge=True,
            reference_curie="PMID:12345"
        )

        assert process_id == "abc123"
        mock_post.assert_called_once()

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.requests.post")
    def test_raises_error_when_no_process_id(self, mock_post):
        """Test that ValueError is raised when no process_id returned."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "error"}
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        with pytest.raises(ValueError) as exc_info:
            submit_pdf_to_pdfx(
                file_content=b"pdf_content",
                token="test_token"
            )
        assert "No process_id" in str(exc_info.value)

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.time.sleep")
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.requests.post")
    def test_retries_on_connection_error(self, mock_post, mock_sleep):
        """Test that connection errors trigger retries."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"process_id": "abc123"}
        mock_response.raise_for_status = MagicMock()

        # First two calls fail, third succeeds
        mock_post.side_effect = [
            requests.exceptions.ConnectionError("Connection failed"),
            requests.exceptions.ConnectionError("Connection failed"),
            mock_response
        ]

        process_id = submit_pdf_to_pdfx(
            file_content=b"pdf_content",
            token="test_token",
            max_retries=3,
            retry_delay=1
        )

        assert process_id == "abc123"
        assert mock_post.call_count == 3
        assert mock_sleep.call_count == 2

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.requests.post")
    def test_raises_after_max_retries(self, mock_post):
        """Test that exception is raised after max retries."""
        mock_post.side_effect = requests.exceptions.ConnectionError("Failed")

        with pytest.raises(requests.exceptions.ConnectionError):
            submit_pdf_to_pdfx(
                file_content=b"pdf_content",
                token="test_token",
                max_retries=2,
                retry_delay=0
            )

        assert mock_post.call_count == 2


class TestPollPdfxStatus:
    """Test poll_pdfx_status function."""

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.requests.get")
    def test_returns_completed_status(self, mock_get):
        """Test that completed status is returned."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": "completed",
            "results": {"grobid": "success"}
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        status = poll_pdfx_status("abc123", "test_token")

        assert status["status"] == "completed"
        assert "results" in status

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.time.sleep")
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.time.time")
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.requests.get")
    def test_polls_until_complete(self, mock_get, mock_time, mock_sleep):
        """Test that polling continues until complete."""
        # Simulate time passing
        mock_time.side_effect = [0, 5, 10, 15, 20]

        pending_response = MagicMock()
        pending_response.json.return_value = {"status": "processing"}
        pending_response.raise_for_status = MagicMock()

        complete_response = MagicMock()
        complete_response.json.return_value = {"status": "completed"}
        complete_response.raise_for_status = MagicMock()

        mock_get.side_effect = [pending_response, pending_response, complete_response]

        status = poll_pdfx_status("abc123", "test_token", timeout=100, poll_interval=5)

        assert status["status"] == "completed"
        assert mock_get.call_count == 3

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.requests.get")
    def test_raises_runtime_error_on_failed_status(self, mock_get):
        """Test that RuntimeError is raised on failed status."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": "failed",
            "error": "Processing error"
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        with pytest.raises(RuntimeError) as exc_info:
            poll_pdfx_status("abc123", "test_token")
        assert "PDFX job failed" in str(exc_info.value)

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.time.time")
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.requests.get")
    def test_raises_timeout_error(self, mock_get, mock_time):
        """Test that TimeoutError is raised when timeout exceeded."""
        # Simulate time passing beyond timeout
        mock_time.side_effect = [0, 500, 1000]

        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "processing"}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        with pytest.raises(TimeoutError):
            poll_pdfx_status("abc123", "test_token", timeout=10)


class TestDownloadPdfxResult:
    """Test download_pdfx_result function."""

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.requests.get")
    def test_successful_download(self, mock_get):
        """Test successful result download."""
        mock_response = MagicMock()
        mock_response.content = b"# Markdown content"
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        content = download_pdfx_result("abc123", "grobid", "test_token")

        assert content == b"# Markdown content"
        mock_get.assert_called_once()
        call_url = mock_get.call_args[0][0]
        assert "abc123" in call_url
        assert "grobid" in call_url


class TestResolveCurieToReference:
    """Test resolve_curie_to_reference function."""

    def test_returns_none_for_invalid_curie(self):
        """Test that None is returned for invalid curie."""
        mock_db = MagicMock()

        with patch(
            "agr_literature_service.lit_processing.pdf2md.pdf2md_utils.normalize_reference_curie"
        ) as mock_normalize:
            from fastapi import HTTPException
            mock_normalize.side_effect = HTTPException(
                status_code=404,
                detail="Reference not found"
            )

            result = resolve_curie_to_reference(mock_db, "INVALID:123")
            assert result is None

    def test_returns_reference_for_valid_curie(self):
        """Test that reference is returned for valid curie."""
        mock_db = MagicMock()
        mock_reference = MagicMock()
        mock_reference.curie = "AGRKB:101000000000001"

        with patch(
            "agr_literature_service.lit_processing.pdf2md.pdf2md_utils.normalize_reference_curie"
        ) as mock_normalize, patch(
            "agr_literature_service.lit_processing.pdf2md.pdf2md_utils.get_reference"
        ) as mock_get_ref:
            mock_normalize.return_value = "AGRKB:101000000000001"
            mock_get_ref.return_value = mock_reference

            result = resolve_curie_to_reference(mock_db, "PMID:12345")

            assert result == mock_reference
            mock_normalize.assert_called_once_with(mock_db, "PMID:12345")


class TestGetPdfFilesForReference:
    """Test get_pdf_files_for_reference function."""

    def test_filters_by_main_pdf_type(self):
        """Test that main PDFs are filtered correctly."""
        mock_db = MagicMock()
        mock_query = MagicMock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = []

        get_pdf_files_for_reference(mock_db, 123, "main")

        mock_db.query.assert_called_once()
        # Verify filter was called with appropriate arguments
        assert mock_query.filter.called

    def test_filters_by_supplement_pdf_type(self):
        """Test that supplement PDFs are filtered correctly."""
        mock_db = MagicMock()
        mock_query = MagicMock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = []

        get_pdf_files_for_reference(mock_db, 123, "supplement")

        assert mock_query.filter.called

    def test_filters_by_both_pdf_type(self):
        """Test that both main and supplement PDFs are included."""
        mock_db = MagicMock()
        mock_query = MagicMock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = []

        get_pdf_files_for_reference(mock_db, 123, "both")

        assert mock_query.filter.called


class TestGetNxmlReferencefile:
    """Test get_nxml_referencefile function."""

    def test_returns_nxml_when_present(self):
        """A final nXML file is returned when present."""
        mock_db = MagicMock()
        mock_query = MagicMock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query

        expected = MagicMock()
        expected.file_class = "nXML"
        mock_query.first.return_value = expected

        result = get_nxml_referencefile(mock_db, 123)

        assert result is expected
        assert mock_query.filter.called
        assert mock_query.order_by.called

    def test_returns_none_when_absent(self):
        """Returns None when no nXML present for the reference."""
        mock_db = MagicMock()
        mock_query = MagicMock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.first.return_value = None

        result = get_nxml_referencefile(mock_db, 123)

        assert result is None


class TestProcessNxmlToMarkdown:
    """Test process_nxml_to_markdown function."""

    def _nxml_ref(self, md5sum="abc123", display_name="paper", ref_id=42):
        mock_ref = MagicMock()
        mock_ref.md5sum = md5sum
        mock_ref.display_name = display_name
        mock_ref.referencefile_id = ref_id
        return mock_ref

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.file_upload")
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.convert_xml_to_markdown")
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.download_xml_from_s3")
    def test_successful_conversion_and_upload(
        self, mock_download, mock_convert, mock_upload
    ):
        """XML is downloaded, converted, and uploaded with correct metadata."""
        mock_db = MagicMock()
        mock_download.return_value = b"<xml>content</xml>"
        mock_convert.return_value = "# Converted markdown content"
        mock_s3 = MagicMock()

        success, error = process_nxml_to_markdown(
            db=mock_db,
            nxml_ref_file=self._nxml_ref(),
            reference_curie="AGRKB:101000000000001",
            mod_abbreviation="WB",
            s3_client=mock_s3
        )

        assert success is True
        assert error is None
        mock_download.assert_called_once_with(mock_s3, "abc123")
        mock_convert.assert_called_once_with(b"<xml>content</xml>", "jats")
        mock_upload.assert_called_once()
        _args, kwargs = mock_upload.call_args
        metadata = kwargs["metadata"]
        assert metadata["file_class"] == "converted_merged_main"
        assert metadata["file_extension"] == "md"
        assert metadata["reference_curie"] == "AGRKB:101000000000001"
        assert metadata["mod_abbreviation"] == "WB"
        assert metadata["display_name"] == "paper_nxml"

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.download_xml_from_s3")
    def test_empty_s3_content_returns_failure(self, mock_download):
        """Empty S3 content produces a failure tuple."""
        mock_download.return_value = b""

        success, error = process_nxml_to_markdown(
            db=MagicMock(),
            nxml_ref_file=self._nxml_ref(),
            reference_curie="AGRKB:1",
            mod_abbreviation=None,
            s3_client=MagicMock()
        )

        assert success is False
        assert "Empty nXML content" in error

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.convert_xml_to_markdown")
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.download_xml_from_s3")
    def test_conversion_exception_returns_failure(self, mock_download, mock_convert):
        """Parser exceptions are caught and returned as failure."""
        mock_download.return_value = b"<xml/>"
        mock_convert.side_effect = ValueError("bad xml")

        success, error = process_nxml_to_markdown(
            db=MagicMock(),
            nxml_ref_file=self._nxml_ref(),
            reference_curie="AGRKB:1",
            mod_abbreviation=None,
            s3_client=MagicMock()
        )

        assert success is False
        assert "bad xml" in error

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.convert_xml_to_markdown")
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.download_xml_from_s3")
    def test_minimal_markdown_returns_failure(self, mock_download, mock_convert):
        """Conversion producing too-short content is rejected."""
        mock_download.return_value = b"<xml/>"
        mock_convert.return_value = "."

        success, error = process_nxml_to_markdown(
            db=MagicMock(),
            nxml_ref_file=self._nxml_ref(),
            reference_curie="AGRKB:1",
            mod_abbreviation=None,
            s3_client=MagicMock()
        )

        assert success is False
        assert "empty or minimal" in error


class TestProcessSupplementalPdfs:
    """Test process_supplemental_pdfs function."""

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.get_pdf_files_for_reference")
    def test_no_supplements_returns_zero_counts(self, mock_get_pdfs):
        """Zero supplements -> (0, 0, [])."""
        mock_get_pdfs.return_value = []

        succeeded, failed, errors = process_supplemental_pdfs(
            db=MagicMock(),
            reference_id=42,
            reference_curie="AGRKB:1",
            token="t"
        )

        assert succeeded == 0
        assert failed == 0
        assert errors == []

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils._process_single_pdf_file")
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.get_pdf_files_for_reference")
    def test_mixed_success_and_failure(self, mock_get_pdfs, mock_process):
        """Mix of succeeding/failing supplements counted separately."""
        s1 = MagicMock(display_name="s1")
        s2 = MagicMock(display_name="s2")
        s3 = MagicMock(display_name="s3")
        mock_get_pdfs.return_value = [s1, s2, s3]
        mock_process.side_effect = [
            (True, ["grobid"], None),
            (False, [], "pdfx err"),
            (True, ["merged"], None),
        ]

        succeeded, failed, errors = process_supplemental_pdfs(
            db=MagicMock(),
            reference_id=42,
            reference_curie="AGRKB:1",
            token="t"
        )

        assert succeeded == 2
        assert failed == 1
        assert len(errors) == 1
        assert "s2" in errors[0]
        assert "pdfx err" in errors[0]

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils._process_single_pdf_file")
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md_utils.get_pdf_files_for_reference")
    def test_exception_in_helper_counted_as_failure(self, mock_get_pdfs, mock_process):
        """Exceptions from the per-file helper are caught."""
        s1 = MagicMock(display_name="s1")
        mock_get_pdfs.return_value = [s1]
        mock_process.side_effect = RuntimeError("boom")

        succeeded, failed, errors = process_supplemental_pdfs(
            db=MagicMock(),
            reference_id=42,
            reference_curie="AGRKB:1",
            token="t"
        )

        assert succeeded == 0
        assert failed == 1
        assert len(errors) == 1
        assert "boom" in errors[0]
