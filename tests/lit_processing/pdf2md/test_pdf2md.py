"""
Unit tests for pdf2md.py

Tests cover:
- get_newest_main_pdfs
- get_unprocessed_pdfs_since_year
- process_single_pdf
- process_newest_pdfs
- process_pdfs_since_year
"""
from unittest.mock import MagicMock, patch

from agr_literature_service.lit_processing.pdf2md.pdf2md import (
    get_newest_main_pdfs,
    get_unprocessed_pdfs_since_year,
    process_single_pdf,
    EXTRACTION_METHODS,
)


class TestGetNewestMainPdfs:
    """Test get_newest_main_pdfs function."""

    def test_returns_empty_list_when_no_pdfs(self):
        """Test that empty list is returned when no PDFs found."""
        mock_db = MagicMock()
        mock_query = MagicMock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.all.return_value = []

        result = get_newest_main_pdfs(mock_db, limit=10)

        assert result == []

    def test_returns_pdf_info_dicts(self):
        """Test that PDF info dicts are returned."""
        mock_db = MagicMock()
        mock_query = MagicMock()

        # Create mock referencefile
        mock_reffile = MagicMock()
        mock_reffile.referencefile_id = 123
        mock_reffile.reference_id = 456
        mock_reffile.display_name = "test_paper"
        mock_reffile.file_extension = "pdf"
        mock_reffile.referencefile_mods = []

        # Create mock reference
        mock_reference = MagicMock()
        mock_reference.reference_id = 456
        mock_reference.curie = "AGRKB:101000000000001"

        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.all.return_value = [mock_reffile]
        mock_query.one_or_none.return_value = mock_reference

        result = get_newest_main_pdfs(mock_db, limit=10)

        assert len(result) == 1
        assert result[0]["referencefile_id"] == 123
        assert result[0]["reference_curie"] == "AGRKB:101000000000001"

    def test_skip_xml_filters_references_with_xml(self):
        """Test that skip_xml parameter filters out references with XML."""
        mock_db = MagicMock()
        mock_query = MagicMock()
        mock_db.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.all.return_value = []

        get_newest_main_pdfs(mock_db, limit=10, skip_xml=True)

        # Verify that filter was called multiple times (base filter + xml filter)
        assert mock_query.filter.call_count >= 2


class TestGetUnprocessedPdfsSinceYear:
    """Test get_unprocessed_pdfs_since_year function."""

    def test_returns_empty_list_when_no_unprocessed_pdfs(self):
        """Test that empty list is returned when all PDFs are processed."""
        mock_db = MagicMock()
        mock_query = MagicMock()
        mock_db.query.return_value = mock_query
        mock_query.join.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.all.return_value = []

        result = get_unprocessed_pdfs_since_year(mock_db, since_year=2025)

        assert result == []

    def test_returns_pdf_info_with_date_published(self):
        """Test that PDF info includes date_published field."""
        mock_db = MagicMock()
        mock_query = MagicMock()

        # Create mock referencefile
        mock_reffile = MagicMock()
        mock_reffile.referencefile_id = 123
        mock_reffile.reference_id = 456
        mock_reffile.display_name = "test_paper"
        mock_reffile.file_extension = "pdf"
        mock_reffile.referencefile_mods = []

        # Create mock reference
        mock_reference = MagicMock()
        mock_reference.reference_id = 456
        mock_reference.curie = "AGRKB:101000000000001"
        mock_reference.date_published = "2025-03-15"

        mock_db.query.return_value = mock_query
        mock_query.join.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.all.return_value = [mock_reffile]
        mock_query.one_or_none.return_value = mock_reference

        result = get_unprocessed_pdfs_since_year(mock_db, since_year=2025)

        assert len(result) == 1
        assert "date_published" in result[0]
        assert result[0]["date_published"] == "2025-03-15"

    def test_extracts_mod_abbreviation(self):
        """Test that MOD abbreviation is extracted from referencefile_mods."""
        mock_db = MagicMock()
        mock_query = MagicMock()

        # Create mock MOD
        mock_mod = MagicMock()
        mock_mod.abbreviation = "WB"

        # Create mock referencefile_mod
        mock_reffile_mod = MagicMock()
        mock_reffile_mod.mod = mock_mod

        # Create mock referencefile
        mock_reffile = MagicMock()
        mock_reffile.referencefile_id = 123
        mock_reffile.reference_id = 456
        mock_reffile.display_name = "test_paper"
        mock_reffile.file_extension = "pdf"
        mock_reffile.referencefile_mods = [mock_reffile_mod]

        # Create mock reference
        mock_reference = MagicMock()
        mock_reference.reference_id = 456
        mock_reference.curie = "AGRKB:101000000000001"
        mock_reference.date_published = "2025-03-15"

        mock_db.query.return_value = mock_query
        mock_query.join.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.all.return_value = [mock_reffile]
        mock_query.one_or_none.return_value = mock_reference

        result = get_unprocessed_pdfs_since_year(mock_db, since_year=2025)

        assert len(result) == 1
        assert result[0]["mod_abbreviation"] == "WB"


class TestProcessSinglePdf:
    """Test process_single_pdf function."""

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.download_pdfx_result")
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.poll_pdfx_status")
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.submit_pdf_to_pdfx")
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.download_file")
    def test_successful_processing(
        self, mock_download, mock_submit, mock_poll, mock_download_result
    ):
        """Test successful PDF processing."""
        mock_db = MagicMock()
        mock_download.return_value = b"pdf_content"
        mock_submit.return_value = "process_123"
        mock_poll.return_value = {"status": "completed"}
        mock_download_result.return_value = b"# Markdown content"

        with patch(
            "agr_literature_service.lit_processing.pdf2md.pdf2md.file_upload"
        ) as mock_upload:
            ref_file_info = {
                "referencefile_id": 123,
                "reference_id": 456,
                "reference_curie": "AGRKB:101000000000001",
                "display_name": "test_paper",
                "file_extension": "pdf",
                "mod_abbreviation": "WB"
            }

            success, error = process_single_pdf(
                mock_db, ref_file_info, "test_token"
            )

            assert success is True
            assert error is None
            mock_submit.assert_called_once()
            mock_poll.assert_called_once()
            # Should upload for each extraction method
            assert mock_upload.call_count == len(EXTRACTION_METHODS)

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.download_file")
    def test_handles_download_exception(self, mock_download):
        """Test that exceptions during download are handled."""
        mock_db = MagicMock()
        mock_download.side_effect = Exception("Download failed")

        ref_file_info = {
            "referencefile_id": 123,
            "reference_id": 456,
            "reference_curie": "AGRKB:101000000000001",
            "display_name": "test_paper",
            "file_extension": "pdf",
            "mod_abbreviation": "WB"
        }

        success, error = process_single_pdf(
            mock_db, ref_file_info, "test_token"
        )

        assert success is False
        assert "Download failed" in error

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.download_pdfx_result")
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.poll_pdfx_status")
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.submit_pdf_to_pdfx")
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.download_file")
    def test_returns_failure_when_no_methods_succeed(
        self, mock_download, mock_submit, mock_poll, mock_download_result
    ):
        """Test that failure is returned when no extraction methods succeed."""
        mock_db = MagicMock()
        mock_download.return_value = b"pdf_content"
        mock_submit.return_value = "process_123"
        mock_poll.return_value = {"status": "completed"}
        mock_download_result.return_value = b""  # Empty content

        ref_file_info = {
            "referencefile_id": 123,
            "reference_id": 456,
            "reference_curie": "AGRKB:101000000000001",
            "display_name": "test_paper",
            "file_extension": "pdf",
            "mod_abbreviation": "WB"
        }

        success, error = process_single_pdf(
            mock_db, ref_file_info, "test_token"
        )

        assert success is False
        assert "No methods successfully extracted" in error

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.download_pdfx_result")
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.poll_pdfx_status")
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.submit_pdf_to_pdfx")
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.download_file")
    def test_processes_specific_methods(
        self, mock_download, mock_submit, mock_poll, mock_download_result
    ):
        """Test that only specified methods are processed."""
        mock_db = MagicMock()
        mock_download.return_value = b"pdf_content"
        mock_submit.return_value = "process_123"
        mock_poll.return_value = {"status": "completed"}
        mock_download_result.return_value = b"# Markdown content"

        with patch(
            "agr_literature_service.lit_processing.pdf2md.pdf2md.file_upload"
        ) as mock_upload:
            ref_file_info = {
                "referencefile_id": 123,
                "reference_id": 456,
                "reference_curie": "AGRKB:101000000000001",
                "display_name": "test_paper",
                "file_extension": "pdf",
                "mod_abbreviation": "WB"
            }

            success, error = process_single_pdf(
                mock_db,
                ref_file_info,
                "test_token",
                methods_to_extract=["grobid", "docling"]
            )

            assert success is True
            # Should only upload for specified methods
            assert mock_upload.call_count == 2


class TestExtractionMethodsConstant:
    """Test EXTRACTION_METHODS imported from utils."""

    def test_extraction_methods_imported_correctly(self):
        """Test that EXTRACTION_METHODS is properly imported."""
        assert EXTRACTION_METHODS is not None
        assert isinstance(EXTRACTION_METHODS, dict)
        assert "grobid" in EXTRACTION_METHODS
        assert "docling" in EXTRACTION_METHODS
        assert "marker" in EXTRACTION_METHODS
        assert "merged" in EXTRACTION_METHODS
