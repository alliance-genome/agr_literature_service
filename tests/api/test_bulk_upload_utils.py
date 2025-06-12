"""
Tests for bulk upload utility functions.
"""

import io
import os
import tarfile
import tempfile
import zipfile
from unittest.mock import Mock, patch

import pytest

from agr_literature_service.api.utils.bulk_upload_utils import (
    classify_and_parse_file,
    extract_and_classify_files,
    parse_filename_by_mod,
    parse_supplement_file,
    process_single_file,
    validate_archive_structure
)


class TestParseFilenameByMod:
    """Test MOD-specific filename parsing."""

    def test_wb_wbpaper_id_parsing(self):
        """Test WB filename parsing with WBPaper IDs."""
        # WB pattern: {wbpaper_id}_{author_year}[_{options}].{ext}
        result = parse_filename_by_mod("12345_Doe2023.pdf", "WB")

        assert result["reference_curie"] == "WB:WBPaper12345"
        assert result["display_name"] == "12345_Doe2023"
        assert result["file_extension"] == "pdf"
        assert result["file_publication_status"] == "final"
        assert result["pdf_type"] is None
        assert result["author_and_year"] == "Doe2023"
        assert result["mod_abbreviation"] == "WB"

    def test_wb_with_options(self):
        """Test WB filename with additional options."""
        result = parse_filename_by_mod("12345_Smith2022_temp.pdf", "WB")

        assert result["reference_curie"] == "WB:WBPaper12345"
        assert result["file_publication_status"] == "temp"
        assert result["pdf_type"] is None

    def test_wb_with_pdf_type(self):
        """Test WB filename with PDF type options."""
        result = parse_filename_by_mod("12345_Jones2021_ocr.pdf", "WB")

        assert result["reference_curie"] == "WB:WBPaper12345"
        assert result["file_publication_status"] == "final"
        assert result["pdf_type"] == "ocr"

    def test_fb_pmid_parsing(self):
        """Test FB filename parsing with PMID patterns."""
        # FB pattern: {pmid}_{author_year}[_{options}].{ext}
        result = parse_filename_by_mod("12345678_Brown2023.pdf", "FB")

        assert result["reference_curie"] == "PMID:12345678"
        assert result["display_name"] == "12345678_Brown2023"
        assert result["file_extension"] == "pdf"
        assert result["file_publication_status"] == "final"
        assert result["pdf_type"] is None
        assert result["author_and_year"] == "Brown2023"
        assert result["mod_abbreviation"] == "FB"

    def test_fb_with_html_type(self):
        """Test FB filename with HTML type."""
        result = parse_filename_by_mod("87654321_Wilson2022_html.html", "FB")

        assert result["reference_curie"] == "PMID:87654321"
        assert result["pdf_type"] == "html"
        assert result["file_extension"] == "html"

    def test_fb_with_htm_type(self):
        """Test FB filename with HTM type (should convert to html)."""
        result = parse_filename_by_mod("87654321_Wilson2022_htm.htm", "FB")

        assert result["reference_curie"] == "PMID:87654321"
        assert result["pdf_type"] == "html"  # Should convert htm to html
        assert result["file_extension"] == "htm"

    def test_agrkb_15_digit_id(self):
        """Test 15-digit AGRKB ID parsing."""
        result = parse_filename_by_mod("123456789012345_Author2023.pdf", "SGD")

        assert result["reference_curie"] == "AGRKB:123456789012345"
        assert result["display_name"] == "123456789012345_Author2023"
        assert result["author_and_year"] == "Author2023"

    def test_numbers_only_filename(self):
        """Test filename with numbers only (no author/year)."""
        result = parse_filename_by_mod("12345.pdf", "WB")

        assert result["reference_curie"] == "WB:WBPaper12345"
        assert result["display_name"] == "12345"
        assert result["author_and_year"] == ""

    def test_invalid_filename_pattern(self):
        """Test invalid filename pattern raises ValueError."""
        with pytest.raises(ValueError, match="does not match expected patterns"):
            parse_filename_by_mod("invalid_filename_pattern.pdf", "WB")

    def test_all_pdf_types(self):
        """Test all supported PDF type options."""
        pdf_types = ["aut", "ocr", "html", "htm", "lib", "tif"]

        for pdf_type in pdf_types:
            result = parse_filename_by_mod(f"12345_Author2023_{pdf_type}.pdf", "WB")
            expected_type = "html" if pdf_type == "htm" else pdf_type
            assert result["pdf_type"] == expected_type


class TestParseSupplementFile:
    """Test supplement file parsing."""

    def test_wb_supplement_parsing(self):
        """Test WB supplement file parsing."""
        result = parse_supplement_file("supplementary_data.xlsx", "12345", "WB")

        assert result["reference_curie"] == "WB:WBPaper12345"
        assert result["display_name"] == "supplementary_data"
        assert result["file_extension"] == "xlsx"
        assert result["file_publication_status"] == "final"
        assert result["pdf_type"] is None
        assert result["mod_abbreviation"] == "WB"

    def test_fb_supplement_parsing(self):
        """Test FB supplement file parsing."""
        result = parse_supplement_file("figure_1.png", "87654321", "FB")

        assert result["reference_curie"] == "PMID:87654321"
        assert result["display_name"] == "figure_1"
        assert result["file_extension"] == "png"

    def test_agrkb_supplement_parsing(self):
        """Test AGRKB 15-digit supplement file parsing."""
        result = parse_supplement_file("data.csv", "123456789012345", "SGD")

        assert result["reference_curie"] == "AGRKB:123456789012345"


class TestClassifyAndParseFile:
    """Test file classification and parsing."""

    def test_main_file_classification(self):
        """Test main file classification (root directory)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a main file
            main_file = os.path.join(temp_dir, "12345_Doe2023.pdf")

            result = classify_and_parse_file(main_file, temp_dir, "WB")

            assert result["file_class"] == "main"
            assert result["reference_curie"] == "WB:WBPaper12345"
            assert result["is_annotation"] is False

    def test_supplement_file_classification(self):
        """Test supplement file classification (subdirectory)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create subdirectory and supplement file
            subdir = os.path.join(temp_dir, "12345")
            os.makedirs(subdir)
            supplement_file = os.path.join(subdir, "figure_1.png")

            result = classify_and_parse_file(supplement_file, temp_dir, "WB")

            assert result["file_class"] == "supplement"
            assert result["reference_curie"] == "WB:WBPaper12345"
            assert result["display_name"] == "figure_1"


class TestExtractAndClassifyFiles:
    """Test archive extraction and file classification."""

    def create_test_tar_archive(self) -> io.BytesIO:
        """Create a test tar.gz archive with WB and FB files."""
        archive_buffer = io.BytesIO()

        with tarfile.open(fileobj=archive_buffer, mode="w:gz") as tar:
            # Add main files
            wb_content = b"WB main file content" + b"x" * 79
            wb_main = tarfile.TarInfo("12345_Doe2023.pdf")
            wb_main.size = len(wb_content)
            tar.addfile(wb_main, io.BytesIO(wb_content))

            fb_content = b"FB main file content" + b"y" * 79
            fb_main = tarfile.TarInfo("87654321_Smith2022.pdf")
            fb_main.size = len(fb_content)
            tar.addfile(fb_main, io.BytesIO(fb_content))

            # Add supplement files in subdirectories
            wb_supp_content = b"WB supplement content" + b"z" * 27
            wb_supp = tarfile.TarInfo("12345/figure_1.png")
            wb_supp.size = len(wb_supp_content)
            tar.addfile(wb_supp, io.BytesIO(wb_supp_content))

            fb_supp_content = b"FB supplement content" + b"w" * 27
            fb_supp = tarfile.TarInfo("87654321/data.csv")
            fb_supp.size = len(fb_supp_content)
            tar.addfile(fb_supp, io.BytesIO(fb_supp_content))

        archive_buffer.seek(0)
        return archive_buffer

    def create_test_zip_archive(self) -> io.BytesIO:
        """Create a test zip archive with WB and FB files."""
        archive_buffer = io.BytesIO()

        with zipfile.ZipFile(archive_buffer, 'w') as zip_file:
            # Add main files
            zip_file.writestr("12345_Doe2023.pdf", b"WB main file content" + b"x" * 79)
            zip_file.writestr("87654321_Smith2022.pdf", b"FB main file content" + b"y" * 79)

            # Add supplement files
            zip_file.writestr("12345/figure_1.png", b"WB supplement content" + b"z" * 27)
            zip_file.writestr("87654321/data.csv", b"FB supplement content" + b"w" * 27)

        archive_buffer.seek(0)
        return archive_buffer

    def test_tar_extraction(self):
        """Test tar.gz archive extraction and classification."""
        archive = self.create_test_tar_archive()

        with tempfile.TemporaryDirectory() as temp_dir:
            files = extract_and_classify_files(archive, temp_dir)

            assert len(files) == 4

            # Check that we have both main and supplement files
            main_files = [f for f in files if f[1]]  # is_main = True
            supplement_files = [f for f in files if not f[1]]  # is_main = False

            assert len(main_files) == 2
            assert len(supplement_files) == 2

            # Verify files exist
            for file_path, _ in files:
                assert os.path.exists(file_path)

    def test_zip_extraction(self):
        """Test zip archive extraction and classification."""
        archive = self.create_test_zip_archive()

        with tempfile.TemporaryDirectory() as temp_dir:
            files = extract_and_classify_files(archive, temp_dir)

            assert len(files) == 4

            main_files = [f for f in files if f[1]]
            supplement_files = [f for f in files if not f[1]]

            assert len(main_files) == 2
            assert len(supplement_files) == 2

    def test_invalid_archive_format(self):
        """Test invalid archive format raises ValueError."""
        invalid_archive = io.BytesIO(b"not an archive")

        with tempfile.TemporaryDirectory() as temp_dir:
            with pytest.raises(ValueError, match="Archive format not supported"):
                extract_and_classify_files(invalid_archive, temp_dir)


class TestValidateArchiveStructure:
    """Test archive structure validation."""

    def test_valid_tar_archive_validation(self):
        """Test validation of valid tar.gz archive."""
        archive_buffer = io.BytesIO()

        with tarfile.open(fileobj=archive_buffer, mode="w:gz") as tar:
            # Add files
            main_file = tarfile.TarInfo("12345_Doe2023.pdf")
            main_file.size = 100
            tar.addfile(main_file, io.BytesIO(b"x" * 100))

            supp_file = tarfile.TarInfo("12345/figure_1.png")
            supp_file.size = 50
            tar.addfile(supp_file, io.BytesIO(b"y" * 50))

        archive_buffer.seek(0)
        result = validate_archive_structure(archive_buffer)

        assert result["valid"] is True
        assert result["total_files"] == 2
        assert result["main_files"] == 1
        assert result["supplement_files"] == 1
        assert len(result["main_file_list"]) == 1
        assert len(result["supplement_file_list"]) == 1

    def test_valid_zip_archive_validation(self):
        """Test validation of valid zip archive."""
        archive_buffer = io.BytesIO()

        with zipfile.ZipFile(archive_buffer, 'w') as zip_file:
            zip_file.writestr("87654321_Smith2022.pdf", b"x" * 100)
            zip_file.writestr("87654321/data.csv", b"y" * 50)

        archive_buffer.seek(0)
        result = validate_archive_structure(archive_buffer)

        assert result["valid"] is True
        assert result["total_files"] == 2
        assert result["main_files"] == 1
        assert result["supplement_files"] == 1

    def test_empty_archive_validation(self):
        """Test validation of empty archive."""
        archive_buffer = io.BytesIO()

        with tarfile.open(fileobj=archive_buffer, mode="w:gz"):
            pass  # Empty archive

        archive_buffer.seek(0)
        result = validate_archive_structure(archive_buffer)

        assert result["valid"] is True
        assert result["total_files"] == 0
        assert result["main_files"] == 0
        assert result["supplement_files"] == 0

    def test_invalid_archive_validation(self):
        """Test validation of invalid archive."""
        invalid_archive = io.BytesIO(b"not an archive")

        result = validate_archive_structure(invalid_archive)

        assert result["valid"] is False
        assert "error" in result
        assert result["total_files"] == 0


class TestProcessSingleFile:
    """Test single file processing with mocked dependencies."""

    @patch('agr_literature_service.api.crud.referencefile_crud.file_upload')
    def test_successful_file_processing(self, mock_file_upload):
        """Test successful file processing."""
        mock_file_upload.return_value = None  # Successful upload

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(b"test file content")
            temp_file_path = temp_file.name

        try:
            metadata = {
                "reference_curie": "WB:WBPaper12345",
                "display_name": "12345_Doe2023",
                "file_class": "main",
                "file_extension": "pdf",
                "file_publication_status": "final",
                "pdf_type": None,
                "mod_abbreviation": "WB"
            }

            mock_db = Mock()
            result = process_single_file(temp_file_path, metadata, mock_db)

            assert result["status"] == "success"
            assert result["reference_curie"] == "WB:WBPaper12345"
            assert result["file_class"] == "main"

            # Verify file_upload was called
            mock_file_upload.assert_called_once()
            call_args = mock_file_upload.call_args
            assert call_args[0][0] == mock_db  # db session
            assert call_args[0][1] == metadata  # metadata
            assert call_args[1]["upload_if_already_converted"] is True

        finally:
            os.unlink(temp_file_path)

    @patch('agr_literature_service.api.crud.referencefile_crud.file_upload')
    def test_failed_file_processing(self, mock_file_upload):
        """Test failed file processing."""
        mock_file_upload.side_effect = Exception("Upload failed")

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(b"test file content")
            temp_file_path = temp_file.name

        try:
            metadata = {
                "reference_curie": "FB:87654321",
                "display_name": "87654321_Smith2022",
                "file_class": "main"
            }

            mock_db = Mock()
            result = process_single_file(temp_file_path, metadata, mock_db)

            assert result["status"] == "error"
            assert "Upload failed" in result["error"]
            assert result["reference_curie"] == "FB:87654321"

        finally:
            os.unlink(temp_file_path)

    def test_file_not_found(self):
        """Test processing non-existent file."""
        metadata = {"reference_curie": "WB:WBPaper12345"}
        mock_db = Mock()

        result = process_single_file("/non/existent/file.pdf", metadata, mock_db)

        assert result["status"] == "error"
        assert "No such file" in result["error"]


class TestIntegrationScenarios:
    """Integration tests with complete WB and FB scenarios."""

    def create_realistic_wb_archive(self) -> io.BytesIO:
        """Create a realistic WB archive with multiple files."""
        archive_buffer = io.BytesIO()

        with tarfile.open(fileobj=archive_buffer, mode="w:gz") as tar:
            # Main papers
            papers = [
                ("12345_Doe2023.pdf", b"WB paper 12345 content"),
                ("67890_Smith2022_temp.pdf", b"WB paper 67890 temp content"),
                ("11111_Jones2021_ocr.pdf", b"WB paper 11111 OCR content"),
                ("22222.pdf", b"WB paper 22222 no author content"),
            ]

            for filename, content in papers:
                info = tarfile.TarInfo(filename)
                info.size = len(content)
                tar.addfile(info, io.BytesIO(content))

            # Supplement files
            supplements = [
                ("12345/figure_1.png", b"Figure 1 for paper 12345"),
                ("12345/data.xlsx", b"Data file for paper 12345"),
                ("67890/supplementary.pdf", b"Supplementary for paper 67890"),
            ]

            for filename, content in supplements:
                info = tarfile.TarInfo(filename)
                info.size = len(content)
                tar.addfile(info, io.BytesIO(content))

        archive_buffer.seek(0)
        return archive_buffer

    def create_realistic_fb_archive(self) -> io.BytesIO:
        """Create a realistic FB archive with multiple files."""
        archive_buffer = io.BytesIO()

        with zipfile.ZipFile(archive_buffer, 'w') as zip_file:
            # Main papers
            papers = [
                ("12345678_Brown2023.pdf", b"FB paper 12345678 content"),
                ("87654321_Wilson2022_html.html", b"FB paper 87654321 HTML content"),
                ("11223344_Taylor2021.pdf", b"FB paper 11223344 content"),
            ]

            for filename, content in papers:
                zip_file.writestr(filename, content)

            # Supplement files
            supplements = [
                ("12345678/figure_1.png", b"Figure 1 for paper 12345678"),
                ("12345678/data.csv", b"Data for paper 12345678"),
                ("87654321/protocol.txt", b"Protocol for paper 87654321"),
            ]

            for filename, content in supplements:
                zip_file.writestr(filename, content)

        archive_buffer.seek(0)
        return archive_buffer

    def test_wb_archive_processing(self):
        """Test complete WB archive processing workflow."""
        archive = self.create_realistic_wb_archive()

        # Test validation
        validation = validate_archive_structure(archive)
        assert validation["valid"] is True
        assert validation["total_files"] == 7  # 4 main + 3 supplements
        assert validation["main_files"] == 4
        assert validation["supplement_files"] == 3

        # Test extraction
        archive.seek(0)  # Reset for extraction
        with tempfile.TemporaryDirectory() as temp_dir:
            files = extract_and_classify_files(archive, temp_dir)

            assert len(files) == 7

            # Test file classification and parsing
            for file_path, is_main in files:
                result = classify_and_parse_file(file_path, temp_dir, "WB")

                assert result["mod_abbreviation"] == "WB"
                assert result["reference_curie"].startswith("WB:WBPaper")
                assert result["file_class"] in ["main", "supplement"]
                assert result["is_annotation"] is False

                if is_main:
                    assert result["file_class"] == "main"
                else:
                    assert result["file_class"] == "supplement"

    def test_fb_archive_processing(self):
        """Test complete FB archive processing workflow."""
        archive = self.create_realistic_fb_archive()

        # Test validation
        validation = validate_archive_structure(archive)
        assert validation["valid"] is True
        assert validation["total_files"] == 6  # 3 main + 3 supplements
        assert validation["main_files"] == 3
        assert validation["supplement_files"] == 3

        # Test extraction
        archive.seek(0)
        with tempfile.TemporaryDirectory() as temp_dir:
            files = extract_and_classify_files(archive, temp_dir)

            assert len(files) == 6

            # Test file classification and parsing
            for file_path, is_main in files:
                result = classify_and_parse_file(file_path, temp_dir, "FB")

                assert result["mod_abbreviation"] == "FB"
                assert result["reference_curie"].startswith("PMID:")
                assert result["file_class"] in ["main", "supplement"]

                if is_main:
                    assert result["file_class"] == "main"
                else:
                    assert result["file_class"] == "supplement"

    def test_mixed_file_types_validation(self):
        """Test archive with various file types and extensions."""
        archive_buffer = io.BytesIO()

        with tarfile.open(fileobj=archive_buffer, mode="w:gz") as tar:
            # Various file types
            files = [
                ("12345_Paper2023.pdf", b"PDF content"),
                ("67890_Paper2022.html", b"HTML content"),
                ("11111_Paper2021.txt", b"Text content"),
                ("12345/figure.png", b"PNG image"),
                ("12345/data.xlsx", b"Excel data"),
                ("67890/protocol.docx", b"Word document"),
            ]

            for filename, content in files:
                info = tarfile.TarInfo(filename)
                info.size = len(content)
                tar.addfile(info, io.BytesIO(content))

        archive_buffer.seek(0)
        validation = validate_archive_structure(archive_buffer)

        assert validation["valid"] is True
        assert validation["total_files"] == 6
        assert validation["main_files"] == 3
        assert validation["supplement_files"] == 3
