"""
Tests for bulk upload filename pattern parsing.
"""

import pytest

from agr_literature_service.api.utils.bulk_upload_utils import parse_filename_by_mod


class TestWBPatterns:
    """Test WormBase (WB) specific filename patterns."""

    def test_wb_basic_patterns(self):
        """Test basic WB filename patterns."""
        test_cases = [
            # Standard pattern: {wbpaper_id}_{author_year}.{ext}
            ("12345_Doe2023.pdf", "WB:WBPaper12345", "Doe2023", "final", None),
            ("678_Smith2022.html", "WB:WBPaper678", "Smith2022", "final", None),
            ("999999_Johnson2021.txt", "WB:WBPaper999999", "Johnson2021", "final", None),

            # Numbers only: {wbpaper_id}.{ext}
            ("12345.pdf", "WB:WBPaper12345", "", "final", None),
            ("1.pdf", "WB:WBPaper1", "", "final", None),
            ("999999.html", "WB:WBPaper999999", "", "final", None),
        ]

        for filename, expected_curie, expected_author, expected_status, expected_pdf_type in test_cases:
            result = parse_filename_by_mod(filename, "WB")

            assert result["reference_curie"] == expected_curie
            assert result["author_and_year"] == expected_author
            assert result["file_publication_status"] == expected_status
            assert result["pdf_type"] == expected_pdf_type
            assert result["mod_abbreviation"] == "WB"

    def test_wb_with_temp_status(self):
        """Test WB filenames with temp status."""
        test_cases = [
            ("12345_Doe2023_temp.pdf", "temp"),
            ("678_Smith2022_temp.html", "temp"),
            ("999_Jones2021_TEMP.txt", "temp"),  # Case insensitive
        ]

        for filename, expected_status in test_cases:
            result = parse_filename_by_mod(filename, "WB")
            assert result["file_publication_status"] == expected_status
            assert result["pdf_type"] is None

    def test_wb_with_pdf_types(self):
        """Test WB filenames with PDF type options."""
        test_cases = [
            ("12345_Doe2023_aut.pdf", "aut"),
            ("678_Smith2022_ocr.pdf", "ocr"),
            ("999_Jones2021_html.html", "html"),
            ("111_Wilson2020_htm.htm", "html"),  # htm converts to html
            ("222_Brown2019_lib.pdf", "lib"),
            ("333_Taylor2018_tif.tif", "tif"),
            ("444_Davis2017_AUT.pdf", "aut"),  # Case insensitive
            ("555_Miller2016_OCR.pdf", "ocr"),  # Case insensitive
        ]

        for filename, expected_pdf_type in test_cases:
            result = parse_filename_by_mod(filename, "WB")
            assert result["pdf_type"] == expected_pdf_type
            assert result["file_publication_status"] == "final"

    def test_wb_real_world_examples(self):
        """Test real-world WB filename examples."""
        test_cases = [
            # Real WB paper patterns
            ("00001234_Brenner1974.pdf", "WB:WBPaper00001234", "Brenner1974"),
            ("00005678_Fire1998_temp.pdf", "WB:WBPaper00005678", "Fire1998"),
            ("00009999_Mello2006_ocr.pdf", "WB:WBPaper00009999", "Mello2006"),
            ("00001111_Horvitz2002.html", "WB:WBPaper00001111", "Horvitz2002"),

            # Edge cases
            ("1_A2023.pdf", "WB:WBPaper1", "A2023"),
            ("123456789_VeryLongAuthorName2023.pdf", "WB:WBPaper123456789", "VeryLongAuthorName2023"),
            ("42_AuthorWith123Numbers2023.pdf", "WB:WBPaper42", "AuthorWith123Numbers2023"),
        ]

        for filename, expected_curie, expected_author in test_cases:
            result = parse_filename_by_mod(filename, "WB")
            assert result["reference_curie"] == expected_curie
            assert result["author_and_year"] == expected_author

    def test_wb_file_extensions(self):
        """Test WB files with various extensions."""
        extensions = ["pdf", "html", "htm", "txt", "doc", "docx", "xml", "tei", "json"]

        for ext in extensions:
            filename = f"12345_Author2023.{ext}"
            result = parse_filename_by_mod(filename, "WB")

            assert result["reference_curie"] == "WB:WBPaper12345"
            assert result["file_extension"] == ext
            assert result["display_name"] == "12345_Author2023"

    def test_wb_complex_author_patterns(self):
        """Test WB files with complex author/year patterns."""
        test_cases = [
            # The regex pattern ^([0-9]+)[_]([^_]+)[_]?(.*)?\..*$ captures until the first underscore
            # So "Smith_Jones2021" would be captured as "Smith" (author) and "Jones2021" (options)
            ("12345_SmithAndJones2023.pdf", "SmithAndJones2023"),  # No underscore in author
            ("678_Smith-Jones2022.pdf", "Smith-Jones2022"),        # Dash is allowed
            ("999_Smith_Jones2021.pdf", "Smith"),                  # First underscore splits it

            # Year variations
            ("12345_Author23.pdf", "Author23"),
            ("678_Author2023a.pdf", "Author2023a"),
            ("999_Author2023b.pdf", "Author2023b"),

            # Special characters in author names (no underscores)
            ("12345_O'Brien2023.pdf", "O'Brien2023"),
            ("678_van-der-Berg2022.pdf", "van-der-Berg2022"),  # Using dash instead of underscore
            ("999_Al-Smith2021.pdf", "Al-Smith2021"),
        ]

        for filename, expected_author in test_cases:
            result = parse_filename_by_mod(filename, "WB")
            assert result["author_and_year"] == expected_author


class TestFBPatterns:
    """Test FlyBase (FB) specific filename patterns."""

    def test_fb_basic_patterns(self):
        """Test basic FB filename patterns with PMID."""
        test_cases = [
            # Standard pattern: {pmid}_{author_year}.{ext}
            ("12345678_Doe2023.pdf", "PMID:12345678", "Doe2023", "final", None),
            ("87654321_Smith2022.html", "PMID:87654321", "Smith2022", "final", None),
            ("11111111_Johnson2021.txt", "PMID:11111111", "Johnson2021", "final", None),

            # Numbers only: {pmid}.{ext}
            ("12345678.pdf", "PMID:12345678", "", "final", None),
            ("87654321.html", "PMID:87654321", "", "final", None),
            ("11111111.txt", "PMID:11111111", "", "final", None),
        ]

        for filename, expected_curie, expected_author, expected_status, expected_pdf_type in test_cases:
            result = parse_filename_by_mod(filename, "FB")

            assert result["reference_curie"] == expected_curie
            assert result["author_and_year"] == expected_author
            assert result["file_publication_status"] == expected_status
            assert result["pdf_type"] == expected_pdf_type
            assert result["mod_abbreviation"] == "FB"

    def test_fb_pmid_variations(self):
        """Test FB with various PMID lengths and formats."""
        test_cases = [
            # Short PMIDs (older papers)
            ("123_Author2023.pdf", "PMID:123"),
            ("1234_Author2022.pdf", "PMID:1234"),
            ("12345_Author2021.pdf", "PMID:12345"),

            # Standard PMIDs
            ("1234567_Author2020.pdf", "PMID:1234567"),
            ("12345678_Author2019.pdf", "PMID:12345678"),

            # Longer PMIDs (newer papers)
            ("123456789_Author2018.pdf", "PMID:123456789"),
            ("1234567890_Author2017.pdf", "PMID:1234567890"),
        ]

        for filename, expected_curie in test_cases:
            result = parse_filename_by_mod(filename, "FB")
            assert result["reference_curie"] == expected_curie

    def test_fb_with_temp_status(self):
        """Test FB filenames with temp status."""
        test_cases = [
            ("12345678_Doe2023_temp.pdf", "temp"),
            ("87654321_Smith2022_temp.html", "temp"),
            ("11111111_Jones2021_TEMP.txt", "temp"),  # Case insensitive
        ]

        for filename, expected_status in test_cases:
            result = parse_filename_by_mod(filename, "FB")
            assert result["file_publication_status"] == expected_status
            assert result["pdf_type"] is None

    def test_fb_with_pdf_types(self):
        """Test FB filenames with PDF type options."""
        test_cases = [
            ("12345678_Doe2023_aut.pdf", "aut"),
            ("87654321_Smith2022_ocr.pdf", "ocr"),
            ("11111111_Jones2021_html.html", "html"),
            ("22222222_Wilson2020_htm.htm", "html"),  # htm converts to html
            ("33333333_Brown2019_lib.pdf", "lib"),
            ("44444444_Taylor2018_tif.tif", "tif"),
        ]

        for filename, expected_pdf_type in test_cases:
            result = parse_filename_by_mod(filename, "FB")
            assert result["pdf_type"] == expected_pdf_type
            assert result["file_publication_status"] == "final"

    def test_fb_real_world_examples(self):
        """Test real-world FB filename examples."""
        test_cases = [
            # Real PubMed ID patterns
            ("12345678_Lewis1978.pdf", "PMID:12345678", "Lewis1978"),
            ("87654321_Nusslein-Volhard1980_temp.pdf", "PMID:87654321", "Nusslein-Volhard1980"),
            ("11111111_Wieschaus1984_ocr.pdf", "PMID:11111111", "Wieschaus1984"),
            ("22222222_Brand1993.html", "PMID:22222222", "Brand1993"),

            # Edge cases
            ("1_A2023.pdf", "PMID:1", "A2023"),
            ("999999999_VeryLongAuthorName2023.pdf", "PMID:999999999", "VeryLongAuthorName2023"),
            ("12345_AuthorWith123Numbers2023.pdf", "PMID:12345", "AuthorWith123Numbers2023"),
        ]

        for filename, expected_curie, expected_author in test_cases:
            result = parse_filename_by_mod(filename, "FB")
            assert result["reference_curie"] == expected_curie
            assert result["author_and_year"] == expected_author

    def test_fb_file_extensions(self):
        """Test FB files with various extensions."""
        extensions = ["pdf", "html", "htm", "txt", "doc", "docx", "xml", "tei", "json"]

        for ext in extensions:
            filename = f"12345678_Author2023.{ext}"
            result = parse_filename_by_mod(filename, "FB")

            assert result["reference_curie"] == "PMID:12345678"
            assert result["file_extension"] == ext
            assert result["display_name"] == "12345678_Author2023"

    def test_fb_html_files_special_handling(self):
        """Test FB HTML files with specific handling."""
        test_cases = [
            ("12345678_Author2023_html.html", "html", "html"),
            ("87654321_Author2022_htm.htm", "html", "htm"),  # htm -> html conversion
            ("11111111_Author2021.html", None, "html"),      # No pdf_type for regular HTML
        ]

        for filename, expected_pdf_type, expected_extension in test_cases:
            result = parse_filename_by_mod(filename, "FB")
            assert result["pdf_type"] == expected_pdf_type
            assert result["file_extension"] == expected_extension


class TestOtherMODPatterns:
    """Test other MOD patterns (SGD, MGI, RGD, ZFIN)."""

    def test_sgd_patterns(self):
        """Test SGD filename patterns."""
        test_cases = [
            ("12345_Author2023.pdf", "SGD", "AGRKB:12345"),  # Short ID
            ("123456789012345_Author2023.pdf", "SGD", "AGRKB:123456789012345"),  # 15-digit AGRKB
        ]

        for filename, mod, expected_curie in test_cases:
            result = parse_filename_by_mod(filename, mod)
            assert result["reference_curie"] == expected_curie
            assert result["mod_abbreviation"] == mod

    def test_mgi_patterns(self):
        """Test MGI filename patterns."""
        test_cases = [
            ("12345_Author2023.pdf", "MGI", "AGRKB:12345"),
            ("123456789012345_Author2023.pdf", "MGI", "AGRKB:123456789012345"),
        ]

        for filename, mod, expected_curie in test_cases:
            result = parse_filename_by_mod(filename, mod)
            assert result["reference_curie"] == expected_curie

    def test_agrkb_15_digit_detection(self):
        """Test 15-digit AGRKB ID detection across MODs."""
        mods = ["SGD", "MGI", "RGD", "ZFIN", "XB"]

        for mod in mods:
            # 15-digit ID should become AGRKB
            result = parse_filename_by_mod("123456789012345_Author2023.pdf", mod)
            assert result["reference_curie"] == "AGRKB:123456789012345"

            # Shorter ID should use fallback
            result = parse_filename_by_mod("12345_Author2023.pdf", mod)
            assert result["reference_curie"] == "AGRKB:12345"


class TestEdgeCasesAndErrorConditions:
    """Test edge cases and error conditions."""

    def test_invalid_filename_patterns(self):
        """Test filenames that should raise ValueError."""
        invalid_patterns = [
            "invalid_filename.pdf",           # No numbers at start
            "text_only_filename.pdf",         # No numbers at all
            "123-456_Author2023.pdf",         # Dash instead of underscore
            "123 456_Author2023.pdf",         # Space instead of underscore
            "_Author2023.pdf",                # Starts with underscore
            "Author2023.pdf",                 # No ID at all
            "123_",                           # Incomplete pattern
            "",                               # Empty filename
            ".pdf",                           # Only extension
        ]

        for invalid_pattern in invalid_patterns:
            with pytest.raises(ValueError, match="does not match expected patterns"):
                parse_filename_by_mod(invalid_pattern, "WB")

    def test_unusual_but_valid_patterns(self):
        """Test unusual but valid filename patterns."""
        test_cases = [
            # Very short IDs
            ("1_A.pdf", "WB", "WB:WBPaper1", "A"),
            ("12_AB.pdf", "FB", "PMID:12", "AB"),

            # Very long author/year (no underscores in author section)
            ("123_VeryVeryVeryLongAuthorNameWithLotsOfCharacters2023.pdf", "WB",
             "WB:WBPaper123", "VeryVeryVeryLongAuthorNameWithLotsOfCharacters2023"),

            # Numbers in author names (regex captures only up to first underscore after author)
            ("123_Author123_2023.pdf", "WB", "WB:WBPaper123", "Author123"),

            # Special characters in filenames (that are still valid)
            ("123_Author-Smith2023.pdf", "WB", "WB:WBPaper123", "Author-Smith2023"),
            ("123_Author.Smith2023.pdf", "WB", "WB:WBPaper123", "Author.Smith2023"),
        ]

        for filename, mod, expected_curie, expected_author in test_cases:
            result = parse_filename_by_mod(filename, mod)
            assert result["reference_curie"] == expected_curie
            assert result["author_and_year"] == expected_author

    def test_case_insensitive_options(self):
        """Test that PDF type and temp options are case insensitive."""
        test_cases = [
            # Different cases for temp
            ("123_Author2023_temp.pdf", "temp"),
            ("123_Author2023_TEMP.pdf", "temp"),
            ("123_Author2023_Temp.pdf", "temp"),
            ("123_Author2023_TeMp.pdf", "temp"),

            # Different cases for PDF types
            ("123_Author2023_aut.pdf", "aut"),
            ("123_Author2023_AUT.pdf", "aut"),
            ("123_Author2023_Aut.pdf", "aut"),
            ("123_Author2023_ocr.pdf", "ocr"),
            ("123_Author2023_OCR.pdf", "ocr"),
            ("123_Author2023_html.pdf", "html"),
            ("123_Author2023_HTML.pdf", "html"),
            ("123_Author2023_Html.pdf", "html"),
        ]

        for filename, expected_value in test_cases:
            result = parse_filename_by_mod(filename, "WB")
            if expected_value == "temp":
                assert result["file_publication_status"] == expected_value
            else:
                assert result["pdf_type"] == expected_value

    def test_filename_without_extension(self):
        """Test filenames without extensions."""
        # These should still work as the regex focuses on the base name
        with pytest.raises(ValueError):
            # This will fail because the pattern expects an extension
            parse_filename_by_mod("123_Author2023", "WB")

    def test_multiple_underscores_in_author(self):
        """Test filenames with multiple underscores in author section."""
        result = parse_filename_by_mod("123_Author_Name_2023.pdf", "WB")
        assert result["reference_curie"] == "WB:WBPaper123"
        # Regex captures only up to first underscore: Author (not Author_Name_2023)
        assert result["author_and_year"] == "Author"

    def test_empty_additional_options(self):
        """Test filenames with empty additional options."""
        # Pattern: 123_Author2023_.pdf (trailing underscore)
        result = parse_filename_by_mod("123_Author2023_.pdf", "WB")
        assert result["reference_curie"] == "WB:WBPaper123"
        assert result["author_and_year"] == "Author2023"
        assert result["file_publication_status"] == "final"  # Empty option -> final
        assert result["pdf_type"] is None


class TestComprehensiveMODComparison:
    """Test comprehensive comparison across different MODs."""

    def test_same_filename_different_mods(self):
        """Test how the same filename is parsed for different MODs."""
        filename = "12345_Author2023.pdf"

        # WB should create WBPaper reference
        wb_result = parse_filename_by_mod(filename, "WB")
        assert wb_result["reference_curie"] == "WB:WBPaper12345"
        assert wb_result["mod_abbreviation"] == "WB"

        # FB should create PMID reference
        fb_result = parse_filename_by_mod(filename, "FB")
        assert fb_result["reference_curie"] == "PMID:12345"
        assert fb_result["mod_abbreviation"] == "FB"

        # Other MODs should create AGRKB reference
        for mod in ["SGD", "MGI", "RGD", "ZFIN", "XB"]:
            result = parse_filename_by_mod(filename, mod)
            assert result["reference_curie"] == "AGRKB:12345"
            assert result["mod_abbreviation"] == mod

    def test_15_digit_ids_across_mods(self):
        """Test 15-digit IDs are consistently handled across MODs."""
        filename = "123456789012345_Author2023.pdf"

        # All MODs should recognize 15-digit as AGRKB
        for mod in ["WB", "FB", "SGD", "MGI", "RGD", "ZFIN", "XB"]:
            result = parse_filename_by_mod(filename, mod)
            assert result["reference_curie"] == "AGRKB:123456789012345"
            assert result["mod_abbreviation"] == mod

    def test_consistent_metadata_across_mods(self):
        """Test that metadata fields are consistent across MODs."""
        filename = "12345_Author2023_temp.pdf"

        for mod in ["WB", "FB", "SGD", "MGI", "RGD", "ZFIN", "XB"]:
            result = parse_filename_by_mod(filename, mod)

            # These fields should be the same regardless of MOD
            assert result["display_name"] == "12345_Author2023_temp"
            assert result["file_extension"] == "pdf"
            assert result["file_publication_status"] == "temp"
            assert result["pdf_type"] is None
            assert result["author_and_year"] == "Author2023"
            assert result["mod_abbreviation"] == mod

            # Only reference_curie should differ by MOD
            if mod == "WB":
                assert result["reference_curie"] == "WB:WBPaper12345"
            elif mod == "FB":
                assert result["reference_curie"] == "PMID:12345"
            else:
                assert result["reference_curie"] == "AGRKB:12345"
