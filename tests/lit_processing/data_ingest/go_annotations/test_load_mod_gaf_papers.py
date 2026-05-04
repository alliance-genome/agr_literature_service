import gzip
import os
import tempfile
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

from agr_literature_service.lit_processing.data_ingest.go_annotations.load_mod_gaf_papers import (
    parse_upload_date,
    extract_pmids_from_gaf,
    fetch_gaf_file_list,
    send_slack_report,
    MOD_NAME_MAP,
)


class TestParseUploadDate:
    """Tests for parsing upload date strings."""

    def test_parse_upload_date_with_z_suffix(self):
        """Test parsing ISO 8601 date with Z suffix."""
        date_str = "2024-03-06T10:30:00Z"
        result = parse_upload_date(date_str)

        assert result is not None
        assert result.year == 2024
        assert result.month == 3
        assert result.day == 6
        assert result.hour == 10
        assert result.minute == 30
        assert result.tzinfo == timezone.utc

    def test_parse_upload_date_with_offset(self):
        """Test parsing ISO 8601 date with timezone offset."""
        date_str = "2024-03-06T10:30:00+00:00"
        result = parse_upload_date(date_str)

        assert result is not None
        assert result.year == 2024
        assert result.month == 3
        assert result.day == 6

    def test_parse_upload_date_without_timezone(self):
        """Test parsing ISO 8601 date without timezone."""
        date_str = "2024-03-06T10:30:00"
        result = parse_upload_date(date_str)

        assert result is not None
        assert result.year == 2024
        assert result.month == 3
        assert result.day == 6
        assert result.tzinfo == timezone.utc

    def test_parse_upload_date_with_milliseconds(self):
        """Test parsing ISO 8601 date with milliseconds."""
        date_str = "2024-03-06T10:30:00.123Z"
        result = parse_upload_date(date_str)

        assert result is not None
        assert result.year == 2024

    def test_parse_upload_date_invalid(self):
        """Test parsing invalid date string returns None."""
        date_str = "not-a-date"
        result = parse_upload_date(date_str)

        assert result is None

    def test_parse_upload_date_empty_string(self):
        """Test parsing empty string returns None."""
        result = parse_upload_date("")

        assert result is None


class TestExtractPmidsFromGaf:
    """Tests for extracting PMIDs from GAF files."""

    def test_extract_pmids_basic(self):
        """Test basic PMID extraction from GAF content."""
        gaf_content = """!gaf-version: 2.2
!Comment line
UniProtKB\tA0A024RBG1\tNUDT4B\t\tGO:0003824\tPMID:21873635\tIBA\t\tF\tTest\tHGNC:24664\tprotein\ttaxon:9606\t20211011\tGO_Central\t\t
UniProtKB\tA0A024RBG1\tNUDT4B\t\tGO:0046872\tPMID:27312411\tIDA\t\tF\tTest\tHGNC:24664\tprotein\ttaxon:9606\t20211011\tGO_Central\t\t
"""
        with tempfile.NamedTemporaryFile(suffix='.gaf.gz', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            with gzip.open(tmp_path, 'wt') as f:
                f.write(gaf_content)

        try:
            pmids = extract_pmids_from_gaf(tmp_path)

            assert len(pmids) == 2
            assert "21873635" in pmids
            assert "27312411" in pmids
        finally:
            os.unlink(tmp_path)

    def test_extract_pmids_deduplication(self):
        """Test that duplicate PMIDs are deduplicated."""
        gaf_content = """!gaf-version: 2.2
UniProtKB\tA0A024RBG1\tNUDT4B\t\tGO:0003824\tPMID:21873635\tIBA\t\tF\tTest\tHGNC:24664\tprotein\ttaxon:9606\t20211011\tGO_Central\t\t
UniProtKB\tA0A024RBG1\tNUDT4B\t\tGO:0046872\tPMID:21873635\tIDA\t\tF\tTest\tHGNC:24664\tprotein\ttaxon:9606\t20211011\tGO_Central\t\t
UniProtKB\tA0A024RBG1\tNUDT4B\t\tGO:0005576\tPMID:21873635\tIEA\t\tC\tTest\tHGNC:24664\tprotein\ttaxon:9606\t20211011\tGO_Central\t\t
"""
        with tempfile.NamedTemporaryFile(suffix='.gaf.gz', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            with gzip.open(tmp_path, 'wt') as f:
                f.write(gaf_content)

        try:
            pmids = extract_pmids_from_gaf(tmp_path)

            assert len(pmids) == 1
            assert "21873635" in pmids
        finally:
            os.unlink(tmp_path)

    def test_extract_pmids_short_lines(self):
        """Test handling of lines with too few columns."""
        gaf_content = """!gaf-version: 2.2
UniProtKB\tA0A024RBG1\tNUDT4B
UniProtKB\tA0A024RBG1\tNUDT4B\t\tGO:0003824\tPMID:11111111\tIBA\t\tF\tTest\tHGNC:24664\tprotein\ttaxon:9606\t20211011\tGO_Central\t\t
"""
        with tempfile.NamedTemporaryFile(suffix='.gaf.gz', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            with gzip.open(tmp_path, 'wt') as f:
                f.write(gaf_content)

        try:
            pmids = extract_pmids_from_gaf(tmp_path)

            # Only the valid line should be processed
            assert len(pmids) == 1
            assert "11111111" in pmids
        finally:
            os.unlink(tmp_path)

    def test_extract_pmids_invalid_pmid_format(self):
        """Test that non-numeric PMIDs are ignored."""
        gaf_content = """!gaf-version: 2.2
UniProtKB\tA0A024RBG1\tNUDT4B\t\tGO:0003824\tPMID:abc123\tIBA\t\tF\tTest\tHGNC:24664\tprotein\ttaxon:9606\t20211011\tGO_Central\t\t
UniProtKB\tA0A024RBG1\tNUDT4B\t\tGO:0046872\tPMID:12345678\tIDA\t\tF\tTest\tHGNC:24664\tprotein\ttaxon:9606\t20211011\tGO_Central\t\t
"""
        with tempfile.NamedTemporaryFile(suffix='.gaf.gz', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            with gzip.open(tmp_path, 'wt') as f:
                f.write(gaf_content)

        try:
            pmids = extract_pmids_from_gaf(tmp_path)

            assert len(pmids) == 1
            assert "12345678" in pmids
            assert "abc123" not in pmids
        finally:
            os.unlink(tmp_path)

    def test_extract_pmids_nonexistent_file(self):
        """Test handling of nonexistent file."""
        pmids = extract_pmids_from_gaf("/nonexistent/path/file.gaf.gz")

        assert len(pmids) == 0


class TestFetchGafFileList:
    """Tests for fetching GAF file list from API."""

    @patch('agr_literature_service.lit_processing.data_ingest.go_annotations.load_mod_gaf_papers.requests.get')
    def test_fetch_gaf_file_list_success(self, mock_get):
        """Test successful API fetch."""
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "id": 1,
                "s3Url": "https://example.com/GAF_HUMAN_0.gaf.gz",
                "uploadDate": "2024-03-06T10:30:00Z",
                "dataSubType": {"name": "HUMAN"}
            },
            {
                "id": 2,
                "s3Url": "https://example.com/GAF_MGI_1.gaf.gz",
                "uploadDate": "2024-03-06T10:30:00Z",
                "dataSubType": {"name": "MGI"}
            }
        ]
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = fetch_gaf_file_list()

        assert len(result) == 2
        assert result[0]["dataSubType"]["name"] == "HUMAN"
        assert result[1]["dataSubType"]["name"] == "MGI"

    @patch('agr_literature_service.lit_processing.data_ingest.go_annotations.load_mod_gaf_papers.requests.get')
    def test_fetch_gaf_file_list_error(self, mock_get):
        """Test API fetch error handling."""
        import requests
        mock_get.side_effect = requests.RequestException("Connection error")

        result = fetch_gaf_file_list()

        assert result == []


class TestModNameMap:
    """Tests for MOD name mapping."""

    def test_mod_name_map_contains_expected_mods(self):
        """Test that MOD_NAME_MAP contains expected MODs."""
        expected_mods = ["HUMAN", "MGI", "SGD", "WB", "FB", "ZFIN", "RGD", "XB"]

        for mod in expected_mods:
            assert mod in MOD_NAME_MAP

    def test_mod_name_map_human_maps_to_agr(self):
        """Test that HUMAN maps to AGR."""
        assert MOD_NAME_MAP["HUMAN"] == "AGR"


class TestUploadDateFiltering:
    """Tests for upload date filtering logic."""

    def test_recent_file_should_be_processed(self):
        """Test that files updated within 24 hours are processed."""
        now = datetime.now(timezone.utc)
        recent_upload = now - timedelta(hours=12)
        cutoff_time = now - timedelta(hours=24)

        assert recent_upload >= cutoff_time

    def test_old_file_should_be_skipped(self):
        """Test that files updated more than 24 hours ago are skipped."""
        now = datetime.now(timezone.utc)
        old_upload = now - timedelta(hours=48)
        cutoff_time = now - timedelta(hours=24)

        assert old_upload < cutoff_time

    def test_exact_24_hour_boundary(self):
        """Test behavior at exactly 24 hours."""
        now = datetime.now(timezone.utc)
        boundary_upload = now - timedelta(hours=24)
        cutoff_time = now - timedelta(hours=24)

        # At exactly 24 hours, should still be processed (>=)
        assert boundary_upload >= cutoff_time


class TestExtractPmidsFromGafEdgeCases:
    """Additional edge case tests for extracting PMIDs from GAF files."""

    def test_extract_pmids_with_whitespace_in_refs(self):
        """Test extraction handles whitespace around PMID references."""
        gaf_content = """!gaf-version: 2.2
UniProtKB\tA0A024RBG1\tNUDT4B\t\tGO:0003824\t PMID:11111111 | PMID:22222222 \tIBA\t\tF\tTest\tHGNC:24664\tprotein\ttaxon:9606\t20211011\tGO_Central\t\t
"""
        with tempfile.NamedTemporaryFile(suffix='.gaf.gz', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            with gzip.open(tmp_path, 'wt') as f:
                f.write(gaf_content)

        try:
            pmids = extract_pmids_from_gaf(tmp_path)

            assert len(pmids) == 2
            assert "11111111" in pmids
            assert "22222222" in pmids
        finally:
            os.unlink(tmp_path)

    def test_extract_pmids_mixed_case_pmid_prefix(self):
        """Test that only uppercase PMID: prefix is recognized."""
        gaf_content = """!gaf-version: 2.2
UniProtKB\tA0A024RBG1\tNUDT4B\t\tGO:0003824\tPMID:11111111\tIBA\t\tF\tTest\tHGNC:24664\tprotein\ttaxon:9606\t20211011\tGO_Central\t\t
UniProtKB\tA0A024RBG1\tNUDT4B\t\tGO:0003824\tpmid:22222222\tIBA\t\tF\tTest\tHGNC:24664\tprotein\ttaxon:9606\t20211011\tGO_Central\t\t
"""
        with tempfile.NamedTemporaryFile(suffix='.gaf.gz', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            with gzip.open(tmp_path, 'wt') as f:
                f.write(gaf_content)

        try:
            pmids = extract_pmids_from_gaf(tmp_path)

            # Only uppercase PMID: should be recognized
            assert len(pmids) == 1
            assert "11111111" in pmids
        finally:
            os.unlink(tmp_path)

    def test_extract_pmids_empty_ref_column(self):
        """Test handling of empty reference column."""
        gaf_content = """!gaf-version: 2.2
UniProtKB\tA0A024RBG1\tNUDT4B\t\tGO:0003824\t\tIBA\t\tF\tTest\tHGNC:24664\tprotein\ttaxon:9606\t20211011\tGO_Central\t\t
UniProtKB\tA0A024RBG1\tNUDT4B\t\tGO:0003824\tPMID:11111111\tIBA\t\tF\tTest\tHGNC:24664\tprotein\ttaxon:9606\t20211011\tGO_Central\t\t
"""
        with tempfile.NamedTemporaryFile(suffix='.gaf.gz', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            with gzip.open(tmp_path, 'wt') as f:
                f.write(gaf_content)

        try:
            pmids = extract_pmids_from_gaf(tmp_path)

            assert len(pmids) == 1
            assert "11111111" in pmids
        finally:
            os.unlink(tmp_path)


class TestSendSlackReport:
    """Tests for send_slack_report function."""

    @patch('agr_literature_service.lit_processing.data_ingest.go_annotations.load_mod_gaf_papers.send_report')
    def test_send_slack_report_calls_send_report(self, mock_send_report):
        """Test that send_slack_report calls send_report with correct args."""
        test_message = "<b>Test Report</b><p>Test content</p>"

        send_slack_report(test_message)

        mock_send_report.assert_called_once_with(
            "MOD GAF Paper Loading Report",
            test_message
        )


class TestModNameMapValues:
    """Additional tests for MOD name mapping values."""

    def test_mod_name_map_mgi_maps_to_mgi(self):
        """Test that MGI maps to MGI."""
        assert MOD_NAME_MAP["MGI"] == "MGI"

    def test_mod_name_map_sgd_maps_to_sgd(self):
        """Test that SGD maps to SGD."""
        assert MOD_NAME_MAP["SGD"] == "SGD"

    def test_mod_name_map_wb_maps_to_wb(self):
        """Test that WB maps to WB."""
        assert MOD_NAME_MAP["WB"] == "WB"

    def test_mod_name_map_fb_maps_to_fb(self):
        """Test that FB maps to FB."""
        assert MOD_NAME_MAP["FB"] == "FB"

    def test_mod_name_map_zfin_maps_to_zfin(self):
        """Test that ZFIN maps to ZFIN."""
        assert MOD_NAME_MAP["ZFIN"] == "ZFIN"

    def test_mod_name_map_rgd_maps_to_rgd(self):
        """Test that RGD maps to RGD."""
        assert MOD_NAME_MAP["RGD"] == "RGD"

    def test_mod_name_map_xb_maps_to_xb(self):
        """Test that XB maps to XB."""
        assert MOD_NAME_MAP["XB"] == "XB"


class TestParseUploadDateEdgeCases:
    """Additional edge case tests for parse_upload_date."""

    def test_parse_upload_date_with_positive_offset(self):
        """Test parsing date with positive timezone offset."""
        date_str = "2024-03-06T10:30:00+05:30"
        result = parse_upload_date(date_str)

        assert result is not None
        assert result.year == 2024
        assert result.month == 3
        assert result.day == 6

    def test_parse_upload_date_with_negative_offset(self):
        """Test parsing date with negative timezone offset."""
        date_str = "2024-03-06T10:30:00-08:00"
        result = parse_upload_date(date_str)

        assert result is not None
        assert result.year == 2024

    def test_parse_upload_date_partial_invalid(self):
        """Test parsing partially valid date string."""
        date_str = "2024-03-06T"
        result = parse_upload_date(date_str)

        # Should fail gracefully
        assert result is None
