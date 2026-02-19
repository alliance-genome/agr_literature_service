"""
Unit tests for download_pdfs_from_europepmc.py
"""
import gzip
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, Mock

import pytest
import requests

from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.download_pdfs_from_europepmc import (
    normalize_pmcid,
    europepmc_pdf_url,
    check_pdf_available,
    download_pdf_by_pmcid,
    gzip_file,
    upload_pdf_file_to_s3,
    parse_yn,
    chunked,
    load_cache,
    save_cache,
    fetch_batch_core,
    is_oa_from_cache_entry,
    needs_promotion,
    process_single_item,
    PmcMeta,
    RfInfo,
    reset_error_stats,
)


@pytest.fixture(autouse=True)
def reset_error_tracking():
    """Reset error stats before each test to avoid cross-test contamination."""
    reset_error_stats()
    yield
    reset_error_stats()


class TestNormalizePmcid:
    """Tests for normalize_pmcid function."""

    def test_normalize_pmcid_with_prefix(self):
        """Test normalizing PMCID with PMCID: prefix."""
        assert normalize_pmcid("PMCID:PMC1234567") == "PMC1234567"

    def test_normalize_pmcid_without_prefix(self):
        """Test normalizing PMCID without prefix."""
        assert normalize_pmcid("PMC1234567") == "PMC1234567"

    def test_normalize_pmcid_lowercase(self):
        """Test normalizing lowercase PMCID."""
        assert normalize_pmcid("pmcid:pmc1234567") == "PMC1234567"

    def test_normalize_pmcid_with_whitespace(self):
        """Test normalizing PMCID with surrounding whitespace."""
        assert normalize_pmcid("  PMCID:PMC1234567  ") == "PMC1234567"

    def test_normalize_pmcid_mixed_case(self):
        """Test normalizing mixed case PMCID."""
        assert normalize_pmcid("PmCiD:PmC9999999") == "PMC9999999"


class TestEuropepmcPdfUrl:
    """Tests for europepmc_pdf_url function."""

    def test_europepmc_pdf_url_basic(self):
        """Test URL generation for a basic PMC ID."""
        url = europepmc_pdf_url("PMC1234567")
        assert url == "https://europepmc.org/backend/ptpmcrender.fcgi?accid=PMC1234567&blobtype=pdf"

    def test_europepmc_pdf_url_different_id(self):
        """Test URL generation for a different PMC ID."""
        url = europepmc_pdf_url("PMC9876543")
        expected = "https://europepmc.org/backend/ptpmcrender.fcgi?accid=PMC9876543&blobtype=pdf"
        assert url == expected


class TestCheckPdfAvailable:
    """Tests for check_pdf_available function."""

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.download_pdfs_from_europepmc.requests.head')
    def test_check_pdf_available_success(self, mock_head):
        """Test checking PDF availability when PDF is available."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/pdf"}
        mock_response.history = []  # No redirects
        mock_response.url = "https://europepmc.org/backend/ptpmcrender.fcgi?accid=PMC1234567&blobtype=pdf"
        mock_head.return_value = mock_response

        result = check_pdf_available("PMC1234567")
        assert result is True

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.download_pdfs_from_europepmc.requests.head')
    def test_check_pdf_available_not_found(self, mock_head):
        """Test checking PDF availability when PDF is not found."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.headers = {}
        mock_response.history = []
        mock_response.url = "https://europepmc.org/backend/ptpmcrender.fcgi?accid=PMC0000000&blobtype=pdf"
        mock_head.return_value = mock_response

        result = check_pdf_available("PMC0000000")
        assert result is False

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.download_pdfs_from_europepmc.requests.head')
    def test_check_pdf_available_wrong_content_type(self, mock_head):
        """Test checking PDF availability when content type is not PDF."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "text/html"}
        mock_response.history = []
        mock_response.url = "https://europepmc.org/backend/ptpmcrender.fcgi?accid=PMC1234567&blobtype=pdf"
        mock_head.return_value = mock_response

        result = check_pdf_available("PMC1234567")
        assert result is False

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.download_pdfs_from_europepmc.requests.head')
    def test_check_pdf_available_request_exception(self, mock_head):
        """Test checking PDF availability when request fails."""
        mock_head.side_effect = requests.RequestException("Connection error")

        result = check_pdf_available("PMC1234567")
        assert result is False

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.download_pdfs_from_europepmc.requests.head')
    def test_check_pdf_available_with_pmcid_prefix(self, mock_head):
        """Test that PMCID prefix is properly normalized."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/pdf"}
        mock_response.history = []
        mock_response.url = "https://europepmc.org/backend/ptpmcrender.fcgi?accid=PMC1234567&blobtype=pdf"
        mock_head.return_value = mock_response

        result = check_pdf_available("PMCID:PMC1234567")
        assert result is True
        # Verify the URL was constructed with normalized PMCID
        call_args = mock_head.call_args
        assert "PMC1234567" in call_args[0][0]


class TestDownloadPdfByPmcid:
    """Tests for download_pdf_by_pmcid function."""

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.download_pdfs_from_europepmc.requests.get')
    def test_download_pdf_success(self, mock_get):
        """Test successful PDF download."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/pdf"}
        mock_response.history = []
        mock_response.url = "https://europepmc.org/backend/ptpmcrender.fcgi?accid=PMC1234567&blobtype=pdf"
        mock_response.iter_content = Mock(return_value=[b"PDF content here"])
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)
        mock_get.return_value = mock_response

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "PMC1234567.pdf")
            result = download_pdf_by_pmcid("PMC1234567", output_path)

            assert result is True
            assert os.path.exists(output_path)
            with open(output_path, "rb") as f:
                assert f.read() == b"PDF content here"

    def test_download_pdf_file_exists(self):
        """Test that download is skipped if file already exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "PMC1234567.pdf")
            # Create existing file
            with open(output_path, "w") as f:
                f.write("existing content")

            result = download_pdf_by_pmcid("PMC1234567", output_path)
            assert result is False

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.download_pdfs_from_europepmc.requests.get')
    def test_download_pdf_not_found(self, mock_get):
        """Test download when PDF is not found."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.headers = {}
        mock_response.history = []
        mock_response.url = "https://europepmc.org/backend/ptpmcrender.fcgi?accid=PMC0000000&blobtype=pdf"
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)
        mock_get.return_value = mock_response

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "PMC0000000.pdf")
            result = download_pdf_by_pmcid("PMC0000000", output_path)

            assert result is False
            assert not os.path.exists(output_path)

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.download_pdfs_from_europepmc.requests.get')
    def test_download_pdf_wrong_content_type(self, mock_get):
        """Test download when content type is not PDF."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "text/html"}
        mock_response.history = []
        mock_response.url = "https://europepmc.org/backend/ptpmcrender.fcgi?accid=PMC1234567&blobtype=pdf"
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)
        mock_get.return_value = mock_response

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "PMC1234567.pdf")
            result = download_pdf_by_pmcid("PMC1234567", output_path)

            assert result is False
            assert not os.path.exists(output_path)

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.download_pdfs_from_europepmc.requests.get')
    def test_download_pdf_request_exception(self, mock_get):
        """Test download when request fails."""
        mock_get.side_effect = requests.RequestException("Connection error")

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "PMC1234567.pdf")
            result = download_pdf_by_pmcid("PMC1234567", output_path)

            assert result is False
            assert not os.path.exists(output_path)


class TestGzipFile:
    """Tests for gzip_file function."""

    def test_gzip_file_success(self):
        """Test successful gzip compression."""
        with tempfile.TemporaryDirectory() as tmpdir:
            pdf_path = os.path.join(tmpdir, "test.pdf")
            with open(pdf_path, "wb") as f:
                f.write(b"Test PDF content")

            gz_path = gzip_file(pdf_path)

            assert gz_path is not None
            assert gz_path == pdf_path + ".gz"
            assert os.path.exists(gz_path)

            # Verify content
            with gzip.open(gz_path, "rb") as f:
                assert f.read() == b"Test PDF content"

    def test_gzip_file_overwrites_existing(self):
        """Test that existing .gz file is overwritten."""
        with tempfile.TemporaryDirectory() as tmpdir:
            pdf_path = os.path.join(tmpdir, "test.pdf")
            gz_path = pdf_path + ".gz"

            # Create existing gz file
            with open(gz_path, "w") as f:
                f.write("old content")

            with open(pdf_path, "wb") as f:
                f.write(b"New PDF content")

            result = gzip_file(pdf_path)

            assert result == gz_path
            with gzip.open(gz_path, "rb") as f:
                assert f.read() == b"New PDF content"

    def test_gzip_file_nonexistent_file(self):
        """Test gzip on nonexistent file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            pdf_path = os.path.join(tmpdir, "nonexistent.pdf")
            result = gzip_file(pdf_path)
            assert result is None


class TestUploadPdfFileToS3:
    """Tests for upload_pdf_file_to_s3 function."""

    @patch.dict(os.environ, {}, clear=True)
    def test_upload_skipped_no_env_state(self):
        """Test upload is skipped when ENV_STATE is not set."""
        result = upload_pdf_file_to_s3("/path/to/file.gz", "abc123")
        assert result is None

    @patch.dict(os.environ, {"ENV_STATE": "test"}, clear=True)
    def test_upload_skipped_test_env(self):
        """Test upload is skipped when ENV_STATE is 'test'."""
        result = upload_pdf_file_to_s3("/path/to/file.gz", "abc123")
        assert result is None

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.download_pdfs_from_europepmc.upload_file_to_s3')
    @patch.dict(os.environ, {"ENV_STATE": "develop"}, clear=True)
    def test_upload_develop_env(self, mock_upload):
        """Test upload with develop environment."""
        mock_upload.return_value = True

        with tempfile.NamedTemporaryFile(suffix=".gz") as f:
            result = upload_pdf_file_to_s3(f.name, "abcd1234")

        assert result is True
        mock_upload.assert_called_once()
        call_args = mock_upload.call_args
        assert "develop/reference/documents/" in call_args[0][2]
        assert call_args[0][3] == "STANDARD"

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.download_pdfs_from_europepmc.upload_file_to_s3')
    @patch.dict(os.environ, {"ENV_STATE": "prod"}, clear=True)
    def test_upload_prod_env(self, mock_upload):
        """Test upload with prod environment."""
        mock_upload.return_value = True

        with tempfile.NamedTemporaryFile(suffix=".gz") as f:
            result = upload_pdf_file_to_s3(f.name, "abcd1234")

        assert result is True
        mock_upload.assert_called_once()
        call_args = mock_upload.call_args
        assert "prod/reference/documents/" in call_args[0][2]
        assert call_args[0][3] == "GLACIER_IR"


class TestParseYn:
    """Tests for parse_yn function."""

    def test_parse_yn_yes(self):
        """Test parsing 'Y' value."""
        assert parse_yn("Y") is True

    def test_parse_yn_no(self):
        """Test parsing 'N' value."""
        assert parse_yn("N") is False

    def test_parse_yn_none(self):
        """Test parsing None value."""
        assert parse_yn(None) is None

    def test_parse_yn_other(self):
        """Test parsing other values."""
        assert parse_yn("maybe") is None
        assert parse_yn("") is None
        assert parse_yn("Yes") is None


class TestChunked:
    """Tests for chunked function."""

    def test_chunked_basic(self):
        """Test basic chunking."""
        items = ["a", "b", "c", "d", "e"]
        chunks = list(chunked(items, 2))
        assert chunks == [["a", "b"], ["c", "d"], ["e"]]

    def test_chunked_exact_size(self):
        """Test chunking when items divide evenly."""
        items = ["a", "b", "c", "d"]
        chunks = list(chunked(items, 2))
        assert chunks == [["a", "b"], ["c", "d"]]

    def test_chunked_larger_chunk(self):
        """Test chunking when chunk size > items."""
        items = ["a", "b"]
        chunks = list(chunked(items, 5))
        assert chunks == [["a", "b"]]

    def test_chunked_empty(self):
        """Test chunking empty list."""
        items = []
        chunks = list(chunked(items, 3))
        assert chunks == []

    def test_chunked_single_item(self):
        """Test chunking single item."""
        items = ["a"]
        chunks = list(chunked(items, 3))
        assert chunks == [["a"]]


class TestLoadSaveCache:
    """Tests for load_cache and save_cache functions."""

    def test_save_and_load_cache(self):
        """Test saving and loading cache."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "cache.json"
            cache_data = {
                "PMC1234567": {"hit": True, "is_open_access": True},
                "PMC7654321": {"hit": False},
            }

            save_cache(cache_path, cache_data)
            loaded = load_cache(cache_path)

            assert loaded == cache_data

    def test_load_cache_nonexistent(self):
        """Test loading nonexistent cache file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "nonexistent.json"
            loaded = load_cache(cache_path)
            assert loaded == {}

    def test_load_cache_invalid_json(self):
        """Test loading invalid JSON cache file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "invalid.json"
            cache_path.write_text("not valid json {{{")
            loaded = load_cache(cache_path)
            assert loaded == {}

    def test_save_cache_creates_parent_dirs(self):
        """Test that save_cache creates parent directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "subdir" / "nested" / "cache.json"
            cache_data = {"test": "data"}

            save_cache(cache_path, cache_data)

            assert cache_path.exists()
            loaded = load_cache(cache_path)
            assert loaded == cache_data


class TestFetchBatchCore:
    """Tests for fetch_batch_core function."""

    @patch.object(requests.Session, 'get')
    def test_fetch_batch_core_success(self, mock_get):
        """Test successful batch fetch."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.history = []
        mock_response.url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {
            "resultList": {
                "result": [
                    {
                        "pmcid": "PMC1234567",
                        "isOpenAccess": "Y",
                        "license": "CC-BY",
                        "hasPDF": "Y",
                        "inPMC": "Y",
                    },
                    {
                        "pmcid": "PMC7654321",
                        "isOpenAccess": "N",
                        "license": None,
                        "hasPDF": "N",
                        "inPMC": "Y",
                    },
                ]
            }
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        session = requests.Session()
        result = fetch_batch_core(["PMC1234567", "PMC7654321"], session)

        assert "PMC1234567" in result
        assert result["PMC1234567"].is_open_access is True
        assert result["PMC1234567"].has_pdf is True
        assert result["PMC1234567"].hit is True

        assert "PMC7654321" in result
        assert result["PMC7654321"].is_open_access is False
        assert result["PMC7654321"].has_pdf is False

    @patch.object(requests.Session, 'get')
    def test_fetch_batch_core_empty_results(self, mock_get):
        """Test batch fetch with empty results."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.history = []
        mock_response.url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {"resultList": {"result": []}}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        session = requests.Session()
        result = fetch_batch_core(["PMC0000000"], session)

        assert result == {}

    @patch.object(requests.Session, 'get')
    def test_fetch_batch_core_no_pmcid(self, mock_get):
        """Test batch fetch with result missing pmcid."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.history = []
        mock_response.url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {
            "resultList": {
                "result": [
                    {"isOpenAccess": "Y", "hasPDF": "Y"},  # Missing pmcid
                ]
            }
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        session = requests.Session()
        result = fetch_batch_core(["PMC1234567"], session)

        assert result == {}


class TestIsOaFromCacheEntry:
    """Tests for is_oa_from_cache_entry function."""

    def test_is_oa_full_oa(self):
        """Test OA check with full OA entry."""
        cache_entry = {
            "hit": True,
            "is_open_access": True,
            "has_pdf": True,
        }
        assert is_oa_from_cache_entry(cache_entry, require_has_pdf=True) is True

    def test_is_oa_no_pdf(self):
        """Test OA check when has_pdf is False and required."""
        cache_entry = {
            "hit": True,
            "is_open_access": True,
            "has_pdf": False,
        }
        assert is_oa_from_cache_entry(cache_entry, require_has_pdf=True) is False

    def test_is_oa_no_pdf_not_required(self):
        """Test OA check when has_pdf is False but not required."""
        cache_entry = {
            "hit": True,
            "is_open_access": True,
            "has_pdf": False,
        }
        assert is_oa_from_cache_entry(cache_entry, require_has_pdf=False) is True

    def test_is_oa_not_open_access(self):
        """Test OA check when is_open_access is False."""
        cache_entry = {
            "hit": True,
            "is_open_access": False,
            "has_pdf": True,
        }
        assert is_oa_from_cache_entry(cache_entry, require_has_pdf=True) is False

    def test_is_oa_no_hit(self):
        """Test OA check when hit is False."""
        cache_entry = {
            "hit": False,
            "is_open_access": True,
            "has_pdf": True,
        }
        assert is_oa_from_cache_entry(cache_entry, require_has_pdf=True) is False

    def test_is_oa_empty_entry(self):
        """Test OA check with empty entry."""
        assert is_oa_from_cache_entry({}) is False

    def test_is_oa_none_entry(self):
        """Test OA check with None entry."""
        assert is_oa_from_cache_entry(None) is False


class TestNeedsPromotion:
    """Tests for needs_promotion function."""

    def test_needs_promotion_supplement(self):
        """Test that supplement PDF needs promotion."""
        info = RfInfo(
            referencefile_id=1,
            file_class="supplement",
            file_extension="pdf",
            file_publication_status="final",
            pdf_type="pdf",
            display_name="test.pdf",
        )
        assert needs_promotion(info) is True

    def test_needs_promotion_main_pdf(self):
        """Test that main PDF does not need promotion."""
        info = RfInfo(
            referencefile_id=1,
            file_class="main",
            file_extension="pdf",
            file_publication_status="final",
            pdf_type="pdf",
            display_name="test.pdf",
        )
        assert needs_promotion(info) is False

    def test_needs_promotion_none(self):
        """Test needs_promotion with None."""
        assert needs_promotion(None) is False

    def test_needs_promotion_non_pdf(self):
        """Test needs_promotion with non-PDF file."""
        info = RfInfo(
            referencefile_id=1,
            file_class="supplement",
            file_extension="txt",
            file_publication_status="final",
            pdf_type=None,
            display_name="test.txt",
        )
        assert needs_promotion(info) is False

    def test_needs_promotion_empty_extension(self):
        """Test needs_promotion with empty extension."""
        info = RfInfo(
            referencefile_id=1,
            file_class="supplement",
            file_extension="",
            file_publication_status="draft",
            pdf_type=None,
            display_name="test",
        )
        assert needs_promotion(info) is True

    def test_needs_promotion_different_publication_status(self):
        """Test needs_promotion with different publication status."""
        info = RfInfo(
            referencefile_id=1,
            file_class="main",
            file_extension="pdf",
            file_publication_status="draft",  # Not "final"
            pdf_type="pdf",
            display_name="test.pdf",
        )
        assert needs_promotion(info) is True


class TestProcessSingleItem:
    """Tests for process_single_item function."""

    def test_process_single_item_already_downloaded(self):
        """Test processing when PDF already exists on disk."""
        with tempfile.TemporaryDirectory() as tmpdir:
            pdf_path = os.path.join(tmpdir, "PMC1234567.pdf")
            with open(pdf_path, "w") as f:
                f.write("existing pdf")

            # Args: (index, reference_id, pmcid, dry_run, output_dir, download_sleep)
            args = (1, 100, "PMCID:PMC1234567", False, tmpdir, 0.0)
            result = process_single_item(args)

            assert result["available"] is True
            assert result["downloaded"] is True
            assert result["already_downloaded"] is True
            assert result["reference_id"] == 100
            assert result["pmcid"] == "PMCID:PMC1234567"

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.download_pdfs_from_europepmc.check_pdf_available')
    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.download_pdfs_from_europepmc.download_pdf_by_pmcid')
    def test_process_single_item_download_success(self, mock_download, mock_check):
        """Test processing when PDF needs to be downloaded."""
        mock_check.return_value = True
        mock_download.return_value = True

        with tempfile.TemporaryDirectory() as tmpdir:
            # Args: (index, reference_id, pmcid, dry_run, output_dir, download_sleep)
            args = (1, 100, "PMCID:PMC1234567", False, tmpdir, 0.0)
            result = process_single_item(args)

            assert result["available"] is True
            assert result["downloaded"] is True
            assert result["already_downloaded"] is False
            mock_download.assert_called_once()

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.download_pdfs_from_europepmc.check_pdf_available')
    def test_process_single_item_not_available(self, mock_check):
        """Test processing when PDF is not available (dry-run mode)."""
        mock_check.return_value = False

        with tempfile.TemporaryDirectory() as tmpdir:
            # Use dry_run=True since check_pdf_available is only called in dry-run mode
            # Args: (index, reference_id, pmcid, dry_run, output_dir, download_sleep)
            args = (1, 100, "PMCID:PMC1234567", True, tmpdir, 0.0)
            result = process_single_item(args)

            assert result["available"] is False
            assert result["downloaded"] is False
            mock_check.assert_called_once()

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.download_pdfs_from_europepmc.check_pdf_available')
    def test_process_single_item_dry_run(self, mock_check):
        """Test processing in dry run mode."""
        mock_check.return_value = True

        with tempfile.TemporaryDirectory() as tmpdir:
            # Args: (index, reference_id, pmcid, dry_run, output_dir, download_sleep)
            args = (1, 100, "PMCID:PMC1234567", True, tmpdir, 0.0)  # dry_run=True
            result = process_single_item(args)

            assert result["available"] is True
            assert result["downloaded"] is False  # Not downloaded in dry run

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.download_pdfs_from_europepmc.check_pdf_available')
    def test_process_single_item_exception(self, mock_check):
        """Test processing when an exception occurs (dry-run mode)."""
        mock_check.side_effect = Exception("Test error")

        with tempfile.TemporaryDirectory() as tmpdir:
            # Use dry_run=True since check_pdf_available is only called in dry-run mode
            # Args: (index, reference_id, pmcid, dry_run, output_dir, download_sleep)
            args = (1, 100, "PMCID:PMC1234567", True, tmpdir, 0.0)
            result = process_single_item(args)

            assert result["available"] is False
            assert result["downloaded"] is False
            assert "Exception" in result["error"]
            assert "Test error" in result["error"]


class TestPmcMeta:
    """Tests for PmcMeta dataclass."""

    def test_pmc_meta_creation(self):
        """Test PmcMeta dataclass creation."""
        meta = PmcMeta(
            pmcid="PMC1234567",
            hit=True,
            is_open_access=True,
            license="CC-BY",
            has_pdf=True,
            in_pmc=True,
        )
        assert meta.pmcid == "PMC1234567"
        assert meta.hit is True
        assert meta.is_open_access is True
        assert meta.license == "CC-BY"
        assert meta.has_pdf is True
        assert meta.in_pmc is True

    def test_pmc_meta_frozen(self):
        """Test that PmcMeta is frozen (immutable)."""
        meta = PmcMeta(
            pmcid="PMC1234567",
            hit=True,
            is_open_access=True,
            license="CC-BY",
            has_pdf=True,
            in_pmc=True,
        )
        with pytest.raises(AttributeError):
            meta.pmcid = "PMC7654321"


class TestRfInfo:
    """Tests for RfInfo dataclass."""

    def test_rf_info_creation(self):
        """Test RfInfo dataclass creation."""
        info = RfInfo(
            referencefile_id=123,
            file_class="main",
            file_extension="pdf",
            file_publication_status="final",
            pdf_type="pdf",
            display_name="test.pdf",
        )
        assert info.referencefile_id == 123
        assert info.file_class == "main"
        assert info.file_extension == "pdf"
        assert info.file_publication_status == "final"
        assert info.pdf_type == "pdf"
        assert info.display_name == "test.pdf"

    def test_rf_info_with_none_values(self):
        """Test RfInfo with None values."""
        info = RfInfo(
            referencefile_id=123,
            file_class=None,
            file_extension=None,
            file_publication_status=None,
            pdf_type=None,
            display_name=None,
        )
        assert info.referencefile_id == 123
        assert info.file_class is None
        assert info.file_extension is None

    def test_rf_info_mutable(self):
        """Test that RfInfo is mutable (not frozen)."""
        info = RfInfo(
            referencefile_id=123,
            file_class="supplement",
            file_extension="pdf",
            file_publication_status="draft",
            pdf_type="pdf",
            display_name="test.pdf",
        )
        info.file_class = "main"
        assert info.file_class == "main"
