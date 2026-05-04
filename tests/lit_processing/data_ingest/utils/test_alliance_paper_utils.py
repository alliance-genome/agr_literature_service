import os
import tempfile
from unittest.mock import MagicMock, patch

from agr_literature_service.lit_processing.data_ingest.utils.alliance_paper_utils import (
    associate_papers_with_alliance,
    search_pubmed_for_validity,
    clean_up_tmp_directories,
)


class TestAssociatePapersWithAlliance:
    """Tests for the associate_papers_with_alliance shared function."""

    def test_associate_empty_pmids_returns_zero(self):
        """Test that empty PMIDs set returns 0."""
        mock_session = MagicMock()
        result = associate_papers_with_alliance(mock_session, set())
        assert result == 0

    def test_associate_mod_not_found_returns_zero(self):
        """Test that missing MOD returns 0."""
        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.first.return_value = None

        result = associate_papers_with_alliance(mock_session, {"12345678"}, "AGR")
        assert result == 0

    def test_associate_no_references_found_returns_zero(self):
        """Test that no matching references returns 0."""
        mock_session = MagicMock()
        mock_mod = MagicMock()
        mock_mod.mod_id = 1
        mock_session.query.return_value.filter.return_value.first.return_value = mock_mod
        mock_session.execute.return_value.fetchall.return_value = []

        result = associate_papers_with_alliance(mock_session, {"12345678"}, "AGR")
        assert result == 0

    def test_associate_default_mod_is_agr(self):
        """Test that default MOD is AGR."""
        mock_session = MagicMock()
        mock_mod = MagicMock()
        mock_mod.mod_id = 1
        mock_session.query.return_value.filter.return_value.first.return_value = mock_mod
        mock_session.execute.return_value.fetchall.return_value = []

        associate_papers_with_alliance(mock_session, {"12345678"})

        # Verify the query was for AGR
        filter_call = mock_session.query.return_value.filter
        assert filter_call.called

    def test_associate_success_with_new_papers(self):
        """Test successful association of new papers."""
        mock_session = MagicMock()
        mock_mod = MagicMock()
        mock_mod.mod_id = 1
        mock_session.query.return_value.filter.return_value.first.return_value = mock_mod

        # First execute returns reference IDs
        # Second execute returns empty (no papers already in corpus)
        mock_session.execute.return_value.fetchall.side_effect = [
            [("PMID:12345678", 100), ("PMID:87654321", 101)],  # refs found
            [],  # no existing associations
        ]

        result = associate_papers_with_alliance(mock_session, {"12345678", "87654321"}, "AGR")

        assert result == 2
        mock_session.commit.assert_called_once()
        assert mock_session.add.call_count == 2

    def test_associate_papers_already_in_corpus(self):
        """Test that papers already in corpus are not re-associated."""
        mock_session = MagicMock()
        mock_mod = MagicMock()
        mock_mod.mod_id = 1
        mock_session.query.return_value.filter.return_value.first.return_value = mock_mod

        # First execute returns reference IDs
        # Second execute returns those refs as already in corpus
        mock_session.execute.return_value.fetchall.side_effect = [
            [("PMID:12345678", 100)],  # ref found
            [(100,)],  # already in corpus
        ]

        result = associate_papers_with_alliance(mock_session, {"12345678"}, "AGR")

        assert result == 0
        mock_session.commit.assert_not_called()

    def test_associate_partial_existing_papers(self):
        """Test association when some papers are already in corpus."""
        mock_session = MagicMock()
        mock_mod = MagicMock()
        mock_mod.mod_id = 1
        mock_session.query.return_value.filter.return_value.first.return_value = mock_mod

        # First execute returns reference IDs
        # Second execute returns one as already in corpus
        mock_session.execute.return_value.fetchall.side_effect = [
            [("PMID:12345678", 100), ("PMID:87654321", 101)],  # refs found
            [(100,)],  # only 100 is already in corpus
        ]

        result = associate_papers_with_alliance(mock_session, {"12345678", "87654321"}, "AGR")

        assert result == 1
        mock_session.commit.assert_called_once()
        assert mock_session.add.call_count == 1


class TestSearchPubmedForValidity:
    """Tests for the search_pubmed_for_validity shared function."""

    @patch('agr_literature_service.lit_processing.data_ingest.utils.alliance_paper_utils.requests.get')
    def test_search_pubmed_valid_pmid(self, mock_get):
        """Test that valid PMIDs return in valid set."""
        mock_response = MagicMock()
        mock_response.text = "<PubmedArticleSet><PubmedArticle>content</PubmedArticle></PubmedArticleSet>"
        mock_get.return_value = mock_response

        obsolete, valid = search_pubmed_for_validity({"12345678"})

        assert len(valid) == 1
        assert "12345678" in valid
        assert len(obsolete) == 0

    @patch('agr_literature_service.lit_processing.data_ingest.utils.alliance_paper_utils.requests.get')
    def test_search_pubmed_obsolete_pmid(self, mock_get):
        """Test that obsolete PMIDs return in obsolete set."""
        mock_response = MagicMock()
        mock_response.text = "<PubmedArticleSet></PubmedArticleSet>"
        mock_get.return_value = mock_response

        obsolete, valid = search_pubmed_for_validity({"99999999"})

        assert len(obsolete) == 1
        assert "99999999" in obsolete
        assert len(valid) == 0

    @patch('agr_literature_service.lit_processing.data_ingest.utils.alliance_paper_utils.requests.get')
    def test_search_pubmed_multiple_pmids(self, mock_get):
        """Test handling of multiple PMIDs."""
        def side_effect(url, timeout=30):
            mock_response = MagicMock()
            if "11111111" in url:
                mock_response.text = "<PubmedArticleSet><PubmedArticle>content</PubmedArticle></PubmedArticleSet>"
            else:
                mock_response.text = "<PubmedArticleSet></PubmedArticleSet>"
            return mock_response

        mock_get.side_effect = side_effect

        obsolete, valid = search_pubmed_for_validity({"11111111", "22222222"})

        assert "11111111" in valid
        assert "22222222" in obsolete

    @patch('agr_literature_service.lit_processing.data_ingest.utils.alliance_paper_utils.requests.get')
    def test_search_pubmed_request_exception(self, mock_get):
        """Test that request exceptions result in PMIDs being marked valid."""
        import requests
        mock_get.side_effect = requests.RequestException("Connection error")

        obsolete, valid = search_pubmed_for_validity({"12345678"})

        assert len(valid) == 1
        assert "12345678" in valid
        assert len(obsolete) == 0

    @patch('agr_literature_service.lit_processing.data_ingest.utils.alliance_paper_utils.requests.get')
    def test_search_pubmed_with_api_key(self, mock_get):
        """Test that API key is included in the request URL."""
        mock_response = MagicMock()
        mock_response.text = "<PubmedArticleSet><PubmedArticle>content</PubmedArticle></PubmedArticleSet>"
        mock_get.return_value = mock_response

        search_pubmed_for_validity({"12345678"}, api_key="test_api_key")

        call_url = mock_get.call_args[0][0]
        assert "api_key=test_api_key" in call_url

    def test_search_pubmed_empty_set(self):
        """Test that empty PMID set returns empty sets."""
        obsolete, valid = search_pubmed_for_validity(set())

        assert len(obsolete) == 0
        assert len(valid) == 0

    @patch('agr_literature_service.lit_processing.data_ingest.utils.alliance_paper_utils.requests.get')
    def test_search_pubmed_with_custom_timeout(self, mock_get):
        """Test that custom timeout is passed to requests."""
        mock_response = MagicMock()
        mock_response.text = "<PubmedArticleSet><PubmedArticle>content</PubmedArticle></PubmedArticleSet>"
        mock_get.return_value = mock_response

        search_pubmed_for_validity({"12345678"}, timeout=60)

        mock_get.assert_called_once()
        call_kwargs = mock_get.call_args[1]
        assert call_kwargs.get("timeout") == 60

    @patch('agr_literature_service.lit_processing.data_ingest.utils.alliance_paper_utils.requests.get')
    def test_search_pubmed_response_with_newlines(self, mock_get):
        """Test that newlines in response are handled correctly."""
        mock_response = MagicMock()
        mock_response.text = "<PubmedArticleSet>\n</PubmedArticleSet>\n"
        mock_get.return_value = mock_response

        obsolete, valid = search_pubmed_for_validity({"99999999"})

        assert len(obsolete) == 1
        assert "99999999" in obsolete


class TestCleanUpTmpDirectories:
    """Tests for the clean_up_tmp_directories shared function."""

    def test_clean_up_creates_directories(self):
        """Test that directories are created after cleanup."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            paths = [
                os.path.join(tmp_dir, "dir1"),
                os.path.join(tmp_dir, "dir2"),
                os.path.join(tmp_dir, "dir3"),
            ]

            clean_up_tmp_directories(paths)

            for path in paths:
                assert os.path.exists(path)
                assert os.path.isdir(path)

    def test_clean_up_removes_existing_directories(self):
        """Test that existing directories are removed and recreated."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            test_dir = os.path.join(tmp_dir, "test_dir")
            os.makedirs(test_dir)
            test_file = os.path.join(test_dir, "test_file.txt")
            with open(test_file, "w") as f:
                f.write("test content")

            assert os.path.exists(test_file)

            clean_up_tmp_directories([test_dir])

            # Directory should exist but file should be gone
            assert os.path.exists(test_dir)
            assert not os.path.exists(test_file)

    def test_clean_up_handles_nonexistent_directories(self):
        """Test that nonexistent directories are created without error."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            nonexistent_path = os.path.join(tmp_dir, "nonexistent", "nested", "dir")

            # This should not raise an exception
            clean_up_tmp_directories([nonexistent_path])

            assert os.path.exists(nonexistent_path)

    def test_clean_up_empty_list(self):
        """Test that empty path list doesn't cause errors."""
        # Should not raise any exception
        clean_up_tmp_directories([])
